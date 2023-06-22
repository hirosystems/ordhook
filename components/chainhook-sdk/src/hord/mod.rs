pub mod db;
pub mod inscription;
pub mod ord;

use bitcoincore_rpc::bitcoin::hashes::hex::FromHex;
use bitcoincore_rpc::bitcoin::{Address, Network, Script};
use bitcoincore_rpc_json::bitcoin::Witness;
use chainhook_types::{
    BitcoinBlockData, BitcoinNetwork, OrdinalInscriptionCurseType, OrdinalInscriptionRevealData,
    OrdinalInscriptionTransferData, OrdinalOperation, TransactionIdentifier,
};
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};
use hiro_system_kit::slog;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rocksdb::DB;
use rusqlite::Connection;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::ops::Div;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use threadpool::ThreadPool;

use crate::hord::db::format_outpoint_to_watch;
use crate::indexer::bitcoin::BitcoinTransactionFullBreakdown;
use crate::{
    hord::{
        db::{
            find_inscription_with_ordinal_number, find_inscriptions_at_wached_outpoint,
            insert_entry_in_blocks, insert_entry_in_inscriptions,
            insert_entry_in_ordinal_activities, retrieve_satoshi_point_using_lazy_storage,
            update_transfered_inscription,
        },
        ord::height::Height,
    },
    utils::Context,
};

use self::db::{
    find_inscription_with_id, find_latest_cursed_inscription_number_at_block_height,
    find_latest_inscription_number_at_block_height, format_satpoint_to_watch,
    parse_satpoint_to_watch, remove_entry_from_blocks, remove_entry_from_inscriptions, LazyBlock,
    LazyBlockTransaction, TraversalResult, WatchedSatpoint,
};
use self::inscription::InscriptionParser;
use self::ord::inscription_id::InscriptionId;

#[derive(Clone, Debug)]
pub struct HordConfig {
    pub network_thread_max: usize,
    pub ingestion_thread_max: usize,
    pub cache_size: usize,
    pub db_path: PathBuf,
    pub first_inscription_height: u64,
}

pub fn parse_ordinal_operations(
    tx: &BitcoinTransactionFullBreakdown,
    _ctx: &Context,
) -> Vec<OrdinalOperation> {
    // This should eventually become a loop once/if there is settlement on https://github.com/casey/ord/issues/2000.
    let mut operations = vec![];
    for (input_index, input) in tx.vin.iter().enumerate() {
        if let Some(ref witness_data) = input.txinwitness {
            let witness = Witness::from_vec(witness_data.clone());
            let mut inscription = match InscriptionParser::parse(&witness) {
                Ok(inscription) => inscription,
                Err(_e) => {
                    let mut cursed_inscription = None;
                    for bytes in witness_data.iter() {
                        let script = Script::from(bytes.to_vec());
                        let parser = InscriptionParser {
                            instructions: script.instructions().peekable(),
                        };

                        let mut inscription = match parser.parse_script() {
                            Ok(inscription) => inscription,
                            Err(_) => continue,
                        };
                        inscription.curse = Some(OrdinalInscriptionCurseType::P2wsh);
                        cursed_inscription = Some(inscription);
                        break;
                    }
                    match cursed_inscription {
                        Some(inscription) => inscription,
                        None => continue,
                    }
                }
            };

            let inscription_id = InscriptionId {
                txid: tx.txid.clone(),
                index: input_index as u32,
            };

            if input_index > 0 {
                inscription.curse = Some(OrdinalInscriptionCurseType::Batch);
            }

            let no_content_bytes = vec![];
            let inscription_content_bytes = inscription.body().take().unwrap_or(&no_content_bytes);

            let payload = OrdinalInscriptionRevealData {
                content_type: inscription.content_type().unwrap_or("unknown").to_string(),
                content_bytes: format!("0x{}", hex::encode(&inscription_content_bytes)),
                content_length: inscription_content_bytes.len(),
                inscription_id: inscription_id.to_string(),
                inscription_input_index: input_index,
                inscription_output_value: 0,
                inscription_fee: 0,
                inscription_number: 0,
                inscriber_address: None,
                ordinal_number: 0,
                ordinal_block_height: 0,
                ordinal_offset: 0,
                transfers_pre_inscription: 0,
                satpoint_post_inscription: format!(""),
                curse_type: inscription.curse.take(),
            };

            operations.push(match &payload.curse_type {
                Some(_) => OrdinalOperation::CursedInscriptionRevealed(payload),
                None => OrdinalOperation::InscriptionRevealed(payload),
            });
        }
    }
    operations
}

pub fn get_inscriptions_revealed_in_block(
    block: &BitcoinBlockData,
) -> Vec<&OrdinalInscriptionRevealData> {
    let mut ops = vec![];
    for tx in block.transactions.iter() {
        for op in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionRevealed(op) = op {
                ops.push(op);
            }
        }
    }
    ops
}

pub fn revert_hord_db_with_augmented_bitcoin_block(
    block: &BitcoinBlockData,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) -> Result<(), String> {
    // Remove block from
    remove_entry_from_blocks(block.block_identifier.index as u32, &blocks_db_rw, ctx);
    for tx_index in 1..=block.transactions.len() {
        // Undo the changes in reverse order
        let tx = &block.transactions[block.transactions.len() - tx_index];
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            match ordinal_event {
                OrdinalOperation::InscriptionRevealed(data)
                | OrdinalOperation::CursedInscriptionRevealed(data) => {
                    // We remove any new inscription created
                    remove_entry_from_inscriptions(
                        &data.inscription_id,
                        &inscriptions_db_conn_rw,
                        ctx,
                    );
                }
                OrdinalOperation::InscriptionTransferred(data) => {
                    // We revert the outpoint to the pre-transfer value
                    let comps = data.satpoint_pre_transfer.split(":").collect::<Vec<_>>();
                    let outpoint_pre_transfer = format!("{}:{}", comps[0], comps[1]);
                    let offset_pre_transfer = comps[2]
                        .parse::<u64>()
                        .map_err(|e| format!("hord_db corrupted {}", e.to_string()))?;
                    update_transfered_inscription(
                        &&data.inscription_id,
                        &outpoint_pre_transfer,
                        offset_pre_transfer,
                        &inscriptions_db_conn_rw,
                        &ctx,
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn new_traversals_cache(
) -> DashMap<(u32, [u8; 8]), (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>), BuildHasherDefault<FxHasher>>
{
    let hasher = FxBuildHasher::default();
    DashMap::with_hasher(hasher)
}

pub fn new_traversals_lazy_cache(
    cache_size: usize,
) -> DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>> {
    let hasher = FxBuildHasher::default();
    DashMap::with_capacity_and_hasher(
        ((cache_size.saturating_sub(500)) * 1000 * 1000)
            .div(LazyBlockTransaction::get_average_bytes_size()),
        hasher,
    )
}

pub fn retrieve_inscribed_satoshi_points_from_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn: Option<&Connection>,
    hord_config: &HordConfig,
    traversals_cache: &Arc<
        DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>,
    >,
    ctx: &Context,
) -> HashMap<(TransactionIdentifier, usize), TraversalResult> {
    let mut transactions_ids = vec![];
    let mut traversals = HashMap::new();

    for tx in block.transactions.iter().skip(1) {
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            let (inscription_data, _is_cursed) = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription_data) => {
                    (inscription_data, false)
                }
                OrdinalOperation::CursedInscriptionRevealed(inscription_data) => {
                    (inscription_data, false)
                }
                OrdinalOperation::InscriptionTransferred(_) => {
                    continue;
                }
            };
            if let Some(inscriptions_db_conn) = inscriptions_db_conn {
                // TODO: introduce scanning context
                if let Some(traversal) = find_inscription_with_id(
                    &inscription_data.inscription_id,
                    &block.block_identifier.hash,
                    inscriptions_db_conn,
                    ctx,
                ) {
                    traversals.insert(
                        (
                            tx.transaction_identifier.clone(),
                            inscription_data.inscription_input_index,
                        ),
                        traversal,
                    );
                } else {
                    // Enqueue for traversals
                    transactions_ids.push((
                        tx.transaction_identifier.clone(),
                        inscription_data.inscription_input_index,
                    ));
                }
            } else {
                // Enqueue for traversals
                transactions_ids.push((
                    tx.transaction_identifier.clone(),
                    inscription_data.inscription_input_index,
                ));
            }
        }
    }

    if !transactions_ids.is_empty() {
        let expected_traversals = transactions_ids.len();
        let (traversal_tx, traversal_rx) = channel::<Result<TraversalResult, _>>();
        let traversal_data_pool = ThreadPool::new(hord_config.ingestion_thread_max);

        let mut rng = thread_rng();
        transactions_ids.shuffle(&mut rng);
        for (transaction_id, input_index) in transactions_ids.into_iter() {
            let moved_traversal_tx = traversal_tx.clone();
            let moved_ctx = ctx.clone();
            let block_identifier = block.block_identifier.clone();
            let moved_hord_db_path = hord_config.db_path.clone();
            let local_cache = traversals_cache.clone();
            traversal_data_pool.execute(move || {
                let traversal = retrieve_satoshi_point_using_lazy_storage(
                    &moved_hord_db_path,
                    &block_identifier,
                    &transaction_id,
                    input_index,
                    0,
                    local_cache,
                    &moved_ctx,
                );
                let _ = moved_traversal_tx.send(traversal);
            });
        }

        let mut traversals_received = 0;
        while let Ok(traversal_result) = traversal_rx.recv() {
            traversals_received += 1;
            match traversal_result {
                Ok(traversal) => {
                    ctx.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times (progress: {traversals_received}/{expected_traversals}).",
                            traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                            )
                    });
                    traversals.insert(
                        (
                            traversal.transaction_identifier.clone(),
                            traversal.input_index,
                        ),
                        traversal,
                    );
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::error!(logger, "Unable to compute inscription's Satoshi: {e}",)
                    });
                }
            }
            if traversals_received == expected_traversals {
                break;
            }
        }
        let _ = traversal_data_pool.join();
    }

    traversals
}

pub fn update_hord_db_and_augment_bitcoin_block(
    new_block: &mut BitcoinBlockData,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    write_block: bool,
    hord_config: &HordConfig,
    traversals_cache: &Arc<
        DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>,
    >,
    ctx: &Context,
) -> Result<(), String> {
    if write_block {
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Updating hord.sqlite with Bitcoin block #{} for future traversals",
                new_block.block_identifier.index,
            )
        });

        let compacted_block = LazyBlock::from_standardized_block(&new_block).map_err(|e| {
            format!(
                "unable to serialize block {}: {}",
                new_block.block_identifier.index,
                e.to_string()
            )
        })?;
        insert_entry_in_blocks(
            new_block.block_identifier.index as u32,
            &compacted_block,
            &blocks_db_rw,
            &ctx,
        );
        let _ = blocks_db_rw.flush();
    }

    let traversals = retrieve_inscribed_satoshi_points_from_block(
        &new_block,
        Some(inscriptions_db_conn_rw),
        &hord_config,
        traversals_cache,
        ctx,
    );

    let mut storage = Storage::Sqlite(inscriptions_db_conn_rw);
    let any_inscription_revealed =
        update_storage_and_augment_bitcoin_block_with_inscription_reveal_data(
            new_block,
            &mut storage,
            &traversals,
            &inscriptions_db_conn_rw,
            &ctx,
        )?;

    // Have inscriptions been transfered?
    let any_inscription_transferred =
        update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
            new_block,
            &mut storage,
            &ctx,
        )?;

    if any_inscription_revealed || any_inscription_transferred {
        insert_entry_in_ordinal_activities(
            new_block.block_identifier.index as u32,
            inscriptions_db_conn_rw,
            ctx,
        )
    }

    Ok(())
}

#[derive(Debug)]
pub enum Storage<'a> {
    Sqlite(&'a Connection),
    Memory(BTreeMap<String, Vec<WatchedSatpoint>>),
}

pub fn update_storage_and_augment_bitcoin_block_with_inscription_reveal_data(
    block: &mut BitcoinBlockData,
    storage: &mut Storage,
    traversals: &HashMap<(TransactionIdentifier, usize), TraversalResult>,
    inscriptions_db_conn: &Connection,
    ctx: &Context,
) -> Result<bool, String> {
    let mut storage_updated = false;
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };
    let mut latest_inscription_number = match find_latest_inscription_number_at_block_height(
        &block.block_identifier.index,
        &inscriptions_db_conn,
        &ctx,
    )? {
        None => 0,
        Some(inscription_number) => inscription_number + 1,
    };

    let mut latest_cursed_inscription_number =
        match find_latest_cursed_inscription_number_at_block_height(
            &block.block_identifier.index,
            &inscriptions_db_conn,
            &ctx,
        )? {
            None => -1,
            Some(inscription_number) => inscription_number - 1,
        };

    let mut sats_overflow = vec![];

    for new_tx in block.transactions.iter_mut().skip(1) {
        let mut ordinals_events_indexes_to_discard = VecDeque::new();
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for (ordinal_event_index, ordinal_event) in
            new_tx.metadata.ordinal_operations.iter_mut().enumerate()
        {
            let (inscription, is_cursed) = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription) => (inscription, false),
                OrdinalOperation::CursedInscriptionRevealed(inscription) => (inscription, true),
                OrdinalOperation::InscriptionTransferred(_) => continue,
            };

            let inscription_number = if is_cursed {
                latest_cursed_inscription_number
            } else {
                latest_inscription_number
            };

            let transaction_identifier = new_tx.transaction_identifier.clone();
            let traversal = match traversals
                .get(&(transaction_identifier, inscription.inscription_input_index))
            {
                Some(traversal) => traversal,
                None => {
                    ctx.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Unable to retrieve cached inscription data for inscription {}",
                            new_tx.transaction_identifier.hash
                        );
                    });
                    ordinals_events_indexes_to_discard.push_front(ordinal_event_index);
                    continue;
                }
            };

            let outputs = new_tx.metadata.outputs.clone();
            inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
            inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
            inscription.ordinal_number = traversal.ordinal_number;
            inscription.inscription_number = traversal.inscription_number;
            inscription.transfers_pre_inscription = traversal.transfers;
            inscription.inscription_fee = new_tx.metadata.fee;
            inscription.satpoint_post_inscription = format_satpoint_to_watch(
                &traversal.transaction_identifier,
                traversal.output_index,
                traversal.inscription_offset_intra_output,
            );
            if let Some(output) = new_tx.metadata.outputs.get(traversal.output_index) {
                inscription.inscription_output_value = output.value;
                inscription.inscriber_address = {
                    let script_pub_key = output.get_script_pubkey_hex();
                    match Script::from_hex(&script_pub_key) {
                        Ok(script) => match Address::from_script(&script, network) {
                            Ok(a) => Some(a.to_string()),
                            _ => None,
                        },
                        _ => None,
                    }
                };
            } else {
                ctx.try_log(|logger| {
                    slog::warn!(
                        logger,
                        "Database corrupted, skipping cursed inscription => {:?} / {:?}",
                        traversal,
                        outputs
                    );
                });
            }
            match storage {
                Storage::Sqlite(rw_hord_db_conn) => {
                    if traversal.ordinal_number == 0 {
                        // If the satoshi inscribed correspond to a sat overflow, we will store the inscription
                        // and assign an inscription number after the other inscriptions, to mimick the
                        // bug in ord.
                        sats_overflow.push(inscription.clone());
                        continue;
                    }

                    if let Some(_entry) = find_inscription_with_ordinal_number(
                        &traversal.ordinal_number,
                        &inscriptions_db_conn,
                        &ctx,
                    ) {
                        ctx.try_log(|logger| {
                                slog::warn!(
                                    logger,
                                    "Transaction {} in block {} is overriding an existing inscription {}",
                                    new_tx.transaction_identifier.hash,
                                    block.block_identifier.index,
                                    traversal.ordinal_number
                                );
                            });
                        ordinals_events_indexes_to_discard.push_front(ordinal_event_index);
                        continue;
                    }

                    if is_cursed {
                        latest_cursed_inscription_number -= 1;
                    } else {
                        latest_inscription_number += 1;
                    };

                    inscription.inscription_number = inscription_number;
                    ctx.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Inscription {} (#{}) detected on Satoshi {} (block {}, {} transfers)",
                            inscription.inscription_id,
                            inscription.inscription_number,
                            inscription.ordinal_number,
                            block.block_identifier.index,
                            inscription.transfers_pre_inscription,
                        );
                    });
                    insert_entry_in_inscriptions(
                        &inscription,
                        &block.block_identifier,
                        &rw_hord_db_conn,
                        &ctx,
                    );
                }
                Storage::Memory(map) => {
                    let (tx, output_index, offset) =
                        parse_satpoint_to_watch(&inscription.satpoint_post_inscription);
                    let outpoint = format_outpoint_to_watch(&tx, output_index);
                    map.insert(
                        outpoint,
                        vec![WatchedSatpoint {
                            inscription_id: inscription.inscription_id.clone(),
                            inscription_number: inscription.inscription_number,
                            ordinal_number: inscription.ordinal_number,
                            offset,
                        }],
                    );
                }
            }
            storage_updated = true;
        }

        for index in ordinals_events_indexes_to_discard.into_iter() {
            new_tx.metadata.ordinal_operations.remove(index);
        }
    }

    for inscription in sats_overflow.iter_mut() {
        match storage {
            Storage::Sqlite(rw_hord_db_conn) => {
                inscription.inscription_number = latest_inscription_number;
                ctx.try_log(|logger| {
                            slog::info!(
                        logger,
                        "Inscription {} (#{}) detected on Satoshi overflow {} (block {}, {} transfers)",
                        inscription.inscription_id,
                        inscription.inscription_number,
                        inscription.ordinal_number,
                        block.block_identifier.index,
                        inscription.transfers_pre_inscription,
                    );
                });
                insert_entry_in_inscriptions(
                    &inscription,
                    &block.block_identifier,
                    &rw_hord_db_conn,
                    &ctx,
                );
                latest_inscription_number += 1;
                storage_updated = true;
            }
            _ => {}
        }
    }

    Ok(storage_updated)
}

/// For each input of each transaction in the block, we retrieve the UTXO spent (outpoint_pre_transfer)
/// and we check using a `storage` (in-memory or sqlite) absctraction if we have some existing inscriptions
/// for this entry.
/// When this is the case, it means that an inscription_transfer event needs to be produced. We need to
/// compute the output index (if any) `post_transfer_output` that will now include the inscription.
/// When identifying the output index, we will also need to provide an updated offset for pin pointing
/// the satoshi location.
pub fn update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
    block: &mut BitcoinBlockData,
    storage: &mut Storage,
    ctx: &Context,
) -> Result<bool, String> {
    let mut storage_updated = false;
    let mut cumulated_fees = 0;
    let first_sat_post_subsidy = Height(block.block_identifier.index).starting_sat().0;
    let coinbase_txid = &block.transactions[0].transaction_identifier.hash.clone();

    // todo: handle ordinals coinbase spend

    for new_tx in block.transactions.iter_mut().skip(1) {
        // Have inscriptions been transfered?
        let mut sats_in_offset = 0;

        for input in new_tx.metadata.inputs.iter() {
            // input.previous_output.txid
            let outpoint_pre_transfer = format_outpoint_to_watch(
                &input.previous_output.txid,
                input.previous_output.vout as usize,
            );

            let entries = match storage {
                Storage::Sqlite(rw_hord_db_conn) => {
                    find_inscriptions_at_wached_outpoint(&outpoint_pre_transfer, &rw_hord_db_conn)?
                }
                Storage::Memory(ref mut map) => match map.remove(&outpoint_pre_transfer) {
                    Some(entries) => entries,
                    None => vec![],
                },
            };

            for mut watched_satpoint in entries.into_iter() {
                let mut post_transfer_output_index = 0;
                let mut sats_out_offset = 0;
                let mut next_output_value = new_tx.metadata.outputs[0].value;
                let satpoint_pre_transfer =
                    format!("{}:{}", outpoint_pre_transfer, watched_satpoint.offset);

                // Question is: are inscriptions moving to a new output,
                // burnt or lost in fees and transfered to the miner?

                let post_transfer_output: Option<usize> = loop {
                    if sats_out_offset + next_output_value
                        > sats_in_offset + watched_satpoint.offset
                    {
                        break Some(post_transfer_output_index);
                    }
                    sats_out_offset += next_output_value;
                    post_transfer_output_index += 1;
                    if post_transfer_output_index >= new_tx.metadata.outputs.len() {
                        break None;
                    } else {
                        next_output_value =
                            new_tx.metadata.outputs[post_transfer_output_index].value;
                    }
                };

                let (
                    outpoint_post_transfer,
                    offset_post_transfer,
                    updated_address,
                    post_transfer_output_value,
                ) = match post_transfer_output {
                    Some(index) => {
                        let outpoint =
                            format_outpoint_to_watch(&new_tx.transaction_identifier, index);
                        let offset = (sats_in_offset + watched_satpoint.offset) - sats_out_offset;
                        let script_pub_key_hex =
                            new_tx.metadata.outputs[index].get_script_pubkey_hex();
                        let updated_address = match Script::from_hex(&script_pub_key_hex) {
                            Ok(script) => match Address::from_script(&script, Network::Bitcoin) {
                                Ok(address) => Some(address.to_string()),
                                Err(e) => {
                                    ctx.try_log(|logger| {
                                            slog::warn!(
                                                logger,
                                                "unable to retrieve address from {script_pub_key_hex}: {}", e.to_string()
                                            )
                                        });
                                    None
                                }
                            },
                            Err(e) => {
                                ctx.try_log(|logger| {
                                    slog::warn!(
                                        logger,
                                        "unable to retrieve address from {script_pub_key_hex}: {}",
                                        e.to_string()
                                    )
                                });
                                None
                            }
                        };

                        (
                            outpoint,
                            offset,
                            updated_address,
                            Some(new_tx.metadata.outputs[post_transfer_output_index].value),
                        )
                    }
                    None => {
                        // Get Coinbase TX
                        let offset =
                            first_sat_post_subsidy + cumulated_fees + watched_satpoint.offset;
                        let outpoint = format!("{}:0", &coinbase_txid[2..]);
                        (outpoint, offset, None, None)
                    }
                };

                // At this point we know that inscriptions are being moved.
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Inscription {} (#{}) moved from {} to {} (block: {})",
                        watched_satpoint.inscription_id,
                        watched_satpoint.inscription_number,
                        satpoint_pre_transfer,
                        outpoint_post_transfer,
                        block.block_identifier.index,
                    )
                });

                // Update watched outpoint
                match storage {
                    Storage::Sqlite(rw_hord_db_conn) => {
                        update_transfered_inscription(
                            &watched_satpoint.inscription_id,
                            &outpoint_post_transfer,
                            offset_post_transfer,
                            &rw_hord_db_conn,
                            &ctx,
                        );
                    }
                    Storage::Memory(ref mut map) => {
                        watched_satpoint.offset = offset_post_transfer;
                        map.entry(outpoint_post_transfer.clone())
                            .and_modify(|v| v.push(watched_satpoint.clone()))
                            .or_insert(vec![watched_satpoint.clone()]);
                    }
                };
                storage_updated = true;

                let satpoint_post_transfer =
                    format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

                let event_data = OrdinalInscriptionTransferData {
                    inscription_id: watched_satpoint.inscription_id.clone(),
                    inscription_number: watched_satpoint.inscription_number,
                    ordinal_number: watched_satpoint.ordinal_number,
                    updated_address,
                    satpoint_pre_transfer,
                    satpoint_post_transfer,
                    post_transfer_output_value,
                };

                // Attach transfer event
                new_tx
                    .metadata
                    .ordinal_operations
                    .push(OrdinalOperation::InscriptionTransferred(event_data));
            }
            sats_in_offset += input.previous_output.value;
        }
        cumulated_fees += new_tx.metadata.fee;
    }
    Ok(storage_updated)
}
