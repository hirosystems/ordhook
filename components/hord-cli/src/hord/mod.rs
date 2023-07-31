pub mod inscription;
pub mod ordinals;

use chainhook_sdk::bitcoincore_rpc::bitcoin::hashes::hex::FromHex;
use chainhook_sdk::bitcoincore_rpc::bitcoin::{Address, Network, Script};
use chainhook_sdk::bitcoincore_rpc_json::bitcoin::{Txid, Witness};
use chainhook_sdk::types::{
    BitcoinBlockData, BitcoinNetwork, OrdinalInscriptionCurseType, OrdinalInscriptionRevealData,
    OrdinalInscriptionTransferData, OrdinalOperation, TransactionIdentifier,
};
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};
use hiro_system_kit::slog;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rocksdb::DB;
use rusqlite::{Connection, Transaction};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::ops::Div;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use threadpool::ThreadPool;

use std::sync::mpsc::Sender;

use chainhook_sdk::{
    bitcoincore_rpc::{Auth, Client, RpcApi},
    observer::BitcoinConfig,
    utils::Context,
};

use crate::db::{
    find_inscription_with_ordinal_number, find_inscriptions_at_wached_outpoint,
    get_any_entry_in_ordinal_activities, insert_entry_in_blocks, insert_entry_in_inscriptions,
    insert_transfer_in_locations_tx, retrieve_satoshi_point_using_lazy_storage,
    retrieve_satoshi_point_using_lazy_storage_v3, InscriptionHeigthHint,
};
use crate::ord::height::Height;

use crate::config::Config;

use crate::db::format_outpoint_to_watch;
use chainhook_sdk::indexer::bitcoin::{
    standardize_bitcoin_block, BitcoinBlockFullBreakdown, BitcoinTransactionFullBreakdown,
};

use crate::db::{
    fetch_and_cache_blocks_in_hord_db, find_last_block_inserted,
    find_latest_inscription_block_height, initialize_hord_db, open_readonly_hord_db_conn,
    open_readonly_hord_db_conn_rocks_db, open_readwrite_hord_db_conn,
    open_readwrite_hord_db_conn_rocks_db,
};

use self::inscription::InscriptionParser;
use crate::db::{
    find_inscription_with_id, find_latest_cursed_inscription_number_at_block_height,
    find_latest_inscription_number_at_block_height, format_satpoint_to_watch,
    insert_transfer_in_locations, parse_satpoint_to_watch, remove_entry_from_blocks,
    remove_entry_from_inscriptions, LazyBlock, LazyBlockTransaction, TraversalResult,
    WatchedSatpoint,
};
use crate::ord::inscription_id::InscriptionId;

#[derive(Clone, Debug)]
pub struct HordConfig {
    pub network_thread_max: usize,
    pub ingestion_thread_max: usize,
    pub cache_size: usize,
    pub db_path: PathBuf,
    pub first_inscription_height: u64,
}

pub fn parse_ordinals_and_standardize_block(
    raw_block: BitcoinBlockFullBreakdown,
    network: &BitcoinNetwork,
    ctx: &Context,
) -> Result<BitcoinBlockData, (String, bool)> {
    let mut ordinal_operations = BTreeMap::new();

    for tx in raw_block.tx.iter() {
        ordinal_operations.insert(tx.txid.to_string(), parse_ordinal_operations(&tx, ctx));
    }

    let mut block = standardize_bitcoin_block(raw_block, network, ctx)?;

    for tx in block.transactions.iter_mut() {
        if let Some(ordinal_operations) =
            ordinal_operations.remove(tx.transaction_identifier.get_hash_bytes_str())
        {
            tx.metadata.ordinal_operations = ordinal_operations;
        }
    }
    Ok(block)
}

pub fn parse_ordinal_operations(
    tx: &BitcoinTransactionFullBreakdown,
    _ctx: &Context,
) -> Vec<OrdinalOperation> {
    // This should eventually become a loop once/if there is settlement on https://github.com/casey/ord/issues/2000.
    let mut operations = vec![];
    for (input_index, input) in tx.vin.iter().enumerate() {
        if let Some(ref witness_data) = input.txinwitness {
            let witness_data_hex: Vec<Vec<u8>> = witness_data
                .iter()
                .map(|w| hex::decode(w).unwrap())
                .collect();
            let witness = Witness::from_vec(witness_data_hex.clone());
            let mut inscription = match InscriptionParser::parse(&witness) {
                Ok(inscription) => inscription,
                Err(_e) => {
                    let mut cursed_inscription = None;
                    for bytes in witness_data_hex.iter() {
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
                txid: Txid::from_hex(&tx.txid).unwrap(),
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
                tx_index: 0,
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
            if let OrdinalOperation::CursedInscriptionRevealed(op) = op {
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
                OrdinalOperation::InscriptionTransferred(transfer_data) => {
                    // We revert the outpoint to the pre-transfer value
                    insert_transfer_in_locations(
                        transfer_data,
                        &block.block_identifier,
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
                if let Ok(Some((traversal, _))) = find_inscription_with_id(
                    &inscription_data.inscription_id,
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
                    &local_cache,
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
                        info!(
                            logger,
                            "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times (progress: {traversals_received}/{expected_traversals}).",
                            traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                            )
                    });
                    traversals.insert(
                        (
                            traversal.transaction_identifier_inscription.clone(),
                            traversal.inscription_input_index,
                        ),
                        traversal,
                    );
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        error!(logger, "Unable to compute inscription's Satoshi: {e}",)
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
    inscriptions_db_conn_rw: &mut Connection,
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

    let discard_changes: bool = get_any_entry_in_ordinal_activities(
        &new_block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    );

    let inner_ctx = if discard_changes {
        Context::empty()
    } else {
        ctx.clone()
    };

    let mut inscription_height_hint = InscriptionHeigthHint::new();

    let transaction = inscriptions_db_conn_rw.transaction().unwrap();
    let any_inscription_revealed =
        update_storage_and_augment_bitcoin_block_with_inscription_reveal_data_tx(
            new_block,
            &transaction,
            &traversals,
            &mut inscription_height_hint,
            &inner_ctx,
        )?;

    // Have inscriptions been transfered?
    let any_inscription_transferred =
        update_storage_and_augment_bitcoin_block_with_inscription_transfer_data_tx(
            new_block,
            &transaction,
            &inner_ctx,
        )?;

    if !any_inscription_revealed && !any_inscription_transferred {
        return Ok(());
    }

    if discard_changes {
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Ignoring updates for block #{}, activities present in database",
                new_block.block_identifier.index,
            )
        });
    } else {
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Saving updates for block {}",
                new_block.block_identifier.index,
            )
        });
        transaction.commit().unwrap();
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Updates saved for block {}",
                new_block.block_identifier.index,
            )
        });
    }

    let inscriptions_revealed = get_inscriptions_revealed_in_block(&new_block)
        .iter()
        .map(|d| d.inscription_number.to_string())
        .collect::<Vec<String>>();

    ctx.try_log(|logger| {
        slog::info!(
            logger,
            "Block #{} processed through hord, revealing {} inscriptions [{}]",
            new_block.block_identifier.index,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        )
    });
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
    hint: InscriptionHeigthHint,
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
        &hint.blessed,
        &inscriptions_db_conn,
        &ctx,
    )? {
        None => 0,
        Some(inscription_number) => inscription_number + 1,
    };

    let mut latest_cursed_inscription_number =
        match find_latest_cursed_inscription_number_at_block_height(
            &block.block_identifier.index,
            &hint.cursed,
            &inscriptions_db_conn,
            &ctx,
        )? {
            None => -1,
            Some(inscription_number) => inscription_number - 1,
        };

    let mut sats_overflow = vec![];

    for (tx_index, new_tx) in block.transactions.iter_mut().skip(1).enumerate() {
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

            let outputs = &new_tx.metadata.outputs;
            inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
            inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
            inscription.ordinal_number = traversal.ordinal_number;
            inscription.inscription_number = traversal.inscription_number;
            inscription.transfers_pre_inscription = traversal.transfers;
            inscription.inscription_fee = new_tx.metadata.fee;
            inscription.tx_index = tx_index;
            inscription.satpoint_post_inscription = format_satpoint_to_watch(
                &traversal.transfer_data.transaction_identifier_location,
                traversal.transfer_data.output_index,
                traversal.transfer_data.inscription_offset_intra_output,
            );
            if let Some(output) = outputs.get(traversal.transfer_data.output_index) {
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
    let subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };
    // todo: handle ordinals coinbase spend

    for (tx_index, new_tx) in block.transactions.iter_mut().skip(1).enumerate() {
        for (input_index, input) in new_tx.metadata.inputs.iter().enumerate() {
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

            // For each satpoint inscribed retrieved, we need to compute the next
            // outpoint to watch
            for mut watched_satpoint in entries.into_iter() {
                let satpoint_pre_transfer =
                    format!("{}:{}", outpoint_pre_transfer, watched_satpoint.offset);

                // Question is: are inscriptions moving to a new output,
                // burnt or lost in fees and transfered to the miner?

                let inputs = new_tx
                    .metadata
                    .inputs
                    .iter()
                    .map(|o| o.previous_output.value)
                    .collect::<_>();
                let outputs = new_tx
                    .metadata
                    .outputs
                    .iter()
                    .map(|o| o.value)
                    .collect::<_>();
                let post_transfer_data = compute_next_satpoint_data(
                    input_index,
                    watched_satpoint.offset,
                    &inputs,
                    &outputs,
                );

                let (
                    outpoint_post_transfer,
                    offset_post_transfer,
                    updated_address,
                    post_transfer_output_value,
                ) = match post_transfer_data {
                    SatPosition::Output((output_index, offset)) => {
                        let outpoint =
                            format_outpoint_to_watch(&new_tx.transaction_identifier, output_index);
                        let script_pub_key_hex =
                            new_tx.metadata.outputs[output_index].get_script_pubkey_hex();
                        let updated_address = match Script::from_hex(&script_pub_key_hex) {
                            Ok(script) => match Address::from_script(&script, network.clone()) {
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

                        // At this point we know that inscriptions are being moved.
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Inscription {} moved from {} to {} (block: {})",
                                watched_satpoint.inscription_id,
                                satpoint_pre_transfer,
                                outpoint,
                                block.block_identifier.index,
                            )
                        });

                        (
                            outpoint,
                            offset,
                            updated_address,
                            Some(new_tx.metadata.outputs[output_index].value),
                        )
                    }
                    SatPosition::Fee(offset) => {
                        // Get Coinbase TX
                        let total_offset = subsidy + cumulated_fees + offset;
                        let outpoint = format_outpoint_to_watch(&coinbase_txid, 0);
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Inscription {} spent in fees ({}+{}+{})",
                                watched_satpoint.inscription_id,
                                subsidy,
                                cumulated_fees,
                                offset
                            )
                        });
                        (outpoint, total_offset, None, None)
                    }
                };

                let satpoint_post_transfer =
                    format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

                let transfer_data = OrdinalInscriptionTransferData {
                    inscription_id: watched_satpoint.inscription_id.clone(),
                    updated_address,
                    tx_index,
                    satpoint_pre_transfer,
                    satpoint_post_transfer,
                    post_transfer_output_value,
                };

                // Update watched outpoint
                match storage {
                    Storage::Sqlite(rw_hord_db_conn) => {
                        insert_transfer_in_locations(
                            &transfer_data,
                            &block.block_identifier,
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

                // Attach transfer event
                new_tx
                    .metadata
                    .ordinal_operations
                    .push(OrdinalOperation::InscriptionTransferred(transfer_data));
            }
        }
        cumulated_fees += new_tx.metadata.fee;
    }
    Ok(storage_updated)
}

#[derive(PartialEq, Debug)]
pub enum SatPosition {
    Output((usize, u64)),
    Fee(u64),
}

pub fn compute_next_satpoint_data(
    input_index: usize,
    offset_intra_input: u64,
    inputs: &Vec<u64>,
    outputs: &Vec<u64>,
) -> SatPosition {
    let mut offset_cross_inputs = 0;
    for (index, input_value) in inputs.iter().enumerate() {
        if index == input_index {
            break;
        }
        offset_cross_inputs += input_value;
    }
    offset_cross_inputs += offset_intra_input;

    let mut offset_intra_outputs = 0;
    let mut output_index = 0;
    let mut floating_bound = 0;

    for (index, output_value) in outputs.iter().enumerate() {
        floating_bound += output_value;
        output_index = index;
        if floating_bound > offset_cross_inputs {
            break;
        }
        offset_intra_outputs += output_value;
    }

    if output_index == (outputs.len() - 1) && offset_cross_inputs >= floating_bound {
        // Satoshi spent in fees
        return SatPosition::Fee(offset_cross_inputs - floating_bound);
    }
    SatPosition::Output((output_index, (offset_cross_inputs - offset_intra_outputs)))
}

pub fn should_sync_hord_db(config: &Config, ctx: &Context) -> Result<Option<(u64, u64)>, String> {
    let auth = Auth::UserPass(
        config.network.bitcoind_rpc_username.clone(),
        config.network.bitcoind_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoind_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("Bitcoin RPC error: {}", message.to_string()));
        }
    };

    let mut start_block =
        match open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx) {
            Ok(blocks_db) => find_last_block_inserted(&blocks_db) as u64,
            Err(err) => {
                ctx.try_log(|logger| {
                    warn!(logger, "{}", err);
                });
                0
            }
        };

    if start_block == 0 {
        let _ = initialize_hord_db(&config.expected_cache_path(), &ctx);
    }

    let inscriptions_db_conn = open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    match find_latest_inscription_block_height(&inscriptions_db_conn, ctx)? {
        Some(height) => {
            start_block = start_block.min(height);
        }
        None => {
            start_block = start_block.min(config.get_hord_config().first_inscription_height);
        }
    };

    let end_block = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks.saturating_sub(1),
        Err(e) => {
            return Err(format!(
                "unable to retrieve Bitcoin chain tip ({})",
                e.to_string()
            ));
        }
    };

    if start_block < end_block {
        Ok(Some((start_block, end_block)))
    } else {
        Ok(None)
    }
}

pub async fn perform_hord_db_update(
    start_block: u64,
    end_block: u64,
    hord_config: &HordConfig,
    config: &Config,
    block_post_processor: Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) -> Result<(), String> {
    info!(
        ctx.expect_logger(),
        "Syncing hord_db: {} blocks to download ({start_block}: {end_block})",
        end_block - start_block + 1
    );

    let bitcoin_config = BitcoinConfig {
        username: config.network.bitcoind_rpc_username.clone(),
        password: config.network.bitcoind_rpc_password.clone(),
        rpc_url: config.network.bitcoind_rpc_url.clone(),
        network: config.network.bitcoin_network.clone(),
        bitcoin_block_signaling: config.network.bitcoin_block_signaling.clone(),
    };

    let blocks_db = open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
    let mut inscriptions_db_conn_rw =
        open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    let _ = fetch_and_cache_blocks_in_hord_db(
        &bitcoin_config,
        &blocks_db,
        &mut inscriptions_db_conn_rw,
        start_block,
        end_block,
        hord_config,
        block_post_processor,
        &ctx,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
pub mod tests;

#[test]
fn test_identify_next_output_index_destination() {
    assert_eq!(
        compute_next_satpoint_data(0, 10, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((0, 10))
    );
    assert_eq!(
        compute_next_satpoint_data(0, 20, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 5, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((1, 5))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 6, &vec![20, 30, 45], &vec![20, 5, 45]),
        SatPosition::Output((2, 1))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 10, &vec![10, 10, 10], &vec![30]),
        SatPosition::Output((0, 20))
    );
    assert_eq!(
        compute_next_satpoint_data(0, 30, &vec![10, 10, 10], &vec![30]),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, 0, &vec![10, 10, 10], &vec![30]),
        SatPosition::Output((0, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(2, 45, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(
            2,
            0,
            &vec![1000, 600, 546, 63034],
            &vec![1600, 10000, 15000]
        ),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(
            3,
            0,
            &vec![6100, 148660, 103143, 7600],
            &vec![81434, 173995]
        ),
        SatPosition::Fee(2474)
    );
}

#[test]
fn test_ordinal_inscription_parsing() {
    let bytes = hex::decode("208737bc46923c3e64c7e6768c0346879468bf3aba795a5f5f56efca288f50ed2aac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c9948656c6c6f2030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030300a68").unwrap();

    let script = Script::from(bytes);
    let parser = InscriptionParser {
        instructions: script.instructions().peekable(),
    };

    let inscription = match parser.parse_script() {
        Ok(inscription) => inscription,
        Err(_) => panic!(),
    };

    println!("{:?}", inscription);
}

pub fn get_transactions_to_process(
    block: &BitcoinBlockData,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    inscriptions_db_conn: &mut Connection,
    ctx: &Context,
) -> Vec<(TransactionIdentifier, usize)> {
    let mut transactions_ids: Vec<(TransactionIdentifier, usize)> = vec![];

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

            if cache_l1.contains_key(&(
                tx.transaction_identifier.clone(),
                inscription_data.inscription_input_index,
            )) {
                continue;
            }

            if let Ok(Some((traversal, _))) = find_inscription_with_id(
                &inscription_data.inscription_id,
                &inscriptions_db_conn,
                ctx,
            ) {
                cache_l1.insert(
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
        }
    }
    transactions_ids
}

pub fn retrieve_inscribed_satoshi_points_from_block_v3(
    block: &BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_conn: &mut Connection,
    hord_config: &HordConfig,
    ctx: &Context,
) -> Result<bool, String> {
    let mut transactions_ids =
        get_transactions_to_process(block, cache_l1, inscriptions_db_conn, ctx);

    let has_transactions_to_process = !transactions_ids.is_empty();

    if has_transactions_to_process {
        let expected_traversals = transactions_ids.len();
        let (traversal_tx, traversal_rx) = channel();
        let traversal_data_pool = ThreadPool::new(hord_config.ingestion_thread_max);

        let mut tx_thread_pool = vec![];
        for thread_index in 0..hord_config.ingestion_thread_max {
            let (tx, rx) = channel();
            tx_thread_pool.push(tx);

            let moved_traversal_tx = traversal_tx.clone();
            let moved_ctx = ctx.clone();
            let moved_hord_db_path = hord_config.db_path.clone();
            let local_cache = cache_l2.clone();

            traversal_data_pool.execute(move || {
                while let Ok(Some((transaction_id, block_identifier, input_index, prioritary))) =
                    rx.recv()
                {
                    let traversal: Result<TraversalResult, String> =
                        retrieve_satoshi_point_using_lazy_storage_v3(
                            &moved_hord_db_path,
                            &block_identifier,
                            &transaction_id,
                            input_index,
                            0,
                            &local_cache,
                            &moved_ctx,
                        );
                    let _ = moved_traversal_tx.send((traversal, prioritary, thread_index));
                }
            });
        }

        ctx.try_log(|logger| {
            info!(
                logger,
                "Number of inscriptions in block #{} to process: {}",
                block.block_identifier.index,
                transactions_ids.len()
            )
        });

        let mut rng = thread_rng();
        transactions_ids.shuffle(&mut rng);
        let mut priority_queue = VecDeque::new();
        let mut warmup_queue = VecDeque::new();

        for (transaction_id, input_index) in transactions_ids.into_iter() {
            priority_queue.push_back((
                transaction_id,
                block.block_identifier.clone(),
                input_index,
                true,
            ));
        }

        // Feed each workers with 2 workitems each
        for thread_index in 0..hord_config.ingestion_thread_max {
            let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
        }
        for thread_index in 0..hord_config.ingestion_thread_max {
            let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
        }

        let mut next_block_iter = next_blocks.iter();
        let mut traversals_received = 0;
        while let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.recv() {
            if prioritary {
                traversals_received += 1;
            }
            match traversal_result {
                Ok(traversal) => {
                    ctx.try_log(|logger| {
                        info!(
                            logger,
                            "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times (progress: {traversals_received}/{expected_traversals}) (priority queue: {prioritary}, thread: {thread_index}).",
                            traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                            )
                    });
                    cache_l1.insert(
                        (
                            traversal.transaction_identifier_inscription.clone(),
                            traversal.inscription_input_index,
                        ),
                        traversal,
                    );
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        error!(logger, "Unable to compute inscription's Satoshi: {e}",)
                    });
                }
            }
            if traversals_received == expected_traversals {
                break;
            }

            if let Some(w) = priority_queue.pop_front() {
                let _ = tx_thread_pool[thread_index].send(Some(w));
            } else {
                if let Some(w) = warmup_queue.pop_front() {
                    let _ = tx_thread_pool[thread_index].send(Some(w));
                } else {
                    if let Some(next_block) = next_block_iter.next() {
                        let mut transactions_ids = get_transactions_to_process(
                            next_block,
                            cache_l1,
                            inscriptions_db_conn,
                            ctx,
                        );

                        ctx.try_log(|logger| {
                            info!(
                                logger,
                                "Number of inscriptions in block #{} to pre-process: {}",
                                block.block_identifier.index,
                                transactions_ids.len()
                            )
                        });

                        transactions_ids.shuffle(&mut rng);
                        for (transaction_id, input_index) in transactions_ids.into_iter() {
                            warmup_queue.push_back((
                                transaction_id,
                                next_block.block_identifier.clone(),
                                input_index,
                                false,
                            ));
                        }
                        let _ = tx_thread_pool[thread_index].send(warmup_queue.pop_front());
                    }
                }
            }
        }
        for thread_index in 0..hord_config.ingestion_thread_max {
            let _ = tx_thread_pool[thread_index].send(None);
        }

        let _ = hiro_system_kit::thread_named("Garbage collection").spawn(move || {
            let _ = traversal_data_pool.join();
        });
    } else {
        ctx.try_log(|logger| {
            info!(
                logger,
                "No inscriptions to index in block #{}", block.block_identifier.index
            )
        });
    }
    Ok(has_transactions_to_process)
}

pub fn update_hord_db_and_augment_bitcoin_block_v3(
    new_block: &mut BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscription_height_hint: &mut InscriptionHeigthHint,
    inscriptions_db_conn_rw: &mut Connection,
    hord_config: &HordConfig,
    ctx: &Context,
) -> Result<(), String> {
    let transactions_processed = retrieve_inscribed_satoshi_points_from_block_v3(
        &new_block,
        &next_blocks,
        cache_l1,
        cache_l2,
        inscriptions_db_conn_rw,
        &hord_config,
        ctx,
    )?;

    if !transactions_processed {
        return Ok(());
    }

    let discard_changes: bool = get_any_entry_in_ordinal_activities(
        &new_block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    );

    let inner_ctx = if discard_changes {
        Context::empty()
    } else {
        ctx.clone()
    };

    let transaction = inscriptions_db_conn_rw.transaction().unwrap();
    let any_inscription_revealed =
        update_storage_and_augment_bitcoin_block_with_inscription_reveal_data_tx(
            new_block,
            &transaction,
            &cache_l1,
            inscription_height_hint,
            &inner_ctx,
        )?;

    // Have inscriptions been transfered?
    let any_inscription_transferred =
        update_storage_and_augment_bitcoin_block_with_inscription_transfer_data_tx(
            new_block,
            &transaction,
            &inner_ctx,
        )?;

    if !any_inscription_revealed && !any_inscription_transferred {
        return Ok(());
    }

    if discard_changes {
        ctx.try_log(|logger| {
            info!(
                logger,
                "Ignoring updates for block #{}, activities present in database",
                new_block.block_identifier.index,
            )
        });
    } else {
        ctx.try_log(|logger| {
            info!(
                logger,
                "Saving updates for block {}", new_block.block_identifier.index,
            )
        });
        transaction.commit().unwrap();
        ctx.try_log(|logger| {
            info!(
                logger,
                "Updates saved for block {}", new_block.block_identifier.index,
            )
        });
    }

    let inscriptions_revealed = get_inscriptions_revealed_in_block(&new_block)
        .iter()
        .map(|d| d.inscription_number.to_string())
        .collect::<Vec<String>>();

    ctx.try_log(|logger| {
        info!(
            logger,
            "Block #{} processed through hord, revealing {} inscriptions [{}]",
            new_block.block_identifier.index,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        )
    });
    Ok(())
}

pub fn update_storage_and_augment_bitcoin_block_with_inscription_transfer_data_tx(
    block: &mut BitcoinBlockData,
    hord_db_tx: &Transaction,
    ctx: &Context,
) -> Result<bool, String> {
    let mut storage_updated = false;
    let mut cumulated_fees = 0;
    let subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };
    // todo: handle ordinals coinbase spend

    for (tx_index, new_tx) in block.transactions.iter_mut().skip(1).enumerate() {
        for (input_index, input) in new_tx.metadata.inputs.iter().enumerate() {
            // input.previous_output.txid
            let outpoint_pre_transfer = format_outpoint_to_watch(
                &input.previous_output.txid,
                input.previous_output.vout as usize,
            );

            let entries =
                find_inscriptions_at_wached_outpoint(&outpoint_pre_transfer, &hord_db_tx)?;

            // For each satpoint inscribed retrieved, we need to compute the next
            // outpoint to watch
            for watched_satpoint in entries.into_iter() {
                let satpoint_pre_transfer =
                    format!("{}:{}", outpoint_pre_transfer, watched_satpoint.offset);

                // Question is: are inscriptions moving to a new output,
                // burnt or lost in fees and transfered to the miner?

                let inputs = new_tx
                    .metadata
                    .inputs
                    .iter()
                    .map(|o| o.previous_output.value)
                    .collect::<_>();
                let outputs = new_tx
                    .metadata
                    .outputs
                    .iter()
                    .map(|o| o.value)
                    .collect::<_>();
                let post_transfer_data = compute_next_satpoint_data(
                    input_index,
                    watched_satpoint.offset,
                    &inputs,
                    &outputs,
                );

                let (
                    outpoint_post_transfer,
                    offset_post_transfer,
                    updated_address,
                    post_transfer_output_value,
                ) = match post_transfer_data {
                    SatPosition::Output((output_index, offset)) => {
                        let outpoint =
                            format_outpoint_to_watch(&new_tx.transaction_identifier, output_index);
                        let script_pub_key_hex =
                            new_tx.metadata.outputs[output_index].get_script_pubkey_hex();
                        let updated_address = match Script::from_hex(&script_pub_key_hex) {
                            Ok(script) => match Address::from_script(&script, network.clone()) {
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

                        // At this point we know that inscriptions are being moved.
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Inscription {} moved from {} to {} (block: {})",
                                watched_satpoint.inscription_id,
                                satpoint_pre_transfer,
                                outpoint,
                                block.block_identifier.index,
                            )
                        });

                        (
                            outpoint,
                            offset,
                            updated_address,
                            Some(new_tx.metadata.outputs[output_index].value),
                        )
                    }
                    SatPosition::Fee(offset) => {
                        // Get Coinbase TX
                        let total_offset = subsidy + cumulated_fees + offset;
                        let outpoint = format_outpoint_to_watch(&coinbase_txid, 0);
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Inscription {} spent in fees ({}+{}+{})",
                                watched_satpoint.inscription_id,
                                subsidy,
                                cumulated_fees,
                                offset
                            )
                        });
                        (outpoint, total_offset, None, None)
                    }
                };

                let satpoint_post_transfer =
                    format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

                let transfer_data = OrdinalInscriptionTransferData {
                    inscription_id: watched_satpoint.inscription_id.clone(),
                    updated_address,
                    tx_index,
                    satpoint_pre_transfer,
                    satpoint_post_transfer,
                    post_transfer_output_value,
                };

                // Update watched outpoint

                insert_transfer_in_locations_tx(
                    &transfer_data,
                    &block.block_identifier,
                    &hord_db_tx,
                    &ctx,
                );
                storage_updated = true;

                // Attach transfer event
                new_tx
                    .metadata
                    .ordinal_operations
                    .push(OrdinalOperation::InscriptionTransferred(transfer_data));
            }
        }
        cumulated_fees += new_tx.metadata.fee;
    }
    Ok(storage_updated)
}

pub fn update_storage_and_augment_bitcoin_block_with_inscription_reveal_data_tx(
    block: &mut BitcoinBlockData,
    transaction: &Transaction,
    cache_l1: &HashMap<(TransactionIdentifier, usize), TraversalResult>,
    inscription_height_hint: &mut InscriptionHeigthHint,
    ctx: &Context,
) -> Result<bool, String> {
    let mut storage_updated = false;
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };

    let mut latest_cursed_inscription_loaded = false;
    let mut latest_cursed_inscription_number = 0;

    let mut latest_blessed_inscription_loaded = false;
    let mut latest_blessed_inscription_number = 0;

    let mut sats_overflow = vec![];

    for (tx_index, new_tx) in block.transactions.iter_mut().skip(1).enumerate() {
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
                latest_cursed_inscription_number = if !latest_cursed_inscription_loaded {
                    latest_cursed_inscription_loaded = true;
                    match find_latest_cursed_inscription_number_at_block_height(
                        &block.block_identifier.index,
                        &inscription_height_hint.cursed,
                        &transaction,
                        &ctx,
                    )? {
                        None => -1,
                        Some(inscription_number) => inscription_number - 1,
                    }
                } else {
                    latest_cursed_inscription_number - 1
                };
                latest_cursed_inscription_number
            } else {
                latest_blessed_inscription_number = if !latest_blessed_inscription_loaded {
                    latest_blessed_inscription_loaded = true;
                    match find_latest_inscription_number_at_block_height(
                        &block.block_identifier.index,
                        &inscription_height_hint.blessed,
                        &transaction,
                        &ctx,
                    )? {
                        None => 0,
                        Some(inscription_number) => inscription_number + 1,
                    }
                } else {
                    latest_blessed_inscription_number + 1
                };
                latest_blessed_inscription_number
            };

            let transaction_identifier = new_tx.transaction_identifier.clone();
            let traversal = match cache_l1
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

            let outputs = &new_tx.metadata.outputs;
            inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
            inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
            inscription.ordinal_number = traversal.ordinal_number;
            inscription.inscription_number = traversal.inscription_number;
            inscription.transfers_pre_inscription = traversal.transfers;
            inscription.inscription_fee = new_tx.metadata.fee;
            inscription.tx_index = tx_index;
            inscription.satpoint_post_inscription = format_satpoint_to_watch(
                &traversal.transfer_data.transaction_identifier_location,
                traversal.transfer_data.output_index,
                traversal.transfer_data.inscription_offset_intra_output,
            );
            if let Some(output) = outputs.get(traversal.transfer_data.output_index) {
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

            if traversal.ordinal_number == 0 {
                // If the satoshi inscribed correspond to a sat overflow, we will store the inscription
                // and assign an inscription number after the other inscriptions, to mimick the
                // bug in ord.
                sats_overflow.push(inscription.clone());
                continue;
            }

            if let Some(_entry) =
                find_inscription_with_ordinal_number(&traversal.ordinal_number, &transaction, &ctx)
            {
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
            insert_entry_in_inscriptions(&inscription, &block.block_identifier, &transaction, &ctx);

            storage_updated = true;
        }

        for index in ordinals_events_indexes_to_discard.into_iter() {
            new_tx.metadata.ordinal_operations.remove(index);
        }
    }

    for inscription in sats_overflow.iter_mut() {
        inscription.inscription_number = latest_blessed_inscription_number;
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
        insert_entry_in_inscriptions(&inscription, &block.block_identifier, &transaction, &ctx);
        latest_blessed_inscription_number += 1;
        storage_updated = true;
    }

    if latest_cursed_inscription_loaded {
        inscription_height_hint.cursed = Some(block.block_identifier.index);
    }
    if latest_blessed_inscription_loaded {
        inscription_height_hint.blessed = Some(block.block_identifier.index);
    }

    Ok(storage_updated)
}
