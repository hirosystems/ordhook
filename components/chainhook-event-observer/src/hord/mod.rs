pub mod db;
pub mod inscription;
pub mod ord;

use bitcoincore_rpc::bitcoin::hashes::hex::FromHex;
use bitcoincore_rpc::bitcoin::{Address, Network, Script};
use chainhook_types::{
    BitcoinBlockData, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    OrdinalOperation, TransactionIdentifier,
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
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use threadpool::ThreadPool;

use crate::indexer::bitcoin::BitcoinTransactionFullBreakdown;
use crate::{
    hord::{
        db::{
            find_inscription_with_ordinal_number, find_inscriptions_at_wached_outpoint,
            insert_entry_in_blocks, retrieve_satoshi_point_using_local_storage,
            store_new_inscription, update_transfered_inscription, CompactedBlock,
        },
        ord::height::Height,
    },
    utils::Context,
};

use self::db::{
    find_inscription_with_id, find_latest_inscription_number_at_block_height,
    open_readonly_hord_db_conn_rocks_db, remove_entry_from_blocks, remove_entry_from_inscriptions,
    TraversalResult, WatchedSatpoint,
};
use self::inscription::InscriptionParser;
use self::ord::inscription_id::InscriptionId;

pub fn try_parse_ordinal_operation(
    tx: &BitcoinTransactionFullBreakdown,
    _block_height: u64,
    _ctx: &Context,
) -> Option<OrdinalOperation> {
    // This should eventually become a loop once/if there is settlement on https://github.com/casey/ord/issues/2000.
    if let Some(first_input) = tx.vin.get(0) {
        if let Some(ref witnesses) = first_input.txinwitness {
            for bytes in witnesses.iter() {
                let script = Script::from(bytes.to_vec());
                let parser = InscriptionParser {
                    instructions: script.instructions().peekable(),
                };

                let inscription = match parser.parse_script() {
                    Ok(inscription) => inscription,
                    Err(_) => continue,
                };

                let inscription_id = InscriptionId {
                    txid: tx.txid.clone(),
                    index: 0,
                };

                let inscription_output_value = tx
                    .vout
                    .get(0)
                    .and_then(|o| Some(o.value.to_sat()))
                    .unwrap_or(0);

                let no_content_bytes = vec![];
                let inscription_content_bytes = inscription.body().unwrap_or(&no_content_bytes);

                let inscriber_address = if let Ok(authors) = Address::from_script(
                    &tx.vout[0].script_pub_key.script().unwrap(),
                    bitcoincore_rpc::bitcoin::Network::Bitcoin,
                ) {
                    Some(authors.to_string())
                } else {
                    None
                };

                return Some(OrdinalOperation::InscriptionRevealed(
                    OrdinalInscriptionRevealData {
                        content_type: inscription.content_type().unwrap_or("unknown").to_string(),
                        content_bytes: format!("0x{}", hex::encode(&inscription_content_bytes)),
                        content_length: inscription_content_bytes.len(),
                        inscription_id: inscription_id.to_string(),
                        inscriber_address,
                        inscription_output_value,
                        inscription_fee: 0,
                        inscription_number: 0,
                        ordinal_number: 0,
                        ordinal_block_height: 0,
                        ordinal_offset: 0,
                        transfers_pre_inscription: 0,
                        satpoint_post_inscription: format!("{}:0:0", tx.txid.clone()),
                    },
                ));
            }
        }
    }
    None
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
                OrdinalOperation::InscriptionRevealed(data) => {
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

pub fn retrieve_inscribed_satoshi_points_from_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn: Option<&Connection>,
    hord_db_path: &PathBuf,
    traversals_cache: &Arc<
        DashMap<
            (u32, [u8; 8]),
            (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>),
            BuildHasherDefault<FxHasher>,
        >,
    >,
    ctx: &Context,
) -> HashMap<TransactionIdentifier, TraversalResult> {
    let mut transactions_ids = vec![];
    let mut traversals = HashMap::new();

    for tx in block.transactions.iter().skip(1) {
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionRevealed(inscription_data) = ordinal_event {
                if let Some(inscriptions_db_conn) = inscriptions_db_conn {
                    if let Some(traversal) = find_inscription_with_id(
                        &inscription_data.inscription_id,
                        &block.block_identifier.hash,
                        inscriptions_db_conn,
                        ctx,
                    ) {
                        traversals.insert(tx.transaction_identifier.clone(), traversal);
                    } else {
                        // Enqueue for traversals
                        transactions_ids.push(tx.transaction_identifier.clone());
                    }
                } else {
                    // Enqueue for traversals
                    transactions_ids.push(tx.transaction_identifier.clone());
                }
            }
        }
    }

    if !transactions_ids.is_empty() {
        let expected_traversals = transactions_ids.len();
        let (traversal_tx, traversal_rx) = channel::<(TransactionIdentifier, _)>();
        let traversal_data_pool = ThreadPool::new(10);

        let mut rng = thread_rng();
        transactions_ids.shuffle(&mut rng);
        for transaction_id in transactions_ids.into_iter() {
            let moved_traversal_tx = traversal_tx.clone();
            let moved_ctx = ctx.clone();
            let block_identifier = block.block_identifier.clone();
            let moved_hord_db_path = hord_db_path.clone();
            let local_cache = traversals_cache.clone();
            traversal_data_pool.execute(move || loop {
                match open_readonly_hord_db_conn_rocks_db(&moved_hord_db_path, &moved_ctx) {
                    Ok(blocks_db) => {
                        let traversal = retrieve_satoshi_point_using_local_storage(
                            &blocks_db,
                            &block_identifier,
                            &transaction_id,
                            0,
                            local_cache,
                            &moved_ctx,
                        );
                        let _ = moved_traversal_tx.send((transaction_id, traversal));
                        break;
                    }
                    Err(e) => {
                        moved_ctx.try_log(|logger| {
                            slog::error!(
                                logger,
                                "Unable to retrieve satoshi point in {} ({}): {e}",
                                transaction_id.hash,
                                block_identifier.index
                            );
                        });
                    }
                }
            });
        }

        let mut traversals_received = 0;
        while let Ok((transaction_identifier, traversal_result)) = traversal_rx.recv() {
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
                    traversals.insert(transaction_identifier, traversal);
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::error!(
                            logger,
                            "Unable to compute inscription's Satoshi from transaction {}: {e}",
                            transaction_identifier.hash
                        )
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
    hord_db_path: &PathBuf,
    traversals_cache: &Arc<
        DashMap<
            (u32, [u8; 8]),
            (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>),
            BuildHasherDefault<FxHasher>,
        >,
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

        let compacted_block = CompactedBlock::from_standardized_block(&new_block);
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
        hord_db_path,
        traversals_cache,
        ctx,
    );

    let mut storage = Storage::Sqlite(inscriptions_db_conn_rw);
    update_storage_and_augment_bitcoin_block_with_inscription_reveal_data(
        new_block,
        &mut storage,
        &traversals,
        &inscriptions_db_conn_rw,
        &ctx,
    );

    // Have inscriptions been transfered?
    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
        new_block,
        &mut storage,
        &ctx,
    )?;
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
    traversals: &HashMap<TransactionIdentifier, TraversalResult>,
    inscription_db_conn: &Connection,
    ctx: &Context,
) {
    let mut latest_inscription_number = match find_latest_inscription_number_at_block_height(
        &block.block_identifier.index,
        &inscription_db_conn,
        &ctx,
    ) {
        Ok(None) => 0,
        Ok(Some(inscription_number)) => inscription_number + 1,
        Err(e) => {
            ctx.try_log(|logger| {
                slog::error!(
                    logger,
                    "unable to retrieve inscription number: {}",
                    e.to_string()
                );
            });
            return;
        }
    };
    for new_tx in block.transactions.iter_mut().skip(1) {
        let mut ordinals_events_indexes_to_discard = VecDeque::new();
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for (ordinal_event_index, ordinal_event) in
            new_tx.metadata.ordinal_operations.iter_mut().enumerate()
        {
            if let OrdinalOperation::InscriptionRevealed(inscription) = ordinal_event {
                let inscription_number = latest_inscription_number;
                let traversal = match traversals.get(&new_tx.transaction_identifier) {
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
                inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
                inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
                inscription.ordinal_number = traversal.ordinal_number;
                inscription.inscription_number = traversal.inscription_number;
                inscription.transfers_pre_inscription = traversal.transfers;
                inscription.inscription_fee = new_tx.metadata.fee;

                match storage {
                    Storage::Sqlite(rw_hord_db_conn) => {
                        if traversal.ordinal_number > 0 {
                            if let Some(_entry) = find_inscription_with_ordinal_number(
                                &traversal.ordinal_number,
                                &inscription_db_conn,
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
                        } else {
                            // If the satoshi inscribed correspond to a sat overflow, we will store the inscription
                            // but exclude it from the block data
                            ordinals_events_indexes_to_discard.push_front(ordinal_event_index);
                        }
                        latest_inscription_number += 1;
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
                        store_new_inscription(
                            &inscription,
                            &block.block_identifier,
                            &rw_hord_db_conn,
                            &ctx,
                        );
                    }
                    Storage::Memory(map) => {
                        let outpoint = inscription.satpoint_post_inscription
                            [0..inscription.satpoint_post_inscription.len() - 2]
                            .to_string();
                        map.insert(
                            outpoint,
                            vec![WatchedSatpoint {
                                inscription_id: inscription.inscription_id.clone(),
                                inscription_number: inscription.inscription_number,
                                ordinal_number: inscription.ordinal_number,
                                offset: 0,
                            }],
                        );
                    }
                }
            }
        }

        for index in ordinals_events_indexes_to_discard.into_iter() {
            new_tx.metadata.ordinal_operations.remove(index);
        }
    }
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
) -> Result<(), String> {
    let mut cumulated_fees = 0;
    let first_sat_post_subsidy = Height(block.block_identifier.index).starting_sat().0;
    let coinbase_txid = &block.transactions[0].transaction_identifier.hash.clone();

    // todo: handle ordinals coinbase spend

    for new_tx in block.transactions.iter_mut().skip(1) {
        // Have inscriptions been transfered?
        let mut sats_in_offset = 0;

        for input in new_tx.metadata.inputs.iter() {
            // input.previous_output.txid
            let outpoint_pre_transfer = format!(
                "{}:{}",
                &input.previous_output.txid[2..],
                input.previous_output.vout
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
                            format!("{}:{}", &new_tx.transaction_identifier.hash[2..], index);
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
    Ok(())
}
