use std::{
    collections::{HashMap, VecDeque},
    hash::BuildHasherDefault,
    sync::Arc,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{hashes::hex::FromHex, Address, Network, Script},
    types::{
        BitcoinBlockData, BitcoinNetwork, OrdinalInscriptionCurseType,
        OrdinalInscriptionTransferData, OrdinalOperation, TransactionIdentifier,
    },
    utils::Context,
};
use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::{Connection, Transaction};
use std::sync::mpsc::Sender;

use crate::{
    db::{
        find_inscription_with_ordinal_number, find_inscriptions_at_wached_outpoint,
        find_latest_cursed_inscription_number_at_block_height,
        find_latest_inscription_number_at_block_height, format_outpoint_to_watch,
        format_satpoint_to_watch, get_any_entry_in_ordinal_activities,
        insert_entry_in_inscriptions, insert_transfer_in_locations_tx, InscriptionHeigthHint,
        LazyBlockTransaction, TraversalResult,
    },
    ord::height::Height,
};

use super::{
    compute_next_satpoint_data, get_inscriptions_revealed_in_block,
    protocol::retrieve_inscribed_satoshi_points_from_block_v3, HordConfig, SatPosition,
};

pub fn process_blocks(
    mut next_blocks: Vec<BitcoinBlockData>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscription_height_hint: &mut InscriptionHeigthHint,
    inscriptions_db_conn_rw: &mut Connection,
    hord_config: &HordConfig,
    post_processor: &Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) {
    let mut cache_l1 = HashMap::new();

    for _cursor in 0..next_blocks.len() {
        let mut block = next_blocks.remove(0);

        let _ = process_block(
            &mut block,
            &next_blocks,
            &mut cache_l1,
            cache_l2,
            inscription_height_hint,
            inscriptions_db_conn_rw,
            hord_config,
            ctx,
        );

        if let Some(post_processor_tx) = post_processor {
            let _ = post_processor_tx.send(block);
        }
    }
}

pub fn process_block(
    block: &mut BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscription_height_hint: &mut InscriptionHeigthHint,
    inscriptions_db_conn_rw: &mut Connection,
    hord_config: &HordConfig,
    ctx: &Context,
) -> Result<(), String> {
    update_hord_db_and_augment_bitcoin_block_v3(
        block,
        next_blocks,
        cache_l1,
        cache_l2,
        inscription_height_hint,
        inscriptions_db_conn_rw,
        hord_config,
        ctx,
    )
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
        if hord_config.logs.ordinals_computation {
            ctx.clone()
        } else {
            Context::empty()
        }
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
            "Block #{} revealed {} inscriptions [{}]",
            new_block.block_identifier.index,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        )
    });
    Ok(())
}

/// For each input of each transaction in the block, we retrieve the UTXO spent (outpoint_pre_transfer)
/// and we check using a `storage` (in-memory or sqlite) absctraction if we have some existing inscriptions
/// for this entry.
/// When this is the case, it means that an inscription_transfer event needs to be produced. We need to
/// compute the output index (if any) `post_transfer_output` that will now include the inscription.
/// When identifying the output index, we will also need to provide an updated offset for pin pointing
/// the satoshi location.
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
                                            warn!(
                                                logger,
                                                "unable to retrieve address from {script_pub_key_hex}: {}", e.to_string()
                                            )
                                        });
                                    None
                                }
                            },
                            Err(e) => {
                                ctx.try_log(|logger| {
                                    warn!(
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
                            info!(
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
                            info!(
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
    let mut cursed_inscription_sequence_updated = false;

    let mut latest_blessed_inscription_loaded = false;
    let mut latest_blessed_inscription_number = 0;
    let mut blessed_inscription_sequence_updated = false;

    let mut sats_overflow = vec![];

    for (tx_index, new_tx) in block.transactions.iter_mut().skip(1).enumerate() {
        let mut ordinals_events_indexes_to_discard = VecDeque::new();
        let mut ordinals_events_indexes_to_curse = VecDeque::new();

        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for (ordinal_event_index, ordinal_event) in
            new_tx.metadata.ordinal_operations.iter_mut().enumerate()
        {
            let (inscription, is_cursed) = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription) => (inscription, false),
                OrdinalOperation::CursedInscriptionRevealed(inscription) => (inscription, true),
                OrdinalOperation::InscriptionTransferred(_) => continue,
            };

            let mut inscription_number = if is_cursed {
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
                        info!(
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
                    warn!(
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
                    info!(
                        logger,
                        "Transaction {} in block {} is overriding an existing inscription {}",
                        new_tx.transaction_identifier.hash,
                        block.block_identifier.index,
                        traversal.ordinal_number
                    );
                });

                inscription_number = if !latest_cursed_inscription_loaded {
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
                inscription.curse_type = Some(OrdinalInscriptionCurseType::Batch);

                if !is_cursed {
                    ordinals_events_indexes_to_curse.push_front(ordinal_event_index);
                    latest_blessed_inscription_number -= 1;
                }
            }

            inscription.inscription_number = inscription_number;
            ctx.try_log(|logger| {
                info!(
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
            if inscription.curse_type.is_some() {
                cursed_inscription_sequence_updated = true;
            } else {
                blessed_inscription_sequence_updated = true;
            }
            storage_updated = true;
        }

        for index in ordinals_events_indexes_to_curse.into_iter() {
            match new_tx.metadata.ordinal_operations.remove(index) {
                OrdinalOperation::InscriptionRevealed(inscription_data)
                | OrdinalOperation::CursedInscriptionRevealed(inscription_data) => {
                    ctx.try_log(|logger| {
                        info!(
                            logger,
                            "Inscription {} (#{}) transitioned from blessed to cursed",
                            inscription_data.inscription_id,
                            inscription_data.inscription_number,
                        );
                    });
                    new_tx.metadata.ordinal_operations.insert(
                        index,
                        OrdinalOperation::CursedInscriptionRevealed(inscription_data),
                    );
                }
                _ => unreachable!(),
            }
        }

        for index in ordinals_events_indexes_to_discard.into_iter() {
            new_tx.metadata.ordinal_operations.remove(index);
        }
    }

    for inscription in sats_overflow.iter_mut() {
        inscription.inscription_number = latest_blessed_inscription_number;
        ctx.try_log(|logger| {
            info!(
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
        if inscription.curse_type.is_some() {
            cursed_inscription_sequence_updated = true;
        } else {
            blessed_inscription_sequence_updated = true;
        }
    }

    if cursed_inscription_sequence_updated {
        inscription_height_hint.cursed = Some(block.block_identifier.index);
    }
    if blessed_inscription_sequence_updated {
        inscription_height_hint.blessed = Some(block.block_identifier.index);
    }

    Ok(storage_updated)
}
