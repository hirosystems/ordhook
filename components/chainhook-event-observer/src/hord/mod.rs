pub mod db;
pub mod inscription;
pub mod ord;

use std::collections::VecDeque;
use bitcoincore_rpc::bitcoin::hashes::hex::FromHex;
use bitcoincore_rpc::bitcoin::{
    Address, Script, Network,
};
use chainhook_types::{BitcoinBlockData, OrdinalInscriptionTransferData, OrdinalOperation};
use hiro_system_kit::slog;

use crate::{
    hord::{
        db::{
            find_inscription_with_ordinal_number, find_inscriptions_at_wached_outpoint,
            find_last_inscription_number, open_readonly_hord_db_conn, open_readwrite_hord_db_conn,
            retrieve_satoshi_point_using_local_storage, store_new_inscription,
            update_transfered_inscription, write_compacted_block_to_index, CompactedBlock,
        },
        ord::height::Height,
    },
    observer::EventObserverConfig,
    utils::Context,
};

pub fn process_bitcoin_block_using_hord(
    config: &EventObserverConfig,
    new_block: &mut BitcoinBlockData,
    ctx: &Context,
) {
    {
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Persisting in local storage Bitcoin block #{} for further traversals",
                new_block.block_identifier.index,
            )
        });

        let compacted_block = CompactedBlock::from_standardized_block(&new_block);
        let storage_rw_conn =
            open_readwrite_hord_db_conn(&config.get_cache_path_buf(), &ctx).unwrap(); // TODO(lgalabru)
        write_compacted_block_to_index(
            new_block.block_identifier.index as u32,
            &compacted_block,
            &storage_rw_conn,
            &ctx,
        );
    }

    let mut cumulated_fees = 0;
    let coinbase_txid = &new_block.transactions[0]
        .transaction_identifier
        .hash
        .clone();
    let first_sat_post_subsidy = Height(new_block.block_identifier.index).starting_sat().0;

    for new_tx in new_block.transactions.iter_mut().skip(1) {
        let mut ordinals_events_indexes_to_discard = VecDeque::new();
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for (ordinal_event_index, ordinal_event) in
            new_tx.metadata.ordinal_operations.iter_mut().enumerate()
        {
            if let OrdinalOperation::InscriptionRevealed(inscription) = ordinal_event {
                let hord_db_conn =
                    open_readonly_hord_db_conn(&config.get_cache_path_buf(), &ctx).unwrap(); // TODO(lgalabru)

                let (ordinal_block_height, ordinal_offset, ordinal_number) = {
                    // Are we looking at a re-inscription?
                    let res = retrieve_satoshi_point_using_local_storage(
                        &hord_db_conn,
                        &new_block.block_identifier,
                        &new_tx.transaction_identifier,
                        &ctx,
                    );

                    match res {
                        Ok(res) => res,
                        Err(e) => {
                            ctx.try_log(|logger| {
                                slog::error!(
                                    logger,
                                    "unable to retrieve satoshi point: {}",
                                    e.to_string()
                                );
                            });
                            continue;
                        }
                    }
                };

                if let Some(_entry) =
                    find_inscription_with_ordinal_number(&ordinal_number, &hord_db_conn, &ctx)
                {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "Transaction {} in block {} is overriding an existing inscription {}",
                            new_tx.transaction_identifier.hash,
                            new_block.block_identifier.index,
                            ordinal_number
                        );
                    });
                    ordinals_events_indexes_to_discard.push_front(ordinal_event_index);
                } else {
                    inscription.ordinal_offset = ordinal_offset;
                    inscription.ordinal_block_height = ordinal_block_height;
                    inscription.ordinal_number = ordinal_number;
                    inscription.inscription_number =
                        match find_last_inscription_number(&hord_db_conn, &ctx) {
                            Ok(inscription_number) => inscription_number,
                            Err(e) => {
                                ctx.try_log(|logger| {
                                    slog::error!(
                                        logger,
                                        "unable to retrieve satoshi number: {}",
                                        e.to_string()
                                    );
                                });
                                continue;
                            }
                        };
                    ctx.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Transaction {} in block {} includes a new inscription {}",
                            new_tx.transaction_identifier.hash,
                            new_block.block_identifier.index,
                            ordinal_number
                        );
                    });

                    {
                        let storage_rw_conn =
                            open_readwrite_hord_db_conn(&config.get_cache_path_buf(), &ctx)
                                .unwrap(); // TODO(lgalabru)
                        store_new_inscription(
                            &inscription,
                            &new_block.block_identifier,
                            &storage_rw_conn,
                            &ctx,
                        )
                    }
                }
            }
        }

        // Have inscriptions been transfered?
        let mut sats_in_offset = 0;
        let mut sats_out_offset = 0;
        let hord_db_conn = open_readonly_hord_db_conn(&config.get_cache_path_buf(), &ctx).unwrap(); // TODO(lgalabru)

        for input in new_tx.metadata.inputs.iter() {
            // input.previous_output.txid
            let outpoint_pre_transfer = format!(
                "{}:{}",
                &input.previous_output.txid[2..],
                input.previous_output.vout
            );

            let mut post_transfer_output_index = 0;

            let entries =
                find_inscriptions_at_wached_outpoint(&outpoint_pre_transfer, &hord_db_conn);

            ctx.try_log(|logger| {
                slog::info!(
                    logger,
                    "Checking if {} is part of our watch outpoints set: {}",
                    outpoint_pre_transfer,
                    entries.len(),
                )
            });

            for (inscription_id, inscription_number, ordinal_number, offset) in entries.into_iter()
            {
                let satpoint_pre_transfer = format!("{}:{}", outpoint_pre_transfer, offset);
                // At this point we know that inscriptions are being moved.
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Detected transaction {} involving txin {} that includes watched ordinals",
                        new_tx.transaction_identifier.hash,
                        satpoint_pre_transfer,
                    )
                });

                // Question is: are inscriptions moving to a new output,
                // burnt or lost in fees and transfered to the miner?
                let post_transfer_output = loop {
                    if sats_out_offset >= sats_in_offset + offset {
                        break Some(post_transfer_output_index);
                    }
                    if post_transfer_output_index >= new_tx.metadata.outputs.len() {
                        break None;
                    }
                    sats_out_offset += new_tx.metadata.outputs[post_transfer_output_index].value;
                    post_transfer_output_index += 1;
                };

                let (outpoint_post_transfer, offset_post_transfer, updated_address) =
                    match post_transfer_output {
                        Some(index) => {
                            let outpoint =
                                format!("{}:{}", &new_tx.transaction_identifier.hash[2..], index);
                            let offset = 0;
                            let script_pub_key_hex =
                                new_tx.metadata.outputs[index].get_script_pubkey_hex();
                            let updated_address = match Script::from_hex(&script_pub_key_hex) {
                                Ok(script) => match Address::from_script(&script, Network::Bitcoin)
                                {
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
                                            "unable to retrieve address from {script_pub_key_hex}: {}", e.to_string()
                                        )
                                    });
                                    None
                                }
                            };

                            // let vout = new_tx.metadata.outputs[index];
                            (outpoint, offset, updated_address)
                        }
                        None => {
                            // Get Coinbase TX
                            let offset = first_sat_post_subsidy + cumulated_fees;
                            let outpoint = coinbase_txid.clone();
                            (outpoint, offset, None)
                        }
                    };

                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Updating watched outpoint {} to outpoint {}",
                        outpoint_post_transfer,
                        outpoint_pre_transfer,
                    )
                });

                // Update watched outpoint
                {
                    let storage_rw_conn =
                        open_readwrite_hord_db_conn(&config.get_cache_path_buf(), &ctx).unwrap(); // TODO(lgalabru)
                    update_transfered_inscription(
                        &inscription_id,
                        &outpoint_post_transfer,
                        offset_post_transfer,
                        &storage_rw_conn,
                        &ctx,
                    );
                }

                let satpoint_post_transfer =
                    format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

                let event_data = OrdinalInscriptionTransferData {
                    inscription_id,
                    inscription_number,
                    ordinal_number,
                    updated_address,
                    satpoint_pre_transfer,
                    satpoint_post_transfer,
                };

                // Attach transfer event
                new_tx
                    .metadata
                    .ordinal_operations
                    .push(OrdinalOperation::InscriptionTransferred(event_data));
            }

            sats_in_offset += input.previous_output.value;
        }

        // - clean new_tx.metadata.ordinal_operations with ordinals_events_indexes_to_ignore
        for index in ordinals_events_indexes_to_discard.into_iter() {
            new_tx.metadata.ordinal_operations.remove(index);
        }

        cumulated_fees += new_tx.metadata.fee;
    }
}
