use std::collections::HashSet;

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{Address, Network, ScriptBuf},
    types::{
        BitcoinBlockData, BitcoinTransactionData, OrdinalInscriptionTransferData,
        OrdinalInscriptionTransferDestination, OrdinalOperation,
    },
    utils::Context,
};

use crate::{
    core::{compute_next_satpoint_data, SatPosition},
    db::{
        find_inscribed_ordinals_at_wached_outpoint, format_outpoint_to_watch,
        insert_ordinal_transfer_in_locations_tx, parse_satpoint_to_watch, OrdinalLocation,
    },
    ord::height::Height,
};
use rusqlite::Transaction;

use super::inscription_sequencing::get_bitcoin_network;

pub fn augment_block_with_ordinals_transfer_data(
    block: &mut BitcoinBlockData,
    inscriptions_db_tx: &Transaction,
    update_db_tx: bool,
    ctx: &Context,
) -> bool {
    let mut any_event = false;

    let network = get_bitcoin_network(&block.metadata.network);
    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_tx = &block.transactions[0].clone();
    let mut cumulated_fees = 0;
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        let transfers = augment_transaction_with_ordinals_transfers_data(
            tx,
            tx_index,
            &network,
            &coinbase_tx,
            coinbase_subsidy,
            &mut cumulated_fees,
            inscriptions_db_tx,
            ctx,
        );
        any_event |= !transfers.is_empty();

        if update_db_tx {
            // Store transfers between each iteration
            for transfer_data in transfers.into_iter() {
                let (tx, output_index, offset) =
                    parse_satpoint_to_watch(&transfer_data.satpoint_post_transfer);
                let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
                let data = OrdinalLocation {
                    offset,
                    block_height: block.block_identifier.index,
                    tx_index: transfer_data.tx_index,
                };
                insert_ordinal_transfer_in_locations_tx(
                    transfer_data.ordinal_number,
                    &outpoint_to_watch,
                    data,
                    inscriptions_db_tx,
                    &ctx,
                );
            }
        }
    }

    any_event
}

pub fn compute_satpoint_post_transfer(
    tx: &BitcoinTransactionData,
    input_index: usize,
    relative_pointer_value: u64,
    network: &Network,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    ctx: &Context,
) -> (OrdinalInscriptionTransferDestination, String, Option<u64>) {
    let inputs: Vec<u64> = tx
        .metadata
        .inputs
        .iter()
        .map(|o| o.previous_output.value)
        .collect::<_>();
    let outputs = tx.metadata.outputs.iter().map(|o| o.value).collect::<_>();
    let post_transfer_data = compute_next_satpoint_data(
        input_index,
        &inputs,
        &outputs,
        relative_pointer_value,
        Some(ctx),
    );

    let (outpoint_post_transfer, offset_post_transfer, destination, post_transfer_output_value) =
        match post_transfer_data {
            SatPosition::Output((output_index, offset)) => {
                let outpoint = format_outpoint_to_watch(&tx.transaction_identifier, output_index);
                let script_pub_key_hex = tx.metadata.outputs[output_index].get_script_pubkey_hex();
                let updated_address = match ScriptBuf::from_hex(&script_pub_key_hex) {
                    Ok(script) => match Address::from_script(&script, network.clone()) {
                        Ok(address) => {
                            OrdinalInscriptionTransferDestination::Transferred(address.to_string())
                        }
                        Err(e) => {
                            ctx.try_log(|logger| {
                                info!(
                                    logger,
                                    "unable to retrieve address from {script_pub_key_hex}: {}",
                                    e.to_string()
                                )
                            });
                            OrdinalInscriptionTransferDestination::Burnt(script.to_string())
                        }
                    },
                    Err(e) => {
                        ctx.try_log(|logger| {
                            info!(
                                logger,
                                "unable to retrieve address from {script_pub_key_hex}: {}",
                                e.to_string()
                            )
                        });
                        OrdinalInscriptionTransferDestination::Burnt(script_pub_key_hex.to_string())
                    }
                };

                (
                    outpoint,
                    offset,
                    updated_address,
                    Some(tx.metadata.outputs[output_index].value),
                )
            }
            SatPosition::Fee(offset) => {
                // Get Coinbase TX
                let total_offset = coinbase_subsidy + *cumulated_fees + offset;
                let outputs = coinbase_tx.metadata.outputs.iter().map(|o| o.value).collect();
                let post_transfer_data = compute_next_satpoint_data(
                    0,
                    &vec![total_offset],
                    &outputs,
                    total_offset,
                    Some(ctx),
                );

                // Identify the correct output
                let (output_index, offset) = match post_transfer_data {
                    SatPosition::Output(pos) => pos,
                    _ => {
                        ctx.try_log(|logger| {
                            info!(
                                logger,
                                "unable to locate satoshi in coinbase outputs",
                            )
                        });        
                        (0, total_offset)
                    }
                };

                let outpoint = format_outpoint_to_watch(&coinbase_tx.transaction_identifier, output_index);
                (
                    outpoint,
                    offset,
                    OrdinalInscriptionTransferDestination::SpentInFees,
                    None,
                )
            }
        };
    let satpoint_post_transfer = format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

    (
        destination,
        satpoint_post_transfer,
        post_transfer_output_value,
    )
}

pub fn augment_transaction_with_ordinals_transfers_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    network: &Network,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    inscriptions_db_tx: &Transaction,
    ctx: &Context,
) -> Vec<OrdinalInscriptionTransferData> {
    let mut transfers = vec![];

    // The transfers are inserted in storage after the inscriptions.
    // We have a unicity constraing, and can only have 1 ordinals per satpoint.
    let mut updated_sats = HashSet::new();
    for op in tx.metadata.ordinal_operations.iter() {
        if let OrdinalOperation::InscriptionRevealed(data) = op {
            updated_sats.insert(data.ordinal_number);
        }
    }

    for (input_index, input) in tx.metadata.inputs.iter().enumerate() {
        let outpoint_pre_transfer = format_outpoint_to_watch(
            &input.previous_output.txid,
            input.previous_output.vout as usize,
        );

        let entries = find_inscribed_ordinals_at_wached_outpoint(
            &outpoint_pre_transfer,
            &inscriptions_db_tx,
            ctx,
        );
        // For each satpoint inscribed retrieved, we need to compute the next
        // outpoint to watch
        for watched_satpoint in entries.into_iter() {
            if updated_sats.contains(&watched_satpoint.ordinal_number) {
                continue;
            }
            let satpoint_pre_transfer =
                format!("{}:{}", outpoint_pre_transfer, watched_satpoint.offset);

            let (destination, satpoint_post_transfer, post_transfer_output_value) =
                compute_satpoint_post_transfer(
                    &&*tx,
                    input_index,
                    watched_satpoint.offset,
                    network,
                    coinbase_tx,
                    coinbase_subsidy,
                    cumulated_fees,
                    ctx,
                );

            let transfer_data = OrdinalInscriptionTransferData {
                ordinal_number: watched_satpoint.ordinal_number,
                destination,
                tx_index,
                satpoint_pre_transfer,
                satpoint_post_transfer,
                post_transfer_output_value,
            };

            transfers.push(transfer_data.clone());

            // Attach transfer event
            tx.metadata
                .ordinal_operations
                .push(OrdinalOperation::InscriptionTransferred(transfer_data));
        }
    }
    *cumulated_fees += tx.metadata.fee;

    transfers
}
