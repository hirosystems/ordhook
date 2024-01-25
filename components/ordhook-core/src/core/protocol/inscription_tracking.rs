use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{Address, Network, ScriptBuf},
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, OrdinalInscriptionTransferData,
        OrdinalInscriptionTransferDestination, OrdinalOperation, TransactionIdentifier,
    },
    utils::Context,
};

use crate::{
    core::{compute_next_satpoint_data, SatPosition},
    db::{
        find_inscriptions_at_wached_outpoint, format_outpoint_to_watch,
        insert_transfer_in_locations_tx,
    },
    ord::height::Height,
};
use rusqlite::Transaction;

pub fn augment_block_with_ordinals_transfer_data(
    block: &mut BitcoinBlockData,
    inscriptions_db_tx: &Transaction,
    update_db_tx: bool,
    ctx: &Context,
) -> bool {
    let mut any_event = false;

    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
        BitcoinNetwork::Signet => Network::Signet,
    };

    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let mut cumulated_fees = 0;
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        let transfers = augment_transaction_with_ordinals_transfers_data(
            tx,
            tx_index,
            &network,
            &coinbase_txid,
            coinbase_subsidy,
            &mut cumulated_fees,
            inscriptions_db_tx,
            ctx,
        );
        any_event |= !transfers.is_empty();

        if update_db_tx {
            // Store transfers between each iteration
            for transfer_data in transfers.into_iter() {
                insert_transfer_in_locations_tx(
                    &transfer_data,
                    &block.block_identifier,
                    &inscriptions_db_tx,
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
    inscription_pointer: u64,
    network: &Network,
    coinbase_txid: &TransactionIdentifier,
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
    let post_transfer_data =
        compute_next_satpoint_data(input_index, 0, &inputs, &outputs, inscription_pointer);

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
                let outpoint = format_outpoint_to_watch(&coinbase_txid, 0);
                (
                    outpoint,
                    total_offset,
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
    coinbase_txid: &TransactionIdentifier,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    inscriptions_db_tx: &Transaction,
    ctx: &Context,
) -> Vec<OrdinalInscriptionTransferData> {
    let mut transfers = vec![];

    for (input_index, input) in tx.metadata.inputs.iter().enumerate() {
        let outpoint_pre_transfer = format_outpoint_to_watch(
            &input.previous_output.txid,
            input.previous_output.vout as usize,
        );

        let entries =
            find_inscriptions_at_wached_outpoint(&outpoint_pre_transfer, &inscriptions_db_tx, ctx);
        // For each satpoint inscribed retrieved, we need to compute the next
        // outpoint to watch
        for watched_satpoint in entries.into_iter() {
            let satpoint_pre_transfer =
                format!("{}:{}", outpoint_pre_transfer, watched_satpoint.offset);

            let (destination, satpoint_post_transfer, post_transfer_output_value) =
                compute_satpoint_post_transfer(
                    &&*tx,
                    input_index,
                    0,
                    network,
                    coinbase_txid,
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
