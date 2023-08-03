use std::{
    collections::BTreeMap,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{hashes::hex::FromHex, Address, Network, Script},
    types::{BitcoinBlockData, BitcoinNetwork, OrdinalInscriptionTransferData, OrdinalOperation},
    utils::Context,
};
use crossbeam_channel::{Sender, TryRecvError};

use crate::{
    core::protocol::sequencing::update_storage_and_augment_bitcoin_block_with_inscription_transfer_data_tx,
    db::{
        find_all_inscriptions_in_block, format_satpoint_to_watch, insert_entry_in_locations,
        parse_satpoint_to_watch, remove_entries_from_locations_at_block_height,
    },
};

use crate::{
    config::Config,
    core::pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    db::open_readwrite_hord_db_conn,
};

pub fn start_transfers_recomputing_processor(
    config: &Config,
    ctx: &Context,
    post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Inscription indexing runloop")
        .spawn(move || {
            let mut inscriptions_db_conn_rw =
                open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let mut empty_cycles = 0;

            if let Ok(PostProcessorCommand::Start) = commands_rx.recv() {
                info!(ctx.expect_logger(), "Start transfers recomputing runloop");
            }

            loop {
                let mut blocks = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(_, blocks)) => {
                        empty_cycles = 0;
                        blocks
                    }
                    Ok(PostProcessorCommand::Terminate) => break,
                    Ok(PostProcessorCommand::Start) => continue,
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 10 {
                                empty_cycles = 0;
                                let _ = events_tx.send(PostProcessorEvent::EmptyQueue);
                            }
                            sleep(Duration::from_secs(1));
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };

                info!(ctx.expect_logger(), "Processing {} blocks", blocks.len());

                for block in blocks.iter_mut() {
                    let network = match block.metadata.network {
                        BitcoinNetwork::Mainnet => Network::Bitcoin,
                        BitcoinNetwork::Regtest => Network::Regtest,
                        BitcoinNetwork::Testnet => Network::Testnet,
                    };

                    info!(
                        ctx.expect_logger(),
                        "Cleaning transfers from block {}", block.block_identifier.index
                    );
                    let inscriptions = find_all_inscriptions_in_block(
                        &block.block_identifier.index,
                        &inscriptions_db_conn_rw,
                        &ctx,
                    );
                    info!(
                        ctx.expect_logger(),
                        "{} inscriptions retrieved at block {}",
                        inscriptions.len(),
                        block.block_identifier.index
                    );
                    let mut operations = BTreeMap::new();

                    let transaction = inscriptions_db_conn_rw.transaction().unwrap();

                    remove_entries_from_locations_at_block_height(
                        &block.block_identifier.index,
                        &transaction,
                        &ctx,
                    );

                    for (_, entry) in inscriptions.iter() {
                        let inscription_id = entry.get_inscription_id();
                        info!(
                            ctx.expect_logger(),
                            "Processing inscription {}", inscription_id
                        );
                        insert_entry_in_locations(
                            &inscription_id,
                            block.block_identifier.index,
                            &entry.transfer_data,
                            &transaction,
                            &ctx,
                        );

                        operations.insert(
                            entry.transaction_identifier_inscription.clone(),
                            OrdinalInscriptionTransferData {
                                inscription_id: entry.get_inscription_id(),
                                updated_address: None,
                                satpoint_pre_transfer: format_satpoint_to_watch(
                                    &entry.transaction_identifier_inscription,
                                    entry.inscription_input_index,
                                    0,
                                ),
                                satpoint_post_transfer: format_satpoint_to_watch(
                                    &entry.transfer_data.transaction_identifier_location,
                                    entry.transfer_data.output_index,
                                    entry.transfer_data.inscription_offset_intra_output,
                                ),
                                post_transfer_output_value: None,
                                tx_index: 0,
                            },
                        );
                    }

                    info!(
                        ctx.expect_logger(),
                        "Rewriting transfers for block {}", block.block_identifier.index
                    );

                    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
                        tx.metadata.ordinal_operations.clear();
                        if let Some(mut entry) = operations.remove(&tx.transaction_identifier) {
                            let (_, output_index, _) =
                                parse_satpoint_to_watch(&entry.satpoint_post_transfer);

                            let script_pub_key_hex =
                                tx.metadata.outputs[output_index].get_script_pubkey_hex();
                            let updated_address = match Script::from_hex(&script_pub_key_hex) {
                                Ok(script) => {
                                    match Address::from_script(&script, network.clone()) {
                                        Ok(address) => Some(address.to_string()),
                                        Err(_e) => None,
                                    }
                                }
                                Err(_e) => None,
                            };

                            entry.updated_address = updated_address;
                            entry.post_transfer_output_value =
                                Some(tx.metadata.outputs[output_index].value);
                            entry.tx_index = tx_index;
                            tx.metadata
                                .ordinal_operations
                                .push(OrdinalOperation::InscriptionTransferred(entry));
                        }
                    }

                    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data_tx(
                        block,
                        &transaction,
                        &ctx,
                    )
                    .unwrap();

                    info!(
                        ctx.expect_logger(),
                        "Saving supdates for block {}", block.block_identifier.index
                    );
                    transaction.commit().unwrap();

                    info!(
                        ctx.expect_logger(),
                        "Transfers in block {} repaired", block.block_identifier.index
                    );

                    if let Some(ref post_processor) = post_processor {
                        let _ = post_processor.send(block.clone());
                    }
                }
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}
