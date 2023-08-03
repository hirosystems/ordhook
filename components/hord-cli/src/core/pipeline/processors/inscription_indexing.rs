use std::{
    sync::{mpsc::Sender, Arc},
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{hashes::hex::FromHex, Address, Network, Script},
    types::{
        BitcoinBlockData, BitcoinNetwork, OrdinalInscriptionCurseType, OrdinalOperation,
        TransactionIdentifier, OrdinalInscriptionTransferData,
    },
    utils::Context,
};
use crossbeam_channel::TryRecvError;

use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::Connection;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use crate::{
    core::{protocol::sequencing::update_hord_db_and_augment_bitcoin_block_v3, HordConfig},
    db::{find_all_inscriptions_in_block, find_all_transfers_in_block, format_satpoint_to_watch},
};

use crate::db::{LazyBlockTransaction, TraversalResult};

use crate::{
    config::Config,
    core::{
        new_traversals_lazy_cache,
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    },
    db::{
        insert_entry_in_blocks, open_readwrite_hord_db_conn, open_readwrite_hord_db_conn_rocks_db,
        InscriptionHeigthHint,
    },
};

pub fn start_inscription_indexing_processor(
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
            let cache_l2 = Arc::new(new_traversals_lazy_cache(1024));
            let garbage_collect_every_n_blocks = 100;
            let mut garbage_collect_nth_block = 0;

            let mut inscriptions_db_conn_rw =
                open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let hord_config = config.get_hord_config();
            let mut num_writes = 0;
            let mut total_writes = 0;
            let blocks_db_rw =
                open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx).unwrap();

            let mut inscription_height_hint = InscriptionHeigthHint::new();
            let mut empty_cycles = 0;

            if let Ok(PostProcessorCommand::Start) = commands_rx.recv() {
                info!(ctx.expect_logger(), "Start inscription indexing runloop");
            }

            loop {
                let (compacted_blocks, mut blocks) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        empty_cycles = 0;
                        (compacted_blocks, blocks)
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

                for (block_height, compacted_block) in compacted_blocks.into_iter() {
                    insert_entry_in_blocks(
                        block_height as u32,
                        &compacted_block,
                        &blocks_db_rw,
                        &ctx,
                    );
                    num_writes += 1;
                    total_writes += 1;
                }
                info!(ctx.expect_logger(), "{total_writes} blocks saved to disk");

                // Early return
                if blocks.is_empty() {
                    if num_writes % 128 == 0 {
                        ctx.try_log(|logger| {
                            info!(logger, "Flushing DB to disk ({num_writes} inserts)");
                        });
                        if let Err(e) = blocks_db_rw.flush() {
                            ctx.try_log(|logger| {
                                error!(logger, "{}", e.to_string());
                            });
                        }
                        num_writes = 0;
                    }
                    continue;
                }

                info!(ctx.expect_logger(), "Processing {} blocks", blocks.len());

                // Write blocks to disk, before traversals
                if let Err(e) = blocks_db_rw.flush() {
                    ctx.try_log(|logger| {
                        error!(logger, "{}", e.to_string());
                    });
                }
                garbage_collect_nth_block += blocks.len();

                process_blocks(
                    &mut blocks,
                    &cache_l2,
                    &mut inscription_height_hint,
                    &mut inscriptions_db_conn_rw,
                    &hord_config,
                    &post_processor,
                    &ctx,
                );

                // Clear L2 cache on a regular basis
                if garbage_collect_nth_block > garbage_collect_every_n_blocks {
                    info!(
                        ctx.expect_logger(),
                        "Clearing cache L2 ({} entries)",
                        cache_l2.len()
                    );
                    cache_l2.clear();
                    garbage_collect_nth_block = 0;
                }
            }

            if let Err(e) = blocks_db_rw.flush() {
                ctx.try_log(|logger| {
                    error!(logger, "{}", e.to_string());
                });
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}

pub fn process_blocks(
    next_blocks: &mut Vec<BitcoinBlockData>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscription_height_hint: &mut InscriptionHeigthHint,
    inscriptions_db_conn_rw: &mut Connection,
    hord_config: &HordConfig,
    post_processor: &Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) -> Vec<BitcoinBlockData> {
    let mut cache_l1 = HashMap::new();
    let mut updated_blocks = vec![];
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
            let _ = post_processor_tx.send(block.clone());
        }
        updated_blocks.push(block);
    }
    updated_blocks
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

pub fn re_augment_block_with_ordinals_operations(
    block: &mut BitcoinBlockData,
    inscriptions_db_conn: &Connection,
    ctx: &Context,
) {
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };

    // Restore inscriptions data
    let mut inscriptions =
        find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_conn, ctx);
    
    let mut should_become_cursed = vec![];
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        for (op_index, operation) in tx.metadata.ordinal_operations.iter_mut().enumerate() {
            let (inscription, is_cursed) = match operation {
                OrdinalOperation::CursedInscriptionRevealed(ref mut inscription) => {
                    (inscription, true)
                }
                OrdinalOperation::InscriptionRevealed(ref mut inscription) => (inscription, false),
                OrdinalOperation::InscriptionTransferred(_) => continue,
            };

            let Some(traversal) = inscriptions.remove(&(tx.transaction_identifier.clone(), inscription.inscription_input_index)) else {
                continue;
            };

            inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
            inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
            inscription.ordinal_number = traversal.ordinal_number;
            inscription.inscription_number = traversal.inscription_number;
            inscription.transfers_pre_inscription = traversal.transfers;
            inscription.inscription_fee = tx.metadata.fee;
            inscription.tx_index = tx_index;
            inscription.satpoint_post_inscription = format_satpoint_to_watch(
                &traversal.transfer_data.transaction_identifier_location,
                traversal.transfer_data.output_index,
                traversal.transfer_data.inscription_offset_intra_output,
            );

            let Some(output) = tx.metadata.outputs.get(traversal.transfer_data.output_index) else {
                continue;
            };
            inscription.inscription_output_value = output.value;
            inscription.inscriber_address = {
                let script_pub_key = output.get_script_pubkey_hex();
                match Script::from_hex(&script_pub_key) {
                    Ok(script) => match Address::from_script(&script, network.clone()) {
                        Ok(a) => Some(a.to_string()),
                        _ => None,
                    },
                    _ => None,
                }
            };

            if !is_cursed && inscription.inscription_number < 0 {
                inscription.curse_type = Some(OrdinalInscriptionCurseType::Reinscription);
                should_become_cursed.push((tx_index, op_index));
            }
        }
    }

    for (tx_index, op_index) in should_become_cursed.into_iter() {
        let Some(tx) = block.transactions.get_mut(tx_index) else {
            continue;
        };
        let OrdinalOperation::InscriptionRevealed(inscription) = tx.metadata.ordinal_operations.remove(op_index) else {
            continue;
        };
        tx.metadata.ordinal_operations.insert(op_index, OrdinalOperation::CursedInscriptionRevealed(inscription));
    }

    // TODO: Handle transfers


}
