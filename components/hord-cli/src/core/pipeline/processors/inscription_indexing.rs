use std::{
    sync::{mpsc::Sender, Arc},
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{
    types::{BitcoinBlockData, TransactionIdentifier},
    utils::Context,
};
use crossbeam_channel::TryRecvError;

use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::Connection;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use crate::core::{protocol::sequencing::update_hord_db_and_augment_bitcoin_block_v3, HordConfig};

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
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Batch receiver")
        .spawn(move || {
            let cache_l2 = Arc::new(new_traversals_lazy_cache(1024));
            let garbage_collect_every_n_blocks = 100;
            let mut garbage_collect_nth_block = 0;

            let mut inscriptions_db_conn_rw =
                open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let hord_config = config.get_hord_config();
            let mut num_writes = 0;

            let blocks_db_rw =
                open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx).unwrap();

            let mut inscription_height_hint = InscriptionHeigthHint::new();
            let mut empty_cycles = 0;

            loop {
                let blocks_to_process = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(blocks)) => blocks,
                    Ok(PostProcessorCommand::Terminate) => break,
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;

                            if empty_cycles == 30 {
                                let _ = events_tx.send(PostProcessorEvent::EmptyQueue);
                            }
                            sleep(Duration::from_secs(1));
                            if empty_cycles > 120 {
                                break;
                            }
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };

                info!(
                    ctx.expect_logger(),
                    "Processing {} blocks",
                    blocks_to_process.len()
                );

                let mut blocks = vec![];
                for (block, compacted_block) in blocks_to_process.into_iter() {
                    insert_entry_in_blocks(
                        block.block_identifier.index as u32,
                        &compacted_block,
                        &blocks_db_rw,
                        &ctx,
                    );
                    num_writes += 1;

                    if block.block_identifier.index >= hord_config.first_inscription_height {
                        blocks.push(block);
                    }
                }

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
