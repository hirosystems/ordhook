use std::{
    sync::{mpsc::Sender, Arc},
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::TryRecvError;

use crate::{
    config::Config,
    core::{
        new_traversals_lazy_cache,
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
        protocol::sequencing::process_blocks,
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
                    blocks,
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
