use std::{
    sync::mpsc::Sender,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::TryRecvError;

use crate::{
    config::Config,
    core::pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    db::{insert_entry_in_blocks, open_readwrite_hord_db_conn_rocks_db},
};

pub fn start_block_ingestion_processor(
    config: &Config,
    ctx: &Context,
    _post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Processor Runloop")
        .spawn(move || {
            let mut num_writes = 0;
            let blocks_db_rw =
                open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx).unwrap();

            let mut empty_cycles = 0;
            let mut tip: u64 = 0;

            if let Ok(PostProcessorCommand::Start) = commands_rx.recv() {
                info!(ctx.expect_logger(), "Start block indexing runloop");
            }


            loop {
                let (compacted_blocks, _) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        (compacted_blocks, blocks)
                    }
                    Ok(PostProcessorCommand::Terminate) => break,
                    Ok(PostProcessorCommand::Start) => continue,
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

                let batch_size = compacted_blocks.len();
                num_writes += batch_size;
                for (block_height, compacted_block) in compacted_blocks.into_iter() {
                    tip = tip.max(block_height);
                    insert_entry_in_blocks(
                        block_height as u32,
                        &compacted_block,
                        &blocks_db_rw,
                        &ctx,
                    );
                }
                info!(ctx.expect_logger(), "{batch_size} blocks saved to disk (total: {tip})");

                // Early return
                if num_writes >= 512 {
                    ctx.try_log(|logger| {
                        info!(logger, "Flushing DB to disk ({num_writes} inserts)");
                    });
                    if let Err(e) = blocks_db_rw.flush() {
                        ctx.try_log(|logger| {
                            error!(logger, "{}", e.to_string());
                        });
                    }
                    num_writes = 0;
                    continue;
                }

                // Write blocks to disk, before traversals
                if let Err(e) = blocks_db_rw.flush() {
                    ctx.try_log(|logger| {
                        error!(logger, "{}", e.to_string());
                    });
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
