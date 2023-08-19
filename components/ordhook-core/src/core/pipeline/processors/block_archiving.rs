use std::{
    sync::mpsc::Sender,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::TryRecvError;
use rocksdb::DB;

use crate::{
    config::Config,
    core::pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    db::{
        insert_entry_in_blocks, open_ordhook_db_conn_rocks_db_loop,
        open_readwrite_ordhook_db_conn_rocks_db, LazyBlock,
    },
};

pub fn start_block_archiving_processor(
    config: &Config,
    ctx: &Context,
    update_tip: bool,
    _post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Processor Runloop")
        .spawn(move || {
            let mut blocks_db_rw =
                open_readwrite_ordhook_db_conn_rocks_db(&config.expected_cache_path(), &ctx)
                    .unwrap();
            let mut empty_cycles = 0;
            let mut processed_blocks = 0;

            if let Ok(PostProcessorCommand::Start) = commands_rx.recv() {
                let _ = events_tx.send(PostProcessorEvent::Started);
                debug!(ctx.expect_logger(), "Start block indexing runloop");
            }

            loop {
                debug!(ctx.expect_logger(), "Tick");
                let (compacted_blocks, _) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        (compacted_blocks, blocks)
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        debug!(ctx.expect_logger(), "Terminating block processor");
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Ok(PostProcessorCommand::Start) => unreachable!(),
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 30 {
                                warn!(ctx.expect_logger(), "Block processor reached expiration");
                                let _ = events_tx.send(PostProcessorEvent::Expired);
                                break;
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
                processed_blocks += compacted_blocks.len();
                store_compacted_blocks(compacted_blocks, update_tip, &blocks_db_rw, &ctx);

                if processed_blocks % 10_000 == 0 {
                    let _ = blocks_db_rw.flush_wal(true);
                    blocks_db_rw = open_ordhook_db_conn_rocks_db_loop(
                        true,
                        &config.expected_cache_path(),
                        &ctx,
                    );
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

pub fn store_compacted_blocks(
    mut compacted_blocks: Vec<(u64, LazyBlock)>,
    update_tip: bool,
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    compacted_blocks.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (block_height, compacted_block) in compacted_blocks.into_iter() {
        insert_entry_in_blocks(
            block_height as u32,
            &compacted_block,
            update_tip,
            &blocks_db_rw,
            &ctx,
        );
        ctx.try_log(|logger| {
            info!(logger, "Block #{block_height} saved to disk");
        });
    }

    if let Err(e) = blocks_db_rw.flush() {
        ctx.try_log(|logger| {
            error!(logger, "{}", e.to_string());
        });
    }
}
