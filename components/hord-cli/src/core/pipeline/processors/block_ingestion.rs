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
    db::{insert_entry_in_blocks, open_readwrite_hord_db_conn_rocks_db, LazyBlock},
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
            let blocks_db_rw =
                open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx).unwrap();
            let mut empty_cycles = 0;

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
                store_compacted_blocks(compacted_blocks, &blocks_db_rw, &ctx);
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
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    compacted_blocks.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (block_height, compacted_block) in compacted_blocks.into_iter() {
        insert_entry_in_blocks(block_height as u32, &compacted_block, &blocks_db_rw, &ctx);
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
