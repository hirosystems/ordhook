use std::{sync::Arc, thread::JoinHandle};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};

use crate::{
    config::Config,
    db::{
        insert_entry_in_blocks, open_readwrite_hord_db_conn, open_readwrite_hord_db_conn_rocks_db,
        process_blocks, InscriptionHeigthHint, LazyBlock,
    },
};

use super::new_traversals_lazy_cache;

pub fn start_ordinals_number_processor(
    config: &Config,
    ctx: &Context,
) -> (
    crossbeam_channel::Sender<Vec<(BitcoinBlockData, LazyBlock)>>,
    JoinHandle<()>,
) {
    // let mut batches = HashMap::new();
    let hord_config = config.get_hord_config();

    let (tx, rx) = crossbeam_channel::bounded::<Vec<(BitcoinBlockData, LazyBlock)>>(
        hord_config.ingestion_thread_max,
    );
    // let (inner_tx, inner_rx) = channel();

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

            while let Ok(raw_blocks) = rx.recv() {
                info!(
                    ctx.expect_logger(),
                    "Processing {} blocks",
                    raw_blocks.len()
                );

                let mut blocks = vec![];
                for (block, compacted_block) in raw_blocks.into_iter() {
                    insert_entry_in_blocks(
                        block.block_identifier.index as u32,
                        &compacted_block,
                        &blocks_db_rw,
                        &ctx,
                    );

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

                let detailed_ctx = if config.logs.ordinals_computation {
                    ctx.clone()
                } else {
                    Context::empty()
                };

                process_blocks(
                    blocks,
                    &cache_l2,
                    &mut inscription_height_hint,
                    &mut inscriptions_db_conn_rw,
                    &hord_config,
                    &detailed_ctx,
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

    (tx, handle)
}
