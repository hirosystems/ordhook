pub mod processors;

use chainhook_sdk::observer::BitcoinConfig;
use chainhook_sdk::types::BitcoinBlockData;
use chainhook_sdk::utils::Context;
use crossbeam_channel::bounded;
use std::collections::{HashMap, VecDeque};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::config::Config;
use crate::db::LazyBlock;

use chainhook_sdk::indexer::bitcoin::{
    build_http_client, parse_downloaded_block, try_download_block_bytes_with_retry,
};

use super::protocol::inscription_parsing::parse_ordinals_and_standardize_block;

pub enum PostProcessorCommand {
    Start,
    ProcessBlocks(Vec<(u64, LazyBlock)>, Vec<BitcoinBlockData>),
    Terminate,
}

pub enum PostProcessorEvent {
    Started,
    Terminated,
    Expired,
}

pub struct PostProcessorController {
    pub commands_tx: crossbeam_channel::Sender<PostProcessorCommand>,
    pub events_rx: crossbeam_channel::Receiver<PostProcessorEvent>,
    pub thread_handle: JoinHandle<()>,
}

pub async fn download_and_pipeline_blocks(
    config: &Config,
    start_block: u64,
    end_block: u64,
    start_sequencing_blocks_at_height: u64,
    blocks_post_processor: Option<&PostProcessorController>,
    speed: usize,
    ctx: &Context,
) -> Result<(), String> {
    // let guard = pprof::ProfilerGuardBuilder::default()
    //     .frequency(20)
    //     .blocklist(&["libc", "libgcc", "pthread", "vdso"])
    //     .build()
    //     .unwrap();

    let bitcoin_config = BitcoinConfig {
        username: config.network.bitcoind_rpc_username.clone(),
        password: config.network.bitcoind_rpc_password.clone(),
        rpc_url: config.network.bitcoind_rpc_url.clone(),
        network: config.network.bitcoin_network.clone(),
        bitcoin_block_signaling: config.network.bitcoin_block_signaling.clone(),
    };

    let hord_config = config.get_hord_config();

    let number_of_blocks_to_process = end_block - start_block + 1;

    let (block_compressed_tx, block_compressed_rx) = crossbeam_channel::bounded(speed);
    let http_client = build_http_client();

    let moved_config = bitcoin_config.clone();
    let moved_ctx = ctx.clone();
    let moved_http_client = http_client.clone();

    let mut set = JoinSet::new();

    let mut block_heights = VecDeque::from((start_block..=end_block).collect::<Vec<u64>>());

    for _ in 0..hord_config.ingestion_thread_queue_size {
        if let Some(block_height) = block_heights.pop_front() {
            let config = moved_config.clone();
            let ctx = moved_ctx.clone();
            let http_client = moved_http_client.clone();
            sleep(Duration::from_millis(500));
            set.spawn(try_download_block_bytes_with_retry(
                http_client,
                block_height,
                config,
                ctx,
            ));
        }
    }

    let moved_ctx: Context = ctx.clone();
    let moved_bitcoin_network = bitcoin_config.network.clone();

    let mut tx_thread_pool = vec![];
    let mut rx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];

    for _ in 0..hord_config.ingestion_thread_max {
        let (tx, rx) = bounded::<Option<Vec<u8>>>(hord_config.ingestion_thread_queue_size);
        tx_thread_pool.push(tx);
        rx_thread_pool.push(rx);
    }

    for (thread_index, rx) in rx_thread_pool.into_iter().enumerate() {
        let block_compressed_tx_moved = block_compressed_tx.clone();
        let moved_ctx: Context = moved_ctx.clone();
        let moved_bitcoin_network = moved_bitcoin_network.clone();

        let handle = hiro_system_kit::thread_named("Block data compression")
            .spawn(move || {
                while let Ok(Some(block_bytes)) = rx.recv() {
                    let raw_block_data =
                        parse_downloaded_block(block_bytes).expect("unable to parse block");
                    let compressed_block = LazyBlock::from_full_block(&raw_block_data)
                        .expect("unable to compress block");
                    let block_height = raw_block_data.height as u64;
                    let block_data = if block_height >= start_sequencing_blocks_at_height {
                        let block_data = parse_ordinals_and_standardize_block(
                            raw_block_data,
                            &moved_bitcoin_network,
                            &moved_ctx,
                        )
                        .expect("unable to deserialize block");
                        Some(block_data)
                    } else {
                        None
                    };
                    let _ = block_compressed_tx_moved.send(Some((
                        block_height,
                        block_data,
                        compressed_block,
                    )));
                }
                moved_ctx
                    .try_log(|logger| debug!(logger, "Exiting processing thread {thread_index}"));
            })
            .expect("unable to spawn thread");
        thread_pool_handles.push(handle);
    }

    let cloned_ctx = ctx.clone();

    let blocks_post_processor_commands_tx = blocks_post_processor
        .as_ref()
        .and_then(|p| Some(p.commands_tx.clone()));

    let storage_thread = hiro_system_kit::thread_named("Block processor dispatcher")
        .spawn(move || {
            let mut inbox = HashMap::new();
            let mut inbox_cursor = start_sequencing_blocks_at_height.max(start_block);
            let mut blocks_processed = 0;
            let mut processor_started = false;
            let mut stop_runloop = false;

            loop {
                if stop_runloop {
                    cloned_ctx.try_log(|logger| {
                        info!(
                            logger,
                            "#{blocks_processed} blocks successfully sent to processor"
                        )
                    });
                    break;
                }

                // Dequeue all the blocks available
                let mut new_blocks = vec![];
                while let Ok(message) = block_compressed_rx.try_recv() {
                    match message {
                        Some((block_height, block, compacted_block)) => {
                            blocks_processed += 1;
                            new_blocks.push((block_height, block, compacted_block));
                            // Max batch size: 10_000 blocks
                            if new_blocks.len() >= 10_000 {
                                break;
                            }
                        }
                        None => {
                            stop_runloop = true;
                        }
                    }
                }

                if blocks_processed == number_of_blocks_to_process {
                    stop_runloop = true;
                }

                // Early "continue"
                if new_blocks.is_empty() {
                    sleep(Duration::from_millis(500));
                    continue;
                }

                if let Some(ref blocks_tx) = blocks_post_processor_commands_tx {
                    if !processor_started {
                        processor_started = true;
                        let _ = blocks_tx.send(PostProcessorCommand::Start);
                    }
                }

                let mut ooo_compacted_blocks = vec![];
                for (block_height, block_opt, compacted_block) in new_blocks.into_iter() {
                    if let Some(block) = block_opt {
                        inbox.insert(block_height, (block, compacted_block));
                    } else {
                        ooo_compacted_blocks.push((block_height, compacted_block));
                    }
                }

                // Early "continue"
                if inbox.is_empty() {
                    if let Some(ref blocks_tx) = blocks_post_processor_commands_tx {
                        let _ = blocks_tx.send(PostProcessorCommand::ProcessBlocks(
                            ooo_compacted_blocks,
                            vec![],
                        ));
                    }
                    continue;
                }

                // In order processing: construct the longest sequence of known blocks
                let mut compacted_blocks = vec![];
                let mut blocks = vec![];
                while let Some((block, compacted_block)) = inbox.remove(&inbox_cursor) {
                    compacted_blocks.push((inbox_cursor, compacted_block));
                    blocks.push(block);
                    inbox_cursor += 1;
                }

                if !blocks.is_empty() {
                    if let Some(ref blocks_tx) = blocks_post_processor_commands_tx {
                        let _ = blocks_tx.send(PostProcessorCommand::ProcessBlocks(
                            compacted_blocks,
                            blocks,
                        ));
                    }
                }
            }
            ()
        })
        .expect("unable to spawn thread");

    let mut thread_index = 0;
    while let Some(res) = set.join_next().await {
        let block = res.unwrap().unwrap();

        let _ = tx_thread_pool[thread_index].send(Some(block));
        if let Some(block_height) = block_heights.pop_front() {
            let config = moved_config.clone();
            let ctx = ctx.clone();
            let http_client = moved_http_client.clone();
            set.spawn(try_download_block_bytes_with_retry(
                http_client,
                block_height,
                config,
                ctx,
            ));
        }
        thread_index = (thread_index + 1) % hord_config.ingestion_thread_max;
    }

    ctx.try_log(|logger| {
        debug!(
            logger,
            "Pipeline successfully fed with sequence of blocks ({} to {})", start_block, end_block
        )
    });

    for tx in tx_thread_pool.iter() {
        let _ = tx.send(None);
    }

    ctx.try_log(|logger| debug!(logger, "Enqueued pipeline termination commands"));

    for handle in thread_pool_handles.into_iter() {
        let _ = handle.join();
    }

    ctx.try_log(|logger| debug!(logger, "Pipeline successfully terminated"));

    if let Some(post_processor) = blocks_post_processor {
        if let Ok(PostProcessorEvent::Started) = post_processor.events_rx.recv() {
            ctx.try_log(|logger| debug!(logger, "Block post processing started"));
            let _ = post_processor
                .commands_tx
                .send(PostProcessorCommand::Terminate);
        }
        loop {
            if let Ok(signal) = post_processor.events_rx.recv() {
                match signal {
                    PostProcessorEvent::Terminated | PostProcessorEvent::Expired => break,
                    PostProcessorEvent::Started => unreachable!(),
                }
            }
        }
    }

    let _ = block_compressed_tx.send(None);

    let _ = storage_thread.join();
    let _ = set.shutdown();

    ctx.try_log(|logger| {
        info!(
            logger,
            "Pipeline successfully processed sequence of blocks ({} to {})", start_block, end_block
        )
    });

    // match guard.report().build() {
    //     Ok(report) => {
    //         ctx.try_log(|logger| {
    //             slog::info!(logger, "Generating report");
    //         });
    //         let file = std::fs::File::create("hord-perf.svg").unwrap();
    //         report.flamegraph(file).unwrap();
    //     }
    //     Err(e) => {
    //         ctx.try_log(|logger| {
    //             slog::error!(logger, "Reporting failed: {}", e.to_string());
    //         });
    //     }
    // }

    Ok(())
}
