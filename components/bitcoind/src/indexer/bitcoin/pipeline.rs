use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

use config::Config;
use crossbeam_channel::bounded;
use reqwest::Client;
use tokio::task::JoinSet;

use crate::{
    indexer::{
        bitcoin::{
            cursor::BlockBytesCursor, parse_downloaded_block, standardize_bitcoin_block,
            try_download_block_bytes_with_retry,
        },
        wait_for_thread_finish, BlockProcessor, BlockProcessorCommand,
    },
    try_debug, try_info,
    types::BitcoinNetwork,
    utils::Context,
};

/// Downloads historical blocks from bitcoind's RPC interface and pushes them to a [BlockProcessor] so they can be indexed
/// or ingested as needed.
pub async fn start_block_download_pipeline(
    config: &Config,
    http_client: &Client,
    block_heights: Vec<u64>,
    start_sequencing_blocks_at_height: u64,
    compress_blocks: bool,
    block_processor: &mut BlockProcessor,
    speed: usize,
    abort_signal: &Arc<AtomicBool>,
    ctx: &Context,
) -> Result<(), String> {
    let number_of_blocks_to_process = block_heights.len() as u64;

    let (block_compressed_tx, block_compressed_rx) = crossbeam_channel::bounded(speed);

    let start_block_height = *block_heights.first().expect("no blocks to pipeline");
    let end_block_height = *block_heights.last().expect("no blocks to pipeline");
    let mut block_heights = VecDeque::from(block_heights);

    // All the requests are being processed on the same thread.
    // As soon as we are getting the bytes back from wire, the
    // processing is moved to a thread pool, to defer the parsing, quite expensive.
    // We are initially seeding the networking thread with N requests,
    // with N being the number of threads in the pool handling the response.
    // We need:
    // - 1 thread for the thread handling networking
    // - 1 thread for the thread handling disk serialization
    let thread_pool_network_response_processing_capacity =
        config.resources.get_optimal_thread_pool_capacity();
    // For each worker in that pool, we want to bound the size of the queue to avoid OOM
    // Blocks size can range from 1 to 4Mb (when packed with witness data).
    // Start blocking networking when each worker has a backlog of 8 blocks seems reasonable.
    let worker_queue_size = 2;

    // BitcoinRpc threads.
    // Responsible for downloading the block bytes from bitcoind's RPC interface.
    try_info!(
        ctx,
        "Pipeline spawning {} BitcoinRpc threads",
        config.resources.bitcoind_rpc_threads
    );
    let mut rpc_handles = JoinSet::new();
    let moved_config = config.bitcoind.clone();
    let moved_ctx = ctx.clone();
    let moved_http_client = http_client.clone();
    for _ in 0..config.resources.bitcoind_rpc_threads {
        if let Some(block_height) = block_heights.pop_front() {
            let config = moved_config.clone();
            let ctx = moved_ctx.clone();
            let http_client = moved_http_client.clone();
            // We interleave the initial requests to avoid DDOSing bitcoind from the get go.
            sleep(Duration::from_millis(500));
            rpc_handles.spawn(try_download_block_bytes_with_retry(
                http_client,
                block_height,
                config,
                ctx,
            ));
        }
    }

    // BlockCompressor threads.
    // Responsible for compressing the block bytes received from bitcoind into a compacted and standardized format.
    try_info!(
        ctx,
        "Pipeline spawning {} BlockCompressor threads",
        thread_pool_network_response_processing_capacity
    );
    let moved_ctx: Context = ctx.clone();
    let moved_bitcoin_network = config.bitcoind.network;

    let mut tx_thread_pool = vec![];
    let mut rx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];

    for _ in 0..thread_pool_network_response_processing_capacity {
        let (tx, rx) = bounded::<Option<Vec<u8>>>(worker_queue_size);
        tx_thread_pool.push(tx);
        rx_thread_pool.push(rx);
    }
    for (thread_index, rx) in rx_thread_pool.into_iter().enumerate() {
        let cloned_abort_signal = abort_signal.clone();
        let block_compressed_tx_moved = block_compressed_tx.clone();
        let moved_ctx: Context = moved_ctx.clone();
        let handle = hiro_system_kit::thread_named("BlockCompressor")
            .spawn(move || {
                loop {
                    if cloned_abort_signal.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Ok(Some(block_bytes)) = rx.recv() {
                        let raw_block_data =
                            parse_downloaded_block(block_bytes).expect("unable to parse block");
                        let compressed_block = if compress_blocks {
                            Some(
                                BlockBytesCursor::from_full_block(&raw_block_data)
                                    .expect("unable to compress block"),
                            )
                        } else {
                            None
                        };
                        let block_height = raw_block_data.height as u64;
                        let block_data = if block_height >= start_sequencing_blocks_at_height {
                            let block = standardize_bitcoin_block(
                                raw_block_data,
                                &BitcoinNetwork::from_network(moved_bitcoin_network),
                                &moved_ctx,
                            )
                            .expect("unable to deserialize block");
                            Some(block)
                        } else {
                            None
                        };
                        let _ = block_compressed_tx_moved.send(Some((
                            block_height,
                            block_data,
                            compressed_block,
                        )));
                    }
                }
                try_info!(moved_ctx, "BlockCompressor thread {thread_index} complete");
            })
            .expect("unable to spawn thread");
        thread_pool_handles.push(handle);
    }

    // BlockDispatcher thread.
    // Responsible for dispatching sorted and standardized blocks to the [BlockProcessor] for canonicalization.
    let cloned_ctx = ctx.clone();
    let cloned_abort_signal = abort_signal.clone();
    let block_processor_commands_tx = block_processor.commands_tx.clone();
    let block_dispatcher_thread = hiro_system_kit::thread_named("BlockDispatcher")
        .spawn(move || {
            let mut inbox = HashMap::new();
            let mut inbox_cursor = start_sequencing_blocks_at_height.max(start_block_height);
            let mut blocks_processed = 0;
            let mut stop_runloop = false;

            loop {
                if stop_runloop {
                    try_debug!(
                        cloned_ctx,
                        "Pipeline successfully sent {blocks_processed} blocks to processor"
                    );
                    let _ = block_processor_commands_tx.send(BlockProcessorCommand::Terminate);
                    break;
                }

                // Dequeue all the blocks available
                let mut new_blocks = vec![];
                while let Ok(message) = block_compressed_rx.try_recv() {
                    match message {
                        Some((block_height, block, compacted_block)) => {
                            new_blocks.push((block_height, block, compacted_block));
                            // Max batch size: 10_000 blocks
                            if new_blocks.len() >= 10_000 {
                                break;
                            }
                        }
                        None => {
                            break;
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

                let mut ooo_compacted_blocks = vec![];
                for (block_height, block_opt, compacted_block) in new_blocks.into_iter() {
                    if let Some(block) = block_opt {
                        inbox.insert(block_height, (block, compacted_block));
                    } else if let Some(compacted_block) = compacted_block {
                        ooo_compacted_blocks.push((block_height, compacted_block));
                    }
                }

                // Early "continue"
                if !ooo_compacted_blocks.is_empty() {
                    blocks_processed += ooo_compacted_blocks.len() as u64;
                    let _ =
                        block_processor_commands_tx.send(BlockProcessorCommand::ProcessBlocks {
                            compacted_blocks: ooo_compacted_blocks,
                            blocks: vec![],
                        });
                }

                if inbox.is_empty() {
                    continue;
                }

                // In order processing: construct the longest sequence of known blocks
                let mut compacted_blocks = vec![];
                let mut blocks = vec![];
                while let Some((block, compacted_block)) = inbox.remove(&inbox_cursor) {
                    if let Some(compacted_block) = compacted_block {
                        compacted_blocks.push((inbox_cursor, compacted_block));
                    }
                    blocks.push(block);
                    inbox_cursor += 1;
                }

                blocks_processed += blocks.len() as u64;

                if !blocks.is_empty() {
                    let _ =
                        block_processor_commands_tx.send(BlockProcessorCommand::ProcessBlocks {
                            compacted_blocks,
                            blocks,
                        });
                }

                if inbox_cursor > end_block_height || cloned_abort_signal.load(Ordering::SeqCst) {
                    stop_runloop = true;
                }
            }
            try_info!(cloned_ctx, "BlockDispatcher thread complete");
        })
        .expect("unable to spawn thread");

    // Poll bitcoind RPC thread handles continuously looking for idle threads to send blocks to.
    let mut round_robin_worker_thread_index = 0;
    while let Some(res) = rpc_handles.join_next().await {
        if abort_signal.load(Ordering::SeqCst) {
            break;
        }
        let block = res
            .expect("unable to retrieve block")
            .expect("unable to deserialize block");

        loop {
            if abort_signal.load(Ordering::SeqCst) {
                break;
            }
            let res = tx_thread_pool[round_robin_worker_thread_index].send(Some(block.clone()));
            round_robin_worker_thread_index = (round_robin_worker_thread_index + 1)
                % thread_pool_network_response_processing_capacity;
            if res.is_ok() {
                break;
            }
            sleep(Duration::from_millis(500));
        }

        if let Some(block_height) = block_heights.pop_front() {
            let config = moved_config.clone();
            let ctx = ctx.clone();
            let http_client = moved_http_client.clone();
            rpc_handles.spawn(try_download_block_bytes_with_retry(
                http_client,
                block_height,
                config,
                ctx,
            ));
        }
    }

    for tx in tx_thread_pool.iter() {
        let _ = tx.send(None);
    }

    try_debug!(ctx, "Enqueued pipeline termination commands");

    for handle in thread_pool_handles.into_iter() {
        let _ = handle.join();
    }

    try_debug!(ctx, "Pipeline successfully terminated");

    wait_for_thread_finish(&mut block_processor.thread_handle)?;

    let _ = block_compressed_tx.send(None);

    let _ = block_dispatcher_thread.join();
    let _ = rpc_handles.shutdown().await;

    try_debug!(
        ctx,
        "Pipeline successfully processed sequence of blocks ({} to {})",
        start_block_height,
        end_block_height
    );

    Ok(())
}
