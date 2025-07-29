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
    pipeline::{
        rpc::{
            parse_downloaded_block, standardize_bitcoin_block, try_download_block_bytes_with_retry,
        },
        wait_for_thread_finish, BlockProcessor, BlockProcessorCommand,
    },
    try_debug, try_info,
    types::{BitcoinNetwork, BlockBytesCursor},
    utils::Context,
};

/// Downloads historical blocks from bitcoind's RPC interface and pushes them to a [BlockProcessor] so they can be indexed
/// or ingested as needed.
pub(crate) async fn start_block_download_pipeline(
    config: &Config,
    rpc_client: &Client,
    block_heights: Vec<u64>,
    start_sequencing_blocks_at_height: u64,
    compress_blocks: bool,
    block_processor: &mut BlockProcessor,
    abort_signal: &Arc<AtomicBool>,
    ctx: &Context,
) -> Result<(), String> {
    let number_of_blocks_to_process = block_heights.len() as u64;
    let start_block_height = *block_heights.first().expect("no blocks to pipeline");
    let end_block_height = *block_heights.last().expect("no blocks to pipeline");
    let mut block_heights = VecDeque::from(block_heights);

    let channel_capacity = config.resources.indexer_channel_capacity;
    let block_compressor_thread_count = config.resources.get_optimal_thread_pool_capacity();
    let rpc_thread_count = config.resources.bitcoind_rpc_threads;

    // BlockCompressor threads
    // ------------------------------------------------------------------------------------------------
    // Responsible for compressing the block bytes received from bitcoind into a compact and standardized format. As soon as we
    // get bytes back from wire, processing is moved to this thread pool to defer parsing.
    try_info!(
        ctx,
        "Pipeline spawning {} BlockCompressor threads",
        block_compressor_thread_count
    );
    // Create the channel that will be used to send parsed blocks to the BlockDispatcher thread for sorting.
    let (block_dispatcher_tx, block_dispatcher_rx) = crossbeam_channel::bounded(channel_capacity);

    let mut compressor_tx_pool = Vec::with_capacity(block_compressor_thread_count);
    let mut compressor_rx_pool = Vec::with_capacity(block_compressor_thread_count);
    let mut compressor_handles = Vec::with_capacity(block_compressor_thread_count);
    for _ in 0..block_compressor_thread_count {
        let (tx, rx) = bounded::<Option<Vec<u8>>>(channel_capacity);
        compressor_tx_pool.push(tx);
        compressor_rx_pool.push(rx);
    }

    let moved_ctx: Context = ctx.clone();
    let moved_bitcoin_network = config.bitcoind.network;
    for (thread_index, rx) in compressor_rx_pool.into_iter().enumerate() {
        let cloned_abort_signal = abort_signal.clone();
        let block_dispatcher_tx_moved = block_dispatcher_tx.clone();
        let moved_ctx: Context = moved_ctx.clone();
        let handle = hiro_system_kit::thread_named(&format!("BlockCompressor[{thread_index}]"))
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
                        let _ = block_dispatcher_tx_moved.send(Some((
                            block_height,
                            block_data,
                            compressed_block,
                        )));
                    }
                }
                try_info!(moved_ctx, "BlockCompressor[{thread_index}] thread complete");
            })
            .expect("unable to spawn thread");
        compressor_handles.push(handle);
    }

    // BlockDispatcher thread
    // ------------------------------------------------------------------------------------------------
    // Responsible for sending sorted and standardized blocks to the [BlockProcessor] for canonicalization. Blocks must be sent in
    // order so the [BlockProcessor] can follow along the canonical chain.
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
                while let Ok(message) = block_dispatcher_rx.try_recv() {
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

    // BitcoinRpc threads
    // ------------------------------------------------------------------------------------------------
    // Responsible for downloading block bytes from bitcoind's RPC interface in parallel. The number of threads is determined by
    // the `bitcoind_rpc_threads` configuration option.
    try_info!(
        ctx,
        "Pipeline spawning {} BitcoinRpc threads",
        rpc_thread_count
    );
    let mut rpc_handles = JoinSet::new();
    for _ in 0..rpc_thread_count {
        if let Some(block_height) = block_heights.pop_front() {
            let config = config.bitcoind.clone();
            let ctx = ctx.clone();
            let rpc_client = rpc_client.clone();
            // We interleave the initial requests to avoid DDOSing bitcoind from the get go.
            sleep(Duration::from_millis(500));
            rpc_handles.spawn(try_download_block_bytes_with_retry(
                rpc_client,
                block_height,
                config,
                ctx,
            ));
        }
    }
    // As soon as we receive block bytes from bitcoind's RPC interface via any of the BitcoinRpc threads, we send them to the
    // BlockCompressor thread pool and download the next block.
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
            let res = compressor_tx_pool[round_robin_worker_thread_index].send(Some(block.clone()));
            round_robin_worker_thread_index =
                (round_robin_worker_thread_index + 1) % block_compressor_thread_count;
            if res.is_ok() {
                break;
            }
            sleep(Duration::from_millis(500));
        }

        if let Some(block_height) = block_heights.pop_front() {
            let config = config.bitcoind.clone();
            let ctx = ctx.clone();
            let rpc_client = rpc_client.clone();
            rpc_handles.spawn(try_download_block_bytes_with_retry(
                rpc_client,
                block_height,
                config,
                ctx,
            ));
        }
    }

    for tx in compressor_tx_pool.iter() {
        let _ = tx.send(None);
    }

    try_debug!(ctx, "Enqueued pipeline termination commands");

    for handle in compressor_handles.into_iter() {
        let _ = handle.join();
    }

    try_debug!(ctx, "Pipeline successfully terminated");

    wait_for_thread_finish(&mut block_processor.thread_handle)?;

    let _ = block_dispatcher_tx.send(None);

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
