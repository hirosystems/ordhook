pub mod bitcoin;
pub mod chain_segment;
pub mod fork_scratch_pad;

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

use bitcoin::{
    build_http_client, download_and_parse_block_with_retry,
    pipeline::start_block_download_pipeline, standardize_bitcoin_block,
};
use config::Config;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use reqwest::Client;

use self::fork_scratch_pad::ForkScratchPad;
use crate::{
    observer::zmq::start_zeromq_pipeline,
    try_debug, try_info,
    types::{BitcoinBlockData, BitcoinNetwork, BlockIdentifier, BlockchainEvent},
    utils::{
        bitcoind::{bitcoind_get_chain_tip, bitcoind_wait_for_chain_tip},
        future_block_on, AbstractBlock, BlockHeights, Context,
    },
};

/// Commands that can be sent to the block processor.
pub enum BlockProcessorCommand {
    ProcessBlocks {
        compacted_blocks: Vec<(u64, Vec<u8>)>,
        blocks: Vec<BitcoinBlockData>,
    },
    Terminate,
}

/// Object that will receive any blocks as they come from bitcoind. These messages do not track any canonical chain alterations.
pub struct BlockProcessor {
    /// Sender for emitting block processor commands.
    pub commands_tx: crossbeam_channel::Sender<BlockProcessorCommand>,
    /// Handle for the block processor thread.
    pub thread_handle: Option<JoinHandle<()>>,
}

/// Commands that can be sent to the indexer.
pub enum IndexerCommand {
    /// Store compacted blocks.
    StoreCompactedBlocks(Vec<(u64, Vec<u8>)>),
    /// Index standardized blocks into the indexer's database.
    IndexBlocks {
        apply_blocks: Vec<BitcoinBlockData>,
        rollback_block_ids: Vec<BlockIdentifier>,
    },
    /// Terminate the indexer gracefully.
    Terminate,
}

/// Object that will receive standardized blocks ready to be indexer or rolled back. Blocks can come from historical downloads or
/// recent block streams.
pub struct Indexer {
    /// Sender for emitting indexer commands.
    pub commands_tx: crossbeam_channel::Sender<IndexerCommand>,
    /// Current index chain tip at launch time.
    pub chain_tip: Option<BlockIdentifier>,
    /// Handle for the indexer thread.
    pub thread_handle: Option<JoinHandle<()>>,
}

/// Helper function to send indexer commands with fullness logging.
fn send_indexer_command(
    tx: &crossbeam_channel::Sender<IndexerCommand>,
    mut cmd: IndexerCommand,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let mut logged = false;
    loop {
        match tx.try_send(cmd) {
            Ok(()) => break,
            Err(TrySendError::Full(returned_cmd)) => {
                cmd = returned_cmd;
                if !logged {
                    try_debug!(
                        ctx,
                        "Indexer command channel full, waiting for space (capacity: {})",
                        config.resources.indexer_channel_capacity
                    );
                    logged = true;
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(TrySendError::Disconnected(_)) => {
                if abort_signal.load(Ordering::SeqCst) {
                    return Ok(());
                }
                return Err("Indexer command channel disconnected".into());
            }
        }
    }
    Ok(())
}

/// Joins a thread handle and returns an error if the thread panics.
fn wait_for_thread_finish(thread_handle: &mut Option<JoinHandle<()>>) -> Result<(), String> {
    thread_handle
        .take()
        .unwrap()
        .join()
        .map_err(|e| format!("Failed to join thread: {:?}", e))
}

/// Moves our block pool with a newly received standardized block
async fn advance_block_pool(
    block: BitcoinBlockData,
    block_pool: &Arc<Mutex<ForkScratchPad>>,
    block_store: &Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    http_client: &Client,
    indexer_commands_tx: &Sender<IndexerCommand>,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    if abort_signal.load(Ordering::SeqCst) {
        return Ok(());
    }
    let network = BitcoinNetwork::from_network(config.bitcoind.network);
    let mut block_ids = VecDeque::new();
    block_ids.push_front(block.block_identifier.clone());

    let block_pool_ref = block_pool.clone();
    let block_store_ref = block_store.clone();

    // Keep incoming block before sending.
    {
        let mut block_store_guard = block_store_ref.lock().unwrap();
        block_store_guard.insert(block.block_identifier.clone(), block);
    }

    while let Some(block_id) = block_ids.pop_front() {
        let (header, canonical) = {
            let mut pool_guard = block_pool_ref.lock().unwrap();
            let mut block_store_guard = block_store_ref.lock().unwrap();
            let block = block_store_guard.get(&block_id).unwrap();
            let header = block.get_header();
            if pool_guard.can_process_header(&header) {
                match pool_guard.process_header(header.clone(), ctx)? {
                    Some(event) => match event {
                        BlockchainEvent::BlockchainUpdatedWithHeaders(event) => {
                            let mut apply_blocks = vec![];
                            for header in event.new_headers.iter() {
                                apply_blocks.push(
                                    block_store_guard.remove(&header.block_identifier).unwrap(),
                                );
                            }
                            send_indexer_command(
                                indexer_commands_tx,
                                IndexerCommand::IndexBlocks {
                                    apply_blocks,
                                    rollback_block_ids: vec![],
                                },
                                abort_signal,
                                config,
                                ctx,
                            )?;
                            (header, true)
                        }
                        BlockchainEvent::BlockchainUpdatedWithReorg(event) => {
                            let mut apply_blocks = vec![];
                            for header in event.headers_to_apply.iter() {
                                apply_blocks.push(
                                    block_store_guard.remove(&header.block_identifier).unwrap(),
                                );
                            }
                            let rollback_block_ids: Vec<BlockIdentifier> = event
                                .headers_to_rollback
                                .iter()
                                .map(|h| h.block_identifier.clone())
                                .collect();
                            send_indexer_command(
                                indexer_commands_tx,
                                IndexerCommand::IndexBlocks {
                                    apply_blocks,
                                    rollback_block_ids,
                                },
                                abort_signal,
                                config,
                                ctx,
                            )?;
                            (header, true)
                        }
                    },
                    None => return Err("Unable to append block".into()),
                }
            } else {
                try_info!(
                    ctx,
                    "Received non-canonical block {}",
                    header.block_identifier
                );
                (header, false)
            }
        };
        if !canonical {
            let parent_block = {
                // Handle a behaviour specific to ZMQ usage in bitcoind.
                // Considering a simple re-org:
                // A (1) - B1 (2) - C1 (3)
                //       \ B2 (4) - C2 (5) - D2 (6)
                // When D2 is being discovered (making A -> B2 -> C2 -> D2 the new canonical fork)
                // it looks like ZMQ is only publishing D2.
                // Without additional operation, we end up with a block that we can't append.
                let parent_block_hash = header
                    .parent_block_identifier
                    .get_hash_bytes_str()
                    .to_string();
                // try_info!(
                //     ctx,
                //     "zmq: Re-org detected, retrieving parent block {parent_block_hash}"
                // );
                let parent_block = download_and_parse_block_with_retry(
                    http_client,
                    &parent_block_hash,
                    &config.bitcoind,
                    ctx,
                )
                .await?;
                standardize_bitcoin_block(parent_block, &network, ctx).map_err(|(e, _)| e)?
            };
            // Keep parent block and repeat the cycle
            {
                let mut block_store_guard = block_store_ref.lock().unwrap();
                block_store_guard
                    .insert(parent_block.block_identifier.clone(), parent_block.clone());
            }
            block_ids.push_front(block_id);
            block_ids.push_front(parent_block.block_identifier.clone());
        }
    }
    Ok(())
}

/// Initialize our block pool with the current index's last seen block, so we can detect any re-orgs or gaps that may come our
/// way with the next blocks.
async fn initialize_block_pool(
    block_pool: &Arc<Mutex<ForkScratchPad>>,
    index_chain_tip: &BlockIdentifier,
    http_client: &Client,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let last_block = download_and_parse_block_with_retry(
        http_client,
        index_chain_tip.get_hash_bytes_str(),
        &config.bitcoind,
        ctx,
    )
    .await?;
    let block_pool_ref = block_pool.clone();
    let mut pool = block_pool_ref.lock().unwrap();
    match pool.process_header(last_block.get_block_header(), ctx) {
        Ok(_) => {
            try_debug!(
                ctx,
                "Primed fork processor with last seen block hash {index_chain_tip}"
            );
        }
        Err(e) => return Err(format!("Unable to load last seen block: {e}")),
    }
    Ok(())
}

/// Runloop designed to receive Bitcoin blocks through a [BlockProcessor] and send them to a [ForkScratchPad] so it can advance
/// the canonical chain.
async fn block_processor_runloop(
    indexer_commands_tx: &Sender<IndexerCommand>,
    index_chain_tip: &Option<BlockIdentifier>,
    block_commands_rx: &Receiver<BlockProcessorCommand>,
    block_pool: &Arc<Mutex<ForkScratchPad>>,
    block_store: &Arc<Mutex<HashMap<BlockIdentifier, BitcoinBlockData>>>,
    http_client: &Client,
    sequence_start_block_height: u64,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // Before starting the loop, check if the index already has progress. If so, prime the block pool with the current tip.
    if let Some(index_chain_tip) = index_chain_tip {
        if index_chain_tip.index >= sequence_start_block_height {
            initialize_block_pool(block_pool, index_chain_tip, http_client, config, ctx).await?;
        }
    }

    loop {
        if abort_signal.load(Ordering::SeqCst) {
            return Ok(());
        }
        let (compacted_blocks, blocks) = match block_commands_rx.recv() {
            Ok(BlockProcessorCommand::ProcessBlocks {
                compacted_blocks,
                blocks,
            }) => (compacted_blocks, blocks),
            Ok(BlockProcessorCommand::Terminate) => {
                try_info!(ctx, "BlockProcessor received Terminate command");
                return Ok(());
            }
            Err(e) => return Err(format!("block ingestion runloop error: {e}")),
        };

        if !compacted_blocks.is_empty() {
            send_indexer_command(
                indexer_commands_tx,
                IndexerCommand::StoreCompactedBlocks(compacted_blocks),
                abort_signal,
                config,
                ctx,
            )?;
        }
        for block in blocks.into_iter() {
            if abort_signal.load(Ordering::SeqCst) {
                return Ok(());
            }
            advance_block_pool(
                block,
                block_pool,
                block_store,
                http_client,
                indexer_commands_tx,
                abort_signal,
                config,
                ctx,
            )
            .await?;
        }
    }
}

/// Starts a bitcoind RPC block download pipeline that will send us all historical bitcoin blocks in a parallel fashion. We will
/// then stream these blocks into our block pool so they can be fed into the configured [Indexer]. This will eventually bring the
/// index chain tip to `target_block_height`.
async fn download_rpc_blocks(
    indexer: &Indexer,
    block_processor: &mut BlockProcessor,
    block_pool: &Arc<Mutex<ForkScratchPad>>,
    http_client: &Client,
    target_block_height: u64,
    sequence_start_block_height: u64,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let blocks = {
        let block_pool_ref = block_pool.clone();
        let pool = block_pool_ref.lock().unwrap();
        let chain_tip = pool.canonical_chain_tip().or(indexer.chain_tip.as_ref());
        let start_block = chain_tip.map_or(0, |ct| ct.index + 1);
        BlockHeights::BlockRange(start_block, target_block_height)
            .get_sorted_entries()
            .map_err(|_e| "Block start / end block spec invalid".to_string())?
    };
    try_debug!(
        ctx,
        "Downloading blocks from #{} to #{}",
        blocks.front().unwrap(),
        blocks.back().unwrap()
    );
    start_block_download_pipeline(
        config,
        http_client,
        blocks.into(),
        sequence_start_block_height,
        compress_blocks,
        block_processor,
        abort_signal,
        ctx,
    )
    .await
}

/// Streams all upcoming blocks from bitcoind through its ZeroMQ interface and pipes them onto the [Indexer] once processed
/// through our block pool. This process will run indefinitely and will make sure our index keeps advancing as new Bitcoin blocks
/// get mined.
async fn stream_zmq_blocks(
    block_processor: &mut BlockProcessor,
    sequence_start_block_height: u64,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    start_zeromq_pipeline(
        block_processor,
        sequence_start_block_height,
        compress_blocks,
        abort_signal,
        config,
        ctx,
    )
    .await
}

/// Starts a Bitcoin block indexer pipeline.
pub async fn start_bitcoin_indexer(
    indexer: &mut Indexer,
    sequence_start_block_height: u64,
    stream_blocks_at_chain_tip: bool,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let mut bitcoind_chain_tip = bitcoind_wait_for_chain_tip(&config.bitcoind, ctx);
    let http_client = build_http_client();

    // Block pool that will track the canonical chain and detect any reorgs that may happen.
    let block_pool_arc = Arc::new(Mutex::new(ForkScratchPad::new()));
    let block_pool = block_pool_arc.clone();
    // Block cache that will keep block data in memory while it is prepared to be sent to indexers.
    let block_store_arc = Arc::new(Mutex::new(HashMap::new()));

    if let Some(index_chain_tip) = &indexer.chain_tip {
        try_info!(ctx, "Index chain tip is at {}", index_chain_tip);
    } else {
        try_info!(ctx, "Index is empty");
    }

    // Build the [BlockProcessor] that will be used to ingest and standardize blocks from bitcoind. This processor will then send
    // blocks to the [Indexer] for indexing.
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<BlockProcessorCommand>(
        config.resources.indexer_channel_capacity,
    );
    let ctx_moved = ctx.clone();
    let config_moved = config.clone();
    let block_pool_moved = block_pool.clone();
    let block_store_moved = block_store_arc.clone();
    let http_client_moved = http_client.clone();
    let indexer_commands_tx_moved = indexer.commands_tx.clone();
    let index_chain_tip_moved = indexer.chain_tip.clone();
    let abort_signal_moved = abort_signal.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("block_download_processor")
        .spawn(move || {
            future_block_on(&ctx_moved.clone(), async move {
                block_processor_runloop(
                    &indexer_commands_tx_moved,
                    &index_chain_tip_moved,
                    &commands_rx,
                    &block_pool_moved,
                    &block_store_moved,
                    &http_client_moved,
                    sequence_start_block_height,
                    &abort_signal_moved,
                    &config_moved,
                    &ctx_moved,
                )
                .await
            });
        })
        .expect("unable to spawn thread");
    let mut block_processor = BlockProcessor {
        commands_tx,
        thread_handle: Some(handle),
    };

    // Sync index from bitcoin RPC until chain tip is reached.
    loop {
        if abort_signal.load(Ordering::SeqCst) {
            break;
        }
        {
            let pool = block_pool.lock().unwrap();
            let chain_tip = pool.canonical_chain_tip().or(indexer.chain_tip.as_ref());
            if let Some(chain_tip) = chain_tip {
                if bitcoind_chain_tip == *chain_tip {
                    try_info!(
                        ctx,
                        "Index has reached bitcoind chain tip at {bitcoind_chain_tip}"
                    );
                    break;
                }
            }
        }
        download_rpc_blocks(
            indexer,
            &mut block_processor,
            &block_pool_arc,
            &http_client,
            bitcoind_chain_tip.index,
            sequence_start_block_height,
            compress_blocks,
            abort_signal,
            config,
            ctx,
        )
        .await?;
        // Bitcoind may have advanced while we were indexing, check its chain tip again.
        bitcoind_chain_tip = bitcoind_get_chain_tip(&config.bitcoind, ctx);
    }

    // Stream new incoming blocks from bitcoind's ZeroMQ interface.
    if stream_blocks_at_chain_tip && !abort_signal.load(Ordering::SeqCst) {
        stream_zmq_blocks(
            &mut block_processor,
            sequence_start_block_height,
            compress_blocks,
            abort_signal,
            config,
            ctx,
        )
        .await?;
    }

    // Send a terminate command to the indexer and wait for it to finish. Absorb the error here in case the indexer is already
    // terminated from the abort signal.
    let _ = indexer.commands_tx.send(IndexerCommand::Terminate);
    wait_for_thread_finish(&mut indexer.thread_handle)?;

    Ok(())
}

#[cfg(test)]
pub mod tests;
