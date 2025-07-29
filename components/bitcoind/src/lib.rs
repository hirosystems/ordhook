extern crate serde;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

pub use bitcoincore_rpc;
use config::Config;

use crate::{
    block_pool::BlockPool,
    pipeline::{
        block_processor_runloop, download_rpc_blocks, rpc::build_http_client, stream_zmq_blocks,
        wait_for_thread_finish, BlockProcessor, BlockProcessorCommand,
    },
    types::{BitcoinBlockData, BlockIdentifier},
    utils::{
        bitcoind::{bitcoind_get_chain_tip, bitcoind_wait_for_chain_tip},
        future_block_on, Context,
    },
};

pub mod block_pool;
pub mod pipeline;
pub mod types;
pub mod utils;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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
    let block_pool_arc = Arc::new(Mutex::new(BlockPool::new()));
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
