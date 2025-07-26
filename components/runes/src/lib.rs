use std::thread::JoinHandle;

use bitcoind::{
    indexer::{start_bitcoin_indexer, Indexer, IndexerCommand},
    try_error, try_info, try_warn,
    types::BlockIdentifier,
    utils::{future_block_on, Context},
};
use config::Config;
use db::{
    cache::index_cache::IndexCache,
    index::{get_rune_genesis_block_height, index_block, roll_back_block},
    pg_connect,
};
use utils::monitoring::{start_serving_prometheus_metrics, PrometheusMonitoring};

extern crate serde;

pub mod db;
pub mod utils;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

async fn new_runes_indexer_runloop(
    prometheus: &PrometheusMonitoring,
    config: &Config,
    ctx: &Context,
) -> Result<Indexer, String> {
    let (commands_tx, commands_rx) =
        crossbeam_channel::bounded(config.resources.indexer_channel_capacity);

    let config_moved = config.clone();
    let ctx_moved = ctx.clone();
    let prometheus_moved = prometheus.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("runes_indexer")
        .spawn(move || {
            future_block_on(&ctx_moved.clone(), async move {
                #[cfg(feature = "dhat-heap")]
                let _profiler = dhat::Profiler::new_heap();

                let mut index_cache = IndexCache::new(
                    &config_moved,
                    &mut pg_connect(&config_moved, false, &ctx_moved).await,
                    &ctx_moved,
                )
                .await;
                loop {
                    match commands_rx.recv() {
                        Ok(command) => match command {
                            IndexerCommand::StoreCompactedBlocks(_) => {
                                // No-op. The Runes indexer has no need for compacted blocks.
                                try_warn!(
                                    ctx_moved,
                                    "Runes indexer received unexpected StoreCompactedBlocks command"
                                );
                            }
                            IndexerCommand::IndexBlocks {
                                mut apply_blocks,
                                rollback_block_ids,
                            } => {
                                let mut pg_client =
                                    pg_connect(&config_moved, false, &ctx_moved).await;
                                for block_id in rollback_block_ids.iter() {
                                    roll_back_block(&mut pg_client, block_id.index, &ctx_moved)
                                        .await;
                                }
                                for block in apply_blocks.iter_mut() {
                                    index_block(
                                        &mut pg_client,
                                        &mut index_cache,
                                        block,
                                        &prometheus_moved,
                                        &ctx_moved,
                                    )
                                    .await;
                                }
                            }
                            IndexerCommand::Terminate => {
                                try_info!(ctx_moved, "Runes indexer received Terminate command");
                                return Ok(());
                            }
                        },
                        Err(error) => {
                            try_error!(
                                ctx_moved,
                                "Runes indexer received invalid command: {}",
                                error
                            );
                            return Err(error.to_string());
                        }
                    }
                }
            });
        })
        .expect("unable to spawn thread");

    let mut pg_client = pg_connect(config, false, ctx).await;
    let chain_tip = db::get_chain_tip(&mut pg_client, ctx)
        .await
        .unwrap_or(BlockIdentifier {
            index: get_rune_genesis_block_height(config.bitcoind.network) - 1,
            hash: "0x0000000000000000000000000000000000000000000000000000000000000000".into(),
        });
    Ok(Indexer {
        commands_tx,
        chain_tip: Some(chain_tip),
        thread_handle: Some(handle),
    })
}

pub async fn get_chain_tip(config: &Config, ctx: &Context) -> Result<BlockIdentifier, String> {
    let mut pg_client = pg_connect(config, false, ctx).await;
    Ok(db::get_chain_tip(&mut pg_client, ctx).await.unwrap())
}

pub async fn rollback_block_range(
    start_block: u64,
    end_block: u64,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let mut pg_client = pg_connect(config, false, ctx).await;
    for block_id in start_block..=end_block {
        roll_back_block(&mut pg_client, block_id, ctx).await;
    }
    Ok(())
}

/// Starts the runes indexing process. Will block the main thread indefinitely until explicitly stopped or it reaches chain tip
/// and `stream_blocks_at_chain_tip` is set to false.
pub async fn start_runes_indexer(
    stream_blocks_at_chain_tip: bool,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    pg_connect(config, true, ctx).await;
    let prometheus = PrometheusMonitoring::new();
    let mut indexer = new_runes_indexer_runloop(&prometheus, config, ctx).await?;

    if let Some(metrics) = &config.metrics {
        if metrics.enabled {
            let registry_moved = prometheus.registry.clone();
            let ctx_cloned = ctx.clone();
            let port = metrics.prometheus_port;
            let _ = std::thread::spawn(move || {
                hiro_system_kit::nestable_block_on(start_serving_prometheus_metrics(
                    port,
                    registry_moved,
                    ctx_cloned,
                ));
            });
        }
    }

    // Initialize metrics with current state
    let mut pg_client = pg_connect(config, false, ctx).await;
    let max_rune_number = db::pg_get_max_rune_number(&pg_client, ctx).await;
    let chain_tip = db::get_chain_tip(&mut pg_client, ctx)
        .await
        .unwrap_or(BlockIdentifier {
            index: get_rune_genesis_block_height(config.bitcoind.network) - 1,
            hash: "0x0000000000000000000000000000000000000000000000000000000000000000".into(),
        });
    prometheus
        .initialize(max_rune_number as u64, chain_tip.index, config, ctx)
        .await?;

    start_bitcoin_indexer(
        &mut indexer,
        get_rune_genesis_block_height(config.bitcoind.network),
        stream_blocks_at_chain_tip,
        false,
        config,
        ctx,
    )
    .await
}
