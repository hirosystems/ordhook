use std::{
    collections::{BTreeMap, HashMap},
    hash::BuildHasherDefault,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bitcoind::{
    indexer::bitcoin::cursor::TransactionBytesCursor,
    try_info, try_warn,
    types::{BitcoinBlockData, TransactionIdentifier},
    utils::Context,
};
use config::Config;
use dashmap::DashMap;
use fxhash::FxHasher;
use postgres::{pg_begin, pg_pool_client};

use crate::{
    core::{
        meta_protocols::brc20::{
            brc20_pg, cache::Brc20MemoryCache, index::index_block_and_insert_brc20_operations,
        },
        protocol::{
            inscription_parsing::parse_inscriptions_in_standardized_block,
            inscription_sequencing::{
                get_bitcoin_network, get_jubilee_block_height,
                parallelize_inscription_data_computations,
                update_block_inscriptions_with_consensus_sequence_data,
            },
            satoshi_numbering::TraversalResult,
            satoshi_tracking::augment_block_with_transfers,
            sequence_cursor::SequenceCursor,
        },
    },
    db::ordinals_pg::{self, get_chain_tip_block_height},
    utils::monitoring::PrometheusMonitoring,
    PgConnectionPools,
};

pub async fn process_blocks(
    next_blocks: &mut Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    brc20_cache: &mut Option<Brc20MemoryCache>,
    prometheus: &PrometheusMonitoring,
    config: &Config,
    pg_pools: &PgConnectionPools,
    ctx: &Context,
    abort_signal: &Arc<AtomicBool>,
) -> Result<Vec<BitcoinBlockData>, String> {
    let mut cache_l1 = BTreeMap::new();
    let mut updated_blocks = vec![];

    for _cursor in 0..next_blocks.len() {
        if abort_signal.load(Ordering::SeqCst) {
            break;
        }
        let mut block = next_blocks.remove(0);

        index_block(
            &mut block,
            next_blocks,
            sequence_cursor,
            &mut cache_l1,
            cache_l2,
            brc20_cache.as_mut(),
            prometheus,
            config,
            pg_pools,
            ctx,
        )
        .await?;

        updated_blocks.push(block);
    }

    Ok(updated_blocks)
}

pub async fn index_block(
    block: &mut BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    brc20_cache: Option<&mut Brc20MemoryCache>,
    prometheus: &PrometheusMonitoring,
    config: &Config,
    pg_pools: &PgConnectionPools,
    ctx: &Context,
) -> Result<(), String> {
    let stopwatch = std::time::Instant::now();
    let block_height = block.block_identifier.index;
    try_info!(
        ctx,
        "Starting inscription indexing for block #{block_height}..."
    );
    // Count total reveals and transfers in the block
    let mut reveals_count = 0;
    let mut transfers_count = 0;

    // Invalidate and recompute cursor when crossing the jubilee height
    if block.block_identifier.index
        == get_jubilee_block_height(&get_bitcoin_network(&block.metadata.network))
    {
        sequence_cursor.reset();
    }

    {
        let mut ord_client = pg_pool_client(&pg_pools.ordinals).await?;
        let ord_tx = pg_begin(&mut ord_client).await?;

        if let Some(chain_tip) = get_chain_tip_block_height(&ord_tx).await? {
            if block_height <= chain_tip {
                try_warn!(ctx, "Block #{block_height} was already indexed, skipping");
                return Ok(());
            }
        }

        // Parsed BRC20 ops will be deposited here for this block.
        let mut brc20_operation_map = HashMap::new();

        // Measure inscription parsing time
        let parsing_start = std::time::Instant::now();
        parse_inscriptions_in_standardized_block(block, &mut brc20_operation_map, config, ctx);
        prometheus
            .metrics_record_inscription_parsing_time(parsing_start.elapsed().as_millis() as f64);

        // Measure ordinal computation time
        let computation_start = std::time::Instant::now();
        let has_inscription_reveals = match parallelize_inscription_data_computations(
            block,
            next_blocks,
            cache_l1,
            cache_l2,
            config,
            ctx,
        ) {
            Ok(result) => result,
            Err(e) => {
                return Err(format!("Failed to compute inscription data: {}", e));
            }
        };
        if has_inscription_reveals {
            if let Err(e) = update_block_inscriptions_with_consensus_sequence_data(
                block,
                sequence_cursor,
                cache_l1,
                &ord_tx,
                ctx,
            )
            .await
            {
                return Err(format!("Failed to update block inscriptions: {}", e));
            }
        }
        prometheus.metrics_record_inscription_computation_time(
            computation_start.elapsed().as_millis() as f64,
        );

        if let Err(e) = augment_block_with_transfers(
            block,
            &ord_tx,
            ctx,
            &mut reveals_count,
            &mut transfers_count,
        )
        .await
        {
            return Err(format!("Failed to augment block with transfers: {}", e));
        }

        // Count inscriptions revealed in this block
        prometheus.metrics_record_inscriptions_per_block(reveals_count as u64);

        // Measure database write time
        let inscription_db_write_start = std::time::Instant::now();
        // Write data
        if let Err(e) = ordinals_pg::insert_block(block, &ord_tx).await {
            return Err(format!("Failed to insert block: {}", e));
        }
        prometheus.metrics_record_inscription_db_write_time(
            inscription_db_write_start.elapsed().as_millis() as f64,
        );

        // BRC-20
        if let (Some(brc20_cache), Some(brc20_pool)) = (brc20_cache, &pg_pools.brc20) {
            let mut brc20_client = pg_pool_client(brc20_pool).await?;
            let brc20_tx = pg_begin(&mut brc20_client).await?;

            // Count BRC-20 operations before processing
            let brc20_ops_count = brc20_operation_map.len() as u64;
            prometheus.metrics_record_brc20_operations_per_block(brc20_ops_count);

            if let Err(e) = index_block_and_insert_brc20_operations(
                block,
                &mut brc20_operation_map,
                brc20_cache,
                &brc20_tx,
                ctx,
                prometheus,
            )
            .await
            {
                return Err(format!("Failed to process BRC-20 operations: {}", e));
            }

            if let Err(e) = brc20_tx.commit().await {
                return Err(format!("unable to commit brc20 pg transaction: {}", e));
            }
        }

        prometheus.metrics_block_indexed(block_height);
        prometheus.metrics_inscription_indexed(
            ordinals_pg::get_highest_inscription_number(&ord_tx)
                .await?
                .unwrap_or(0) as u64,
        );
        prometheus.metrics_classic_blessed_inscription_indexed(
            ordinals_pg::get_blessed_count_from_counts_by_type(&ord_tx)
                .await?
                .unwrap_or(0) as u64,
        );
        prometheus.metrics_classic_cursed_inscription_indexed(
            ordinals_pg::get_cursed_count_from_counts_by_type(&ord_tx)
                .await?
                .unwrap_or(0) as u64,
        );

        if let Err(e) = ord_tx.commit().await {
            return Err(format!("unable to commit ordinals pg transaction: {}", e));
        }
    }

    // Record overall processing time
    let elapsed = stopwatch.elapsed();
    prometheus.metrics_record_block_processing_time(elapsed.as_millis() as f64);
    try_info!(
        ctx,
        "Completed inscription indexing for block #{block_height}: found {reveals_count} inscription reveals and {transfers_count} inscription transfers in {elapsed:.0}s",
        elapsed = elapsed.as_secs_f32(),
    );

    Ok(())
}

pub async fn rollback_block(
    block_height: u64,
    _config: &Config,
    pg_pools: &PgConnectionPools,
    ctx: &Context,
) -> Result<(), String> {
    try_info!(ctx, "Rolling back block #{block_height}");
    {
        let mut ord_client = pg_pool_client(&pg_pools.ordinals).await?;
        let ord_tx = pg_begin(&mut ord_client).await?;

        ordinals_pg::rollback_block(block_height, &ord_tx).await?;

        // BRC-20
        if let Some(brc20_pool) = &pg_pools.brc20 {
            let mut brc20_client = pg_pool_client(brc20_pool).await?;
            let brc20_tx = pg_begin(&mut brc20_client).await?;

            brc20_pg::rollback_block_operations(block_height, &brc20_tx).await?;

            brc20_tx
                .commit()
                .await
                .map_err(|e| format!("unable to commit brc20 pg transaction: {e}"))?;
            try_info!(
                ctx,
                "Rolled back BRC-20 operations at block #{block_height}"
            );
        }

        ord_tx
            .commit()
            .await
            .map_err(|e| format!("unable to commit ordinals pg transaction: {e}"))?;
        try_info!(
            ctx,
            "Rolled back inscription activity at block #{block_height}"
        );
    }
    Ok(())
}
