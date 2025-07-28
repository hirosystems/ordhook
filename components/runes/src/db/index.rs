use std::collections::HashMap;

use bitcoin::{
    absolute::LockTime,
    transaction::{TxOut, Version},
    Amount, Network, ScriptBuf, Transaction,
};
use bitcoind::{
    try_info,
    types::{BitcoinBlockData, BitcoinTransactionData},
    utils::Context,
};
use deadpool_postgres::Client;
use ordinals_parser::{Artifact, Runestone};
use postgres::pg_begin;

use super::{cache::index_cache::IndexCache, pg_get_max_rune_number, pg_roll_back_block};
use crate::{
    db::cache::transaction_location::TransactionLocation, utils::monitoring::PrometheusMonitoring,
};

pub fn get_rune_genesis_block_height(network: Network) -> u64 {
    match network {
        Network::Bitcoin => 840_000,
        Network::Testnet => todo!(),
        Network::Signet => todo!(),
        Network::Regtest => todo!(),
        _ => todo!(),
    }
}

/// Transforms a Bitcoin transaction from a Chainhook format to a rust bitcoin crate format so it can be parsed by the ord crate
/// to look for `Artifact`s. Also, takes all non-OP_RETURN outputs and returns them so they can be used later to receive runes.
fn bitcoin_tx_from_chainhook_tx(
    block: &BitcoinBlockData,
    tx: &BitcoinTransactionData,
) -> (Transaction, HashMap<u32, ScriptBuf>, Option<u32>, u32) {
    let mut outputs = Vec::with_capacity(tx.metadata.outputs.len());
    let mut eligible_outputs = HashMap::new();
    let mut first_eligible_output: Option<u32> = None;
    for (i, output) in tx.metadata.outputs.iter().enumerate() {
        let script = ScriptBuf::from_bytes(output.get_script_pubkey_bytes());
        if !script.is_op_return() {
            eligible_outputs.insert(i as u32, script.clone());
            if first_eligible_output.is_none() {
                first_eligible_output = Some(i as u32);
            }
        }
        outputs.push(TxOut {
            value: Amount::from_sat(output.value),
            script_pubkey: script,
        });
    }
    (
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::from_time(block.timestamp).unwrap(),
            // Inputs don't matter for Runestone parsing.
            input: vec![],
            output: outputs,
        },
        eligible_outputs,
        first_eligible_output,
        tx.metadata.outputs.len() as u32,
    )
}

/// Index a Bitcoin block for runes data.
pub async fn index_block(
    pg_client: &mut Client,
    index_cache: &mut IndexCache,
    block: &mut BitcoinBlockData,
    prometheus: &PrometheusMonitoring,
    ctx: &Context,
) {
    let stopwatch = std::time::Instant::now();
    let block_hash = &block.block_identifier.hash;
    let block_height = block.block_identifier.index;
    try_info!(ctx, "RunesIndexer indexing block #{block_height}...");

    // Track operation counts
    let mut etching_count: u64 = 0;
    let mut mint_count: u64 = 0;
    let mut edict_count: u64 = 0;
    let mut cenotaph_etching_count: u64 = 0;
    let mut cenotaph_mint_count: u64 = 0;
    let mut cenotaph_count: u64 = 0;

    let mut db_tx = pg_begin(pg_client).await.unwrap();
    index_cache.reset_max_rune_number(&mut db_tx).await;

    // Measure parsing time
    let parsing_start = std::time::Instant::now();

    for tx in block.transactions.iter() {
        let (transaction, eligible_outputs, first_eligible_output, total_outputs) =
            bitcoin_tx_from_chainhook_tx(block, tx);
        let tx_index = tx.metadata.index;
        let tx_id = &tx.transaction_identifier.hash;
        let location = TransactionLocation {
            network: index_cache.network,
            block_hash: block_hash.clone(),
            block_height,
            tx_index,
            tx_id: tx_id.clone(),
            timestamp: block.timestamp,
        };
        index_cache
            .begin_transaction(
                location,
                &tx.metadata.inputs,
                eligible_outputs,
                first_eligible_output,
                total_outputs,
                &mut db_tx,
                ctx,
            )
            .await;
        if let Some(artifact) = Runestone::decipher(&transaction) {
            match artifact {
                Artifact::Runestone(runestone) => {
                    index_cache
                        .apply_runestone(&runestone, &mut db_tx, ctx)
                        .await;
                    if let Some(etching) = runestone.etching {
                        index_cache
                            .apply_etching(&etching, &mut db_tx, ctx, &mut etching_count)
                            .await;
                    }
                    if let Some(mint_rune_id) = runestone.mint {
                        index_cache
                            .apply_mint(&mint_rune_id, &mut db_tx, ctx, &mut mint_count)
                            .await;
                    }
                    for edict in runestone.edicts.iter() {
                        index_cache
                            .apply_edict(edict, &mut db_tx, ctx, &mut edict_count)
                            .await;
                    }
                }
                Artifact::Cenotaph(cenotaph) => {
                    index_cache
                        .apply_cenotaph(&cenotaph, &mut db_tx, ctx, &mut cenotaph_count)
                        .await;
                    if let Some(etching) = cenotaph.etching {
                        index_cache
                            .apply_cenotaph_etching(
                                &etching,
                                &mut db_tx,
                                ctx,
                                &mut cenotaph_etching_count,
                            )
                            .await;
                    }
                    if let Some(mint_rune_id) = cenotaph.mint {
                        index_cache
                            .apply_cenotaph_mint(
                                &mint_rune_id,
                                &mut db_tx,
                                ctx,
                                &mut cenotaph_mint_count,
                            )
                            .await;
                    }
                }
            }
        }
        index_cache.end_transaction(&mut db_tx, ctx);
    }
    prometheus.metrics_record_rune_parsing_time(parsing_start.elapsed().as_millis() as f64);

    // Measure computation time
    let computation_start = std::time::Instant::now();
    index_cache.end_block();
    prometheus.metrics_record_rune_computation_time(computation_start.elapsed().as_millis() as f64);

    // Measure database write time
    let rune_db_write_start = std::time::Instant::now();
    index_cache.db_cache.flush(&mut db_tx, ctx).await;
    db_tx
        .commit()
        .await
        .expect("Unable to commit pg transaction");
    prometheus.metrics_record_rune_db_write_time(rune_db_write_start.elapsed().as_millis() as f64);

    prometheus.metrics_record_runes_etching_per_block(etching_count);
    prometheus.metrics_record_runes_edict_per_block(edict_count);
    prometheus.metrics_record_runes_mint_per_block(mint_count);
    prometheus.metrics_record_runes_cenotaph_per_block(cenotaph_count);
    prometheus.metrics_record_runes_cenotaph_etching_per_block(cenotaph_etching_count);
    prometheus.metrics_record_runes_cenotaph_mint_per_block(cenotaph_mint_count);

    // Record metrics
    prometheus.metrics_block_indexed(block_height);
    let current_rune_number = pg_get_max_rune_number(pg_client).await;
    prometheus.metrics_rune_indexed(current_rune_number as u64);
    prometheus.metrics_record_runes_per_block(etching_count);

    // Record overall processing time
    let elapsed = stopwatch.elapsed();
    prometheus.metrics_record_block_processing_time(elapsed.as_millis() as f64);
    try_info!(
        ctx,
        "RunesIndexer indexed block #{block_height}: {etching_count} etchings, {mint_count} mints, {edict_count} edicts, {cenotaph_count} cenotaphs ({cenotaph_etching_count} etchings, {cenotaph_mint_count} mints) in {}s",
        elapsed.as_secs_f32()
    );
}

/// Roll back a Bitcoin block because of a re-org.
pub async fn roll_back_block(pg_client: &mut Client, block_height: u64, ctx: &Context) {
    let stopwatch = std::time::Instant::now();
    try_info!(ctx, "Rolling back block {block_height}...");
    let mut db_tx = pg_client
        .transaction()
        .await
        .expect("Unable to begin block roll back pg transaction");
    pg_roll_back_block(block_height, &mut db_tx, ctx).await;
    db_tx
        .commit()
        .await
        .expect("Unable to commit pg transaction");
    try_info!(
        ctx,
        "Block {block_height} rolled back in {elapsed:.4}s",
        elapsed = stopwatch.elapsed().as_secs_f32()
    );
}
