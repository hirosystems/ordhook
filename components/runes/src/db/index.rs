use std::{collections::HashMap, str::FromStr};

use bitcoin::{
    absolute::LockTime,
    blockdata::witness::Witness,
    transaction::{OutPoint, Sequence, TxIn, TxOut, Version},
    Amount, Network, ScriptBuf, Transaction, Txid,
};
use bitcoind::{
    try_info,
    types::{BitcoinBlockData, BitcoinTransactionData},
    utils::Context,
};
use deadpool_postgres::Client;
use hex;
use ordinals_parser::{Artifact, Flaw, Runestone};
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
    let mut inputs = Vec::with_capacity(tx.metadata.inputs.len());
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
    for input in tx.metadata.inputs.iter() {
        // Convert hex strings to bytes for witness data
        let witness_bytes: Vec<Vec<u8>> = input
            .witness
            .iter()
            .map(|hex_str| {
                // Remove "0x" prefix and decode hex string to bytes
                let clean_hex = hex_str.strip_prefix("0x").unwrap_or(hex_str.as_str());
                hex::decode(clean_hex).unwrap_or_default()
            })
            .collect();

        inputs.push(TxIn {
            previous_output: OutPoint {
                txid: Txid::from_str(&input.previous_output.txid.hash[2..]).unwrap(),
                vout: input.previous_output.vout,
            },
            script_sig: ScriptBuf::new(), // Unused
            sequence: Sequence(input.sequence),
            witness: Witness::from_slice(&witness_bytes),
        });
    }
    (
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::from_time(block.timestamp).unwrap(),
            input: inputs,
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
) -> Result<(), String> {
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
    let mut inputs_count: u64 = 0;

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
                            .apply_etching(
                                &etching,
                                &mut db_tx,
                                ctx,
                                &mut etching_count,
                                &transaction,
                                &mut inputs_count,
                            )
                            .await?;
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

                    if cenotaph.flaw != Some(Flaw::Varint) {
                        if let Some(etching) = cenotaph.etching {
                            index_cache
                                .apply_cenotaph_etching(
                                    &etching,
                                    &mut db_tx,
                                    ctx,
                                    &mut cenotaph_etching_count,
                                    &transaction,
                                    &mut inputs_count,
                                )
                                .await?;
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
    prometheus.metrics_record_runes_etching_inputs_checked_per_block(inputs_count);
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

    Ok(())
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

#[cfg(test)]
mod tests {
    use bitcoind::types::{
        bitcoin::{OutPoint as RosettaOutPoint, TxIn as RosettaTxIn, TxOut as RosettaTxOut},
        BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BitcoinTransactionData,
        BitcoinTransactionMetadata, BlockIdentifier, TransactionIdentifier,
    };
    use ordinals_parser::Artifact;

    use super::*;

    fn build_block(block_height: u64, block_hash_hex: &str, timestamp: u32) -> BitcoinBlockData {
        BitcoinBlockData {
            block_identifier: BlockIdentifier {
                hash: format!("0x{}", block_hash_hex.to_lowercase()),
                index: block_height,
            },
            parent_block_identifier: BlockIdentifier {
                hash: "0x0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                index: block_height - 1,
            },
            timestamp,
            transactions: vec![],
            metadata: BitcoinBlockMetadata {
                network: BitcoinNetwork::Mainnet,
            },
        }
    }

    fn build_valid_tx() -> BitcoinTransactionData {
        // txid: 3a11c5bc4eee38645934607ba63e0d7ac834d399e53c7c06a0ced093a711f1a2
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0x1cf46d1a3192e5cdcce62441a7a40691ed4f7e34dc97dd3bfc7f96ff2069846e"
                    .to_string(),
            },
            vout: 0,
            value: 351_058,
            block_height: 840_000,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 5,
            witness: vec![
                "77109e8db640d44dd2528f192e9b58754a1178813550e0805aad4b3cbf8c079783f8bf21b2c59ee5713573b6e4f52f3546eac35367a21a0b37d66428d4d14a01".to_string(),
                "203c7466c6b06e844514e7cfaee750cc3e30b97d0875ebc02e483218c2e0a23e57ac0063036f72645d0927f379cb8bcb678201010118746578742f706c61696e3b636861727365743d7574662d38000e4b4554414d494e454c4f54494f4e68".to_string(),
                "c03c7466c6b06e844514e7cfaee750cc3e30b97d0875ebc02e483218c2e0a23e57".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - p2tr
                script_pubkey:
                    "0x5120f1e73bbd97fd0eac833e781abdbea9c223951aede9e2275ac6c03e8c1b24394b"
                        .to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 1 - p2tr
                script_pubkey:
                    "0x5120f1e73bbd97fd0eac833e781abdbea9c223951aede9e2275ac6c03e8c1b24394b"
                        .to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - OP_RETURN runestone
                script_pubkey:
                    "0x6a5d22020704a7e6e7dbbcf1f2b38203010003800105bded070690c8020a6408aed3191601"
                        .to_string(),
                value: 0,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x3a11c5bc4eee38645934607ba63e0d7ac834d399e53c7c06a0ced093a711f1a2"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 349_966,
                index: 0,
            },
        }
    }

    fn build_invalid_tx() -> BitcoinTransactionData {
        // txid: 66d084fe5e206c7183293d1e379caa2011e7750018c65dfd2fd3174ea9f298fc
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0x871fb5e4042dca1549326da5848bd2257d6e609984a0cfb867e4ff24a56806d0"
                    .to_string(),
            },
            vout: 0,
            value: 300_000,
            block_height: 840_000,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 4_294_967_293,
            witness: vec![
                "558699007abc57d8b87d8ba02a553f08d5b90758a0985fb5c237521dbb0d00e2c66250926de035235078a78aabeb496e087cdfc7553afb43d546fdd9d718dc7c".to_string(),
                "20ab1fce37e4777d107690082ec5ee6213a2f008ed26602b8c23e49de911ba8b0dac0063074f9a9045bfc47c68".to_string(),
                "c15976bfaf05d5b1cfbb8927abe2c93cf293edd2cdd408c2ba1ce484eb5621e980".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - OP_RETURN runestone
                script_pubkey: "0x6a5d1b020304cfb4c2acf497b13e0380068094ebdc030a8094ebdc030801"
                    .to_string(),
                value: 0,
            },
            RosettaTxOut {
                // vout 1 - p2wpkh
                script_pubkey: "0x0014f32b49757996ef8db8d3d029b3dc997560e77d12".to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - p2wpkh
                script_pubkey: "0x001459966e46ce78b8bf4a54827b84144c82ea21811c".to_string(),
                value: 15_765,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x66d084fe5e206c7183293d1e379caa2011e7750018c65dfd2fd3174ea9f298fc"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 283_689,
                index: 1,
            },
        }
    }

    fn build_no_commit_tx() -> BitcoinTransactionData {
        // txid: 27bbb1f7776d3ac5f41c17a5cde59b4feba5e9f5c5e76f19559c787a7c771055
        let prevout0 = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0x99060f5739abacbe53f568723d829cbef21b65d555950cfacf85baf78fc3724d"
                    .to_string(),
            },
            vout: 0,
            value: 66_600,
            block_height: 840_000,
        };
        let prevout1 = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0x99060f5739abacbe53f568723d829cbef21b65d555950cfacf85baf78fc3724d"
                    .to_string(),
            },
            vout: 1,
            value: 1_000_410,
            block_height: 840_000,
        };

        let inputs = vec![
            RosettaTxIn {
                previous_output: prevout0,
                script_sig: "".to_string(),
                sequence: 4_294_967_293,
                witness: vec![
                    "553d08ee034c7a84b790fb51e05ddffdc07b5ed5fd77774de888190cda69e1b14598194a40a6c6cba669c9e56eb2e95d5f0103a1ce8ee6ed3805a6f9b17fd216".to_string(),
                ],
            },
            RosettaTxIn {
                previous_output: prevout1,
                script_sig: "".to_string(),
                sequence: 4_294_967_293,
                witness: vec![
                    "a22561a3a789bd2fdadd5e962d6d402bebe4fe8096a1eb619cba5c8a36eebd00879dd13f698a2b8a9aa19478f6699c300880747082ef8730d4dbadf61631241c".to_string(),
                ],
            },
        ];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - p2tr
                script_pubkey:
                    "0x5120f08e113b9485778ef245f751b5fea5ab38fbe93f6a5991eadf90426a8911570b"
                        .to_string(),
                value: 419_010,
            },
            RosettaTxOut {
                // vout 1 - OP_RETURN runestone
                script_pubkey:
                    "0x6a5d230207049efdc0b9d7cbf3cc1903800105ae4c06d8b9310ab20508b451106012c01f1601"
                        .to_string(),
                value: 0,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x27bbb1f7776d3ac5f41c17a5cde59b4feba5e9f5c5e76f19559c787a7c771055"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 648_000,
                index: 2,
            },
        }
    }

    fn build_rune_wrong_flaw_tx() -> BitcoinTransactionData {
        // txid: 8bf9d4ec8ed69ae7bac256285b06ae6566cec4679dcfb45b5671a323c2f18c3c
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0xbd546ac9aa0e275a7f06a960a54db0a9c0de634ad71805cb2d10418b3befc8e8"
                    .to_string(),
            },
            vout: 0,
            value: 437_000,
            block_height: 840_020,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 4_294_967_293,
            witness: vec![
                "059b4e426c21d39a438c0f047c3c724f3bfb69ecfcbc223746cd03a35cdfaf7ea5e4266b6b14153620f8493a4a52a8fef4a9487b86fd1c5d2afca2e93b32573f".to_string(),
                "200ffc1a61f24b4b064ce03abebdd8de737d1afb5172831315982a3463e37528e9ac00630925d6f33e24da21141068".to_string(),
                "c1481061324ba36899de17e82ce812d32aa9fd94dd9381afc4fcb7d486b09d2228".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - OP_RETURN runestone
                script_pubkey:
                    "0x6a5d20020304a5accff7c3c4f6909420010003a00405b84106a096800ae8070888a401"
                        .to_string(),
                value: 0,
            },
            RosettaTxOut {
                // vout 1 - p2tr
                script_pubkey:
                    "0x51202b35c0d80bce33fe2036479ab82a4ea60dd760fe2310a433427cc4199da59b8a"
                        .to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - p2wpkh
                script_pubkey: "0x00143d71d44fc31fec24a6e3c1e7955e9d388c5ef312".to_string(),
                value: 22_454,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x8bf9d4ec8ed69ae7bac256285b06ae6566cec4679dcfb45b5671a323c2f18c3c"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 414_000,
                index: 3,
            },
        }
    }

    fn build_internetgold_tx() -> BitcoinTransactionData {
        // txid: 66d084fe5e206c7183293d1e379caa2011e7750018c65dfd2fd3174ea9f298fc
        // This is the INTERNETGOLD transaction with truncated LEB128 field
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0x871fb5e4042dca1549326da5848bd2257d6e609984a0cfb867e4ff24a56806d0"
                    .to_string(),
            },
            vout: 0,
            value: 300_000,
            block_height: 840_020,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 4_294_967_293,
            witness: vec![
                "558699007abc57d8b87d8ba02a553f08d5b90758a0985fb5c237521dbb0d00e2c66250926de035235078a78aabeb496e087cdfc7553afb43d546fdd9d718dc7c".to_string(),
                "20ab1fce37e4777d107690082ec5ee6213a2f008ed26602b8c23e49de911ba8b0dac0063074f9a9045bfc47c68".to_string(),
                "c15976bfaf05d5b1cfbb8927abe2c93cf293edd2cdd408c2ba1ce484eb5621e980".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - OP_RETURN runestone with truncated LEB128
                script_pubkey: "0x6a5d1b020304cfb4c2acf497b13e0380068094ebdc030a8094ebdc030801"
                    .to_string(),
                value: 0,
            },
            RosettaTxOut {
                // vout 1 - p2wpkh
                script_pubkey: "0x0014f32b49757996ef8db8d3d029b3dc997560e77d12".to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - p2wpkh
                script_pubkey: "0x001459966e46ce78b8bf4a54827b84144c82ea21811c".to_string(),
                value: 15_765,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x66d084fe5e206c7183293d1e379caa2011e7750018c65dfd2fd3174ea9f298fc"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 283_689,
                index: 3,
            },
        }
    }

    fn build_elonmuskdoge_tx() -> BitcoinTransactionData {
        // txid: 89e8149d38f8b702621fa18310b10794e541c0b52478466b85f156f5622b8fe3
        // This is the ELONMUSKDOGE transaction
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0xde6c56ecf9212e8946c71267108cce91ccc71d378b55aa6a1f41a3d93a82e8cb"
                    .to_string(),
            },
            vout: 0,
            value: 407_000,
            block_height: 840_020,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 4_294_967_293,
            witness: vec![
                "ee500cbe575753f98e50b67c20d53226daa08a12b81e4ec4fb47931b036b41238d586965080356f57ee7526f80774050056dd0bb09fc1d4169151d895d30802a".to_string(),
                "20f06ac7775ba12c567125ad1378251e934aa8d3411a9221f6b2c6c1cb6cfa3a3fac00630746f33f8d50844768".to_string(),
                "c1695fa445b5b9df8ba12a08a6afc6e6dac5bd962f8df317da08c57142d7c8f41b".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - OP_RETURN runestone
                script_pubkey:
                    "0x6a5d1f020304c6e6ffe9888ae1230300054506a096800ac0de810a08e80710f2a233"
                        .to_string(),
                value: 0,
            },
            RosettaTxOut {
                // vout 1 - p2wpkh
                script_pubkey: "0x001426909c2c3de5de4b6eccb8c0f65ab209fe6f4720".to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - p2wpkh
                script_pubkey: "0x001406f10a8f6a0aec21e97e02913f0b39f9aa477bbf".to_string(),
                value: 20_454,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0x89e8149d38f8b702621fa18310b10794e541c0b52478466b85f156f5622b8fe3"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 386_000,
                index: 4,
            },
        }
    }

    fn build_whataremfers_tx() -> BitcoinTransactionData {
        // txid: c075c5eba59ca77c40085fc417c8adafa9d2c9970158c7311d60fb24e00d4b45
        // This is the WHATAREMFERS transaction
        let prevout = RosettaOutPoint {
            txid: TransactionIdentifier {
                hash: "0xee637d9afdaabd9d5fb64fb8bb0396b69b12c6d1f150a00a9692f3bc09c5f6e5"
                    .to_string(),
            },
            vout: 0,
            value: 304_000,
            block_height: 840_020,
        };

        let inputs = vec![RosettaTxIn {
            previous_output: prevout,
            script_sig: "".to_string(),
            sequence: 4_294_967_293,
            witness: vec![
                "541ffd18ea22d83e45332caaf82b4bcfc28117fd97f9430caacf3553306e77df600fd2f439380d53ce848e2d6f9c22cf3ef5729ef557f8d2ba28c8dafe3a8e8c".to_string(),
                "20df9be78e98eaac880ca35fea0f4311832c7a03ad6a2fb1224075a1802267f1f9ac006308fae3bd5c87f52f0168".to_string(),
                "c0553bd58d975c1a22280e79d06fd5e7fc8618ef46dc56c43f777da6f8800cbb62".to_string(),
            ],
        }];

        let outputs = vec![
            RosettaTxOut {
                // vout 0 - OP_RETURN runestone
                script_pubkey: "0x6a5d24020304fac7f7e5f5b0fd97010348054d06a096800a904e0880b191640ca089310ec0843d"
                    .to_string(),
                value: 0,
            },
            RosettaTxOut {
                // vout 1 - p2tr
                script_pubkey: "0x512078a95794567c472013bfac2ecf3e8090e847f7bfd69748613bc0bf6c28be1549"
                    .to_string(),
                value: 546,
            },
            RosettaTxOut {
                // vout 2 - p2wpkh
                script_pubkey: "0x0014a4c6356a3723f8cc900f9b6cbe761f8937917af5"
                    .to_string(),
                value: 16_283,
            },
        ];

        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0xc075c5eba59ca77c40085fc417c8adafa9d2c9970158c7311d60fb24e00d4b45"
                    .to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                ordinal_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 287_171,
                index: 5,
            },
        }
    }

    #[test]
    fn valid_and_invalid_etch_output_selection_and_parsing() {
        // Common block context from the provided fixtures
        let block = build_block(
            840_021,
            "00000000000000000001a6a69ead163c499c0543dcef13c05499a798addb638f",
            1_713_583_272,
        );

        let tx_valid = build_valid_tx();
        let tx_invalid = build_invalid_tx();

        // Valid etch: OP_RETURN is last (vout 2); first eligible should be vout 0
        let (tx1, eligible1, first_eligible1, total_outputs1) =
            bitcoin_tx_from_chainhook_tx(&block, &tx_valid);
        assert_eq!(total_outputs1, 3);
        assert_eq!(first_eligible1, Some(0));
        assert!(eligible1.contains_key(&0));
        assert!(eligible1.contains_key(&1));
        assert!(!eligible1.contains_key(&2)); // OP_RETURN excluded

        // Invalid etch (by output positioning): OP_RETURN is first (vout 0); first eligible should be vout 1
        let (tx2, eligible2, first_eligible2, total_outputs2) =
            bitcoin_tx_from_chainhook_tx(&block, &tx_invalid);
        assert_eq!(total_outputs2, 3);
        assert_eq!(first_eligible2, Some(1));
        assert!(eligible2.contains_key(&1));
        assert!(eligible2.contains_key(&2));
        assert!(!eligible2.contains_key(&0)); // OP_RETURN excluded

        // art1: complete etching
        let art1 = Runestone::decipher(&tx1).expect("runestone");
        let Artifact::Runestone(rs1) = art1 else {
            panic!("expected Runestone");
        };
        let e1 = rs1.etching.as_ref().expect("expected etching");
        assert!(
            e1.divisibility.is_some()
                && e1.premine.is_some()
                && e1.terms.is_some()
                && e1.symbol.is_some()
                && e1.spacers.is_some()
                && e1.rune.is_some()
        );

        // art2: incomplete etching (all optional fields empty, turbo == false)
        let art2 = Runestone::decipher(&tx2).expect("runestone");
        let Artifact::Runestone(rs2) = art2 else {
            // Non-Runestone is acceptable for invalid tx
            return;
        };
        if let Some(e2) = rs2.etching.as_ref() {
            let is_incomplete = e2.divisibility.is_none()
                && e2.premine.is_none()
                && e2.rune.is_none()
                && e2.spacers.is_none()
                && e2.symbol.is_none()
                && e2.terms.is_none()
                && !e2.turbo;
            assert!(
                is_incomplete,
                "invalid tx should not produce a complete etching"
            );
        } else {
            // No etching at all is also acceptable
        }
    }

    #[test]
    fn invalid_varint_payload_yields_cenotaph_without_etching() {
        use bitcoin::{absolute::LockTime, script::Builder, transaction::Version, Amount, TxOut};

        // Construct an OP_RETURN | MAGIC_NUMBER script with a single push containing an invalid varint
        // 19 bytes with MSB set triggers varint error (overflow/truncated)
        let payload = vec![0x80u8; 19];
        let push: &bitcoin::script::PushBytes = payload.as_slice().try_into().unwrap();
        let script = Builder::new()
            .push_opcode(bitcoin::opcodes::all::OP_RETURN)
            .push_opcode(ordinals_parser::Runestone::MAGIC_NUMBER)
            .push_slice(push)
            .into_script();

        let tx = bitcoin::Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(0),
                script_pubkey: script,
            }],
        };

        let artifact = ordinals_parser::Runestone::decipher(&tx)
            .expect("expected Some(Artifact) for malformed runestone");
        match artifact {
            ordinals_parser::Artifact::Cenotaph(c) => {
                // No etching should be present when varint decoding fails
                assert!(c.etching.is_none());
            }
            _ => panic!("expected Cenotaph for invalid varint payload"),
        }
    }

    // TODO: add condition to run only if postgres and bitcoind are running
    #[tokio::test]
    #[ignore]
    async fn index_block_writes_valid_rune_and_rejects_invalid() {
        use config::Config;
        use ordinals_parser::RuneId;
        use postgres::{pg_begin, pg_pool, pg_pool_client};

        let ctx = Context::empty();

        // Ensure DB schema exists
        let mut tokio_pg = crate::db::pg_test_client(true, &ctx).await;
        // Keep `tokio_pg` alive for the duration of the test so the connection stays open.
        let _ = &mut tokio_pg;

        // Create a deadpool pool/client for indexer functions
        let pool = pg_pool(&crate::db::pg_test_config()).expect("pool");
        let mut client = pg_pool_client(&pool).await.expect("client");

        // Build config and index cache (set defaults to match bitcoin.conf, then allow env overrides)
        let mut config = Config::test_default();
        // Defaults from provided bitcoin.conf
        config.bitcoind.rpc_url = "http://127.0.0.1:8332".to_string();
        config.bitcoind.rpc_username = "user".to_string();
        config.bitcoind.rpc_password = "password".to_string();
        config.bitcoind.network = bitcoin::Network::Bitcoin;
        // Environment overrides: BTC_RPC_URL, BTC_RPC_USER, BTC_RPC_PASS, BTC_NETWORK
        if let Ok(url) = std::env::var("BTC_RPC_URL") {
            config.bitcoind.rpc_url = url;
        }
        if let Ok(user) = std::env::var("BTC_RPC_USER") {
            config.bitcoind.rpc_username = user;
        }
        if let Ok(pass) = std::env::var("BTC_RPC_PASS") {
            config.bitcoind.rpc_password = pass;
        }
        if let Ok(net) = std::env::var("BTC_NETWORK") {
            config.bitcoind.network = match net.to_lowercase().as_str() {
                "bitcoin" | "mainnet" => bitcoin::Network::Bitcoin,
                "regtest" => bitcoin::Network::Regtest,
                "testnet" | "testnet3" | "testnet4" => bitcoin::Network::Testnet,
                "signet" => bitcoin::Network::Signet,
                _ => config.bitcoind.network,
            };
        }
        let mut index_cache = IndexCache::new(&config, &pool, &ctx).await;
        let prometheus = PrometheusMonitoring::new();

        // Build block with all transactions
        let mut block = build_block(
            840_021,
            "00000000000000000001a6a69ead163c499c0543dcef13c05499a798addb638f",
            1_713_583_272,
        );
        let mut tx_valid = build_valid_tx();
        let mut tx_invalid = build_invalid_tx();
        let mut tx_no_commit = build_no_commit_tx();
        let mut tx_rune_wrong_flaw = build_rune_wrong_flaw_tx();
        let mut tx_internetgold = build_internetgold_tx();
        let mut tx_elonmuskdoge = build_elonmuskdoge_tx();
        let mut tx_whataremfers = build_whataremfers_tx();
        tx_valid.metadata.index = 0;
        tx_invalid.metadata.index = 1;
        tx_no_commit.metadata.index = 2;
        tx_rune_wrong_flaw.metadata.index = 3;
        tx_internetgold.metadata.index = 4;
        tx_elonmuskdoge.metadata.index = 5;
        tx_whataremfers.metadata.index = 6;
        block.transactions = vec![
            tx_valid,
            tx_invalid,
            tx_no_commit,
            tx_rune_wrong_flaw,
            tx_internetgold,
            tx_elonmuskdoge,
            tx_whataremfers,
        ];

        // Index the block
        let result =
            index_block(&mut client, &mut index_cache, &mut block, &prometheus, &ctx).await;
        // If bitcoind is unreachable, this will Err. Fail fast with context.
        result.expect("index_block should succeed with reachable bitcoind RPC");

        // Verify DB contents: valid rune present, invalid and no-commit runes absent
        let mut db_tx = pg_begin(&mut client).await.expect("tx");
        let valid_id = RuneId::from_str("840021:0").unwrap();
        let invalid_id = RuneId::from_str("840021:1").unwrap();
        let no_commit_id = RuneId::from_str("840021:2").unwrap();
        let rune_wrong_flaw_id = RuneId::from_str("840021:3").unwrap();
        let internetgold_id = RuneId::from_str("840021:4").unwrap();
        let elonmuskdoge_id = RuneId::from_str("840021:5").unwrap();
        let whataremfers_id = RuneId::from_str("840021:6").unwrap();
        let valid = crate::db::pg_get_rune_by_id(&valid_id, &mut db_tx, &ctx).await;
        let invalid = crate::db::pg_get_rune_by_id(&invalid_id, &mut db_tx, &ctx).await;
        let no_commit = crate::db::pg_get_rune_by_id(&no_commit_id, &mut db_tx, &ctx).await;
        let rune_wrong_flaw =
            crate::db::pg_get_rune_by_id(&rune_wrong_flaw_id, &mut db_tx, &ctx).await;
        let internetgold = crate::db::pg_get_rune_by_id(&internetgold_id, &mut db_tx, &ctx).await;
        let elonmuskdoge = crate::db::pg_get_rune_by_id(&elonmuskdoge_id, &mut db_tx, &ctx).await;
        let whataremfers = crate::db::pg_get_rune_by_id(&whataremfers_id, &mut db_tx, &ctx).await;
        assert!(valid.is_some(), "valid etch should be inserted into DB");
        assert!(
            invalid.is_none(),
            "invalid etch should not be inserted into DB"
        );
        assert!(
            no_commit.is_none(),
            "no-commit etch should not be inserted into DB"
        );
        assert!(
            rune_wrong_flaw.is_some(),
            "cenotaph rune etch should be inserted into DB"
        );
        assert!(
            internetgold.is_none(),
            "INTERNETGOLD etch should not be inserted into DB"
        );
        assert!(
            elonmuskdoge.is_none(),
            "ELONMUSKDOGE etch should not be inserted into DB"
        );
        assert!(
            whataremfers.is_none(),
            "WHATAREMFERS etch should not be inserted into DB"
        );
        db_tx.commit().await.unwrap();
    }
}
