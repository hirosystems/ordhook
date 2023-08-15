pub mod pipeline;
pub mod protocol;

use chainhook_sdk::types::{BitcoinBlockData, OrdinalOperation};
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};
use rocksdb::DB;
use rusqlite::Connection;
use std::hash::BuildHasherDefault;
use std::ops::Div;
use std::path::PathBuf;

use chainhook_sdk::{
    bitcoincore_rpc::{Auth, Client, RpcApi},
    utils::Context,
};

use crate::config::{Config, LogConfig};

use crate::db::{
    find_last_block_inserted, find_latest_inscription_block_height, initialize_ordhook_db,
    open_readonly_ordhook_db_conn, open_readonly_ordhook_db_conn_rocks_db,
};

use crate::db::{
    insert_transfer_in_locations, remove_entry_from_blocks, remove_entry_from_inscriptions,
    LazyBlockTransaction,
};

#[derive(Clone, Debug)]
pub struct HordConfig {
    pub network_thread_max: usize,
    pub ingestion_thread_max: usize,
    pub ingestion_thread_queue_size: usize,
    pub cache_size: usize,
    pub db_path: PathBuf,
    pub first_inscription_height: u64,
    pub logs: LogConfig,
}

pub fn revert_ordhook_db_with_augmented_bitcoin_block(
    block: &BitcoinBlockData,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) -> Result<(), String> {
    // Remove block from
    remove_entry_from_blocks(block.block_identifier.index as u32, &blocks_db_rw, ctx);
    for tx_index in 1..=block.transactions.len() {
        // Undo the changes in reverse order
        let tx = &block.transactions[block.transactions.len() - tx_index];
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            match ordinal_event {
                OrdinalOperation::InscriptionRevealed(data) => {
                    // We remove any new inscription created
                    remove_entry_from_inscriptions(
                        &data.inscription_id,
                        &inscriptions_db_conn_rw,
                        ctx,
                    );
                }
                OrdinalOperation::InscriptionTransferred(transfer_data) => {
                    // We revert the outpoint to the pre-transfer value
                    insert_transfer_in_locations(
                        transfer_data,
                        &block.block_identifier,
                        &inscriptions_db_conn_rw,
                        &ctx,
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn new_traversals_cache(
) -> DashMap<(u32, [u8; 8]), (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>), BuildHasherDefault<FxHasher>>
{
    let hasher = FxBuildHasher::default();
    DashMap::with_hasher(hasher)
}

pub fn new_traversals_lazy_cache(
    cache_size: usize,
) -> DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>> {
    let hasher = FxBuildHasher::default();
    DashMap::with_capacity_and_hasher(
        ((cache_size.saturating_sub(500)) * 1000 * 1000)
            .div(LazyBlockTransaction::get_average_bytes_size()),
        hasher,
    )
}

#[derive(PartialEq, Debug)]
pub enum SatPosition {
    Output((usize, u64)),
    Fee(u64),
}

pub fn compute_next_satpoint_data(
    input_index: usize,
    offset_intra_input: u64,
    inputs: &Vec<u64>,
    outputs: &Vec<u64>,
) -> SatPosition {
    let mut offset_cross_inputs = 0;
    for (index, input_value) in inputs.iter().enumerate() {
        if index == input_index {
            break;
        }
        offset_cross_inputs += input_value;
    }
    offset_cross_inputs += offset_intra_input;

    let mut offset_intra_outputs = 0;
    let mut output_index = 0;
    let mut floating_bound = 0;

    for (index, output_value) in outputs.iter().enumerate() {
        floating_bound += output_value;
        output_index = index;
        if floating_bound > offset_cross_inputs {
            break;
        }
        offset_intra_outputs += output_value;
    }

    if output_index == (outputs.len() - 1) && offset_cross_inputs >= floating_bound {
        // Satoshi spent in fees
        return SatPosition::Fee(offset_cross_inputs - floating_bound);
    }
    SatPosition::Output((output_index, (offset_cross_inputs - offset_intra_outputs)))
}

pub fn should_sync_ordhook_db(
    config: &Config,
    ctx: &Context,
) -> Result<Option<(u64, u64, usize)>, String> {
    let auth = Auth::UserPass(
        config.network.bitcoind_rpc_username.clone(),
        config.network.bitcoind_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoind_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("Bitcoin RPC error: {}", message.to_string()));
        }
    };

    let mut start_block =
        match open_readonly_ordhook_db_conn_rocks_db(&config.expected_cache_path(), &ctx) {
            Ok(blocks_db) => find_last_block_inserted(&blocks_db) as u64,
            Err(err) => {
                ctx.try_log(|logger| {
                    warn!(logger, "{}", err);
                });
                0
            }
        };

    if start_block == 0 {
        let _ = initialize_ordhook_db(&config.expected_cache_path(), &ctx);
    }

    let inscriptions_db_conn = open_readonly_ordhook_db_conn(&config.expected_cache_path(), &ctx)?;

    match find_latest_inscription_block_height(&inscriptions_db_conn, ctx)? {
        Some(height) => {
            start_block = start_block.min(height);
        }
        None => {
            start_block = start_block.min(config.get_ordhook_config().first_inscription_height);
        }
    };

    let end_block = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks,
        Err(e) => {
            return Err(format!(
                "unable to retrieve Bitcoin chain tip ({})",
                e.to_string()
            ));
        }
    };

    start_block += 1;

    // TODO: Gracefully handle Regtest, Testnet and Signet
    let (mut end_block, speed) = if start_block <= 200_000 {
        (end_block.min(200_000), 10_000)
    } else if start_block <= 550_000 {
        (end_block.min(550_000), 1_000)
    } else {
        (end_block, 100)
    };

    if start_block < 767430 && end_block > 767430 {
        end_block = 767430;
    }

    if start_block <= end_block {
        Ok(Some((start_block, end_block, speed)))
    } else {
        Ok(None)
    }
}

#[test]
fn test_identify_next_output_index_destination() {
    assert_eq!(
        compute_next_satpoint_data(0, 10, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((0, 10))
    );
    assert_eq!(
        compute_next_satpoint_data(0, 20, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 5, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Output((1, 5))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 6, &vec![20, 30, 45], &vec![20, 5, 45]),
        SatPosition::Output((2, 1))
    );
    assert_eq!(
        compute_next_satpoint_data(1, 10, &vec![10, 10, 10], &vec![30]),
        SatPosition::Output((0, 20))
    );
    assert_eq!(
        compute_next_satpoint_data(0, 30, &vec![10, 10, 10], &vec![30]),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, 0, &vec![10, 10, 10], &vec![30]),
        SatPosition::Output((0, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(2, 45, &vec![20, 30, 45], &vec![20, 30, 45]),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(
            2,
            0,
            &vec![1000, 600, 546, 63034],
            &vec![1600, 10000, 15000]
        ),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(
            3,
            0,
            &vec![6100, 148660, 103143, 7600],
            &vec![81434, 173995]
        ),
        SatPosition::Fee(2474)
    );
}
