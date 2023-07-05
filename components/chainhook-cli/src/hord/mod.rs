use std::sync::mpsc::Sender;

use chainhook_sdk::{
    bitcoincore_rpc::{Auth, Client, RpcApi},
    hord::{
        db::{
            fetch_and_cache_blocks_in_hord_db, find_last_block_inserted,
            find_latest_inscription_block_height, initialize_hord_db, open_readonly_hord_db_conn,
            open_readonly_hord_db_conn_rocks_db, open_readwrite_hord_db_conn,
            open_readwrite_hord_db_conn_rocks_db,
        },
        HordConfig,
    },
    observer::BitcoinConfig,
    utils::Context,
};
use chainhook_types::BitcoinBlockData;

use crate::config::Config;

pub fn should_sync_hord_db(config: &Config, ctx: &Context) -> Result<Option<(u64, u64)>, String> {
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
        match open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx) {
            Ok(blocks_db) => find_last_block_inserted(&blocks_db) as u64,
            Err(err) => {
                warn!(ctx.expect_logger(), "{}", err);
                0
            }
        };

    if start_block == 0 {
        let _ = initialize_hord_db(&config.expected_cache_path(), &ctx);
    }

    let inscriptions_db_conn = open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    match find_latest_inscription_block_height(&inscriptions_db_conn, ctx)? {
        Some(height) => {
            start_block = start_block.min(height);
        }
        None => {
            start_block = start_block.min(config.get_hord_config().first_inscription_height);
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

    if start_block < end_block {
        Ok(Some((start_block, end_block)))
    } else {
        Ok(None)
    }
}

pub async fn perform_hord_db_update(
    start_block: u64,
    end_block: u64,
    hord_config: &HordConfig,
    config: &Config,
    block_post_processor: Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) -> Result<(), String> {
    info!(
        ctx.expect_logger(),
        "Syncing hord_db: {} blocks to download ({start_block}: {end_block})",
        end_block - start_block + 1
    );

    let bitcoin_config = BitcoinConfig {
        username: config.network.bitcoind_rpc_username.clone(),
        password: config.network.bitcoind_rpc_password.clone(),
        rpc_url: config.network.bitcoind_rpc_url.clone(),
        network: config.network.bitcoin_network.clone(),
        bitcoin_block_signaling: config.network.bitcoin_block_signaling.clone(),
    };

    let blocks_db = open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
    let inscriptions_db_conn_rw = open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    let _ = fetch_and_cache_blocks_in_hord_db(
        &bitcoin_config,
        &blocks_db,
        &inscriptions_db_conn_rw,
        start_block,
        end_block,
        hord_config,
        block_post_processor,
        &ctx,
    )
    .await?;

    Ok(())
}
