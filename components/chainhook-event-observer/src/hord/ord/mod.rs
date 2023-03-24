#![allow(dead_code)]
#![allow(unused_variables)]

use hiro_system_kit::slog;
use std::{path::PathBuf};

type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

use chainhook_types::BitcoinNetwork;

use crate::{observer::EventObserverConfig, utils::Context};

pub mod blocktime;
pub mod chain;
pub mod deserialize_from_str;
pub mod epoch;
pub mod height;
pub mod indexing;
pub mod inscription_id;
pub mod sat;
pub mod sat_point;

const DIFFCHANGE_INTERVAL: u64 =
    bitcoincore_rpc::bitcoin::blockdata::constants::DIFFCHANGE_INTERVAL as u64;
const SUBSIDY_HALVING_INTERVAL: u64 =
    bitcoincore_rpc::bitcoin::blockdata::constants::SUBSIDY_HALVING_INTERVAL as u64;
const CYCLE_EPOCHS: u64 = 6;

pub fn initialize_ordinal_index(
    config: &EventObserverConfig,
    index_path: Option<PathBuf>,
    ctx: &Context,
) -> Result<self::indexing::OrdinalIndex, String> {
    let chain = match &config.bitcoin_network {
        BitcoinNetwork::Mainnet => chain::Chain::Mainnet,
        BitcoinNetwork::Testnet => chain::Chain::Testnet,
        BitcoinNetwork::Regtest => chain::Chain::Regtest,
    };
    let index_options = self::indexing::Options {
        rpc_username: config.bitcoin_node_username.clone(),
        rpc_password: config.bitcoin_node_password.clone(),
        data_dir: config.cache_path.clone().into(),
        chain: chain,
        first_inscription_height: Some(chain.first_inscription_height()),
        height_limit: None,
        index: index_path,
        rpc_url: config.bitcoin_node_rpc_url.clone(),
    };
    let index = match self::indexing::OrdinalIndex::open(&index_options) {
        Ok(index) => index,
        Err(e) => {
            ctx.try_log(|logger| {
                slog::error!(logger, "Unable to open ordinal index: {}", e.to_string())
            });
            std::process::exit(1);
        }
    };
    Ok(index)
}
