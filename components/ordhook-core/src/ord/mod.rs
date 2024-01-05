#![allow(dead_code)]
#![allow(unused_variables)]

type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

use chainhook_sdk::types::BitcoinNetwork;

pub mod chain;
pub mod deserialize_from_str;
pub mod envelope;
pub mod epoch;
pub mod height;
pub mod inscription;
pub mod inscription_id;
pub mod media;
pub mod sat;
pub mod sat_point;

const DIFFCHANGE_INTERVAL: u64 =
    chainhook_sdk::bitcoincore_rpc::bitcoin::blockdata::constants::DIFFCHANGE_INTERVAL as u64;
const SUBSIDY_HALVING_INTERVAL: u64 =
    chainhook_sdk::bitcoincore_rpc::bitcoin::blockdata::constants::SUBSIDY_HALVING_INTERVAL as u64;
const CYCLE_EPOCHS: u64 = 6;
pub const COIN_VALUE: u64 = 100_000_000;
