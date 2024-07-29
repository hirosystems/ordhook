#[macro_use]
extern crate rocket;

#[macro_use]
extern crate hiro_system_kit;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate lazy_static;

extern crate serde;

pub extern crate chainhook_sdk;
pub extern crate hex;

pub mod config;
pub mod core;
pub mod db;
pub mod download;
pub mod ord;
pub mod scan;
pub mod service;
pub mod utils;

use core::meta_protocols::brc20::db::initialize_brc20_db;

use chainhook_sdk::utils::Context;
use config::Config;
use db::initialize_ordhook_db;
use rusqlite::Connection;

pub struct DbConnections {
    pub ordhook: Connection,
    pub brc20: Option<Connection>,
}

/// Initializes all SQLite databases required for Ordhook operation, depending if they are requested by the current `Config`.
/// Returns a struct with all the open connections.
pub fn initialize_databases(config: &Config, ctx: &Context) -> DbConnections {
    DbConnections {
        ordhook: initialize_ordhook_db(&config.expected_cache_path(), ctx),
        brc20: match config.meta_protocols.brc20 {
            true => Some(initialize_brc20_db(
                Some(&config.expected_cache_path()),
                ctx,
            )),
            false => None,
        },
    }
}
