#[macro_use]
extern crate rocket;

extern crate serde;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

pub extern crate bitcoincore_rpc;
pub extern crate dashmap;
pub extern crate fxhash;
pub extern crate stacks_rpc_client;

pub use chainhook_types;

pub mod chainhooks;
pub mod indexer;
pub mod observer;
pub mod utils;

#[cfg(feature = "ordinals")]
pub extern crate rocksdb;

#[cfg(feature = "ordinals")]
pub mod hord;

// TODO
// pub mod clarity {
//     pub use stacks_rpc_client::clarity::stacks_common::*;
//     pub use stacks_rpc_client::clarity::vm::*;
//     pub use stacks_rpc_client::clarity::*;
// }
