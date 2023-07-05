#[macro_use]
extern crate rocket;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate hiro_system_kit;

#[macro_use]
extern crate serde_derive;

extern crate serde;

pub mod archive;
pub mod block;
pub mod cli;
pub mod config;
pub mod hord;
pub mod scan;
pub mod service;
pub mod storage;

fn main() {
    cli::main();
}
