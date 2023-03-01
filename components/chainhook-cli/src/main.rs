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
pub mod node;
pub mod scan;
// pub mod storage;
#[cfg(feature = "ordinals")]
pub mod ordinals;

fn main() {
    cli::main();
}
