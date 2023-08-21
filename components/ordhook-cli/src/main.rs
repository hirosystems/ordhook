#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate hiro_system_kit;

pub mod cli;
pub mod config;

fn main() {
    cli::main();
}
