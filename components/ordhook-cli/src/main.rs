#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate hiro_system_kit;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub mod cli;
pub mod config;

fn main() {
    cli::main();
}
