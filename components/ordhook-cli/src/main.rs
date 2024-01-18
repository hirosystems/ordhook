#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate hiro_system_kit;

pub mod cli;
pub mod config;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tcmalloc2::TcMalloc = tcmalloc2::TcMalloc;

fn main() {
    cli::main();
}
