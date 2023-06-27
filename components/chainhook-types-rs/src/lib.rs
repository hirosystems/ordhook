extern crate serde;

#[macro_use]
extern crate serde_derive;

pub mod bitcoin;
mod events;
mod processors;
mod rosetta;

pub use events::*;
pub use processors::*;
pub use rosetta::*;

pub enum Chain {
    Bitcoin,
    Stacks,
}
