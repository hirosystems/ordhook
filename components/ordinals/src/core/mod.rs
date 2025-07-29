pub mod meta_protocols;
pub mod pipeline;
pub mod protocol;
#[cfg(test)]
pub mod test_builders;

use std::{hash::BuildHasherDefault, ops::Div};

use bitcoin::Network;
use bitcoind::{types::TransactionBytesCursor, utils::Context};
use config::Config;
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};

pub fn first_inscription_height(config: &Config) -> u64 {
    match config.bitcoind.network {
        Network::Bitcoin => 767430,
        Network::Regtest => 1,
        Network::Testnet => 2413343,
        Network::Testnet4 => 0,
        Network::Signet => 112402,
        _ => unreachable!(),
    }
}

pub fn new_traversals_cache(
) -> DashMap<(u32, [u8; 8]), (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>), BuildHasherDefault<FxHasher>>
{
    let hasher = FxBuildHasher::default();
    DashMap::with_hasher(hasher)
}

pub fn new_traversals_lazy_cache(
    cache_size: usize,
) -> DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>> {
    let hasher = FxBuildHasher::default();
    DashMap::with_capacity_and_hasher(
        ((cache_size.saturating_sub(500)) * 1000 * 1000)
            .div(TransactionBytesCursor::get_average_bytes_size()),
        hasher,
    )
}

#[derive(PartialEq, Debug)]
pub enum SatPosition {
    Output((usize, u64)),
    Fee(u64),
}

pub fn resolve_absolute_pointer(inputs: &[u64], absolute_pointer_value: u64) -> (usize, u64) {
    let mut selected_index = 0;
    let mut cumulated_input_value = 0;
    // Check for overflow
    let total: u64 = inputs.iter().sum();
    if absolute_pointer_value > total {
        return (0, 0);
    }
    // Identify the input + satoshi offset being inscribed
    for (index, input_value) in inputs.iter().enumerate() {
        if (cumulated_input_value + input_value) > absolute_pointer_value {
            selected_index = index;
            break;
        }
        cumulated_input_value += input_value;
    }
    let relative_pointer_value = absolute_pointer_value - cumulated_input_value;
    (selected_index, relative_pointer_value)
}

pub fn compute_next_satpoint_data(
    input_index: usize,
    inputs: &[u64],
    outputs: &[u64],
    relative_pointer_value: u64,
    _ctx: Option<&Context>,
) -> SatPosition {
    let mut absolute_offset_in_inputs = 0;
    for (index, input_value) in inputs.iter().enumerate() {
        if index == input_index {
            break;
        }
        absolute_offset_in_inputs += input_value;
    }
    absolute_offset_in_inputs += relative_pointer_value;

    let mut absolute_offset_of_first_satoshi_in_selected_output = 0;
    let mut selected_output_index = 0;
    let mut floating_bound = 0;

    for (index, output_value) in outputs.iter().enumerate() {
        floating_bound += output_value;
        selected_output_index = index;
        if floating_bound > absolute_offset_in_inputs {
            break;
        }
        absolute_offset_of_first_satoshi_in_selected_output += output_value;
    }

    if selected_output_index == (outputs.len() - 1) && absolute_offset_in_inputs >= floating_bound {
        // Satoshi spent in fees
        return SatPosition::Fee(absolute_offset_in_inputs - floating_bound);
    }
    let relative_offset_in_selected_output =
        absolute_offset_in_inputs - absolute_offset_of_first_satoshi_in_selected_output;
    SatPosition::Output((selected_output_index, relative_offset_in_selected_output))
}

#[test]
fn test_identify_next_output_index_destination() {
    assert_eq!(
        compute_next_satpoint_data(0, &vec![20, 30, 45], &vec![20, 30, 45], 10, None),
        SatPosition::Output((0, 10))
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![20, 30, 45], &vec![20, 30, 45], 20, None),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![20, 30, 45], &vec![20, 30, 45], 25, None),
        SatPosition::Output((1, 25))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![20, 30, 45], &vec![20, 5, 45], 26, None),
        SatPosition::Output((2, 21))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![10, 10, 10], &vec![30], 20, None),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![10, 10, 10], &vec![30], 30, None),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![10, 10, 10], &vec![30], 0, None),
        SatPosition::Output((0, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(2, &vec![20, 30, 45], &vec![20, 30, 45], 95, None),
        SatPosition::Fee(50)
    );
    assert_eq!(
        compute_next_satpoint_data(
            2,
            &vec![1000, 600, 546, 63034],
            &vec![1600, 10000, 15000],
            1600,
            None
        ),
        SatPosition::Output((1, 1600))
    );
    assert_eq!(
        compute_next_satpoint_data(
            3,
            &vec![6100, 148660, 103143, 7600],
            &vec![81434, 173995],
            257903,
            None
        ),
        SatPosition::Fee(260377)
    );
}
