use std::{hash::BuildHasherDefault, sync::Arc};

use bitcoind::{
    try_error,
    types::{
        BlockBytesCursor, BlockIdentifier, OrdinalInscriptionNumber, TransactionBytesCursor,
        TransactionIdentifier,
    },
    utils::Context,
};
use config::Config;
use dashmap::DashMap;
use fxhash::FxHasher;
use ord::{height::Height, sat::Sat};

use crate::db::blocks::find_pinned_block_bytes_at_block_height;

#[derive(Clone, Debug)]
pub struct TraversalResult {
    pub inscription_number: OrdinalInscriptionNumber,
    pub inscription_input_index: usize,
    pub transaction_identifier_inscription: TransactionIdentifier,
    pub ordinal_number: u64,
    pub transfers: u32,
}

impl TraversalResult {
    pub fn get_ordinal_coinbase_height(&self) -> u64 {
        let sat = Sat(self.ordinal_number);
        sat.height().n() as u64
    }

    pub fn get_ordinal_coinbase_offset(&self) -> u64 {
        let sat = Sat(self.ordinal_number);
        self.ordinal_number - sat.height().starting_sat().n()
    }

    pub fn get_inscription_id(&self) -> String {
        format!(
            "{}i{}",
            self.transaction_identifier_inscription.get_hash_bytes_str(),
            self.inscription_input_index
        )
    }
}

pub fn compute_satoshi_number(
    block_identifier: &BlockIdentifier,
    transaction_identifier: &TransactionIdentifier,
    inscription_input_index: usize,
    inscription_pointer: u64,
    traversals_cache: &Arc<
        DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>,
    >,
    blocks_db: &rocksdb::DB,
    _config: &Config,
    ctx: &Context,
) -> Result<(TraversalResult, u64, Vec<(u32, [u8; 8], usize)>), String> {
    let mut ordinal_offset = inscription_pointer;
    let ordinal_block_number = block_identifier.index as u32;
    let txid = transaction_identifier.get_8_hash_bytes();
    let mut back_track = vec![];

    let (mut tx_cursor, mut ordinal_block_number) = match traversals_cache
        .get(&(block_identifier.index as u32, txid))
    {
        Some(entry) => {
            let tx = entry.value();
            (
                (
                    tx.inputs[inscription_input_index].txin,
                    tx.inputs[inscription_input_index].vout.into(),
                ),
                tx.inputs[inscription_input_index].block_height,
            )
        }
        None => {
            match find_pinned_block_bytes_at_block_height(ordinal_block_number, 3, blocks_db, ctx) {
                None => {
                    return Err(format!("block #{ordinal_block_number} not in database"));
                }
                Some(block_bytes) => {
                    let cursor = BlockBytesCursor::new(block_bytes.as_ref());
                    match cursor.find_and_serialize_transaction_with_txid(&txid) {
                        Some(tx) => (
                            (
                                tx.inputs[inscription_input_index].txin,
                                tx.inputs[inscription_input_index].vout.into(),
                            ),
                            tx.inputs[inscription_input_index].block_height,
                        ),
                        None => return Err(format!("txid not in block #{ordinal_block_number}")),
                    }
                }
            }
        }
    };

    let mut hops: u32 = 0;

    loop {
        hops += 1;
        if hops as u64 > block_identifier.index {
            return Err(format!(
                "Unable to process transaction {} detected after {hops} iterations. Manual investigation required",
                transaction_identifier.hash
            ));
        }

        if let Some(cached_tx) = traversals_cache.get(&(ordinal_block_number, tx_cursor.0)) {
            let tx = cached_tx.value();

            let mut next_found_in_cache = false;
            let mut sats_out = 0;
            for (index, output_value) in tx.outputs.iter().enumerate() {
                if index == tx_cursor.1 {
                    break;
                }
                sats_out += output_value;
            }
            sats_out += ordinal_offset;

            let mut sats_in = 0;
            for input in tx.inputs.iter() {
                sats_in += input.txin_value;

                if sats_out < sats_in {
                    ordinal_offset = sats_out - (sats_in - input.txin_value);
                    ordinal_block_number = input.block_height;
                    tx_cursor = (input.txin, input.vout as usize);
                    next_found_in_cache = true;
                    break;
                }
            }

            if next_found_in_cache {
                continue;
            }

            if sats_in == 0 {
                try_error!(
                    ctx,
                    "Transaction {} is originating from a non spending transaction",
                    transaction_identifier.hash
                );
                return Ok((
                    TraversalResult {
                        inscription_number: OrdinalInscriptionNumber::zero(),
                        ordinal_number: 0,
                        transfers: 0,
                        inscription_input_index,
                        transaction_identifier_inscription: transaction_identifier.clone(),
                    },
                    inscription_pointer,
                    back_track,
                ));
            }
        }

        let pinned_block_bytes = {
            match find_pinned_block_bytes_at_block_height(ordinal_block_number, 3, blocks_db, ctx) {
                Some(block) => block,
                None => {
                    return Err(format!("block #{ordinal_block_number} not in database (traversing {} / {} in progress)", transaction_identifier.hash, block_identifier.index));
                }
            }
        };
        let block_cursor = BlockBytesCursor::new(pinned_block_bytes.as_ref());
        let txid = tx_cursor.0;
        let mut block_cursor_tx_iter = block_cursor.iter_tx();
        let coinbase = block_cursor_tx_iter.next().expect("empty block");

        // evaluate exit condition: did we reach the **final** coinbase transaction
        if coinbase.txid.eq(&txid) {
            let mut intra_coinbase_output_offset = 0;
            for (index, output_value) in coinbase.outputs.iter().enumerate() {
                if index == tx_cursor.1 {
                    break;
                }
                intra_coinbase_output_offset += output_value;
            }
            ordinal_offset += intra_coinbase_output_offset;

            let subsidy = Height(ordinal_block_number).subsidy();
            if ordinal_offset < subsidy {
                // Great!
                break;
            }

            // loop over the transaction fees to detect the right range
            let mut accumulated_fees = subsidy;

            for tx in block_cursor_tx_iter {
                let mut total_in = 0;
                for input in tx.inputs.iter() {
                    total_in += input.txin_value;
                }

                let mut total_out = 0;
                for output_value in tx.outputs.iter() {
                    total_out += output_value;
                }

                let fee = total_in - total_out;
                if accumulated_fees + fee > ordinal_offset {
                    // We are looking at the right transaction
                    // Retraverse the inputs to select the index to be picked
                    let offset_within_fee = ordinal_offset - accumulated_fees;
                    total_out += offset_within_fee;
                    let mut sats_in = 0;

                    for input in tx.inputs.into_iter() {
                        sats_in += input.txin_value;

                        if sats_in > total_out {
                            ordinal_offset = total_out - (sats_in - input.txin_value);
                            ordinal_block_number = input.block_height;
                            tx_cursor = (input.txin, input.vout as usize);
                            break;
                        }
                    }
                    break;
                } else {
                    accumulated_fees += fee;
                }
            }
        } else {
            // isolate the target transaction
            let tx_bytes_cursor = match block_cursor.find_and_serialize_transaction_with_txid(&txid)
            {
                Some(entry) => entry,
                None => {
                    try_error!(
                        ctx,
                        "fatal: unable to retrieve tx ancestor {} in block {ordinal_block_number} (satpoint {}:{inscription_input_index})",
                        hex::encode(txid),
                        transaction_identifier.get_hash_bytes_str(),
                    );
                    std::process::exit(1);
                }
            };

            let mut sats_out = 0;
            for (index, output_value) in tx_bytes_cursor.outputs.iter().enumerate() {
                if index == tx_cursor.1 {
                    break;
                }
                sats_out += output_value;
            }
            sats_out += ordinal_offset;

            let mut sats_in = 0;
            for input in tx_bytes_cursor.inputs.iter() {
                sats_in += input.txin_value;

                if sats_out < sats_in {
                    back_track.push((ordinal_block_number, tx_cursor.0, tx_cursor.1));
                    traversals_cache
                        .insert((ordinal_block_number, tx_cursor.0), tx_bytes_cursor.clone());
                    ordinal_offset = sats_out - (sats_in - input.txin_value);
                    ordinal_block_number = input.block_height;
                    tx_cursor = (input.txin, input.vout as usize);
                    break;
                }
            }

            if sats_in == 0 {
                try_error!(
                    ctx,
                    "Transaction {} is originating from a non spending transaction",
                    transaction_identifier.hash
                );
                return Ok((
                    TraversalResult {
                        inscription_number: OrdinalInscriptionNumber::zero(),
                        ordinal_number: 0,
                        transfers: 0,
                        inscription_input_index,
                        transaction_identifier_inscription: transaction_identifier.clone(),
                    },
                    inscription_pointer,
                    back_track,
                ));
            }
        }
    }

    let height = Height(ordinal_block_number);
    let ordinal_number = height.starting_sat().0 + ordinal_offset;

    Ok((
        TraversalResult {
            inscription_number: OrdinalInscriptionNumber::zero(),
            ordinal_number,
            transfers: hops,
            inscription_input_index,
            transaction_identifier_inscription: transaction_identifier.clone(),
        },
        inscription_pointer,
        back_track,
    ))
}

#[cfg(test)]
mod test {
    use std::{hash::BuildHasherDefault, sync::Arc};

    use bitcoind::{
        types::{
            bitcoin::TxOut, BlockIdentifier, TransactionBytesCursor, TransactionIdentifier,
            TransactionInputBytesCursor,
        },
        utils::Context,
    };
    use config::Config;
    use dashmap::DashMap;
    use fxhash::FxHasher;

    use super::compute_satoshi_number;
    use crate::{
        core::{
            new_traversals_lazy_cache,
            test_builders::{TestBlockBuilder, TestTransactionBuilder, TestTxInBuilder},
        },
        db::{
            blocks::{insert_standardized_block, open_blocks_db_with_retry},
            drop_all_dbs,
        },
    };

    fn store_tx_in_traversals_cache(
        cache: &DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>,
        block_height: u64,
        block_hash: String,
        tx_hash: String,
        input_tx_hash: String,
        input_value: u64,
        input_vout: u16,
        output_value: u64,
    ) {
        let block_identifier = BlockIdentifier {
            index: block_height,
            hash: block_hash,
        };
        let transaction_identifier = TransactionIdentifier {
            hash: tx_hash.clone(),
        };
        let txid = transaction_identifier.get_8_hash_bytes();
        cache.insert(
            (block_identifier.index as u32, txid.clone()),
            TransactionBytesCursor {
                txid,
                inputs: vec![TransactionInputBytesCursor {
                    txin: (TransactionIdentifier {
                        hash: input_tx_hash,
                    })
                    .get_8_hash_bytes(),
                    block_height: (block_height - 1) as u32,
                    vout: input_vout,
                    txin_value: input_value,
                }],
                outputs: vec![output_value],
            },
        );
    }

    #[test]
    fn compute_sat_with_cached_traversals() {
        let ctx = Context::empty();
        let config = Config::test_default();
        drop_all_dbs(&config);
        let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
        let cache = new_traversals_lazy_cache(100);

        // Make cache contain the tx input trace (850000 -> 849999 -> 849998) so it doesn't have to visit rocksdb in every step.
        store_tx_in_traversals_cache(
            &cache,
            850000,
            "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
            "0xa321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac".to_string(),
            9_000,
            0,
            8_000,
        );
        store_tx_in_traversals_cache(
            &cache,
            849999,
            "0x000000000000000000026b072f9347d86942f6786dd1fc362acfd9522715b313".to_string(),
            "0xa321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac".to_string(),
            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string(),
            10_000,
            0,
            9_000,
        );
        // Store the sat coinbase block only (849998), it's the only one we need to access from blocks DB.
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849998)
                .hash(
                    "0x00000000000000000000ec8da633f1fb0f8f281e43c52e5702139fac4f91204a"
                        .to_string(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c"
                                .to_string(),
                        )
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );

        let block_identifier = BlockIdentifier {
            index: 850000,
            hash: "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
        };
        let transaction_identifier = TransactionIdentifier {
            hash: "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
        };
        let Ok((result, pointer, _)) = compute_satoshi_number(
            &block_identifier,
            &transaction_identifier,
            0,
            8_000,
            &Arc::new(cache),
            &blocks_db,
            &config,
            &ctx,
        ) else {
            panic!();
        };

        assert_eq!(result.ordinal_number, 1971874375008000);
        assert_eq!(result.transfers, 2);
        assert_eq!(
            result.transaction_identifier_inscription.hash,
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string()
        );
        assert_eq!(pointer, 8000);
    }

    #[test]
    fn compute_sat_with_rocksdb_traversals() {
        let ctx = Context::empty();
        let config = Config::test_default();
        drop_all_dbs(&config);
        let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
        let cache = new_traversals_lazy_cache(100);

        // Insert blocks directly into rocksdb to test non-cached lookups.
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(850000)
                .hash(
                    "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187"
                        .to_string(),
                )
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9"
                                .to_string(),
                        )
                        .add_input(
                            TestTxInBuilder::new()
                                .prev_out_block_height(849999)
                                .prev_out_tx_hash("0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string())
                                .value(10_000)
                                .build(),
                        )
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849999)
                .hash(
                    "0x00000000000000000000ec8da633f1fb0f8f281e43c52e5702139fac4f91204a"
                        .to_string(),
                )
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c"
                                .to_string(),
                        )
                        .add_input(
                            TestTxInBuilder::new()
                                .prev_out_block_height(849998)
                                .prev_out_tx_hash("0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f".to_string())
                                .value(12_000)
                                .build(),
                        )
                        .add_output(TxOut {
                            value: 10_000,
                            script_pubkey: "0x76a914fb37342f6275b13936799def06f2eb4c0f20151588ac"
                                .to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849998)
                .hash(
                    "0x000000000000000000030b3451d402089d510234c665d130ebc3e8a1355633a0"
                        .to_string(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f"
                                .to_string(),
                        )
                        .add_output(TxOut {
                            value: 12_000,
                            script_pubkey: "".to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );

        let block_identifier = BlockIdentifier {
            index: 850000,
            hash: "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
        };
        let transaction_identifier = TransactionIdentifier {
            hash: "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
        };
        let Ok((result, pointer, _)) = compute_satoshi_number(
            &block_identifier,
            &transaction_identifier,
            0,
            8_000,
            &Arc::new(cache),
            &blocks_db,
            &config,
            &ctx,
        ) else {
            panic!();
        };

        assert_eq!(result.ordinal_number, 1971874375008000);
        assert_eq!(result.transfers, 2);
        assert_eq!(
            result.transaction_identifier_inscription.hash,
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string()
        );
        assert_eq!(pointer, 8000);
    }

    #[test]
    fn compute_sat_from_coinbase_fees() {
        let ctx = Context::empty();
        let config = Config::test_default();
        drop_all_dbs(&config);
        let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
        let cache = new_traversals_lazy_cache(100);

        // Insert blocks such that we land on a coinbase transaction but the inscription sat actually comes from a fee paid by
        // another tx in that block.
        store_tx_in_traversals_cache(
            &cache,
            850000,
            "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string(),
            9_000,
            1,
            8_000,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849999)
                .hash(
                    "0x00000000000000000000ec8da633f1fb0f8f281e43c52e5702139fac4f91204a"
                        .to_string(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c"
                                .to_string(),
                        )
                        .add_output(TxOut {
                            value: 312_500_000, // Full block subsidy.
                            script_pubkey: "".to_string(),
                        })
                        .build(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xfc47db8141ec6a74ee643b839aa73a44619c90d9000621f8752be1f875f2298f"
                                .to_string(),
                        )
                        // The fees will come from a previous block.
                        .add_input(
                            TestTxInBuilder::new()
                                .prev_out_block_height(849998)
                                .prev_out_tx_hash("0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f".to_string())
                                .value(20_000)
                                .build(),
                        )
                        .add_output(TxOut {
                            value: 9_000, // This makes fees spent to be where the inscription goes.
                            script_pubkey: "0x76a914fb37342f6275b13936799def06f2eb4c0f20151588ac"
                                .to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849998)
                .hash(
                    "0x000000000000000000030b3451d402089d510234c665d130ebc3e8a1355633a0"
                        .to_string(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f"
                                .to_string(),
                        )
                        .add_output(TxOut {
                            value: 20_000,
                            script_pubkey: "".to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );

        let block_identifier = BlockIdentifier {
            index: 850000,
            hash: "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
        };
        let transaction_identifier = TransactionIdentifier {
            hash: "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
        };
        let Ok((result, pointer, _)) = compute_satoshi_number(
            &block_identifier,
            &transaction_identifier,
            0,
            8_000,
            &Arc::new(cache),
            &blocks_db,
            &config,
            &ctx,
        ) else {
            panic!();
        };

        assert_eq!(result.ordinal_number, 1971874375017000);
        assert_eq!(result.transfers, 2);
        assert_eq!(
            result.transaction_identifier_inscription.hash,
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string()
        );
        assert_eq!(pointer, 8000);
    }

    #[test]
    fn compute_sat_from_non_spend_cached_transactions() {
        let ctx = Context::empty();
        let config = Config::test_default();
        drop_all_dbs(&config);
        let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
        let cache = new_traversals_lazy_cache(100);

        store_tx_in_traversals_cache(
            &cache,
            850000,
            "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string(),
            9_000,
            0,
            8_000,
        );
        store_tx_in_traversals_cache(
            &cache,
            849999,
            "0x00000000000000000000ec8da633f1fb0f8f281e43c52e5702139fac4f91204a".to_string(),
            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string(),
            "0xfc47db8141ec6a74ee643b839aa73a44619c90d9000621f8752be1f875f2298f".to_string(),
            0, // Non spend
            1,
            8_000,
        );

        let block_identifier = BlockIdentifier {
            index: 850000,
            hash: "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
        };
        let transaction_identifier = TransactionIdentifier {
            hash: "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
        };
        let Ok((result, _, _)) = compute_satoshi_number(
            &block_identifier,
            &transaction_identifier,
            0,
            8_000,
            &Arc::new(cache),
            &blocks_db,
            &config,
            &ctx,
        ) else {
            panic!();
        };

        assert_eq!(result.ordinal_number, 0);
        assert_eq!(result.transfers, 0);
    }

    #[test]
    fn compute_sat_from_non_spend_rocksdb_traversals() {
        let ctx = Context::empty();
        let config = Config::test_default();
        drop_all_dbs(&config);
        let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
        let cache = new_traversals_lazy_cache(100);

        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(850000)
                .hash(
                    "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187"
                        .to_string(),
                )
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9"
                                .to_string(),
                        )
                        .add_input(
                            TestTxInBuilder::new()
                                .prev_out_block_height(849999)
                                .prev_out_tx_hash("0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c".to_string())
                                .value(10_000)
                                .build(),
                        )
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849999)
                .hash(
                    "0x00000000000000000000ec8da633f1fb0f8f281e43c52e5702139fac4f91204a"
                        .to_string(),
                )
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xa077643d3411362c9f75377a832aee6666c73b4358ebccf98f6dad82e57bbe1c"
                                .to_string(),
                        )
                        .add_input(
                            TestTxInBuilder::new()
                                .prev_out_block_height(849998)
                                .prev_out_tx_hash("0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f".to_string())
                                .value(0) // Non spend
                                .build(),
                        )
                        .add_output(TxOut {
                            value: 10_000,
                            script_pubkey: "0x76a914fb37342f6275b13936799def06f2eb4c0f20151588ac"
                                .to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );
        insert_standardized_block(
            &TestBlockBuilder::new()
                .height(849998)
                .hash(
                    "0x000000000000000000030b3451d402089d510234c665d130ebc3e8a1355633a0"
                        .to_string(),
                )
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xcf90b73725382d7485868379b166cfd0614d507b547ea2af8a13cd6c6b7e837f"
                                .to_string(),
                        )
                        .add_output(TxOut {
                            value: 12_000,
                            script_pubkey: "".to_string(),
                        })
                        .build(),
                )
                .build(),
            &blocks_db,
            &ctx,
        );

        let block_identifier = BlockIdentifier {
            index: 850000,
            hash: "0x00000000000000000002a0b5db2a7f8d9087464c2586b546be7bce8eb53b8187".to_string(),
        };
        let transaction_identifier = TransactionIdentifier {
            hash: "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9".to_string(),
        };
        let Ok((result, _, _)) = compute_satoshi_number(
            &block_identifier,
            &transaction_identifier,
            0,
            8_000,
            &Arc::new(cache),
            &blocks_db,
            &config,
            &ctx,
        ) else {
            panic!();
        };

        assert_eq!(result.ordinal_number, 0);
        assert_eq!(result.transfers, 0);
    }
}
