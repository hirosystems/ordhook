use std::collections::{HashMap, HashSet, VecDeque};

use bitcoin::{Address, ScriptBuf};
use bitcoind::{try_debug, try_warn, types::bitcoin::TxIn, utils::Context};
use lru::LruCache;
use ordinals_parser::RuneId;
use tokio_postgres::Transaction;

use super::{input_rune_balance::InputRuneBalance, transaction_location::TransactionLocation};
use crate::db::{
    models::{
        db_ledger_entry::DbLedgerEntry, db_ledger_operation::DbLedgerOperation, db_rune::DbRune,
    },
    pg_get_input_rune_balances,
};

/// Takes all transaction inputs and transforms them into rune balances to be allocated for operations. Looks inside an output LRU
/// cache and the DB when there are cache misses.
///
/// # Arguments
///
/// * `inputs` - Raw transaction inputs
/// * `block_output_cache` - Cache with output balances produced by the current block
/// * `output_cache` - LRU cache with output balances
/// * `consumed_utxos` - HashSet to track UTXOs consumed from output_cache for later cleanup
/// * `db_tx` - DB transaction
/// * `ctx` - Context
pub async fn input_rune_balances_from_tx_inputs(
    inputs: &Vec<TxIn>,
    block_output_cache: &HashMap<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
    output_cache: &mut LruCache<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
    consumed_utxos: &mut HashSet<(String, u32)>,
    db_tx: &mut Transaction<'_>,
    ctx: &Context,
) -> HashMap<RuneId, VecDeque<InputRuneBalance>> {
    // Maps input index to all of its rune balances. Useful in order to keep rune inputs in order.
    let mut indexed_input_runes = HashMap::new();
    let mut cache_misses = vec![];

    // Look in both current block output cache and in long term LRU cache.
    for (i, input) in inputs.iter().enumerate() {
        let tx_id = input.previous_output.txid.hash[2..].to_string();
        let vout = input.previous_output.vout;
        let k = (tx_id.clone(), vout);
        if let Some(map) = block_output_cache.get(&k) {
            indexed_input_runes.insert(i as u32, map.clone());
            // Track block cache UTXO for removal from LRU after successful commit
            consumed_utxos.insert(k);
        } else if let Some(map) = output_cache.get(&k) {
            indexed_input_runes.insert(i as u32, map.clone());
            // Track LRU cache UTXO for removal after successful commit
            consumed_utxos.insert(k);
        } else {
            cache_misses.push((i as u32, tx_id, vout));
        }
    }
    // Look for cache misses in database. We don't need to `flush` the DB cache here because we've already looked in the current
    // block's output cache.
    if !cache_misses.is_empty() {
        let output_balances = pg_get_input_rune_balances(cache_misses, db_tx, ctx).await;
        indexed_input_runes.extend(output_balances);
    }

    let mut final_input_runes: HashMap<RuneId, VecDeque<InputRuneBalance>> = HashMap::new();
    let mut input_keys: Vec<u32> = indexed_input_runes.keys().copied().collect();
    input_keys.sort();
    for key in input_keys.iter() {
        let input_value = indexed_input_runes.get(key).unwrap();
        for (rune_id, vec) in input_value.iter() {
            if let Some(rune) = final_input_runes.get_mut(rune_id) {
                rune.extend(vec.clone());
            } else {
                final_input_runes.insert(*rune_id, VecDeque::from(vec.clone()));
            }
        }
    }
    final_input_runes
}

/// Moves data from the current block's output cache to the long-term LRU output cache. Clears the block output cache when done.
///
/// # Arguments
///
/// * `block_output_cache` - Block output cache
/// * `output_cache` - Output LRU cache
pub fn move_block_output_cache_to_output_cache(
    block_output_cache: &mut HashMap<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
    output_cache: &mut LruCache<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
) {
    for (k, block_output_map) in block_output_cache.iter() {
        if let Some(v) = output_cache.get_mut(k) {
            for (rune_id, balances) in block_output_map.iter() {
                if let Some(rune_balance) = v.get_mut(rune_id) {
                    rune_balance.extend(balances.clone());
                } else {
                    v.insert(*rune_id, balances.clone());
                }
            }
        } else {
            output_cache.push(k.clone(), block_output_map.clone());
        }
    }
    block_output_cache.clear();
}

/// Creates a new ledger entry while incrementing the `next_event_index`.
pub fn new_sequential_ledger_entry(
    location: &TransactionLocation,
    amount: Option<u128>,
    rune_id: RuneId,
    output: Option<u32>,
    address: Option<&String>,
    receiver_address: Option<&String>,
    operation: DbLedgerOperation,
    next_event_index: &mut u32,
) -> DbLedgerEntry {
    let entry = DbLedgerEntry::from_values(
        amount,
        rune_id,
        &location.block_hash,
        location.block_height,
        location.tx_index,
        *next_event_index,
        &location.tx_id,
        output,
        address,
        receiver_address,
        operation,
        location.timestamp,
    );
    *next_event_index += 1;
    entry
}

/// Moves rune balance from transaction inputs into a transaction output.
///
/// # Arguments
///
/// * `location` - Transaction location.
/// * `output` - Output where runes will be moved to. If `None`, runes are burned.
/// * `rune_id` - Rune that is being moved.
/// * `input_balances` - Balances input to this transaction for this rune. This value will be modified by the moves happening in
///   this function.
/// * `outputs` - Transaction outputs eligible to receive runes.
/// * `amount` - Amount of balance to move. If value is zero, all inputs will be moved to the output.
/// * `next_event_index` - Next sequential event index to create. This value will be modified.
/// * `ctx` - Context.
pub fn move_rune_balance_to_output(
    location: &TransactionLocation,
    output: Option<u32>,
    rune_id: &RuneId,
    input_balances: &mut VecDeque<InputRuneBalance>,
    outputs: &HashMap<u32, ScriptBuf>,
    amount: u128,
    next_event_index: &mut u32,
    ctx: &Context,
) -> Vec<DbLedgerEntry> {
    let mut results = vec![];
    // Who is this balance going to?
    let receiver_address = if let Some(output) = output {
        match outputs.get(&output) {
            Some(script) => match Address::from_script(script, location.network) {
                Ok(address) => Some(address.to_string()),
                Err(e) => {
                    try_warn!(
                        ctx,
                        "Unable to decode address for output {output}, {e} {location}"
                    );
                    None
                }
            },
            None => {
                try_debug!(
                    ctx,
                    "Attempted move to non-eligible output {output}, runes will be burnt {location}"
                );
                None
            }
        }
    } else {
        None
    };
    let operation = if receiver_address.is_some() {
        DbLedgerOperation::Send
    } else {
        DbLedgerOperation::Burn
    };

    // Gather balance to be received by taking it from the available inputs until the amount to move is satisfied.
    let mut total_sent = 0;
    let mut senders = vec![];
    loop {
        // Do we still have input balance left to move?
        let Some(input_bal) = input_balances.pop_front() else {
            break;
        };
        // Select the correct move amount.
        let balance_taken = if amount == 0 {
            input_bal.amount
        } else {
            input_bal.amount.min(amount - total_sent)
        };
        total_sent += balance_taken;
        // If the input balance came from an address, add to `Send` operations.
        if let Some(sender_address) = input_bal.address.clone() {
            senders.push((balance_taken, sender_address));
        }
        // Is there still some balance left on this input? If so, keep it for later but break the loop because we've satisfied the
        // move amount.
        if balance_taken < input_bal.amount {
            input_balances.push_front(InputRuneBalance {
                address: input_bal.address,
                amount: input_bal.amount - balance_taken,
            });
            break;
        }
        // Have we finished moving balance?
        if total_sent == amount {
            break;
        }
    }
    // Add the "receive" entry, if applicable.
    if receiver_address.is_some() && total_sent > 0 {
        results.push(new_sequential_ledger_entry(
            location,
            Some(total_sent),
            *rune_id,
            output,
            receiver_address.as_ref(),
            None,
            DbLedgerOperation::Receive,
            next_event_index,
        ));
        try_debug!(
            ctx,
            "{operation} {rune_id} ({total_sent}) {address} {location}",
            operation = DbLedgerOperation::Receive.to_string(),
            address = receiver_address.as_ref().unwrap(),
        );
    }
    // Add the "send"/"burn" entries.
    for (balance_taken, sender_address) in senders.iter() {
        results.push(new_sequential_ledger_entry(
            location,
            Some(*balance_taken),
            *rune_id,
            output,
            Some(sender_address),
            receiver_address.as_ref(),
            operation.clone(),
            next_event_index,
        ));
        try_debug!(
            ctx,
            "{operation} {rune_id} ({balance_taken}) {sender_address} -> {receiver_address:?} {location}"
        );
    }
    results
}

/// Determines if a mint is valid depending on the rune's mint terms.
pub fn is_rune_mintable(
    db_rune: &DbRune,
    total_mints: u128,
    location: &TransactionLocation,
) -> bool {
    if db_rune.cenotaph {
        return false;
    }
    if db_rune.terms_amount.is_none() {
        return false;
    }
    if let Some(terms_cap) = db_rune.terms_cap {
        if total_mints >= terms_cap.0 {
            return false;
        }
    }
    if let Some(terms_height_start) = db_rune.terms_height_start {
        if location.block_height < terms_height_start.0 {
            return false;
        }
    }
    if let Some(terms_height_end) = db_rune.terms_height_end {
        if location.block_height > terms_height_end.0 {
            return false;
        }
    }
    if let Some(terms_offset_start) = db_rune.terms_offset_start {
        if location.block_height < db_rune.block_height.0 + terms_offset_start.0 {
            return false;
        }
    }
    if let Some(terms_offset_end) = db_rune.terms_offset_end {
        if location.block_height > db_rune.block_height.0 + terms_offset_end.0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod test {
    mod move_balance {
        use std::collections::{HashMap, VecDeque};

        use bitcoin::ScriptBuf;
        use bitcoind::utils::Context;
        use maplit::hashmap;
        use ordinals_parser::RuneId;

        use crate::db::{
            cache::{
                input_rune_balance::InputRuneBalance, transaction_location::TransactionLocation,
                utils::move_rune_balance_to_output,
            },
            models::db_ledger_operation::DbLedgerOperation,
        };

        fn dummy_eligible_output() -> HashMap<u32, ScriptBuf> {
            hashmap! {
                0u32 => ScriptBuf::from_hex(
                    "5120388dfba1b0069bbb0ad5eef62c1a94c46e91a3454accf40bf34b80f75e2708db",
                )
                .unwrap()
            }
        }

        #[test]
        fn ledger_writes_receive_before_send() {
            let address =
                Some("bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string());
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.address(address.clone()).amount(1000);
            available_inputs.push_back(input1);
            let mut input2 = InputRuneBalance::dummy();
            input2.address(None).amount(1000);
            available_inputs.push_back(input2);
            let eligible_outputs = dummy_eligible_output();
            let mut next_event_index = 0;

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                0,
                &mut next_event_index,
                &Context::empty(),
            );

            let receive = results.get(0).unwrap();
            assert_eq!(receive.event_index.0, 0u32);
            assert_eq!(receive.operation, DbLedgerOperation::Receive);
            assert_eq!(receive.amount.unwrap().0, 2000u128);

            let send = results.get(1).unwrap();
            assert_eq!(send.event_index.0, 1u32);
            assert_eq!(send.operation, DbLedgerOperation::Send);
            assert_eq!(send.amount.unwrap().0, 1000u128);

            assert_eq!(results.len(), 2);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn move_to_empty_output_is_burned() {
            let address =
                Some("bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string());
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.address(address.clone()).amount(1000);
            available_inputs.push_back(input1);

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                None, // Burn
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &HashMap::new(),
                0,
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 1);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Burn);
            assert_eq!(entry1.address, address);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn moves_partial_input_balance() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(5000); // More than required in this move.
            available_inputs.push_back(input1);
            let eligible_outputs = dummy_eligible_output();

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                1000, // Less than total available in first input.
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 2);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Receive);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            let entry2 = results.get(1).unwrap();
            assert_eq!(entry2.operation, DbLedgerOperation::Send);
            assert_eq!(entry2.amount.unwrap().0, 1000);
            // Remainder is still in available inputs.
            let remaining = available_inputs.get(0).unwrap();
            assert_eq!(remaining.amount, 4000);
        }

        #[test]
        fn moves_insufficient_input_balance() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(1000); // Insufficient.
            available_inputs.push_back(input1);
            let eligible_outputs = dummy_eligible_output();

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                3000, // More than total available in input.
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 2);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Receive);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            let entry2 = results.get(1).unwrap();
            assert_eq!(entry2.operation, DbLedgerOperation::Send);
            assert_eq!(entry2.amount.unwrap().0, 1000);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn moves_all_remaining_balance() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(6000);
            available_inputs.push_back(input1);
            let mut input2 = InputRuneBalance::dummy();
            input2.amount(2000);
            available_inputs.push_back(input2);
            let mut input3 = InputRuneBalance::dummy();
            input3.amount(2000);
            available_inputs.push_back(input3);
            let eligible_outputs = dummy_eligible_output();

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                0, // Move all.
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 4);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Receive);
            assert_eq!(entry1.amount.unwrap().0, 10000);
            let entry2 = results.get(1).unwrap();
            assert_eq!(entry2.operation, DbLedgerOperation::Send);
            assert_eq!(entry2.amount.unwrap().0, 6000);
            let entry3 = results.get(2).unwrap();
            assert_eq!(entry3.operation, DbLedgerOperation::Send);
            assert_eq!(entry3.amount.unwrap().0, 2000);
            let entry4 = results.get(3).unwrap();
            assert_eq!(entry4.operation, DbLedgerOperation::Send);
            assert_eq!(entry4.amount.unwrap().0, 2000);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn move_to_output_with_address_failure_is_burned() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(1000);
            available_inputs.push_back(input1);
            let mut eligible_outputs = HashMap::new();
            // Broken script buf that yields no address.
            eligible_outputs.insert(0u32, ScriptBuf::from_hex("0101010101").unwrap());

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                1000,
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 1);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Burn);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn move_to_nonexistent_output_is_burned() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(1000);
            available_inputs.push_back(input1);
            let eligible_outputs = dummy_eligible_output();

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(5), // Output does not exist.
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                1000,
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 1);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Burn);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            assert_eq!(available_inputs.len(), 0);
        }

        #[test]
        fn send_not_generated_on_minted_balance() {
            let mut available_inputs = VecDeque::new();
            let mut input1 = InputRuneBalance::dummy();
            input1.amount(1000).address(None); // No address because it's a mint.
            available_inputs.push_back(input1);
            let eligible_outputs = dummy_eligible_output();

            let results = move_rune_balance_to_output(
                &TransactionLocation::dummy(),
                Some(0),
                &RuneId::new(840000, 25).unwrap(),
                &mut available_inputs,
                &eligible_outputs,
                1000,
                &mut 0,
                &Context::empty(),
            );

            assert_eq!(results.len(), 1);
            let entry1 = results.get(0).unwrap();
            assert_eq!(entry1.operation, DbLedgerOperation::Receive);
            assert_eq!(entry1.amount.unwrap().0, 1000);
            assert_eq!(available_inputs.len(), 0);
        }
    }

    mod mint_validation {
        use postgres::types::{PgNumericU128, PgNumericU64};
        use test_case::test_case;

        use crate::db::{
            cache::{transaction_location::TransactionLocation, utils::is_rune_mintable},
            models::db_rune::DbRune,
        };

        #[test_case(840000 => false; "early block")]
        #[test_case(840500 => false; "late block")]
        #[test_case(840150 => true; "block in window")]
        #[test_case(840100 => true; "first block")]
        #[test_case(840200 => true; "last block")]
        fn mint_block_height_terms_are_validated(block_height: u64) -> bool {
            let mut rune = DbRune::factory();
            rune.terms_height_start(Some(PgNumericU64(840100)));
            rune.terms_height_end(Some(PgNumericU64(840200)));
            let mut location = TransactionLocation::dummy();
            location.block_height(block_height);
            is_rune_mintable(&rune, 0, &location)
        }

        #[test_case(840000 => false; "early block")]
        #[test_case(840500 => false; "late block")]
        #[test_case(840150 => true; "block in window")]
        #[test_case(840100 => true; "first block")]
        #[test_case(840200 => true; "last block")]
        fn mint_block_offset_terms_are_validated(block_height: u64) -> bool {
            let mut rune = DbRune::factory();
            rune.terms_offset_start(Some(PgNumericU64(100)));
            rune.terms_offset_end(Some(PgNumericU64(200)));
            let mut location = TransactionLocation::dummy();
            location.block_height(block_height);
            is_rune_mintable(&rune, 0, &location)
        }

        #[test_case(0 => true; "first mint")]
        #[test_case(49 => true; "last mint")]
        #[test_case(50 => false; "out of range")]
        fn mint_cap_is_validated(cap: u128) -> bool {
            let mut rune = DbRune::factory();
            rune.terms_cap(Some(PgNumericU128(50)));
            is_rune_mintable(&rune, cap, &TransactionLocation::dummy())
        }
    }

    mod sequential_ledger_entry {
        use ordinals_parser::RuneId;

        use crate::db::{
            cache::{
                transaction_location::TransactionLocation, utils::new_sequential_ledger_entry,
            },
            models::db_ledger_operation::DbLedgerOperation,
        };

        #[test]
        fn increments_event_index() {
            let location = TransactionLocation::dummy();
            let rune_id = RuneId::new(840000, 25).unwrap();
            let address =
                Some("bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string());
            let mut event_index = 0u32;

            let event0 = new_sequential_ledger_entry(
                &location,
                Some(100),
                rune_id,
                Some(0),
                address.as_ref(),
                None,
                DbLedgerOperation::Receive,
                &mut event_index,
            );
            assert_eq!(event0.event_index.0, 0);
            assert_eq!(event0.amount.unwrap().0, 100);
            assert_eq!(event0.address, address);

            let event1 = new_sequential_ledger_entry(
                &location,
                Some(300),
                rune_id,
                Some(0),
                None,
                None,
                DbLedgerOperation::Receive,
                &mut event_index,
            );
            assert_eq!(event1.event_index.0, 1);
            assert_eq!(event1.amount.unwrap().0, 300);
            assert_eq!(event1.address, None);

            assert_eq!(event_index, 2);
        }
    }

    mod input_balances {
        use std::{collections::HashSet, num::NonZeroUsize};

        use bitcoind::{
            types::{
                bitcoin::{OutPoint, TxIn},
                TransactionIdentifier,
            },
            utils::Context,
        };
        use lru::LruCache;
        use maplit::hashmap;
        use ordinals_parser::RuneId;

        use crate::db::{
            cache::{
                input_rune_balance::InputRuneBalance, utils::input_rune_balances_from_tx_inputs,
            },
            models::{db_ledger_entry::DbLedgerEntry, db_ledger_operation::DbLedgerOperation},
            pg_insert_ledger_entries, pg_test_client, pg_test_roll_back_migrations,
        };

        #[tokio::test]
        async fn from_block_cache() {
            let inputs = vec![TxIn {
                previous_output: OutPoint {
                    txid: TransactionIdentifier {
                        hash: "0x045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(),
                    },
                    vout: 1,
                    value: 100,
                    block_height: 840000,
                },
                script_sig: "".to_string(),
                sequence: 0,
                witness: vec![],
            }];
            let rune_id = RuneId::new(840000, 25).unwrap();
            let block_output_cache = hashmap! {
                ("045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(), 1) => hashmap! {
                                rune_id => vec![InputRuneBalance { address: None, amount: 2000 }]
                            }
            };
            let mut output_cache = LruCache::new(NonZeroUsize::new(1).unwrap());
            let mut consumed_utxos = HashSet::new();
            let ctx = Context::empty();

            let mut pg_client = pg_test_client(true, &ctx).await;
            let mut db_tx = pg_client.transaction().await.unwrap();
            let results = input_rune_balances_from_tx_inputs(
                &inputs,
                &block_output_cache,
                &mut output_cache,
                &mut consumed_utxos,
                &mut db_tx,
                &ctx,
            )
            .await;
            let _ = db_tx.rollback().await;
            pg_test_roll_back_migrations(&mut pg_client, &ctx).await;

            assert_eq!(results.len(), 1);
            let rune_results = results.get(&rune_id).unwrap();
            assert_eq!(rune_results.len(), 1);
            let input_bal = rune_results.get(0).unwrap();
            assert_eq!(input_bal.address, None);
            assert_eq!(input_bal.amount, 2000);
        }

        #[tokio::test]
        async fn from_lru_cache() {
            let inputs = vec![TxIn {
                previous_output: OutPoint {
                    txid: TransactionIdentifier {
                        hash: "0x045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(),
                    },
                    vout: 1,
                    value: 100,
                    block_height: 840000,
                },
                script_sig: "".to_string(),
                sequence: 0,
                witness: vec![],
            }];
            let rune_id = RuneId::new(840000, 25).unwrap();
            let block_output_cache = hashmap! {};
            let mut output_cache = LruCache::new(NonZeroUsize::new(1).unwrap());
            output_cache.put(
                (
                    "045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b".to_string(),
                    1,
                ),
                hashmap! {
                    rune_id => vec![InputRuneBalance { address: None, amount: 2000 }]
                },
            );
            let mut consumed_utxos = HashSet::new();
            let ctx = Context::empty();

            let mut pg_client = pg_test_client(true, &ctx).await;
            let mut db_tx = pg_client.transaction().await.unwrap();
            let results = input_rune_balances_from_tx_inputs(
                &inputs,
                &block_output_cache,
                &mut output_cache,
                &mut consumed_utxos,
                &mut db_tx,
                &ctx,
            )
            .await;
            let _ = db_tx.rollback().await;
            pg_test_roll_back_migrations(&mut pg_client, &ctx).await;

            assert_eq!(results.len(), 1);
            let rune_results = results.get(&rune_id).unwrap();
            assert_eq!(rune_results.len(), 1);
            let input_bal = rune_results.get(0).unwrap();
            assert_eq!(input_bal.address, None);
            assert_eq!(input_bal.amount, 2000);
        }

        #[tokio::test]
        async fn from_db() {
            let inputs = vec![TxIn {
                previous_output: OutPoint {
                    txid: TransactionIdentifier {
                        hash: "0x045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(),
                    },
                    vout: 1,
                    value: 100,
                    block_height: 840000,
                },
                script_sig: "".to_string(),
                sequence: 0,
                witness: vec![],
            }];
            let rune_id = RuneId::new(840000, 25).unwrap();
            let block_output_cache = hashmap! {};
            let mut output_cache = LruCache::new(NonZeroUsize::new(1).unwrap());
            let mut consumed_utxos = HashSet::new();
            let ctx = Context::empty();

            let mut pg_client = pg_test_client(true, &ctx).await;
            let mut db_tx = pg_client.transaction().await.unwrap();

            let entry = DbLedgerEntry::from_values(
                Some(2000),
                rune_id,
                &"0x0000000000000000000044642cc1f64c22579d46a2a149ef2a51f9c98cb622e1".to_string(),
                840000,
                0,
                0,
                &"0x045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b".to_string(),
                Some(1),
                None,
                None,
                DbLedgerOperation::Receive,
                0,
            );
            let _ = pg_insert_ledger_entries(&vec![entry], &mut db_tx, &ctx).await;

            let results = input_rune_balances_from_tx_inputs(
                &inputs,
                &block_output_cache,
                &mut output_cache,
                &mut consumed_utxos,
                &mut db_tx,
                &ctx,
            )
            .await;
            let _ = db_tx.rollback().await;
            pg_test_roll_back_migrations(&mut pg_client, &ctx).await;

            assert_eq!(results.len(), 1);
            let rune_results = results.get(&rune_id).unwrap();
            assert_eq!(rune_results.len(), 1);
            let input_bal = rune_results.get(0).unwrap();
            assert_eq!(input_bal.address, None);
            assert_eq!(input_bal.amount, 2000);
        }

        #[tokio::test]
        async fn inputs_without_balances() {
            let inputs = vec![TxIn {
                previous_output: OutPoint {
                    txid: TransactionIdentifier {
                        hash: "0x045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(),
                    },
                    vout: 1,
                    value: 100,
                    block_height: 840000,
                },
                script_sig: "".to_string(),
                sequence: 0,
                witness: vec![],
            }];
            let block_output_cache = hashmap! {};
            let mut output_cache = LruCache::new(NonZeroUsize::new(1).unwrap());
            let mut consumed_utxos = HashSet::new();
            let ctx = Context::empty();

            let mut pg_client = pg_test_client(true, &ctx).await;
            let mut db_tx = pg_client.transaction().await.unwrap();
            let results = input_rune_balances_from_tx_inputs(
                &inputs,
                &block_output_cache,
                &mut output_cache,
                &mut consumed_utxos,
                &mut db_tx,
                &ctx,
            )
            .await;
            let _ = db_tx.rollback().await;
            pg_test_roll_back_migrations(&mut pg_client, &ctx).await;

            assert_eq!(results.len(), 0);
        }
    }

    mod cache_move {
        use std::num::NonZeroUsize;

        use lru::LruCache;
        use maplit::hashmap;
        use ordinals_parser::RuneId;

        use crate::db::cache::{
            input_rune_balance::InputRuneBalance, utils::move_block_output_cache_to_output_cache,
        };

        #[test]
        fn moves_to_lru_output_cache_and_clears() {
            let rune_id = RuneId::new(840000, 25).unwrap();
            let mut block_output_cache = hashmap! {
                ("045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b"
                            .to_string(), 1) => hashmap! {
                                rune_id => vec![InputRuneBalance { address: None, amount: 2000 }]
                            }
            };
            let mut output_cache = LruCache::new(NonZeroUsize::new(1).unwrap());

            move_block_output_cache_to_output_cache(&mut block_output_cache, &mut output_cache);

            let moved_val = output_cache
                .get(&(
                    "045fe33f1174d6a72084e751735a89746a259c6d3e418b65c03ec0740f924c7b".to_string(),
                    1,
                ))
                .unwrap();
            assert_eq!(moved_val.len(), 1);
            let balances = moved_val.get(&rune_id).unwrap();
            assert_eq!(balances.len(), 1);
            let balance = balances.get(0).unwrap();
            assert_eq!(balance.address, None);
            assert_eq!(balance.amount, 2000);
            assert_eq!(block_output_cache.len(), 0);
        }
    }
}
