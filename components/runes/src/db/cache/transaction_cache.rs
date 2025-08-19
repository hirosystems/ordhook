use std::{
    collections::{HashMap, VecDeque},
    vec,
};

use bitcoin::ScriptBuf;
use bitcoind::{try_debug, try_warn, utils::Context};
use ordinals_parser::{Cenotaph, Edict, Etching, Rune, RuneId};

use super::{
    input_rune_balance::InputRuneBalance, transaction_location::TransactionLocation,
    utils::move_rune_balance_to_output,
};
use crate::db::{
    cache::utils::{is_rune_mintable, new_sequential_ledger_entry},
    models::{
        db_ledger_entry::DbLedgerEntry, db_ledger_operation::DbLedgerOperation, db_rune::DbRune,
    },
};

/// Holds cached data relevant to a single transaction during indexing.
pub struct TransactionCache {
    pub location: TransactionLocation,
    /// Sequential index of the ledger entry we're inserting next for this transaction. Will be increased with each generated
    /// entry.
    next_event_index: u32,
    /// Rune etched during this transaction, if any.
    pub etching: Option<DbRune>,
    /// The output where all unallocated runes will be transferred to. Set to the first eligible output by default but can be
    /// overridden by a Runestone.
    pub output_pointer: Option<u32>,
    /// Holds input runes for the current transaction (input to this tx, premined or minted). Balances in the vector are in the
    /// order in which they were input to this transaction.
    pub input_runes: HashMap<RuneId, VecDeque<InputRuneBalance>>,
    /// Non-OP_RETURN outputs in this transaction
    eligible_outputs: HashMap<u32, ScriptBuf>,
    /// Total outputs contained in this transaction, including non-eligible outputs.
    total_outputs: u32,
}

impl TransactionCache {
    pub fn new(
        location: TransactionLocation,
        input_runes: HashMap<RuneId, VecDeque<InputRuneBalance>>,
        eligible_outputs: HashMap<u32, ScriptBuf>,
        first_eligible_output: Option<u32>,
        total_outputs: u32,
    ) -> Self {
        TransactionCache {
            location,
            next_event_index: 0,
            etching: None,
            output_pointer: first_eligible_output,
            input_runes,
            eligible_outputs,
            total_outputs,
        }
    }

    #[cfg(test)]
    pub fn empty(location: TransactionLocation) -> Self {
        TransactionCache {
            location,
            next_event_index: 0,
            etching: None,
            output_pointer: None,
            input_runes: maplit::hashmap! {},
            eligible_outputs: maplit::hashmap! {},
            total_outputs: 0,
        }
    }

    /// Burns the rune balances input to this transaction.
    pub fn apply_cenotaph_input_burn(&mut self, _cenotaph: &Cenotaph) -> Vec<DbLedgerEntry> {
        let mut results = vec![];
        for (rune_id, unallocated) in self.input_runes.iter() {
            for balance in unallocated {
                results.push(new_sequential_ledger_entry(
                    &self.location,
                    Some(balance.amount),
                    *rune_id,
                    None,
                    balance.address.as_ref(),
                    None,
                    DbLedgerOperation::Burn,
                    &mut self.next_event_index,
                ));
            }
        }
        self.input_runes.clear();
        results
    }

    /// Moves remaining input runes to the correct output depending on runestone configuration. Must be called once the processing
    /// for a transaction is complete.
    pub fn allocate_remaining_balances(&mut self, ctx: &Context) -> Vec<DbLedgerEntry> {
        let mut results = vec![];
        for (rune_id, unallocated) in self.input_runes.iter_mut() {
            #[cfg(not(feature = "release"))]
            for input in unallocated.iter() {
                try_debug!(
                    ctx,
                    "Assign unallocated {rune_id} to pointer {output_pointer:?} {address:?} ({amount}) {location}",
                    output_pointer = self.output_pointer,
                    address = &input.address,
                    amount = input.amount,
                    location = self.location.to_string()
                );
            }
            results.extend(move_rune_balance_to_output(
                &self.location,
                self.output_pointer,
                rune_id,
                unallocated,
                &self.eligible_outputs,
                0, // All of it
                &mut self.next_event_index,
                ctx,
            ));
        }
        self.input_runes.clear();
        results
    }

    pub fn apply_etching(
        &mut self,
        etching: &Etching,
        number: u32,
    ) -> (RuneId, DbRune, DbLedgerEntry) {
        let rune_id = self.location.rune_id();
        let db_rune = DbRune::from_etching(etching, number, &self.location);
        self.etching = Some(db_rune.clone());
        // Move pre-mined balance to input runes.
        if let Some(premine) = etching.premine {
            self.add_input_runes(
                &rune_id,
                InputRuneBalance {
                    address: None,
                    amount: premine,
                },
            );
        }
        let entry = new_sequential_ledger_entry(
            &self.location,
            None,
            rune_id,
            None,
            None,
            None,
            DbLedgerOperation::Etching,
            &mut self.next_event_index,
        );
        (rune_id, db_rune, entry)
    }

    pub fn apply_cenotaph_etching(
        &mut self,
        rune: &Rune,
        number: u32,
    ) -> (RuneId, DbRune, DbLedgerEntry) {
        let rune_id = self.location.rune_id();
        // If the runestone that produced the cenotaph contained an etching, the etched rune has supply zero and is unmintable.
        let db_rune = DbRune::from_cenotaph_etching(rune, number, &self.location);
        self.etching = Some(db_rune.clone());
        let entry = new_sequential_ledger_entry(
            &self.location,
            None,
            rune_id,
            None,
            None,
            None,
            DbLedgerOperation::Etching,
            &mut self.next_event_index,
        );
        (rune_id, db_rune, entry)
    }

    pub fn apply_mint(
        &mut self,
        rune_id: &RuneId,
        total_mints: u128,
        db_rune: &DbRune,
        ctx: &Context,
    ) -> Option<DbLedgerEntry> {
        if !is_rune_mintable(db_rune, total_mints, &self.location) {
            try_debug!(
                ctx,
                "Invalid mint {rune_id} {location}",
                location = self.location.to_string()
            );
            return None;
        }
        let terms_amount = db_rune.terms_amount.unwrap();
        try_debug!(
            ctx,
            "MINT {rune_id} ({spaced_name}) {amount} {location}",
            spaced_name = &db_rune.spaced_name,
            amount = terms_amount.0,
            location = self.location.to_string()
        );
        self.add_input_runes(
            rune_id,
            InputRuneBalance {
                address: None,
                amount: terms_amount.0,
            },
        );
        Some(new_sequential_ledger_entry(
            &self.location,
            Some(terms_amount.0),
            *rune_id,
            None,
            None,
            None,
            DbLedgerOperation::Mint,
            &mut self.next_event_index,
        ))
    }

    pub fn apply_cenotaph_mint(
        &mut self,
        rune_id: &RuneId,
        total_mints: u128,
        db_rune: &DbRune,
        ctx: &Context,
    ) -> Option<DbLedgerEntry> {
        if !is_rune_mintable(db_rune, total_mints, &self.location) {
            try_debug!(
                ctx,
                "Invalid mint {rune_id} {location}",
                location = self.location.to_string()
            );
            return None;
        }
        let terms_amount = db_rune.terms_amount.unwrap();
        try_debug!(
            ctx,
            "CENOTAPH MINT {spaced_name} {amount} {location}",
            spaced_name = &db_rune.spaced_name,
            amount = terms_amount.0,
            location = self.location.to_string()
        );
        // This entry does not go in the input runes, it gets burned immediately.
        Some(new_sequential_ledger_entry(
            &self.location,
            Some(terms_amount.0),
            *rune_id,
            None,
            None,
            None,
            DbLedgerOperation::Burn,
            &mut self.next_event_index,
        ))
    }

    pub fn apply_edict(&mut self, edict: &Edict, ctx: &Context) -> Vec<DbLedgerEntry> {
        // Find this rune.
        let rune_id = if edict.id.block == 0 && edict.id.tx == 0 {
            let Some(etching) = self.etching.as_ref() else {
                try_warn!(
                    ctx,
                    "Attempted edict for nonexistent rune 0:0 {location}",
                    location = self.location.to_string()
                );
                return vec![];
            };
            etching.rune_id()
        } else {
            edict.id
        };
        // Take all the available inputs for the rune we're trying to move.
        let Some(available_inputs) = self.input_runes.get_mut(&rune_id) else {
            try_debug!(
                ctx,
                "No unallocated runes {id} remain for edict {location}",
                id = edict.id.to_string(),
                location = self.location.to_string()
            );
            return vec![];
        };
        // Calculate the maximum unallocated balance we can move.
        let unallocated = available_inputs
            .iter()
            .map(|b| b.amount)
            .reduce(|acc, e| acc + e)
            .unwrap_or(0);
        // Perform movements.
        let mut results = vec![];
        if self.eligible_outputs.is_empty() {
            // No eligible outputs means burn.
            try_debug!(
                ctx,
                "No eligible outputs for edict on rune {id} {location}",
                id = edict.id.to_string(),
                location = self.location.to_string()
            );
            results.extend(move_rune_balance_to_output(
                &self.location,
                None, // This will force a burn.
                &rune_id,
                available_inputs,
                &self.eligible_outputs,
                edict.amount,
                &mut self.next_event_index,
                ctx,
            ));
        } else {
            match edict.output {
                // An edict with output equal to the number of transaction outputs allocates `amount` runes to each non-OP_RETURN
                // output in order.
                output if output == self.total_outputs => {
                    let mut output_keys: Vec<u32> = self.eligible_outputs.keys().cloned().collect();
                    output_keys.sort();
                    if edict.amount == 0 {
                        // Divide equally. If the number of unallocated runes is not divisible by the number of non-OP_RETURN outputs,
                        // 1 additional rune is assigned to the first R non-OP_RETURN outputs, where R is the remainder after dividing
                        // the balance of unallocated units of rune id by the number of non-OP_RETURN outputs.
                        let len = self.eligible_outputs.len() as u128;
                        let per_output = unallocated / len;
                        let mut remainder = unallocated % len;
                        for output in output_keys {
                            let mut extra = 0;
                            if remainder > 0 {
                                extra = 1;
                                remainder -= 1;
                            }
                            results.extend(move_rune_balance_to_output(
                                &self.location,
                                Some(output),
                                &rune_id,
                                available_inputs,
                                &self.eligible_outputs,
                                per_output + extra,
                                &mut self.next_event_index,
                                ctx,
                            ));
                        }
                    } else {
                        // Give `amount` to all outputs or until unallocated runs out.
                        for output in output_keys {
                            let amount = edict.amount.min(unallocated);
                            results.extend(move_rune_balance_to_output(
                                &self.location,
                                Some(output),
                                &rune_id,
                                available_inputs,
                                &self.eligible_outputs,
                                amount,
                                &mut self.next_event_index,
                                ctx,
                            ));
                        }
                    }
                }
                // Send balance to the output specified by the edict.
                output if output < self.total_outputs => {
                    let mut amount = edict.amount;
                    if edict.amount == 0 {
                        amount = unallocated;
                    }
                    results.extend(move_rune_balance_to_output(
                        &self.location,
                        Some(edict.output),
                        &rune_id,
                        available_inputs,
                        &self.eligible_outputs,
                        amount,
                        &mut self.next_event_index,
                        ctx,
                    ));
                }
                _ => {
                    try_debug!(
                        ctx,
                        "Edict for {id} attempted move to nonexistent output {output}, amount will be burnt {location}",
                        id = edict.id.to_string(),
                        output = edict.output,
                        location = self.location.to_string()
                    );
                    results.extend(move_rune_balance_to_output(
                        &self.location,
                        None, // Burn.
                        &rune_id,
                        available_inputs,
                        &self.eligible_outputs,
                        edict.amount,
                        &mut self.next_event_index,
                        ctx,
                    ));
                }
            }
        }
        results
    }

    fn add_input_runes(&mut self, rune_id: &RuneId, entry: InputRuneBalance) {
        if let Some(balance) = self.input_runes.get_mut(rune_id) {
            balance.push_back(entry);
        } else {
            let mut vec = VecDeque::new();
            vec.push_back(entry);
            self.input_runes.insert(*rune_id, vec);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use bitcoin::ScriptBuf;
    use bitcoind::utils::Context;
    use maplit::hashmap;
    use ordinals_parser::{Edict, Etching, Rune, Terms};

    use super::TransactionCache;
    use crate::db::{
        cache::{
            input_rune_balance::InputRuneBalance, transaction_location::TransactionLocation,
            utils::is_rune_mintable,
        },
        models::{db_ledger_operation::DbLedgerOperation, db_rune::DbRune},
    };

    #[test]
    fn etches_rune() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());
        let etching = Etching {
            divisibility: Some(2),
            premine: Some(1000),
            rune: Some(Rune::reserved(location.block_height, location.tx_index)),
            spacers: None,
            symbol: Some('x'),
            terms: Some(Terms {
                amount: Some(1000),
                cap: None,
                height: (None, None),
                offset: (None, None),
            }),
            turbo: true,
        };
        let (rune_id, db_rune, db_ledger_entry) = cache.apply_etching(&etching, 1);

        assert_eq!(rune_id.block, 840000);
        assert_eq!(rune_id.tx, 0);
        assert_eq!(db_rune.id, "840000:0");
        assert_eq!(db_rune.name, "AAAAAAAAAAAAAAAAZOMJMODBYFG");
        assert_eq!(db_rune.number.0, 1);
        assert_eq!(db_ledger_entry.operation, DbLedgerOperation::Etching);
        assert_eq!(db_ledger_entry.rune_id, "840000:0");
    }

    #[test]
    fn etches_rune_with_omitted_name_generates_reserved() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());
        let etching = Etching {
            divisibility: Some(2),
            premine: Some(1000),
            rune: None, // Omitted name → should allocate reserved rune
            spacers: None,
            symbol: Some('x'),
            terms: Some(Terms {
                amount: Some(1000),
                cap: None,
                height: (None, None),
                offset: (None, None),
            }),
            turbo: true,
        };

        let (_rune_id, db_rune, db_ledger_entry) = cache.apply_etching(&etching, 1);

        // Expected reserved rune string for this location
        let expected_reserved_name =
            Rune::reserved(location.block_height, location.tx_index).to_string();

        assert_eq!(db_rune.name, expected_reserved_name);
        assert_eq!(db_ledger_entry.operation, DbLedgerOperation::Etching);
    }

    #[test]
    // TODO add cenotaph field to DbRune before filling this in
    fn etches_cenotaph_rune() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());

        // Create a cenotaph rune
        let rune = Rune::reserved(location.block_height, location.tx_index);
        let number = 2;

        let (_rune_id, db_rune, db_ledger_entry) = cache.apply_cenotaph_etching(&rune, number);

        // // the etched rune has supply zero and is unmintable.
        assert_eq!(is_rune_mintable(&db_rune, 0, &location), false);
        assert_eq!(db_ledger_entry.amount, None);
        assert_eq!(db_rune.id, "840000:0");
        assert_eq!(db_ledger_entry.operation, DbLedgerOperation::Etching);
        assert_eq!(db_ledger_entry.rune_id, "840000:0");
    }

    #[test]
    fn mints_rune() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());
        let db_rune = &DbRune::factory();
        let rune_id = &db_rune.rune_id();

        let ledger_entry = cache.apply_mint(&rune_id, 0, &db_rune, &Context::empty());

        assert!(ledger_entry.is_some());
        let ledger_entry = ledger_entry.unwrap();
        assert_eq!(ledger_entry.operation, DbLedgerOperation::Mint);
        assert_eq!(ledger_entry.rune_id, rune_id.to_string());
        // ledger entry is minted with the correct amount
        assert_eq!(ledger_entry.amount, Some(db_rune.terms_amount.unwrap()));

        // minted amount is added to the input runes (`cache.input_runes`)
        assert!(cache.input_runes.contains_key(&rune_id));
    }

    #[test]
    fn does_not_mint_fully_minted_rune() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());
        let etching = Etching {
            divisibility: Some(2),
            premine: Some(1000),
            rune: Some(Rune::reserved(location.block_height, location.tx_index)),
            spacers: None,
            symbol: Some('x'),
            terms: Some(Terms {
                amount: Some(1000),
                cap: Some(1000),
                height: (None, None),
                offset: (None, None),
            }),
            turbo: true,
        };
        let (rune_id, db_rune, _db_ledger_entry) = cache.apply_etching(&etching, 1);
        let ledger_entry = cache.apply_mint(&rune_id, 1000, &db_rune, &Context::empty());
        assert!(ledger_entry.is_none());
    }

    #[test]
    fn burns_cenotaph_mint() {
        let location = TransactionLocation::dummy();
        let mut cache = TransactionCache::empty(location.clone());

        let db_rune = DbRune::factory();
        let rune_id = db_rune.rune_id();
        let ledger_entry = cache.apply_cenotaph_mint(&rune_id, 0, &db_rune, &Context::empty());
        assert!(ledger_entry.is_some());
        let ledger_entry = ledger_entry.unwrap();
        assert_eq!(ledger_entry.operation, DbLedgerOperation::Burn);
        assert_eq!(
            ledger_entry.amount.unwrap().0,
            db_rune.terms_amount.unwrap().0
        );
    }

    #[test]
    fn moves_runes_with_edict() {
        let location = TransactionLocation::dummy();
        let db_rune = &DbRune::factory();
        let rune_id = &db_rune.rune_id();
        let mut balances = VecDeque::new();
        let sender_address =
            "bc1p3v7r3n4hv63z4s7jkhdzxsay9xem98hxul057w2mwur406zhw8xqrpwp9w".to_string();
        let receiver_address =
            "bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string();
        balances.push_back(InputRuneBalance {
            address: Some(sender_address.clone()),
            amount: 1000,
        });
        let input_runes = hashmap! {
            rune_id.clone() => balances
        };
        let eligible_outputs = hashmap! {0=> ScriptBuf::from_hex("5120388dfba1b0069bbb0ad5eef62c1a94c46e91a3454accf40bf34b80f75e2708db").unwrap()};
        let mut cache = TransactionCache::new(location, input_runes, eligible_outputs, Some(0), 1);

        let edict = Edict {
            id: rune_id.clone(),
            amount: 1000,
            output: 0,
        };

        let ledger_entry = cache.apply_edict(&edict, &Context::empty());
        assert_eq!(ledger_entry.len(), 2);
        let receive = ledger_entry.first().unwrap();
        assert_eq!(receive.operation, DbLedgerOperation::Receive);
        assert_eq!(receive.address, Some(receiver_address.clone()));
        let send = ledger_entry.last().unwrap();
        assert_eq!(send.operation, DbLedgerOperation::Send);
        assert_eq!(send.address, Some(sender_address.clone()));
        assert_eq!(send.receiver_address, Some(receiver_address.clone()));
    }

    #[test]
    fn allocates_remaining_runes_to_first_eligible_output() {
        let location = TransactionLocation::dummy();
        let db_rune = &DbRune::factory();
        let rune_id = &db_rune.rune_id();
        let mut balances = VecDeque::new();
        let sender_address =
            "bc1p3v7r3n4hv63z4s7jkhdzxsay9xem98hxul057w2mwur406zhw8xqrpwp9w".to_string();
        let receiver_address =
            "bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string();
        balances.push_back(InputRuneBalance {
            address: Some(sender_address.clone()),
            amount: 1000,
        });
        let input_runes = hashmap! {
            rune_id.clone() => balances
        };
        let eligible_outputs = hashmap! {0=> ScriptBuf::from_hex("5120388dfba1b0069bbb0ad5eef62c1a94c46e91a3454accf40bf34b80f75e2708db").unwrap()};
        let mut cache = TransactionCache::new(location, input_runes, eligible_outputs, Some(0), 1);
        let ledger_entry = cache.allocate_remaining_balances(&Context::empty());

        assert_eq!(ledger_entry.len(), 2);
        let receive = ledger_entry.first().unwrap();
        assert_eq!(receive.operation, DbLedgerOperation::Receive);
        assert_eq!(receive.address, Some(receiver_address.clone()));
        let send = ledger_entry.last().unwrap();
        assert_eq!(send.operation, DbLedgerOperation::Send);
        assert_eq!(send.address, Some(sender_address.clone()));
        assert_eq!(send.receiver_address, Some(receiver_address.clone()));
    }

    #[test]
    fn allocates_remaining_runes_to_runestone_pointer_output() {
        let location = TransactionLocation::dummy();
        let db_rune = &DbRune::factory();
        let rune_id = &db_rune.rune_id();
        let mut balances = VecDeque::new();
        let sender_address =
            "bc1p3v7r3n4hv63z4s7jkhdzxsay9xem98hxul057w2mwur406zhw8xqrpwp9w".to_string();
        let receiver_address =
            "bc1p8zxlhgdsq6dmkzk4ammzcx55c3hfrg69ftx0gzlnfwq0wh38prds0nzqwf".to_string();
        balances.push_back(InputRuneBalance {
            address: Some(sender_address.clone()),
            amount: 1000,
        });
        let input_runes = hashmap! {
            rune_id.clone() => balances
        };
        let eligible_outputs = hashmap! {1=> ScriptBuf::from_hex("5120388dfba1b0069bbb0ad5eef62c1a94c46e91a3454accf40bf34b80f75e2708db").unwrap()};
        let mut cache = TransactionCache::new(location, input_runes, eligible_outputs, Some(0), 2);
        cache.output_pointer = Some(1);
        let ledger_entry = cache.allocate_remaining_balances(&Context::empty());

        assert_eq!(ledger_entry.len(), 2);
        let receive = ledger_entry.first().unwrap();
        assert_eq!(receive.operation, DbLedgerOperation::Receive);
        assert_eq!(receive.address, Some(receiver_address.clone()));
        let send = ledger_entry.last().unwrap();
        assert_eq!(send.operation, DbLedgerOperation::Send);
        assert_eq!(send.address, Some(sender_address.clone()));
        assert_eq!(send.receiver_address, Some(receiver_address.clone()));
    }
}
