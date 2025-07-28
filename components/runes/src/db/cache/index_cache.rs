use std::{collections::HashMap, num::NonZeroUsize, str::FromStr};

use bitcoin::{Network, ScriptBuf};
use bitcoind::{try_debug, try_warn, types::bitcoin::TxIn, utils::Context};
use config::Config;
use deadpool_postgres::{Pool, Transaction};
use lru::LruCache;
use ordinals_parser::{Cenotaph, Edict, Etching, Rune, RuneId, Runestone};
use postgres::pg_pool_client;

use super::{
    db_cache::DbCache, input_rune_balance::InputRuneBalance, transaction_cache::TransactionCache,
    transaction_location::TransactionLocation, utils::move_block_output_cache_to_output_cache,
};
use crate::db::{
    cache::utils::input_rune_balances_from_tx_inputs,
    models::{
        db_balance_change::DbBalanceChange, db_ledger_entry::DbLedgerEntry,
        db_ledger_operation::DbLedgerOperation, db_rune::DbRune, db_supply_change::DbSupplyChange,
    },
    pg_get_max_rune_number, pg_get_rune_by_id, pg_get_rune_total_mints,
};

/// Holds rune data across multiple blocks for faster computations. Processes rune events as they happen during transactions and
/// generates database rows for later insertion.
pub struct IndexCache {
    pub network: Network,
    /// Number to be assigned to the next rune etching.
    next_rune_number: u32,
    /// LRU cache for runes.
    rune_cache: LruCache<RuneId, DbRune>,
    /// LRU cache for total mints for runes.
    rune_total_mints_cache: LruCache<RuneId, u128>,
    /// LRU cache for outputs with rune balances.
    output_cache: LruCache<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
    /// Same as above but only for the current block. We use a `HashMap` instead of an LRU cache to make sure we keep all outputs
    /// in memory while we index this block. Must be cleared every time a new block is processed.
    block_output_cache: HashMap<(String, u32), HashMap<RuneId, Vec<InputRuneBalance>>>,
    /// Holds a single transaction's rune cache. Must be cleared every time a new transaction is processed.
    tx_cache: TransactionCache,
    /// Keeps rows that have not yet been inserted in the DB.
    pub db_cache: DbCache,
}

impl IndexCache {
    pub async fn new(config: &Config, pg_pool: &Pool) -> Self {
        let pg_client = pg_pool_client(pg_pool).await.unwrap();
        let network = config.bitcoind.network;
        let cap = NonZeroUsize::new(config.runes.as_ref().unwrap().lru_cache_size).unwrap();
        IndexCache {
            network,
            next_rune_number: pg_get_max_rune_number(&pg_client).await + 1,
            rune_cache: LruCache::new(cap),
            rune_total_mints_cache: LruCache::new(cap),
            output_cache: LruCache::new(cap),
            block_output_cache: HashMap::new(),
            tx_cache: TransactionCache::new(
                TransactionLocation {
                    network,
                    block_hash: "".to_string(),
                    block_height: 1,
                    timestamp: 0,
                    tx_index: 0,
                    tx_id: "".to_string(),
                },
                HashMap::new(),
                HashMap::new(),
                None,
                0,
            ),
            db_cache: DbCache::new(),
        }
    }

    pub async fn reset_max_rune_number(&mut self, db_tx: &mut Transaction<'_>) {
        self.next_rune_number = pg_get_max_rune_number(db_tx).await + 1;
    }

    /// Creates a fresh transaction index cache.
    pub async fn begin_transaction(
        &mut self,
        location: TransactionLocation,
        tx_inputs: &Vec<TxIn>,
        eligible_outputs: HashMap<u32, ScriptBuf>,
        first_eligible_output: Option<u32>,
        total_outputs: u32,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
    ) {
        let input_runes = input_rune_balances_from_tx_inputs(
            tx_inputs,
            &self.block_output_cache,
            &mut self.output_cache,
            db_tx,
            ctx,
        )
        .await;
        #[cfg(not(feature = "release"))]
        {
            for (rune_id, balances) in input_runes.iter() {
                try_debug!(ctx, "INPUT {rune_id} {balances:?} {location}");
            }
            if !input_runes.is_empty() {
                try_debug!(
                    ctx,
                    "First output: {first_eligible_output:?}, total_outputs: {total_outputs}"
                );
            }
        }
        self.tx_cache = TransactionCache::new(
            location,
            input_runes,
            eligible_outputs,
            first_eligible_output,
            total_outputs,
        );
    }

    /// Finalizes the current transaction index cache by moving all unallocated balances to the correct output.
    pub fn end_transaction(&mut self, _db_tx: &mut Transaction<'_>, ctx: &Context) {
        let entries = self.tx_cache.allocate_remaining_balances(ctx);
        self.add_ledger_entries_to_db_cache(&entries);
    }

    pub fn end_block(&mut self) {
        move_block_output_cache_to_output_cache(
            &mut self.block_output_cache,
            &mut self.output_cache,
        );
    }

    pub async fn apply_runestone(
        &mut self,
        runestone: &Runestone,
        _db_tx: &mut Transaction<'_>,
        ctx: &Context,
    ) {
        try_debug!(ctx, "{:?} {}", runestone, self.tx_cache.location);
        if let Some(new_pointer) = runestone.pointer {
            self.tx_cache.output_pointer = Some(new_pointer);
        }
    }

    pub async fn apply_cenotaph(
        &mut self,
        cenotaph: &Cenotaph,
        _db_tx: &mut Transaction<'_>,
        ctx: &Context,
        cenotaphs_counter: &mut u64,
    ) {
        try_debug!(ctx, "{:?} {}", cenotaph, self.tx_cache.location);
        let entries = self.tx_cache.apply_cenotaph_input_burn(cenotaph);
        self.add_ledger_entries_to_db_cache(&entries);
        *cenotaphs_counter += 1;
    }

    pub async fn apply_etching(
        &mut self,
        etching: &Etching,
        _db_tx: &mut Transaction<'_>,
        ctx: &Context,
        etchings_counter: &mut u64,
    ) {
        let (rune_id, db_rune, entry) = self.tx_cache.apply_etching(etching, self.next_rune_number);
        try_debug!(
            ctx,
            "Etching {spaced_name} ({id}) {location}",
            spaced_name = &db_rune.spaced_name,
            id = &db_rune.id,
            location = self.tx_cache.location.to_string()
        );
        self.db_cache.runes.push(db_rune.clone());
        self.rune_cache.put(rune_id, db_rune);
        self.add_ledger_entries_to_db_cache(&vec![entry]);
        self.next_rune_number += 1;
        *etchings_counter += 1;
    }

    pub async fn apply_cenotaph_etching(
        &mut self,
        rune: &Rune,
        _db_tx: &mut Transaction<'_>,
        ctx: &Context,
        cenotaph_etchings_counter: &mut u64,
    ) {
        let (rune_id, db_rune, entry) = self
            .tx_cache
            .apply_cenotaph_etching(rune, self.next_rune_number);
        try_debug!(
            ctx,
            "Etching cenotaph {spaced_name} ({id}) {location}",
            spaced_name = &db_rune.spaced_name,
            id = &db_rune.id,
            location = self.tx_cache.location.to_string()
        );
        self.db_cache.runes.push(db_rune.clone());
        self.rune_cache.put(rune_id, db_rune);
        self.add_ledger_entries_to_db_cache(&vec![entry]);
        self.next_rune_number += 1;
        *cenotaph_etchings_counter += 1;
    }

    pub async fn apply_mint(
        &mut self,
        rune_id: &RuneId,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
        mints_counter: &mut u64,
    ) {
        let Some(db_rune) = self.get_cached_rune_by_rune_id(rune_id, db_tx, ctx).await else {
            try_warn!(
                ctx,
                "Rune {rune_id} not found for mint {location}",
                location = self.tx_cache.location.to_string()
            );
            return;
        };
        let total_mints = self
            .get_cached_rune_total_mints(rune_id, db_tx, ctx)
            .await
            .unwrap_or(0);
        if let Some(ledger_entry) = self
            .tx_cache
            .apply_mint(rune_id, total_mints, &db_rune, ctx)
        {
            self.add_ledger_entries_to_db_cache(&vec![ledger_entry.clone()]);
            if let Some(total) = self.rune_total_mints_cache.get_mut(rune_id) {
                *total += 1;
            } else {
                self.rune_total_mints_cache.put(*rune_id, 1);
            }
            *mints_counter += 1;
        }
    }

    pub async fn apply_cenotaph_mint(
        &mut self,
        rune_id: &RuneId,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
        cenotaph_mints_counter: &mut u64,
    ) {
        let Some(db_rune) = self.get_cached_rune_by_rune_id(rune_id, db_tx, ctx).await else {
            try_warn!(
                ctx,
                "Rune {rune_id} not found for cenotaph mint {location}",
                location = self.tx_cache.location.to_string()
            );
            return;
        };
        let total_mints = self
            .get_cached_rune_total_mints(rune_id, db_tx, ctx)
            .await
            .unwrap_or(0);
        if let Some(ledger_entry) =
            self.tx_cache
                .apply_cenotaph_mint(rune_id, total_mints, &db_rune, ctx)
        {
            self.add_ledger_entries_to_db_cache(&vec![ledger_entry]);
            if let Some(total) = self.rune_total_mints_cache.get_mut(rune_id) {
                *total += 1;
            } else {
                self.rune_total_mints_cache.put(*rune_id, 1);
            }
            *cenotaph_mints_counter += 1;
        }
    }

    pub async fn apply_edict(
        &mut self,
        edict: &Edict,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
        edicts_number: &mut u64,
    ) {
        let Some(db_rune) = self.get_cached_rune_by_rune_id(&edict.id, db_tx, ctx).await else {
            try_warn!(
                ctx,
                "Rune {id} not found for edict {location}",
                id = edict.id.to_string(),
                location = self.tx_cache.location.to_string()
            );
            return;
        };
        let entries = self.tx_cache.apply_edict(edict, ctx);
        for entry in entries.iter() {
            try_debug!(
                ctx,
                "Edict {spaced_name} {amount} {location}",
                spaced_name = &db_rune.spaced_name,
                amount = entry.amount.unwrap().0,
                location = self.tx_cache.location.to_string()
            );
        }
        *edicts_number += 1;
        self.add_ledger_entries_to_db_cache(&entries);
    }

    async fn get_cached_rune_by_rune_id(
        &mut self,
        rune_id: &RuneId,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
    ) -> Option<DbRune> {
        // Id 0:0 is used to mean the rune being etched in this transaction, if any.
        if rune_id.block == 0 && rune_id.tx == 0 {
            return self.tx_cache.etching.clone();
        }
        if let Some(cached_rune) = self.rune_cache.get(rune_id) {
            return Some(cached_rune.clone());
        }
        // Cache miss, look in DB.
        self.db_cache.flush(db_tx, ctx).await;
        let db_rune = pg_get_rune_by_id(rune_id, db_tx, ctx).await?;
        self.rune_cache.put(*rune_id, db_rune.clone());
        Some(db_rune)
    }

    async fn get_cached_rune_total_mints(
        &mut self,
        rune_id: &RuneId,
        db_tx: &mut Transaction<'_>,
        ctx: &Context,
    ) -> Option<u128> {
        let real_rune_id = if rune_id.block == 0 && rune_id.tx == 0 {
            let etching = self.tx_cache.etching.as_ref()?;
            RuneId::from_str(etching.id.as_str()).unwrap()
        } else {
            *rune_id
        };
        if let Some(total) = self.rune_total_mints_cache.get(&real_rune_id) {
            return Some(*total);
        }
        // Cache miss, look in DB.
        self.db_cache.flush(db_tx, ctx).await;
        let total = pg_get_rune_total_mints(rune_id, db_tx, ctx).await?;
        self.rune_total_mints_cache.put(*rune_id, total);
        Some(total)
    }

    /// Take ledger entries returned by the `TransactionCache` and add them to the `DbCache`. Update global balances and counters
    /// as well.
    fn add_ledger_entries_to_db_cache(&mut self, entries: &Vec<DbLedgerEntry>) {
        self.db_cache.ledger_entries.extend(entries.clone());
        for entry in entries.iter() {
            match entry.operation {
                DbLedgerOperation::Etching => {
                    self.db_cache
                        .supply_changes
                        .entry(entry.rune_id.clone())
                        .and_modify(|i| {
                            i.total_operations += 1;
                        })
                        .or_insert(DbSupplyChange::from_operation(
                            entry.rune_id.clone(),
                            entry.block_height,
                        ));
                }
                DbLedgerOperation::Mint => {
                    self.db_cache
                        .supply_changes
                        .entry(entry.rune_id.clone())
                        .and_modify(|i| {
                            i.minted += entry.amount.unwrap();
                            i.total_mints += 1;
                            i.total_operations += 1;
                        })
                        .or_insert(DbSupplyChange::from_mint(
                            entry.rune_id.clone(),
                            entry.block_height,
                            entry.amount.unwrap(),
                        ));
                }
                DbLedgerOperation::Burn => {
                    self.db_cache
                        .supply_changes
                        .entry(entry.rune_id.clone())
                        .and_modify(|i| {
                            i.burned += entry.amount.unwrap();
                            i.total_burns += 1;
                            i.total_operations += 1;
                        })
                        .or_insert(DbSupplyChange::from_burn(
                            entry.rune_id.clone(),
                            entry.block_height,
                            entry.amount.unwrap(),
                        ));
                }
                DbLedgerOperation::Send => {
                    self.db_cache
                        .supply_changes
                        .entry(entry.rune_id.clone())
                        .and_modify(|i| i.total_operations += 1)
                        .or_insert(DbSupplyChange::from_operation(
                            entry.rune_id.clone(),
                            entry.block_height,
                        ));
                    if let Some(address) = entry.address.clone() {
                        self.db_cache
                            .balance_deductions
                            .entry((entry.rune_id.clone(), address.clone()))
                            .and_modify(|i| i.balance += entry.amount.unwrap())
                            .or_insert(DbBalanceChange::from_operation(
                                entry.rune_id.clone(),
                                entry.block_height,
                                address,
                                entry.amount.unwrap(),
                            ));
                    }
                }
                DbLedgerOperation::Receive => {
                    self.db_cache
                        .supply_changes
                        .entry(entry.rune_id.clone())
                        .and_modify(|i| i.total_operations += 1)
                        .or_insert(DbSupplyChange::from_operation(
                            entry.rune_id.clone(),
                            entry.block_height,
                        ));
                    if let Some(address) = entry.address.clone() {
                        self.db_cache
                            .balance_increases
                            .entry((entry.rune_id.clone(), address.clone()))
                            .and_modify(|i| i.balance += entry.amount.unwrap())
                            .or_insert(DbBalanceChange::from_operation(
                                entry.rune_id.clone(),
                                entry.block_height,
                                address,
                                entry.amount.unwrap(),
                            ));
                        // Add to current block's output cache if it's received balance.
                        let k = (entry.tx_id.clone(), entry.output.unwrap().0);
                        let rune_id = RuneId::from_str(entry.rune_id.as_str()).unwrap();
                        let balance = InputRuneBalance {
                            address: entry.address.clone(),
                            amount: entry.amount.unwrap().0,
                        };
                        let mut default = HashMap::new();
                        default.insert(rune_id, vec![balance.clone()]);
                        self.block_output_cache
                            .entry(k)
                            .and_modify(|i| {
                                i.entry(rune_id)
                                    .and_modify(|v| v.push(balance.clone()))
                                    .or_insert(vec![balance]);
                            })
                            .or_insert(default);
                    }
                }
            }
        }
    }
}
