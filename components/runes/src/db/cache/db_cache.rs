use std::collections::HashMap;

use bitcoind::{try_debug, utils::Context};
use tokio_postgres::Transaction;

use crate::db::{
    models::{
        db_balance_change::DbBalanceChange, db_ledger_entry::DbLedgerEntry, db_rune::DbRune,
        db_supply_change::DbSupplyChange,
    },
    pg_insert_balance_changes, pg_insert_ledger_entries, pg_insert_runes, pg_insert_supply_changes,
};

/// Holds rows that have yet to be inserted into the database.
pub struct DbCache {
    pub runes: Vec<DbRune>,
    pub ledger_entries: Vec<DbLedgerEntry>,
    pub supply_changes: HashMap<String, DbSupplyChange>,
    pub balance_increases: HashMap<(String, String), DbBalanceChange>,
    pub balance_deductions: HashMap<(String, String), DbBalanceChange>,
}

impl Default for DbCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DbCache {
    pub fn new() -> Self {
        DbCache {
            runes: Vec::new(),
            ledger_entries: Vec::new(),
            supply_changes: HashMap::new(),
            balance_increases: HashMap::new(),
            balance_deductions: HashMap::new(),
        }
    }

    /// Insert all data into the DB and clear cache.
    pub async fn flush(&mut self, db_tx: &mut Transaction<'_>, ctx: &Context) {
        try_debug!(ctx, "Flushing DB cache...");
        if !self.runes.is_empty() {
            try_debug!(ctx, "Flushing {} runes", self.runes.len());
            let _ = pg_insert_runes(&self.runes, db_tx, ctx).await;
            self.runes.clear();
        }
        if !self.supply_changes.is_empty() {
            try_debug!(ctx, "Flushing {} supply changes", self.supply_changes.len());
            let _ = pg_insert_supply_changes(
                &self.supply_changes.values().cloned().collect::<Vec<_>>(),
                db_tx,
                ctx,
            )
            .await;
            self.supply_changes.clear();
        }
        if !self.ledger_entries.is_empty() {
            try_debug!(ctx, "Flushing {} ledger entries", self.ledger_entries.len());
            let _ = pg_insert_ledger_entries(&self.ledger_entries, db_tx, ctx).await;
            self.ledger_entries.clear();
        }
        if !self.balance_increases.is_empty() {
            try_debug!(
                ctx,
                "Flushing {} balance increases",
                self.balance_increases.len()
            );
            let _ = pg_insert_balance_changes(
                &self.balance_increases.values().cloned().collect::<Vec<_>>(),
                true,
                db_tx,
                ctx,
            )
            .await;
            self.balance_increases.clear();
        }
        if !self.balance_deductions.is_empty() {
            try_debug!(
                ctx,
                "Flushing {} balance deductions",
                self.balance_deductions.len()
            );
            let _ = pg_insert_balance_changes(
                &self
                    .balance_deductions
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
                false,
                db_tx,
                ctx,
            )
            .await;
            self.balance_deductions.clear();
        }
    }
}
