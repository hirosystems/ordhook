use std::num::NonZeroUsize;

use chainhook_sdk::{
    types::{BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData},
    utils::Context,
};
use lru::LruCache;
use rusqlite::{Connection, Transaction};

use crate::core::meta_protocols::brc20::db::get_unsent_token_transfer_with_sender;

use super::{
    db::{
        get_token, get_token_available_balance_for_address, get_token_minted_supply,
        insert_ledger_rows, insert_token_rows, Brc20DbLedgerRow, Brc20DbTokenRow,
    },
    verifier::{VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData},
};

/// In-memory cache that keeps verified token data to avoid excessive reads to the database.
pub struct Brc20MemoryCache {
    tokens: LruCache<String, Brc20DbTokenRow>,
    token_minted_supplies: LruCache<String, f64>,
    token_addr_avail_balances: LruCache<(String, String), f64>,
    transfers: LruCache<u64, Brc20DbLedgerRow>,
    blacklisted_inscriptions: LruCache<u64, bool>,

    ledger_db_rows: Vec<Brc20DbLedgerRow>,
    token_db_rows: Vec<Brc20DbTokenRow>,
}

impl Brc20MemoryCache {
    pub fn new(lru_size: usize) -> Self {
        Brc20MemoryCache {
            tokens: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_minted_supplies: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_addr_avail_balances: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            transfers: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            blacklisted_inscriptions: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            ledger_db_rows: Vec::new(),
            token_db_rows: Vec::new(),
        }
    }

    pub fn get_token(
        &mut self,
        tick: &str,
        db_tx: &Connection,
        ctx: &Context,
    ) -> Option<Brc20DbTokenRow> {
        if let Some(token) = self.tokens.get(&tick.to_string()) {
            return Some(token.clone());
        }
        match get_token(tick, db_tx, ctx) {
            Some(db_token) => {
                self.tokens.put(tick.to_string(), db_token.clone());
                return Some(db_token);
            }
            None => return None,
        }
    }

    pub fn get_token_minted_supply(
        &mut self,
        tick: &str,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> f64 {
        if let Some(minted) = self.token_minted_supplies.get(&tick.to_string()) {
            return minted.clone();
        }
        let minted_supply = get_token_minted_supply(tick, db_tx, ctx);
        self.token_minted_supplies
            .put(tick.to_string(), minted_supply);
        return minted_supply;
    }

    pub fn get_token_avail_balance_for_address(
        &mut self,
        tick: &str,
        address: &str,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> f64 {
        let key = (tick.to_string(), address.to_string());
        if let Some(balance) = self.token_addr_avail_balances.get(&key) {
            return balance.clone();
        }
        let balance = get_token_available_balance_for_address(tick, address, db_tx, ctx);
        self.token_addr_avail_balances.put(key, balance);
        return balance;
    }

    pub fn get_unsent_token_transfer_with_sender(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Option<Brc20DbLedgerRow> {
        if let Some(row) = self.transfers.get(&ordinal_number) {
            return Some(row.clone());
        }
        if let Some(_) = self.blacklisted_inscriptions.get(&ordinal_number) {
            // Avoid a DB read if this is not a valid transfer.
            return None;
        }
        match get_unsent_token_transfer_with_sender(ordinal_number, db_tx, ctx) {
            Some(row) => {
                self.transfers.put(ordinal_number, row.clone());
                return Some(row);
            }
            None => {
                // Ignore that inscription's future transfers.
                self.blacklisted_inscriptions.put(ordinal_number, true);
                return None;
            }
        }
    }

    pub fn insert_token_deploy(
        &mut self,
        data: &VerifiedBrc20TokenDeployData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        _db_tx: &Connection,
        _ctx: &Context,
    ) {
        let token = Brc20DbTokenRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            max: data.max,
            lim: data.lim,
            dec: data.dec,
            address: data.address.clone(),
            self_mint: data.self_mint,
        };
        self.tokens.put(token.tick.clone(), token.clone());
        self.token_minted_supplies.put(token.tick.clone(), 0.0);
        self.token_db_rows.push(token);
        self.ledger_db_rows.push(Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.address.clone(),
            avail_balance: 0.0,
            trans_balance: 0.0,
            operation: "deploy".to_string(),
        });
    }

    pub fn insert_token_mint(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        _db_tx: &Connection,
        _ctx: &Context,
    ) {
        self.increase_token_minted_supply(&data.tick, data.amt);
        self.update_token_addr_avail_balance(&data.tick, &data.address, data.amt);
        self.ledger_db_rows.push(Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.address.clone(),
            avail_balance: data.amt,
            trans_balance: 0.0,
            operation: "mint".to_string(),
        });
    }

    pub fn insert_token_transfer(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        _db_tx: &Connection,
        _ctx: &Context,
    ) {
        self.update_token_addr_avail_balance(&data.tick, &data.address, data.amt * -1.0);
        let ledger_row = Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.address.clone(),
            avail_balance: data.amt * -1.0,
            trans_balance: data.amt,
            operation: "transfer".to_string(),
        };
        self.transfers
            .put(reveal.ordinal_number, ledger_row.clone());
        self.ledger_db_rows.push(ledger_row);
    }

    pub fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        db_tx: &Connection,
        ctx: &Context,
    ) {
        let transfer_row = self.get_transfer_row(transfer.ordinal_number, db_tx, ctx);
        self.ledger_db_rows.push(Brc20DbLedgerRow {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: transfer.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.sender_address.clone(),
            avail_balance: 0.0,
            trans_balance: data.amt * -1.0,
            operation: "transfer_send".to_string(),
        });
        self.ledger_db_rows.push(Brc20DbLedgerRow {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: transfer.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.receiver_address.clone(),
            avail_balance: data.amt,
            trans_balance: 0.0,
            operation: "transfer_receive".to_string(),
        });
        self.update_token_addr_avail_balance(&data.tick, &data.receiver_address, data.amt);
        self.transfers.demote(&transfer.ordinal_number);
    }

    /// Writes all pending `tokens` and `ledger` DB rows to the database and clears caches. Usually
    /// called once per block.
    pub fn flush_row_cache(&mut self, db_tx: &Transaction, ctx: &Context) {
        insert_token_rows(&self.token_db_rows, db_tx, ctx);
        self.token_db_rows.clear();
        insert_ledger_rows(&self.ledger_db_rows, db_tx, ctx);
        self.ledger_db_rows.clear();
    }

    //
    //
    //

    fn increase_token_minted_supply(&mut self, tick: &str, delta: f64) {
        let Some(minted) = self.token_minted_supplies.get_mut(&tick.to_string()) else {
            self.token_minted_supplies.put(tick.to_string(), delta);
            return;
        };
        *minted += delta;
    }

    fn update_token_addr_avail_balance(&mut self, tick: &str, address: &str, delta: f64) {
        let key = (tick.to_string(), address.to_string());
        let Some(balance) = self.token_addr_avail_balances.get_mut(&key) else {
            self.token_addr_avail_balances.put(key, delta);
            return;
        };
        *balance += delta;
    }

    fn get_transfer_row(
        &mut self,
        ordinal_number: u64,
        db_tx: &Connection,
        ctx: &Context,
    ) -> Brc20DbLedgerRow {
        if let Some(transfer) = self.transfers.get(&ordinal_number) {
            return transfer.clone();
        }
        if let Some(transfer) = get_unsent_token_transfer_with_sender(ordinal_number, db_tx, ctx) {
            self.transfers.put(ordinal_number, transfer.clone());
            return transfer;
        }
        unreachable!("Invalid transfer ordinal number {}", ordinal_number)
    }
}
