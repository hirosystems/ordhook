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
    token_map: LruCache<String, Brc20DbTokenRow>,
    token_minted_supply_map: LruCache<String, f64>,
    token_addr_avail_balance_map: LruCache<(String, String), f64>,
    transfer_cache: LruCache<u64, Brc20DbLedgerRow>,

    ledger_row_cache: Vec<Brc20DbLedgerRow>,
    token_row_cache: Vec<Brc20DbTokenRow>,
}

impl Brc20MemoryCache {
    pub fn new() -> Self {
        Brc20MemoryCache {
            token_map: LruCache::new(NonZeroUsize::new(10_000).unwrap()),
            token_minted_supply_map: LruCache::new(NonZeroUsize::new(10_000).unwrap()),
            token_addr_avail_balance_map: LruCache::new(NonZeroUsize::new(10_000).unwrap()),
            transfer_cache: LruCache::new(NonZeroUsize::new(10_000).unwrap()),
            ledger_row_cache: Vec::new(),
            token_row_cache: Vec::new(),
        }
    }

    pub fn get_token(
        &mut self,
        tick: &str,
        db_tx: &Connection,
        ctx: &Context,
    ) -> Option<Brc20DbTokenRow> {
        if let Some(token) = self.token_map.get(&tick.to_string()) {
            return Some(token.clone());
        }
        match get_token(tick, db_tx, ctx) {
            Some(db_token) => {
                self.token_map.put(tick.to_string(), db_token.clone());
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
        if let Some(minted) = self.token_minted_supply_map.get(&tick.to_string()) {
            return minted.clone();
        }
        let minted_supply = get_token_minted_supply(tick, db_tx, ctx);
        self.token_minted_supply_map
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
        if let Some(balance) = self.token_addr_avail_balance_map.get(&key) {
            return balance.clone();
        }
        let balance = get_token_available_balance_for_address(tick, address, db_tx, ctx);
        self.token_addr_avail_balance_map.put(key, balance);
        return balance;
    }

    pub fn get_unsent_token_transfer_with_sender(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Option<Brc20DbLedgerRow> {
        if let Some(row) = self.transfer_cache.get(&ordinal_number) {
            return Some(row.clone());
        }
        match get_unsent_token_transfer_with_sender(ordinal_number, db_tx, ctx) {
            Some(row) => {
                self.transfer_cache.put(ordinal_number, row.clone());
                return Some(row);
            }
            None => return None,
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
        self.token_map.put(token.tick.clone(), token.clone());
        self.token_minted_supply_map.put(token.tick.clone(), 0.0);
        self.token_row_cache.push(token);
        self.ledger_row_cache.push(Brc20DbLedgerRow {
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
        self.update_token_minted_supply(&data.tick, data.amt);
        self.update_token_avail_balance_for_address(&data.tick, &data.address, data.amt);
        self.ledger_row_cache.push(Brc20DbLedgerRow {
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
        self.update_token_avail_balance_for_address(&data.tick, &data.address, data.amt * -1.0);
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
        self.transfer_cache
            .put(reveal.ordinal_number, ledger_row.clone());
        self.ledger_row_cache.push(ledger_row);
    }

    pub fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        db_tx: &Connection,
        ctx: &Context,
    ) {
        let transfer_row = self.get_transfer_balance_data(transfer.ordinal_number, db_tx, ctx);
        self.ledger_row_cache.push(Brc20DbLedgerRow {
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
        self.ledger_row_cache.push(Brc20DbLedgerRow {
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
        self.update_token_avail_balance_for_address(&data.tick, &data.receiver_address, data.amt);
    }

    pub fn flush_row_cache(&mut self, db_tx: &Transaction, ctx: &Context) {
        insert_token_rows(&self.token_row_cache, db_tx, ctx);
        self.token_row_cache.clear();
        insert_ledger_rows(&self.ledger_row_cache, db_tx, ctx);
        self.ledger_row_cache.clear();
        // TODO: clear data structures here so we can reuse this cache struct?
    }

    //
    //
    //

    fn update_token_minted_supply(&mut self, tick: &str, delta: f64) {
        let Some(minted) = self.token_minted_supply_map.get_mut(&tick.to_string()) else {
            self.token_minted_supply_map.put(tick.to_string(), delta);
            return;
        };
        *minted += delta;
    }

    fn update_token_avail_balance_for_address(&mut self, tick: &str, address: &str, delta: f64) {
        let key = (tick.to_string(), address.to_string());
        let Some(balance) = self.token_addr_avail_balance_map.get_mut(&key) else {
            self.token_addr_avail_balance_map.put(key, delta);
            return;
        };
        *balance += delta;
    }

    fn get_transfer_balance_data(
        &mut self,
        ordinal_number: u64,
        db_tx: &Connection,
        ctx: &Context,
    ) -> Brc20DbLedgerRow {
        if let Some(transfer) = self.transfer_cache.get(&ordinal_number) {
            return transfer.clone();
        }
        if let Some(transfer) = get_unsent_token_transfer_with_sender(ordinal_number, db_tx, ctx) {
            self.transfer_cache.put(ordinal_number, transfer.clone());
            return transfer;
        }
        unreachable!("Invalid transfer ordinal number")
    }
}
