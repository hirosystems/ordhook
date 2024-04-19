use std::{collections::HashMap, num::NonZeroUsize};

use chainhook_sdk::{
    types::{BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData},
    utils::Context,
};
use lru::LruCache;
use rusqlite::{Connection, Transaction};

use crate::core::meta_protocols::brc20::db::get_unsent_token_transfer;

use super::{
    db::{
        get_token, get_token_available_balance_for_address, get_token_minted_supply,
        insert_ledger_rows, insert_token_rows, Brc20DbLedgerRow, Brc20DbTokenRow,
    },
    parser::ParsedBrc20Operation,
    verifier::{VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData},
};

struct BlockCache {
    parsed_operations: HashMap<String, ParsedBrc20Operation>,
    ledger_db_rows: Vec<Brc20DbLedgerRow>,
    token_db_rows: Vec<Brc20DbTokenRow>,
}

impl BlockCache {
    fn new() -> Self {
        BlockCache {
            parsed_operations: HashMap::new(),
            ledger_db_rows: Vec::new(),
            token_db_rows: Vec::new(),
        }
    }

    pub fn get_parsed_operation(
        &mut self,
        inscription_id: String,
    ) -> Option<&ParsedBrc20Operation> {
        self.parsed_operations.get(&inscription_id)
    }

    pub fn flush(&mut self, db_tx: &Transaction, ctx: &Context) {
        insert_token_rows(&self.token_db_rows, db_tx, ctx);
        insert_ledger_rows(&self.ledger_db_rows, db_tx, ctx);
        self.parsed_operations.clear();
        self.ledger_db_rows.clear();
        self.token_db_rows.clear();
    }
}

/// In-memory cache that keeps verified token data to avoid excessive reads to the database.
pub struct Brc20MemoryCache {
    tokens: LruCache<String, Brc20DbTokenRow>,
    token_minted_supplies: LruCache<String, f64>,
    token_addr_avail_balances: LruCache<(String, String), f64>,
    unsent_transfers: LruCache<u64, Brc20DbLedgerRow>,
    ignored_inscriptions: LruCache<u64, bool>,
    pub block_cache: BlockCache,
}

impl Brc20MemoryCache {
    pub fn new(lru_size: usize) -> Self {
        Brc20MemoryCache {
            tokens: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_minted_supplies: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_addr_avail_balances: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            unsent_transfers: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            ignored_inscriptions: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            block_cache: BlockCache::new(),
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

    pub fn get_unsent_token_transfer(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Option<Brc20DbLedgerRow> {
        // Use `get` instead of `contains` so we promote this value in the LRU.
        if let Some(_) = self.ignored_inscriptions.get(&ordinal_number) {
            return None;
        }
        if let Some(row) = self.unsent_transfers.get(&ordinal_number) {
            return Some(row.clone());
        }
        match get_unsent_token_transfer(ordinal_number, db_tx, ctx) {
            Some(row) => {
                self.unsent_transfers.put(ordinal_number, row.clone());
                return Some(row);
            }
            None => {
                // Inscription is not relevant for BRC20.
                self.ignore_inscription(ordinal_number);
                return None;
            }
        }
    }

    /// Marks an ordinal number as ignored so we don't bother computing its transfers for BRC20 purposes.
    pub fn ignore_inscription(&mut self, ordinal_number: u64) {
        self.ignored_inscriptions.put(ordinal_number, true);
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
        self.block_cache.token_db_rows.push(token);
        self.block_cache.ledger_db_rows.push(Brc20DbLedgerRow {
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
        self.ignore_inscription(reveal.ordinal_number);
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
        self.block_cache.ledger_db_rows.push(Brc20DbLedgerRow {
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
        self.ignore_inscription(reveal.ordinal_number);
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
        self.unsent_transfers
            .put(reveal.ordinal_number, ledger_row.clone());
        self.block_cache.ledger_db_rows.push(ledger_row);
        self.ignored_inscriptions.pop(&reveal.ordinal_number); // Just in case.
    }

    pub fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        db_tx: &Connection,
        ctx: &Context,
    ) {
        let transfer_row = self.get_unsent_transfer_row(transfer.ordinal_number, db_tx, ctx);
        self.block_cache.ledger_db_rows.push(Brc20DbLedgerRow {
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
        self.block_cache.ledger_db_rows.push(Brc20DbLedgerRow {
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
        // We're not interested in further transfers.
        self.unsent_transfers.pop(&transfer.ordinal_number);
        self.ignore_inscription(transfer.ordinal_number);
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

    fn get_unsent_transfer_row(
        &mut self,
        ordinal_number: u64,
        db_tx: &Connection,
        ctx: &Context,
    ) -> Brc20DbLedgerRow {
        if let Some(transfer) = self.unsent_transfers.get(&ordinal_number) {
            return transfer.clone();
        }
        if let Some(transfer) = get_unsent_token_transfer(ordinal_number, db_tx, ctx) {
            self.unsent_transfers.put(ordinal_number, transfer.clone());
            return transfer;
        }
        unreachable!("Invalid transfer ordinal number {}", ordinal_number)
    }
}
