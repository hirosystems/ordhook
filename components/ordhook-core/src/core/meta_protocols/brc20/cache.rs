use std::collections::HashMap;

use chainhook_sdk::{
    types::{BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData},
    utils::Context,
};
use rusqlite::{Connection, Transaction};

use super::{
    db::{
        get_token, get_token_available_balance_for_address, get_token_minted_supply,
        insert_ledger_rows, insert_token_rows, Brc20DbLedgerRow, Brc20DbTokenRow,
    },
    verifier::{VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData},
};

/// In-memory cache that keeps verified token data to avoid excessive reads to the database.
pub struct Brc20MemoryCache {
    token_map: HashMap<String, Brc20DbTokenRow>,
    token_minted_supply_map: HashMap<String, f64>,
    token_addr_avail_balance_map: HashMap<(String, String), f64>,

    ledger_row_cache: Vec<Brc20DbLedgerRow>,
    token_row_cache: Vec<Brc20DbTokenRow>,
}

impl Brc20MemoryCache {
    pub fn new() -> Self {
        Brc20MemoryCache {
            token_map: HashMap::new(),
            token_minted_supply_map: HashMap::new(),
            token_addr_avail_balance_map: HashMap::new(),
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
                self.token_map.insert(tick.to_string(), db_token.clone());
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
            .insert(tick.to_string(), minted_supply);
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
        self.token_addr_avail_balance_map.insert(key, balance);
        return balance;
    }

    pub fn insert_token_deploy(
        &mut self,
        data: &VerifiedBrc20TokenDeployData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
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
        self.token_map.insert(token.tick.clone(), token.clone());
        self.token_minted_supply_map.insert(token.tick.clone(), 0.0);
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
    ) {
        self.update_token_avail_balance_for_address(&data.tick, &data.address, data.amt * -1.0);
        self.ledger_row_cache.push(Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.address.clone(),
            avail_balance: data.amt * -1.0,
            trans_balance: data.amt,
            operation: "transfer".to_string(),
        });
    }

    pub fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
    ) {
        // `inscription_id` and `inscription_number` will be calculated upon insert.
        self.ledger_row_cache.push(Brc20DbLedgerRow {
            inscription_id: "".to_string(),
            inscription_number: 0,
            ordinal_number: transfer.ordinal_number,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            address: data.sender_address.clone(),
            avail_balance: 0.0,
            trans_balance: data.amt * -1.0,
            operation: "transfer_send".to_string(),
        });
        self.ledger_row_cache.push(Brc20DbLedgerRow {
            inscription_id: "".to_string(),
            inscription_number: 0,
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
        if let Some(minted) = self.token_minted_supply_map.get(&tick.to_string()) {
            self.token_minted_supply_map
                .insert(tick.to_string(), minted + delta);
        } else {
            self.token_minted_supply_map.insert(tick.to_string(), delta);
        }
    }

    fn update_token_avail_balance_for_address(&mut self, tick: &str, address: &str, delta: f64) {
        let key = (tick.to_string(), address.to_string());
        if let Some(balance) = self.token_addr_avail_balance_map.get(&key) {
            self.token_addr_avail_balance_map
                .insert(key, balance + delta);
        } else {
            self.token_addr_avail_balance_map.insert(key, delta);
        }
    }
}
