use std::num::NonZeroUsize;

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
    verifier::{VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData},
};

/// Keeps BRC20 DB rows before they're inserted into SQLite. Use `flush` to insert.
pub struct Brc20DbCache {
    ledger_rows: Vec<Brc20DbLedgerRow>,
    token_rows: Vec<Brc20DbTokenRow>,
}

impl Brc20DbCache {
    fn new() -> Self {
        Brc20DbCache {
            ledger_rows: Vec::new(),
            token_rows: Vec::new(),
        }
    }

    pub fn flush(&mut self, db_tx: &Transaction, ctx: &Context) {
        if self.token_rows.len() > 0 {
            insert_token_rows(&self.token_rows, db_tx, ctx);
            self.token_rows.clear();
        }
        if self.ledger_rows.len() > 0 {
            insert_ledger_rows(&self.ledger_rows, db_tx, ctx);
            self.ledger_rows.clear();
        }
    }
}

/// In-memory cache that keeps verified token data to avoid excessive reads to the database.
pub struct Brc20MemoryCache {
    tokens: LruCache<String, Brc20DbTokenRow>,
    token_minted_supplies: LruCache<String, f64>,
    token_addr_avail_balances: LruCache<String, f64>, // key format: "tick:address"
    unsent_transfers: LruCache<u64, Brc20DbLedgerRow>,
    ignored_inscriptions: LruCache<u64, bool>,
    pub db_cache: Brc20DbCache,
}

impl Brc20MemoryCache {
    pub fn new(lru_size: usize) -> Self {
        Brc20MemoryCache {
            tokens: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_minted_supplies: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_addr_avail_balances: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            unsent_transfers: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            ignored_inscriptions: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            db_cache: Brc20DbCache::new(),
        }
    }

    pub fn get_token(
        &mut self,
        tick: &str,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Option<Brc20DbTokenRow> {
        if let Some(token) = self.tokens.get(&tick.to_string()) {
            return Some(token.clone());
        }
        self.handle_cache_miss(db_tx, ctx);
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
    ) -> Option<f64> {
        if let Some(minted) = self.token_minted_supplies.get(&tick.to_string()) {
            return Some(minted.clone());
        }
        self.handle_cache_miss(db_tx, ctx);
        if let Some(minted_supply) = get_token_minted_supply(tick, db_tx, ctx) {
            self.token_minted_supplies
                .put(tick.to_string(), minted_supply);
            return Some(minted_supply);
        }
        return None;
    }

    pub fn get_token_address_avail_balance(
        &mut self,
        tick: &str,
        address: &str,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Option<f64> {
        let key = format!("{}:{}", tick, address);
        if let Some(balance) = self.token_addr_avail_balances.get(&key) {
            return Some(balance.clone());
        }
        self.handle_cache_miss(db_tx, ctx);
        if let Some(balance) = get_token_available_balance_for_address(tick, address, db_tx, ctx) {
            self.token_addr_avail_balances.put(key, balance);
            return Some(balance);
        }
        return None;
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
        self.handle_cache_miss(db_tx, ctx);
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
        tx_index: u64,
        _db_tx: &Connection,
        _ctx: &Context,
    ) {
        let token = Brc20DbTokenRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            block_height: block_identifier.index,
            tick: data.tick.clone(),
            display_tick: data.display_tick.clone(),
            max: data.max,
            lim: data.lim,
            dec: data.dec,
            address: data.address.clone(),
            self_mint: data.self_mint,
        };
        self.tokens.put(token.tick.clone(), token.clone());
        self.token_minted_supplies.put(token.tick.clone(), 0.0);
        self.token_addr_avail_balances
            .put(format!("{}:{}", token.tick, data.address), 0.0);
        self.db_cache.token_rows.push(token);
        self.db_cache.ledger_rows.push(Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tx_index,
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
        tx_index: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) {
        let Some(minted) = self.get_token_minted_supply(&data.tick, db_tx, ctx) else {
            unreachable!("BRC-20 deployed token should have a minted supply entry");
        };
        self.token_minted_supplies
            .put(data.tick.clone(), minted + data.amt);
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.address, db_tx, ctx)
            .unwrap_or(0.0);
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance + data.amt, // Increase for minter.
        );
        self.db_cache.ledger_rows.push(Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tx_index,
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
        tx_index: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) {
        let Some(balance) =
            self.get_token_address_avail_balance(&data.tick, &data.address, db_tx, ctx)
        else {
            unreachable!("BRC-20 transfer insert attempted for an address with no balance");
        };
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance - data.amt, // Decrease for sender.
        );
        let ledger_row = Brc20DbLedgerRow {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee as u64,
            ordinal_number: reveal.ordinal_number,
            block_height: block_identifier.index,
            tx_index,
            tick: data.tick.clone(),
            address: data.address.clone(),
            avail_balance: data.amt * -1.0,
            trans_balance: data.amt,
            operation: "transfer".to_string(),
        };
        self.unsent_transfers
            .put(reveal.ordinal_number, ledger_row.clone());
        self.db_cache.ledger_rows.push(ledger_row);
        self.ignored_inscriptions.pop(&reveal.ordinal_number); // Just in case.
    }

    pub fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        tx_index: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) {
        let transfer_row = self.get_unsent_transfer_row(transfer.ordinal_number, db_tx, ctx);
        self.db_cache.ledger_rows.push(Brc20DbLedgerRow {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: transfer.ordinal_number,
            block_height: block_identifier.index,
            tx_index,
            tick: data.tick.clone(),
            address: data.sender_address.clone(),
            avail_balance: 0.0,
            trans_balance: data.amt * -1.0,
            operation: "transfer_send".to_string(),
        });
        self.db_cache.ledger_rows.push(Brc20DbLedgerRow {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: transfer.ordinal_number,
            block_height: block_identifier.index,
            tx_index,
            tick: data.tick.clone(),
            address: data.receiver_address.clone(),
            avail_balance: data.amt,
            trans_balance: 0.0,
            operation: "transfer_receive".to_string(),
        });
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.receiver_address, db_tx, ctx)
            .unwrap_or(0.0);
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.receiver_address),
            balance + data.amt, // Increase for receiver.
        );
        // We're not interested in further transfers.
        self.unsent_transfers.pop(&transfer.ordinal_number);
        self.ignore_inscription(transfer.ordinal_number);
    }

    //
    //
    //

    fn get_unsent_transfer_row(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction,
        ctx: &Context,
    ) -> Brc20DbLedgerRow {
        if let Some(transfer) = self.unsent_transfers.get(&ordinal_number) {
            return transfer.clone();
        }
        self.handle_cache_miss(db_tx, ctx);
        let Some(transfer) = get_unsent_token_transfer(ordinal_number, db_tx, ctx) else {
            unreachable!("Invalid transfer ordinal number {}", ordinal_number)
        };
        self.unsent_transfers.put(ordinal_number, transfer.clone());
        return transfer;
    }

    fn handle_cache_miss(&mut self, db_tx: &Transaction, ctx: &Context) {
        // TODO: Measure this event somewhere
        self.db_cache.flush(db_tx, ctx);
    }
}

#[cfg(test)]
mod test {
    use chainhook_sdk::types::{BitcoinNetwork, BlockIdentifier};
    use test_case::test_case;

    use crate::core::meta_protocols::brc20::{
        db::initialize_brc20_db,
        parser::{ParsedBrc20BalanceData, ParsedBrc20Operation},
        test_utils::{get_test_ctx, Brc20RevealBuilder},
        verifier::{
            verify_brc20_operation, VerifiedBrc20BalanceData, VerifiedBrc20Operation,
            VerifiedBrc20TokenDeployData,
        },
    };

    use super::Brc20MemoryCache;

    #[test]
    fn test_brc20_memory_cache_transfer_miss() {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        // LRU size as 1 so we can test a miss.
        let mut cache = Brc20MemoryCache::new(1);
        cache.insert_token_deploy(
            &VerifiedBrc20TokenDeployData {
                tick: "pepe".to_string(),
                display_tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                self_mint: false,
            },
            &Brc20RevealBuilder::new().inscription_number(0).build(),
            &BlockIdentifier {
                index: 800000,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            0,
            &tx,
            &ctx,
        );
        let block = BlockIdentifier {
            index: 800002,
            hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
        };
        let address1 = "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string();
        let address2 = "bc1pngjqgeamkmmhlr6ft5yllgdmfllvcvnw5s7ew2ler3rl0z47uaesrj6jte".to_string();
        cache.insert_token_mint(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: 1000.0,
                address: address1.clone(),
            },
            &Brc20RevealBuilder::new().inscription_number(1).build(),
            &block,
            0,
            &tx,
            &ctx,
        );
        cache.insert_token_transfer(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: 100.0,
                address: address1.clone(),
            },
            &Brc20RevealBuilder::new().inscription_number(2).build(),
            &block,
            1,
            &tx,
            &ctx,
        );
        // These mint+transfer from a 2nd address will delete the first address' entries from cache.
        cache.insert_token_mint(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: 1000.0,
                address: address2.clone(),
            },
            &Brc20RevealBuilder::new()
                .inscription_number(3)
                .inscriber_address(Some(address2.clone()))
                .build(),
            &block,
            2,
            &tx,
            &ctx,
        );
        cache.insert_token_transfer(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: 100.0,
                address: address2.clone(),
            },
            &Brc20RevealBuilder::new()
                .inscription_number(4)
                .inscriber_address(Some(address2.clone()))
                .build(),
            &block,
            3,
            &tx,
            &ctx,
        );
        // Validate another transfer from the first address. Should pass because we still have 900 avail balance.
        let result = verify_brc20_operation(
            &ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: "100".to_string(),
            }),
            &Brc20RevealBuilder::new()
                .inscription_number(5)
                .inscriber_address(Some(address1.clone()))
                .build(),
            &block,
            &BitcoinNetwork::Mainnet,
            &mut cache,
            &tx,
            &ctx,
        );
        assert!(
            result
                == Ok(VerifiedBrc20Operation::TokenTransfer(
                    VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 100.0,
                        address: address1
                    }
                ))
        )
    }

    #[test_case(500.0 => Ok(Some(500.0)); "with transfer amt")]
    #[test_case(1000.0 => Ok(Some(0.0)); "with transfer to zero")]
    fn test_brc20_memory_cache_transfer_avail_balance(amt: f64) -> Result<Option<f64>, String> {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        let mut cache = Brc20MemoryCache::new(10);
        cache.insert_token_deploy(
            &VerifiedBrc20TokenDeployData {
                tick: "pepe".to_string(),
                display_tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                self_mint: false,
            },
            &Brc20RevealBuilder::new().inscription_number(0).build(),
            &BlockIdentifier {
                index: 800000,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            0,
            &tx,
            &ctx,
        );
        cache.insert_token_mint(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt: 1000.0,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &Brc20RevealBuilder::new().inscription_number(1).build(),
            &BlockIdentifier {
                index: 800001,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            0,
            &tx,
            &ctx,
        );
        assert!(
            cache.get_token_address_avail_balance(
                "pepe",
                "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp",
                &tx,
                &ctx,
            ) == Some(1000.0)
        );
        cache.insert_token_transfer(
            &VerifiedBrc20BalanceData {
                tick: "pepe".to_string(),
                amt,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &Brc20RevealBuilder::new().inscription_number(2).build(),
            &BlockIdentifier {
                index: 800002,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            0,
            &tx,
            &ctx,
        );
        Ok(cache.get_token_address_avail_balance(
            "pepe",
            "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp",
            &tx,
            &ctx,
        ))
    }
}
