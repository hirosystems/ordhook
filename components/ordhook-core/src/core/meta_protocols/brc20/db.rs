use std::{collections::HashMap, path::PathBuf};

use crate::{
    db::{
        create_or_open_readwrite_db, open_existing_readonly_db, perform_query_one,
        perform_query_set,
    },
    try_warn,
};
use chainhook_sdk::{
    types::{
        BitcoinBlockData, BitcoinTransactionData, Brc20BalanceData, Brc20Operation,
        Brc20TokenDeployData, Brc20TransferData, OrdinalInscriptionRevealData, OrdinalOperation,
    },
    utils::Context,
};
use rusqlite::{Connection, ToSql, Transaction};

#[derive(Debug, Clone, PartialEq)]
pub struct Brc20DbTokenRow {
    pub inscription_id: String,
    pub inscription_number: u64,
    pub block_height: u64,
    pub tick: String,
    pub display_tick: String,
    pub max: f64,
    pub lim: f64,
    pub dec: u64,
    pub address: String,
    pub self_mint: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Brc20DbLedgerRow {
    pub inscription_id: String,
    pub inscription_number: u64,
    pub ordinal_number: u64,
    pub block_height: u64,
    pub tx_index: u64,
    pub tick: String,
    pub address: String,
    pub avail_balance: f64,
    pub trans_balance: f64,
    pub operation: String,
}

pub fn get_default_brc20_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("brc20.sqlite");
    destination_path
}

pub fn initialize_brc20_db(base_dir: Option<&PathBuf>, ctx: &Context) -> Connection {
    let db_path = base_dir.map(|dir| get_default_brc20_db_file_path(dir));
    let conn = create_or_open_readwrite_db(db_path.as_ref(), ctx);
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS tokens (
            inscription_id TEXT NOT NULL PRIMARY KEY,
            inscription_number INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            tick TEXT NOT NULL,
            display_tick TEXT NOT NULL,
            max REAL NOT NULL,
            lim REAL NOT NULL,
            dec INTEGER NOT NULL,
            address TEXT NOT NULL,
            self_mint BOOL NOT NULL,
            UNIQUE (inscription_id),
            UNIQUE (inscription_number),
            UNIQUE (tick)
        )",
        [],
    ) {
        try_warn!(ctx, "Unable to create table tokens: {}", e.to_string());
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_tokens_on_block_height ON tokens(block_height);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
    }
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS ledger (
            inscription_id TEXT NOT NULL,
            inscription_number INTEGER NOT NULL,
            ordinal_number INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            tx_index INTEGER NOT NULL,
            tick TEXT NOT NULL,
            address TEXT NOT NULL,
            avail_balance REAL NOT NULL,
            trans_balance REAL NOT NULL,
            operation TEXT NOT NULL CHECK(operation IN ('deploy', 'mint', 'transfer', 'transfer_send', 'transfer_receive'))
        )",
        [],
    ) {
        try_warn!(ctx, "Unable to create table ledger: {}", e.to_string());
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_tick_address ON ledger(tick, address);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_ordinal_number_operation ON ledger(ordinal_number, operation);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_block_height_operation ON ledger(block_height, operation);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_inscription_id ON ledger(inscription_id);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_inscription_number ON ledger(inscription_number);",
            [],
        ) {
            try_warn!(ctx, "unable to create brc20.sqlite: {}", e.to_string());
        }
    }

    conn
}

pub fn open_readwrite_brc20_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let db_path = get_default_brc20_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(Some(&db_path), ctx);
    Ok(conn)
}

pub fn open_readonly_brc20_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let db_path = get_default_brc20_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&db_path, ctx);
    Ok(conn)
}

pub fn delete_activity_in_block_range(
    start_block: u32,
    end_block: u32,
    db_tx: &Connection,
    ctx: &Context,
) {
    while let Err(e) = db_tx.execute(
        "DELETE FROM ledger WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        try_warn!(ctx, "unable to query brc20.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = db_tx.execute(
        "DELETE FROM tokens WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        try_warn!(ctx, "unable to query brc20.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn get_token(tick: &str, db_tx: &Connection, ctx: &Context) -> Option<Brc20DbTokenRow> {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap()];
    let query = "
        SELECT tick, display_tick, max, lim, dec, address, inscription_id, inscription_number, block_height, self_mint
        FROM tokens
        WHERE tick = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| Brc20DbTokenRow {
        tick: row.get(0).unwrap(),
        display_tick: row.get(1).unwrap(),
        max: row.get(2).unwrap(),
        lim: row.get(3).unwrap(),
        dec: row.get(4).unwrap(),
        address: row.get(5).unwrap(),
        inscription_id: row.get(6).unwrap(),
        inscription_number: row.get(7).unwrap(),
        block_height: row.get(8).unwrap(),
        self_mint: row.get(9).unwrap(),
    })
}

pub fn get_token_minted_supply(tick: &str, db_tx: &Transaction, ctx: &Context) -> Option<f64> {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap()];
    let query = "
        SELECT COALESCE(SUM(avail_balance + trans_balance), 0.0) AS minted
        FROM ledger
        WHERE tick = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| row.get(0).unwrap()).unwrap_or(None)
}

pub fn get_token_available_balance_for_address(
    tick: &str,
    address: &str,
    db_tx: &Transaction,
    ctx: &Context,
) -> Option<f64> {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap(), &address.to_sql().unwrap()];
    let query = "
        SELECT SUM(avail_balance) AS avail_balance
        FROM ledger
        WHERE tick = ? AND address = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| row.get(0).unwrap()).unwrap_or(None)
}

pub fn get_unsent_token_transfer(
    ordinal_number: u64,
    db_tx: &Connection,
    ctx: &Context,
) -> Option<Brc20DbLedgerRow> {
    let args: &[&dyn ToSql] = &[
        &ordinal_number.to_sql().unwrap(),
        &ordinal_number.to_sql().unwrap(),
    ];
    let query = "
        SELECT inscription_id, inscription_number, ordinal_number, block_height, tx_index, tick, address, avail_balance, trans_balance, operation
        FROM ledger
        WHERE ordinal_number = ? AND operation = 'transfer'
            AND NOT EXISTS (
                SELECT 1 FROM ledger WHERE ordinal_number = ? AND operation = 'transfer_send'
            )
        LIMIT 1
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| Brc20DbLedgerRow {
        inscription_id: row.get(0).unwrap(),
        inscription_number: row.get(1).unwrap(),
        ordinal_number: row.get(2).unwrap(),
        block_height: row.get(3).unwrap(),
        tx_index: row.get(4).unwrap(),
        tick: row.get(5).unwrap(),
        address: row.get(6).unwrap(),
        avail_balance: row.get(7).unwrap(),
        trans_balance: row.get(8).unwrap(),
        operation: row.get(9).unwrap(),
    })
}

pub fn get_transfer_send_receiver_address(
    ordinal_number: u64,
    db_tx: &Connection,
    ctx: &Context,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "
        SELECT address
        FROM ledger
        WHERE ordinal_number = ? AND operation = 'transfer_receive'
        LIMIT 1
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| row.get(0).unwrap())
}

pub fn insert_ledger_rows(rows: &Vec<Brc20DbLedgerRow>, db_tx: &Connection, ctx: &Context) {
    match db_tx.prepare_cached("INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tx_index, tick, address, avail_balance, trans_balance, operation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") {
        Ok(mut stmt) => {
            for row in rows.iter() {
                while let Err(e) = stmt.execute(rusqlite::params![
                    &row.inscription_id,
                    &row.inscription_number,
                    &row.ordinal_number,
                    &row.block_height,
                    &row.tx_index,
                    &row.tick,
                    &row.address,
                    &row.avail_balance,
                    &row.trans_balance,
                    &row.operation
                ]) {
                    try_warn!(ctx, "unable to insert into brc20.sqlite: {}", e.to_string());
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        },
        Err(error) => {try_warn!(ctx, "unable to prepare statement for brc20.sqlite: {}", error.to_string());}
    }
}

pub fn insert_token_rows(rows: &Vec<Brc20DbTokenRow>, db_tx: &Connection, ctx: &Context) {
    match db_tx.prepare_cached(
        "INSERT INTO tokens
        (inscription_id, inscription_number, block_height, tick, display_tick, max, lim, dec, address, self_mint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ) {
        Ok(mut stmt) => {
            for row in rows.iter() {
                while let Err(e) = stmt.execute(rusqlite::params![
                    &row.inscription_id,
                    &row.inscription_number,
                    &row.block_height,
                    &row.tick,
                    &row.display_tick,
                    &row.max,
                    &row.lim,
                    &row.dec,
                    &row.address,
                    &row.self_mint,
                ]) {
                    try_warn!(ctx, "unable to insert into brc20.sqlite: {}", e.to_string());
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        Err(error) => {
            try_warn!(
                ctx,
                "unable to prepare statement for brc20.sqlite: {}",
                error.to_string()
            );
        }
    }
}

pub fn get_brc20_operations_on_block(
    block_height: u64,
    db_tx: &Connection,
    ctx: &Context,
) -> HashMap<u64, Brc20DbLedgerRow> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "
        SELECT
            inscription_id, inscription_number, ordinal_number, block_height, tx_index, tick, address, avail_balance, trans_balance, operation
        FROM ledger AS l
        WHERE block_height = ? AND operation <> 'transfer_receive'
    ";
    let mut map = HashMap::new();
    let rows = perform_query_set(query, args, &db_tx, &ctx, |row| Brc20DbLedgerRow {
        inscription_id: row.get(0).unwrap(),
        inscription_number: row.get(1).unwrap(),
        ordinal_number: row.get(2).unwrap(),
        block_height: row.get(3).unwrap(),
        tx_index: row.get(4).unwrap(),
        tick: row.get(5).unwrap(),
        address: row.get(6).unwrap(),
        avail_balance: row.get(7).unwrap(),
        trans_balance: row.get(8).unwrap(),
        operation: row.get(9).unwrap(),
    });
    for row in rows.iter() {
        map.insert(row.tx_index, row.clone());
    }
    map
}

/// Searches for the BRC-20 operation happening in this transaction in the `brc20.sqlite` DB and writes it to this transaction
/// object's metadata if it exists.
pub fn augment_transaction_with_brc20_operation_data(
    tx: &mut BitcoinTransactionData,
    token_map: &mut HashMap<String, Brc20DbTokenRow>,
    block_ledger_map: &mut HashMap<u64, Brc20DbLedgerRow>,
    db_conn: &Connection,
    ctx: &Context,
) {
    let Some(entry) = block_ledger_map.remove(&(tx.metadata.index as u64)) else {
        return;
    };
    if token_map.get(&entry.tick) == None {
        let Some(row) = get_token(&entry.tick, &db_conn, &ctx) else {
            unreachable!("BRC-20 token not found when processing operation");
        };
        token_map.insert(entry.tick.clone(), row);
    }
    let token = token_map
        .get(&entry.tick)
        .expect("Token not present in map");
    let dec = token.dec as usize;
    match entry.operation.as_str() {
        "deploy" => {
            tx.metadata.brc20_operation = Some(Brc20Operation::Deploy(Brc20TokenDeployData {
                tick: token.display_tick.clone(),
                max: format!("{:.precision$}", token.max, precision = dec),
                lim: format!("{:.precision$}", token.lim, precision = dec),
                dec: token.dec.to_string(),
                address: token.address.clone(),
                inscription_id: token.inscription_id.clone(),
                self_mint: token.self_mint,
            }));
        }
        "mint" => {
            tx.metadata.brc20_operation = Some(Brc20Operation::Mint(Brc20BalanceData {
                tick: token.display_tick.clone(),
                amt: format!("{:.precision$}", entry.avail_balance, precision = dec),
                address: entry.address.clone(),
                inscription_id: entry.inscription_id.clone(),
            }));
        }
        "transfer" => {
            tx.metadata.brc20_operation = Some(Brc20Operation::Transfer(Brc20BalanceData {
                tick: token.display_tick.clone(),
                amt: format!("{:.precision$}", entry.trans_balance, precision = dec),
                address: entry.address.clone(),
                inscription_id: entry.inscription_id.clone(),
            }));
        }
        "transfer_send" => {
            let Some(receiver_address) =
                get_transfer_send_receiver_address(entry.ordinal_number, &db_conn, &ctx)
            else {
                unreachable!(
                    "Unable to fetch receiver address for transfer_send operation {:?}",
                    entry
                );
            };
            tx.metadata.brc20_operation = Some(Brc20Operation::TransferSend(Brc20TransferData {
                tick: token.display_tick.clone(),
                amt: format!("{:.precision$}", entry.trans_balance.abs(), precision = dec),
                sender_address: entry.address.clone(),
                receiver_address,
                inscription_id: entry.inscription_id,
            }));
        }
        // `transfer_receive` ops are not reflected in transaction metadata, they are sent as part of `transfer_send`.
        _ => {}
    }
}

/// Retrieves the inscription number and ordinal number from a `transfer` operation.
fn get_transfer_inscription_info(
    inscription_id: &String,
    db_tx: &Connection,
    ctx: &Context,
) -> Option<(u64, u64)> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let query = "
        SELECT inscription_number, ordinal_number
        FROM ledger
        WHERE inscription_id = ? AND operation = 'transfer'
        LIMIT 1
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| {
        (row.get(0).unwrap(), row.get(1).unwrap())
    })
}

/// Finds an inscription reveal with a specific inscription ID within an augmented block.
fn find_reveal_in_tx<'a>(
    inscription_id: &String,
    tx: &'a BitcoinTransactionData,
) -> Option<&'a OrdinalInscriptionRevealData> {
    for operation in tx.metadata.ordinal_operations.iter() {
        match operation {
            OrdinalOperation::InscriptionRevealed(reveal) => {
                if reveal.inscription_id == *inscription_id {
                    return Some(reveal);
                }
            }
            OrdinalOperation::InscriptionTransferred(_) => return None,
        }
    }
    None
}

/// Takes a block already augmented with BRC-20 data and writes its operations into the brc20.sqlite DB. Called when
/// receiving a block back from Chainhook SDK.
pub fn write_augmented_block_to_brc20_db(
    block: &BitcoinBlockData,
    db_conn: &Connection,
    ctx: &Context,
) {
    let mut tokens: Vec<Brc20DbTokenRow> = vec![];
    let mut ledger_rows: Vec<Brc20DbLedgerRow> = vec![];
    let mut transfers = HashMap::<String, (u64, u64)>::new();
    for tx in block.transactions.iter() {
        if let Some(brc20_operation) = &tx.metadata.brc20_operation {
            match brc20_operation {
                Brc20Operation::Deploy(token) => {
                    let Some(reveal) = find_reveal_in_tx(&token.inscription_id, tx) else {
                        try_warn!(
                            ctx,
                            "Could not find BRC-20 deploy inscription in augmented block: {}",
                            token.inscription_id
                        );
                        continue;
                    };
                    tokens.push(Brc20DbTokenRow {
                        inscription_id: token.inscription_id.clone(),
                        inscription_number: reveal.inscription_number.jubilee as u64,
                        block_height: block.block_identifier.index,
                        tick: token.tick.to_lowercase(),
                        display_tick: token.tick.clone(),
                        max: token.max.parse::<f64>().unwrap(),
                        lim: token.lim.parse::<f64>().unwrap(),
                        dec: token.dec.parse::<u64>().unwrap(),
                        address: token.address.clone(),
                        self_mint: token.self_mint,
                    });
                    ledger_rows.push(Brc20DbLedgerRow {
                        inscription_id: token.inscription_id.clone(),
                        inscription_number: reveal.inscription_number.jubilee as u64,
                        ordinal_number: reveal.ordinal_number,
                        block_height: block.block_identifier.index,
                        tx_index: tx.metadata.index as u64,
                        tick: token.tick.clone(),
                        address: token.address.clone(),
                        avail_balance: 0.0,
                        trans_balance: 0.0,
                        operation: "deploy".to_string(),
                    });
                }
                Brc20Operation::Mint(balance) => {
                    let Some(reveal) = find_reveal_in_tx(&balance.inscription_id, tx) else {
                        try_warn!(
                            ctx,
                            "Could not find BRC-20 mint inscription in augmented block: {}",
                            balance.inscription_id
                        );
                        continue;
                    };
                    ledger_rows.push(Brc20DbLedgerRow {
                        inscription_id: balance.inscription_id.clone(),
                        inscription_number: reveal.inscription_number.jubilee as u64,
                        ordinal_number: reveal.ordinal_number,
                        block_height: block.block_identifier.index,
                        tx_index: tx.metadata.index as u64,
                        tick: balance.tick.clone(),
                        address: balance.address.clone(),
                        avail_balance: balance.amt.parse::<f64>().unwrap(),
                        trans_balance: 0.0,
                        operation: "mint".to_string(),
                    });
                }
                Brc20Operation::Transfer(balance) => {
                    let Some(reveal) = find_reveal_in_tx(&balance.inscription_id, tx) else {
                        try_warn!(
                            ctx,
                            "Could not find BRC-20 transfer inscription in augmented block: {}",
                            balance.inscription_id
                        );
                        continue;
                    };
                    ledger_rows.push(Brc20DbLedgerRow {
                        inscription_id: balance.inscription_id.clone(),
                        inscription_number: reveal.inscription_number.jubilee as u64,
                        ordinal_number: reveal.ordinal_number,
                        block_height: block.block_identifier.index,
                        tx_index: tx.metadata.index as u64,
                        tick: balance.tick.clone(),
                        address: balance.address.clone(),
                        avail_balance: balance.amt.parse::<f64>().unwrap() * -1.0,
                        trans_balance: balance.amt.parse::<f64>().unwrap(),
                        operation: "transfer".to_string(),
                    });
                    transfers.insert(
                        balance.inscription_id.clone(),
                        (
                            reveal.inscription_number.jubilee as u64,
                            reveal.ordinal_number,
                        ),
                    );
                }
                Brc20Operation::TransferSend(transfer) => {
                    let inscription_number: u64;
                    let ordinal_number: u64;
                    if let Some(info) = transfers.get(&transfer.inscription_id) {
                        inscription_number = info.0;
                        ordinal_number = info.1;
                    } else if let Some(info) =
                        get_transfer_inscription_info(&transfer.inscription_id, db_conn, ctx)
                    {
                        inscription_number = info.0;
                        ordinal_number = info.1;
                    } else {
                        try_warn!(
                            ctx,
                            "Could not find BRC-20 transfer inscription in brc20 db: {}",
                            transfer.inscription_id
                        );
                        continue;
                    };
                    let amt = transfer.amt.parse::<f64>().unwrap().abs();
                    ledger_rows.push(Brc20DbLedgerRow {
                        inscription_id: transfer.inscription_id.clone(),
                        inscription_number,
                        ordinal_number,
                        block_height: block.block_identifier.index,
                        tx_index: tx.metadata.index as u64,
                        tick: transfer.tick.clone(),
                        address: transfer.sender_address.clone(),
                        avail_balance: 0.0,
                        trans_balance: amt * -1.0,
                        operation: "transfer_send".to_string(),
                    });
                    ledger_rows.push(Brc20DbLedgerRow {
                        inscription_id: transfer.inscription_id.clone(),
                        inscription_number,
                        ordinal_number,
                        block_height: block.block_identifier.index,
                        tx_index: tx.metadata.index as u64,
                        tick: transfer.tick.clone(),
                        address: transfer.receiver_address.clone(),
                        avail_balance: amt,
                        trans_balance: 0.0,
                        operation: "transfer_receive".to_string(),
                    });
                }
            }
        }
    }
    insert_token_rows(&tokens, db_conn, ctx);
    insert_ledger_rows(&ledger_rows, db_conn, ctx);
}
