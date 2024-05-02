use std::{collections::HashMap, path::PathBuf};

use crate::db::{
    create_or_open_readwrite_db, format_inscription_id, open_existing_readonly_db,
    perform_query_one, perform_query_set,
};
use chainhook_sdk::{
    types::{
        BitcoinTransactionData, BlockIdentifier, Brc20BalanceData, Brc20Operation,
        Brc20TokenDeployData, Brc20TransferData, OrdinalInscriptionRevealData,
        OrdinalInscriptionTransferData,
    },
    utils::Context,
};
use rusqlite::{Connection, ToSql, Transaction};

use super::verifier::{
    VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Brc20DbTokenRow {
    pub inscription_id: String,
    pub inscription_number: u64,
    pub block_height: u64,
    pub tick: String,
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
        ctx.try_log(|logger| warn!(logger, "Unable to create table tokens: {}", e.to_string()));
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_tokens_on_block_height ON tokens(block_height);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
        }
    }
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY,
            inscription_id TEXT NOT NULL,
            inscription_number INTEGER NOT NULL,
            ordinal_number INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            tick TEXT NOT NULL,
            address TEXT NOT NULL,
            avail_balance REAL NOT NULL,
            trans_balance REAL NOT NULL,
            operation TEXT NOT NULL CHECK(operation IN ('deploy', 'mint', 'transfer', 'transfer_send', 'transfer_receive'))
        )",
        [],
    ) {
        ctx.try_log(|logger| warn!(logger, "Unable to create table ledger: {}", e.to_string()));
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_tick_address ON ledger(tick, address);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_ordinal_number_operation ON ledger(ordinal_number, operation);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_block_height_operation ON ledger(block_height, operation);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_inscription_id ON ledger(inscription_id);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_ledger_on_inscription_number ON ledger(inscription_number);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to create brc20.sqlite: {}", e.to_string()));
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
        ctx.try_log(|logger| warn!(logger, "unable to query brc20.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = db_tx.execute(
        "DELETE FROM tokens WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query brc20.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn get_token(tick: &str, db_tx: &Connection, ctx: &Context) -> Option<Brc20DbTokenRow> {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap()];
    let query = "
        SELECT tick, max, lim, dec, address, inscription_id, inscription_number, block_height, self_mint
        FROM tokens
        WHERE tick = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| Brc20DbTokenRow {
        tick: row.get(0).unwrap(),
        max: row.get(1).unwrap(),
        lim: row.get(2).unwrap(),
        dec: row.get(3).unwrap(),
        address: row.get(4).unwrap(),
        inscription_id: row.get(5).unwrap(),
        inscription_number: row.get(6).unwrap(),
        block_height: row.get(7).unwrap(),
        self_mint: row.get(8).unwrap(),
    })
}

pub fn get_token_minted_supply(tick: &str, db_tx: &Transaction, ctx: &Context) -> f64 {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap()];
    let query = "
        SELECT COALESCE(SUM(avail_balance + trans_balance), 0.0) AS minted
        FROM ledger
        WHERE tick = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| row.get(0).unwrap()).unwrap_or(0.0)
}

pub fn get_token_available_balance_for_address(
    tick: &str,
    address: &str,
    db_tx: &Transaction,
    ctx: &Context,
) -> f64 {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap(), &address.to_sql().unwrap()];
    let query = "
        SELECT COALESCE(SUM(avail_balance), 0.0) AS avail_balance
        FROM ledger
        WHERE tick = ? AND address = ?
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| row.get(0).unwrap()).unwrap_or(0.0)
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
        SELECT inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation
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
        tick: row.get(4).unwrap(),
        address: row.get(5).unwrap(),
        avail_balance: row.get(6).unwrap(),
        trans_balance: row.get(7).unwrap(),
        operation: row.get(8).unwrap(),
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

pub fn insert_ledger_rows(rows: &Vec<Brc20DbLedgerRow>, db_tx: &Transaction, ctx: &Context) {
    match db_tx.prepare_cached("INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)") {
        Ok(mut stmt) => {
            for row in rows.iter() {
                while let Err(e) = stmt.execute(rusqlite::params![
                    &row.inscription_id,
                    &row.inscription_number,
                    &row.ordinal_number,
                    &row.block_height,
                    &row.tick,
                    &row.address,
                    &row.avail_balance,
                    &row.trans_balance,
                    &row.operation
                ]) {
                    ctx.try_log(|logger| warn!(logger, "unable to insert into brc20.sqlite: {}", e.to_string()));
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        },
        Err(error) => ctx.try_log(|logger| warn!(logger, "unable to prepare statement for brc20.sqlite: {}", error.to_string()))
    }
}

pub fn insert_token_rows(rows: &Vec<Brc20DbTokenRow>, db_tx: &Transaction, ctx: &Context) {
    match db_tx.prepare_cached(
        "INSERT INTO tokens
        (inscription_id, inscription_number, block_height, tick, max, lim, dec, address, self_mint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ) {
        Ok(mut stmt) => {
            for row in rows.iter() {
                while let Err(e) = stmt.execute(rusqlite::params![
                    &row.inscription_id,
                    &row.inscription_number,
                    &row.block_height,
                    &row.tick,
                    &row.max,
                    &row.lim,
                    &row.dec,
                    &row.address,
                    &row.self_mint,
                ]) {
                    ctx.try_log(|logger| {
                        warn!(
                            logger,
                            "unable to insert into brc20.sqlite: {}",
                            e.to_string()
                        )
                    });
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        Err(error) => ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to prepare statement for brc20.sqlite: {}",
                error.to_string()
            )
        }),
    }
}

pub fn get_brc20_operations_on_block(
    block_identifier: &BlockIdentifier,
    db_tx: &Connection,
    ctx: &Context,
) -> HashMap<String, Brc20DbLedgerRow> {
    let args: &[&dyn ToSql] = &[&block_identifier.index.to_sql().unwrap()];
    let query = "
        SELECT
            inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation
        FROM ledger AS l
        WHERE block_height = ? AND operation <> 'transfer_receive'
    ";
    let mut map = HashMap::new();
    let rows = perform_query_set(query, args, &db_tx, &ctx, |row| Brc20DbLedgerRow {
        inscription_id: row.get(0).unwrap(),
        inscription_number: row.get(1).unwrap(),
        ordinal_number: row.get(2).unwrap(),
        block_height: row.get(3).unwrap(),
        tick: row.get(4).unwrap(),
        address: row.get(5).unwrap(),
        avail_balance: row.get(6).unwrap(),
        trans_balance: row.get(7).unwrap(),
        operation: row.get(8).unwrap(),
    });
    for row in rows.iter() {
        map.insert(row.inscription_id.clone(), row.clone());
    }
    map
}

pub fn augment_transaction_with_brc20_operation_data(
    tx: &mut BitcoinTransactionData,
    token_map: &mut HashMap<String, Brc20DbTokenRow>,
    block_ledger_map: &mut HashMap<String, Brc20DbLedgerRow>,
    db_conn: &Connection,
    ctx: &Context,
) {
    let inscription_id = format_inscription_id(&tx.transaction_identifier, 0);
    let Some(entry) = block_ledger_map.remove(inscription_id.as_str()) else {
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
                tick: token.tick.clone(),
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
                tick: entry.tick.clone(),
                amt: format!("{:.precision$}", entry.avail_balance, precision = dec),
                address: entry.address.clone(),
                inscription_id: entry.inscription_id.clone(),
            }));
        }
        "transfer" => {
            tx.metadata.brc20_operation = Some(Brc20Operation::Transfer(Brc20BalanceData {
                tick: entry.tick.clone(),
                amt: format!("{:.precision$}", entry.trans_balance, precision = dec),
                address: entry.address.clone(),
                inscription_id: entry.inscription_id.clone(),
            }));
        }
        "transfer_send" => {
            let Some(receiver_address) =
                get_transfer_send_receiver_address(entry.ordinal_number, &db_conn, &ctx)
            else {
                unreachable!("Unable to fetch receiver address for transfer_send operation");
            };
            tx.metadata.brc20_operation = Some(Brc20Operation::TransferSend(Brc20TransferData {
                tick: entry.tick.clone(),
                amt: format!(
                    "{:.precision$}",
                    entry.trans_balance * -1.0,
                    precision = dec
                ),
                sender_address: entry.address.clone(),
                receiver_address,
                inscription_id: entry.inscription_id,
            }));
        }
        _ => {}
    }
}
