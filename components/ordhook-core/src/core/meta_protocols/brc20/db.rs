use std::{collections::HashMap, path::PathBuf};

use crate::db::{
    create_or_open_readwrite_db, format_inscription_id, perform_query_exists, perform_query_one,
    perform_query_set,
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

use super::parser::ParsedBrc20TokenDeployData;

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

pub fn token_exists(data: &ParsedBrc20TokenDeployData, db_tx: &Transaction, ctx: &Context) -> bool {
    let args: &[&dyn ToSql] = &[&data.tick.to_sql().unwrap()];
    let query = "SELECT inscription_id FROM tokens WHERE tick = ?";
    perform_query_exists(query, args, &db_tx, ctx)
}

pub fn get_token(tick: &str, db_tx: &Transaction, ctx: &Context) -> Option<Brc20DbTokenRow> {
    let args: &[&dyn ToSql] = &[&tick.to_sql().unwrap()];
    let query = "
        SELECT tick, max, lim, dec, address, inscription_id, inscription_number, block_height
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

pub fn get_unsent_token_transfer_with_sender(
    ordinal_number: u64,
    db_tx: &Transaction,
    ctx: &Context,
) -> Option<Brc20BalanceData> {
    let args: &[&dyn ToSql] = &[
        &ordinal_number.to_sql().unwrap(),
        &ordinal_number.to_sql().unwrap(),
    ];
    let query = "
        SELECT tick, trans_balance, address, inscription_id
        FROM ledger
        WHERE ordinal_number = ? AND operation = 'transfer'
            AND NOT EXISTS (
                SELECT 1 FROM ledger WHERE ordinal_number = ? AND operation = 'transfer_send'
            )
        LIMIT 1
    ";
    perform_query_one(query, args, &db_tx, ctx, |row| Brc20BalanceData {
        tick: row.get(0).unwrap(),
        amt: row.get(1).unwrap(),
        address: row.get(2).unwrap(),
        inscription_id: row.get(3).unwrap(),
    })
}

pub fn get_transfer_send_receiver_address(
    ordinal_number: u64,
    db_tx: &Transaction,
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

pub fn insert_token(
    data: &Brc20TokenDeployData,
    reveal: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    db_tx: &Transaction,
    ctx: &Context,
) {
    while let Err(e) = db_tx.execute(
        "INSERT INTO tokens
        (inscription_id, inscription_number, block_height, tick, max, lim, dec, address)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(tick) DO NOTHING",
        rusqlite::params![
            &reveal.inscription_id,
            &reveal.inscription_number.jubilee,
            &block_identifier.index,
            &data.tick,
            &data.max,
            &data.lim,
            &data.dec,
            &reveal.inscriber_address
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert deploy in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = db_tx.execute(
        "INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0.0, 0.0, 'deploy')",
        rusqlite::params![
            &reveal.inscription_id,
            &reveal.inscription_number.jubilee,
            &reveal.ordinal_number,
            &block_identifier.index,
            &data.tick,
            &data.address
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert deploy in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_token_mint(
    data: &Brc20BalanceData,
    reveal: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    db_tx: &Transaction,
    ctx: &Context,
) {
    while let Err(e) = db_tx.execute(
        "INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0.0, 'mint')",
        rusqlite::params![
            &reveal.inscription_id,
            &reveal.inscription_number.jubilee,
            &reveal.ordinal_number,
            &block_identifier.index,
            &data.tick,
            &data.address,
            &data.amt
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert mint in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_token_transfer(
    data: &Brc20BalanceData,
    reveal: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    db_tx: &Transaction,
    ctx: &Context,
) {
    while let Err(e) = db_tx.execute(
        "INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'transfer')",
        rusqlite::params![
            &reveal.inscription_id,
            &reveal.inscription_number.jubilee,
            &reveal.ordinal_number,
            &block_identifier.index,
            &data.tick,
            &data.address,
            &data.amt * -1.0,
            &data.amt
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert transfer in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_token_transfer_send(
    data: &Brc20TransferData,
    transfer: &OrdinalInscriptionTransferData,
    block_identifier: &BlockIdentifier,
    db_tx: &Transaction,
    ctx: &Context,
) {
    // Get the `inscription_id` and `inscription_number` first from the original transfer BRC-20 op
    // because those are not included in the `OrdinalInscriptionTransferData` arg.
    let args: &[&dyn ToSql] = &[&transfer.ordinal_number.to_sql().unwrap()];
    let query = "
        SELECT inscription_id, inscription_number
        FROM ledger
        WHERE ordinal_number = ? AND operation = 'transfer'
        LIMIT 1
    ";
    let Some((inscription_id, inscription_number)) =
        perform_query_one(query, args, &db_tx, ctx, |row| {
            (
                row.get::<usize, String>(0).unwrap(),
                row.get::<usize, u64>(1).unwrap(),
            )
        })
    else {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to find transfer on brc20.sqlite: {:?}", data
            )
        });
        return;
    };
    // Write balance change for sender and receiver, using `transfer_send` and `transfer_receive`
    // respectively.
    while let Err(e) = db_tx.execute(
        "INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0.0, ?7, 'transfer_send')",
        rusqlite::params![
            &inscription_id,
            &inscription_number,
            &transfer.ordinal_number,
            &block_identifier.index,
            &data.tick,
            &data.sender_address,
            &data.amt * -1.0
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert transfer_send for sender in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = db_tx.execute(
        "INSERT INTO ledger
        (inscription_id, inscription_number, ordinal_number, block_height, tick, address, avail_balance, trans_balance, operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0.0, 'transfer_receive')",
        rusqlite::params![
            &inscription_id,
            &inscription_number,
            &transfer.ordinal_number,
            &block_identifier.index,
            &data.tick,
            &data.receiver_address,
            &data.amt
        ],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to insert transfer_send for sender in brc20.sqlite: {} - {:?}",
                e.to_string(),
                data
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn get_brc20_operations_on_block(
    block_identifier: &BlockIdentifier,
    db_tx: &Transaction,
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
    db_tx: &Transaction,
    ctx: &Context,
) {
    let inscription_id = format_inscription_id(&tx.transaction_identifier, 0);
    let Some(ledger_entry) = block_ledger_map.remove(inscription_id.as_str()) else {
        return;
    };
    if token_map.get(&ledger_entry.tick) == None {
        let Some(row) = get_token(&ledger_entry.tick, &db_tx, &ctx) else {
            unreachable!("BRC-20 token not found when processing operation");
        };
        token_map.insert(ledger_entry.tick.clone(), row);
    }
    let Some(token) = token_map.get(&ledger_entry.tick) else {
        unreachable!();
    };
    match ledger_entry.operation.as_str() {
        "deploy" => {
            tx.metadata.brc20_operations =
                vec![Brc20Operation::TokenDeploy(Brc20TokenDeployData {
                    tick: token.tick.clone(),
                    max: token.max,
                    lim: token.lim,
                    dec: token.dec,
                    address: token.address.clone(),
                    inscription_id: token.inscription_id.clone(),
                })];
        }
        "mint" => {
            tx.metadata.brc20_operations = vec![Brc20Operation::TokenMint(Brc20BalanceData {
                tick: ledger_entry.tick.clone(),
                amt: ledger_entry.avail_balance,
                address: ledger_entry.address.clone(),
                inscription_id: ledger_entry.inscription_id.clone(),
            })];
        }
        "transfer" => {
            tx.metadata.brc20_operations = vec![Brc20Operation::TokenTransfer(Brc20BalanceData {
                tick: ledger_entry.tick.clone(),
                amt: ledger_entry.avail_balance,
                address: ledger_entry.address.clone(),
                inscription_id: ledger_entry.inscription_id.clone(),
            })];
        }
        "transfer_send" => {
            let Some(receiver_address) =
                get_transfer_send_receiver_address(ledger_entry.ordinal_number, &db_tx, &ctx)
            else {
                unreachable!("Unable to fetch receiver address for transfer_send operation");
            };
            tx.metadata.brc20_operations =
                vec![Brc20Operation::TokenTransferSend(Brc20TransferData {
                    tick: ledger_entry.tick.clone(),
                    amt: ledger_entry.trans_balance * -1.0,
                    sender_address: ledger_entry.address.clone(),
                    receiver_address,
                })];
        }
        _ => {}
    }
}
