use crate::indexer::bitcoin::{download_and_parse_block_with_retry, NewBitcoinBlock};
use crate::indexer::{self, Indexer};
use crate::utils::Context;
use hiro_system_kit::slog;
use rocket::serde::json::{json, Json, Value as JsonValue};
use rocket::State;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};

use super::{
    BitcoinConfig, BitcoinRPCRequest, MempoolAdmissionData, ObserverCommand, ObserverMetrics,
    StacksChainMempoolEvent,
};

#[rocket::get("/ping", format = "application/json")]
pub fn handle_ping(
    ctx: &State<Context>,
    metrics_rw_lock: &State<Arc<RwLock<ObserverMetrics>>>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "GET /ping"));
    Json(json!({
        "status": 200,
        "result": metrics_rw_lock.inner(),
    }))
}

#[post("/new_burn_block", format = "json", data = "<bitcoin_block>")]
pub async fn handle_new_bitcoin_block(
    indexer_rw_lock: &State<Arc<RwLock<Indexer>>>,
    bitcoin_config: &State<BitcoinConfig>,
    bitcoin_block: Json<NewBitcoinBlock>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    if bitcoin_config
        .bitcoin_block_signaling
        .should_ignore_bitcoin_block_signaling_through_stacks()
    {
        return Json(json!({
            "status": 200,
            "result": "Ok",
        }));
    }

    ctx.try_log(|logger| slog::info!(logger, "POST /new_burn_block"));
    // Standardize the structure of the block, and identify the
    // kind of update that this new block would imply, taking
    // into account the last 7 blocks.

    let block_hash = bitcoin_block.burn_block_hash.strip_prefix("0x").unwrap();
    let block = match download_and_parse_block_with_retry(block_hash, bitcoin_config, ctx).await {
        Ok(block) => block,
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable to download_and_parse_block: {}",
                    e.to_string()
                )
            });
            return Json(json!({
                "status": 500,
                "result": "unable to retrieve_full_block",
            }));
        }
    };

    let header = block.get_block_header();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::ProcessBitcoinBlock(block));
        }
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable to acquire background_job_tx: {}",
                    e.to_string()
                )
            });
            return Json(json!({
                "status": 500,
                "result": "Unable to acquire lock",
            }));
        }
    };

    let chain_update = match indexer_rw_lock.inner().write() {
        Ok(mut indexer) => indexer.handle_bitcoin_header(header, &ctx),
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable to acquire indexer_rw_lock: {}",
                    e.to_string()
                )
            });
            return Json(json!({
                "status": 500,
                "result": "Unable to acquire lock",
            }));
        }
    };

    match chain_update {
        Ok(Some(chain_event)) => {
            match background_job_tx.lock() {
                Ok(tx) => {
                    let _ = tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "unable to acquire background_job_tx: {}",
                            e.to_string()
                        )
                    });
                    return Json(json!({
                        "status": 500,
                        "result": "Unable to acquire lock",
                    }));
                }
            };
        }
        Ok(None) => {
            ctx.try_log(|logger| slog::info!(logger, "unable to infer chain progress"));
        }
        Err(e) => {
            ctx.try_log(|logger| slog::error!(logger, "unable to handle bitcoin block: {}", e))
        }
    }

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/new_block", format = "application/json", data = "<marshalled_block>")]
pub fn handle_new_stacks_block(
    indexer_rw_lock: &State<Arc<RwLock<Indexer>>>,
    marshalled_block: Json<JsonValue>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /new_block"));
    // Standardize the structure of the block, and identify the
    // kind of update that this new block would imply, taking
    // into account the last 7 blocks.
    // TODO(lgalabru): use _pox_info
    let (_pox_info, chain_event) = match indexer_rw_lock.inner().write() {
        Ok(mut indexer) => {
            let pox_info = indexer.get_pox_info();
            let chain_event =
                indexer.handle_stacks_marshalled_block(marshalled_block.into_inner(), &ctx);
            (pox_info, chain_event)
        }
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable to acquire indexer_rw_lock: {}",
                    e.to_string()
                )
            });
            return Json(json!({
                "status": 500,
                "result": "Unable to acquire lock",
            }));
        }
    };

    match chain_event {
        Ok(Some(chain_event)) => {
            let background_job_tx = background_job_tx.inner();
            match background_job_tx.lock() {
                Ok(tx) => {
                    let _ = tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "unable to acquire background_job_tx: {}",
                            e.to_string()
                        )
                    });
                    return Json(json!({
                        "status": 500,
                        "result": "Unable to acquire lock",
                    }));
                }
            };
        }
        Ok(None) => {
            ctx.try_log(|logger| slog::info!(logger, "unable to infer chain progress"));
        }
        Err(e) => ctx.try_log(|logger| slog::error!(logger, "{}", e)),
    }

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post(
    "/new_microblocks",
    format = "application/json",
    data = "<marshalled_microblock>"
)]
pub fn handle_new_microblocks(
    indexer_rw_lock: &State<Arc<RwLock<Indexer>>>,
    marshalled_microblock: Json<JsonValue>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /new_microblocks"));
    // Standardize the structure of the microblock, and identify the
    // kind of update that this new microblock would imply
    let chain_event = match indexer_rw_lock.inner().write() {
        Ok(mut indexer) => {
            let chain_event = indexer.handle_stacks_marshalled_microblock_trail(
                marshalled_microblock.into_inner(),
                &ctx,
            );
            chain_event
        }
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable to acquire background_job_tx: {}",
                    e.to_string()
                )
            });
            return Json(json!({
                "status": 500,
                "result": "Unable to acquire lock",
            }));
        }
    };

    match chain_event {
        Ok(Some(chain_event)) => {
            let background_job_tx = background_job_tx.inner();
            match background_job_tx.lock() {
                Ok(tx) => {
                    let _ = tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "unable to acquire background_job_tx: {}",
                            e.to_string()
                        )
                    });
                    return Json(json!({
                        "status": 500,
                        "result": "Unable to acquire lock",
                    }));
                }
            };
        }
        Ok(None) => {
            ctx.try_log(|logger| slog::info!(logger, "unable to infer chain progress"));
        }
        Err(e) => {
            ctx.try_log(|logger| slog::error!(logger, "unable to handle stacks microblock: {}", e));
        }
    }

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/new_mempool_tx", format = "application/json", data = "<raw_txs>")]
pub fn handle_new_mempool_tx(
    raw_txs: Json<Vec<String>>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /new_mempool_tx"));
    let transactions = raw_txs
        .iter()
        .map(|tx_data| {
            let (tx_description, ..) = indexer::stacks::get_tx_description(&tx_data, &vec![])
                .expect("unable to parse transaction");
            MempoolAdmissionData {
                tx_data: tx_data.clone(),
                tx_description,
            }
        })
        .collect::<Vec<_>>();

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::PropagateStacksMempoolEvent(
                StacksChainMempoolEvent::TransactionsAdmitted(transactions),
            ));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/drop_mempool_tx", format = "application/json")]
pub fn handle_drop_mempool_tx(ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /drop_mempool_tx"));
    // TODO(lgalabru): use propagate mempool events
    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/attachments/new", format = "application/json")]
pub fn handle_new_attachement(ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /attachments/new"));
    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/mined_block", format = "application/json", data = "<payload>")]
pub fn handle_mined_block(payload: Json<JsonValue>, ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /mined_block {:?}", payload));
    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/mined_microblock", format = "application/json", data = "<payload>")]
pub fn handle_mined_microblock(payload: Json<JsonValue>, ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /mined_microblock {:?}", payload));
    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[post("/wallet", format = "application/json", data = "<bitcoin_rpc_call>")]
pub async fn handle_bitcoin_wallet_rpc_call(
    bitcoin_config: &State<BitcoinConfig>,
    bitcoin_rpc_call: Json<BitcoinRPCRequest>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /wallet"));

    use base64::encode;
    use reqwest::Client;

    let bitcoin_rpc_call = bitcoin_rpc_call.into_inner().clone();

    let body = rocket::serde::json::serde_json::to_vec(&bitcoin_rpc_call).unwrap_or(vec![]);

    let token = encode(format!(
        "{}:{}",
        bitcoin_config.username, bitcoin_config.password
    ));

    let url = format!("{}", bitcoin_config.rpc_url);
    let client = Client::new();
    let builder = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Basic {}", token))
        .timeout(std::time::Duration::from_secs(5));

    match builder.body(body).send().await {
        Ok(res) => Json(res.json().await.unwrap()),
        Err(_) => Json(json!({
            "status": 500
        })),
    }
}

#[post("/", format = "application/json", data = "<bitcoin_rpc_call>")]
pub async fn handle_bitcoin_rpc_call(
    bitcoin_config: &State<BitcoinConfig>,
    bitcoin_rpc_call: Json<BitcoinRPCRequest>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /"));

    use base64::encode;
    use reqwest::Client;

    let bitcoin_rpc_call = bitcoin_rpc_call.into_inner().clone();
    let method = bitcoin_rpc_call.method.clone();

    let body = rocket::serde::json::serde_json::to_vec(&bitcoin_rpc_call).unwrap_or(vec![]);

    let token = encode(format!(
        "{}:{}",
        bitcoin_config.username, bitcoin_config.password
    ));

    ctx.try_log(|logger| {
        slog::debug!(
            logger,
            "Forwarding {} request to {}",
            method,
            bitcoin_config.rpc_url
        )
    });

    let url = if method == "listunspent" {
        format!("{}/wallet/", bitcoin_config.rpc_url)
    } else {
        bitcoin_config.rpc_url.to_string()
    };

    let client = Client::new();
    let builder = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Basic {}", token))
        .timeout(std::time::Duration::from_secs(5));

    if method == "sendrawtransaction" {
        let background_job_tx = background_job_tx.inner();
        match background_job_tx.lock() {
            Ok(tx) => {
                let _ = tx.send(ObserverCommand::NotifyBitcoinTransactionProxied);
            }
            _ => {}
        };
    }

    match builder.body(body).send().await {
        Ok(res) => {
            let payload = res.json().await.unwrap();
            ctx.try_log(|logger| slog::debug!(logger, "Responding with response {:?}", payload));
            Json(payload)
        }
        Err(_) => Json(json!({
            "status": 500
        })),
    }
}
