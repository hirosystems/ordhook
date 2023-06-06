use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{mpsc::Sender, Arc, Mutex, RwLock},
};

use chainhook_event_observer::{
    chainhooks::types::{ChainhookFullSpecification, ChainhookSpecification},
    observer::{ChainhookStore, ObserverCommand},
    utils::Context,
};
use hiro_system_kit::slog;
use redis::Commands;
use rocket::config::{self, Config, LogLevel};
use rocket::serde::json::{json, Json, Value as JsonValue};
use rocket::State;
use rocket_okapi::openapi;
use rocket_okapi::openapi_get_routes;
use std::error::Error;

use crate::config::PredicatesApiConfig;

pub async fn start_predicate_api_server(
    api_config: &PredicatesApiConfig,
    observer_commands_tx: Sender<ObserverCommand>,
    ctx: Context,
) -> Result<(), Box<dyn Error>> {
    let log_level = if api_config.display_logs {
        LogLevel::Critical
    } else {
        LogLevel::Off
    };

    let mut shutdown_config = config::Shutdown::default();
    shutdown_config.ctrlc = false;
    shutdown_config.grace = 1;
    shutdown_config.mercy = 1;

    let control_config = Config {
        port: api_config.http_port,
        workers: 1,
        address: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        keep_alive: 5,
        temp_dir: std::env::temp_dir().into(),
        log_level,
        cli_colors: false,
        shutdown: shutdown_config,
        ..Config::default()
    };

    let routes = openapi_get_routes![
        handle_ping,
        handle_get_predicates,
        handle_get_predicate,
        handle_create_predicate,
        handle_delete_bitcoin_predicate,
        handle_delete_stacks_predicate
    ];

    let background_job_tx_mutex = Arc::new(Mutex::new(observer_commands_tx.clone()));
    let ctx_cloned = ctx.clone();

    let ignite = rocket::custom(control_config)
        .manage(background_job_tx_mutex)
        .manage(ctx_cloned)
        .mount("/", routes)
        .ignite()
        .await?;

    let _ = std::thread::spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });
    Ok(())
}

#[openapi(tag = "Chainhooks")]
#[get("/ping")]
fn handle_ping(ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "GET /ping"));
    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[openapi(tag = "Chainhooks")]
#[get("/v1/chainhooks", format = "application/json")]
fn handle_get_predicates(
    chainhook_store: &State<Arc<RwLock<ChainhookStore>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "GET /v1/chainhooks"));
    if let Ok(chainhook_store_reader) = chainhook_store.inner().read() {
        let mut predicates = vec![];
        let mut stacks_predicates = chainhook_store_reader
            .predicates
            .get_serialized_stacks_predicates()
            .iter()
            .map(|(uuid, network, predicate)| {
                json!({
                    "chain": "stacks",
                    "uuid": uuid,
                    "network": network,
                    "predicate": predicate,
                })
            })
            .collect::<Vec<_>>();
        predicates.append(&mut stacks_predicates);

        let mut bitcoin_predicates = chainhook_store_reader
            .predicates
            .get_serialized_bitcoin_predicates()
            .iter()
            .map(|(uuid, network, predicate)| {
                json!({
                    "chain": "bitcoin",
                    "uuid": uuid,
                    "network": network,
                    "predicate": predicate,
                })
            })
            .collect::<Vec<_>>();
        predicates.append(&mut bitcoin_predicates);

        Json(json!({
            "status": 200,
            "result": predicates
        }))
    } else {
        Json(json!({
            "status": 500,
            "message": "too many requests",
        }))
    }
}

#[openapi(tag = "Chainhooks")]
#[post("/v1/chainhooks", format = "application/json", data = "<predicate>")]
fn handle_create_predicate(
    predicate: Json<ChainhookFullSpecification>,
    chainhook_store: &State<Arc<RwLock<ChainhookStore>>>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "POST /v1/chainhooks"));
    let predicate = predicate.into_inner();
    if let Err(e) = predicate.validate() {
        return Json(json!({
            "status": 422,
            "error": e,
        }));
    }

    if let Ok(chainhook_store_reader) = chainhook_store.inner().read() {
        if let Some(_) = chainhook_store_reader
            .predicates
            .get_spec_with_uuid(predicate.get_uuid())
        {
            return Json(json!({
                "status": 409,
                "error": "uuid already in use",
            }));
        }
    }

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::RegisterPredicate(predicate));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[openapi(tag = "Chainhooks")]
#[get("/v1/chainhooks/<predicate_uuid>", format = "application/json")]
fn handle_get_predicate(
    predicate_uuid: String,
    chainhook_store: &State<Arc<RwLock<ChainhookStore>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "GET /v1/chainhooks/<predicate_uuid>"));
    if let Ok(chainhook_store_reader) = chainhook_store.inner().read() {
        let predicate = match chainhook_store_reader
            .predicates
            .get_spec_with_uuid(&predicate_uuid)
        {
            Some(ChainhookSpecification::Stacks(spec)) => {
                json!({
                    "chain": "stacks",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                })
            }
            Some(ChainhookSpecification::Bitcoin(spec)) => {
                json!({
                    "chain": "bitcoin",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                })
            }
            None => {
                return Json(json!({
                    "status": 404,
                }))
            }
        };
        return Json(json!({
            "status": 200,
            "result": predicate
        }));
    } else {
        Json(json!({
            "status": 500,
            "message": "too many requests",
        }))
    }
}

#[openapi(tag = "Chainhooks")]
#[delete("/v1/chainhooks/stacks/<predicate_uuid>", format = "application/json")]
fn handle_delete_stacks_predicate(
    predicate_uuid: String,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "DELETE /v1/chainhooks/stacks/<predicate_uuid>"));

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::DeregisterStacksPredicate(predicate_uuid));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

#[openapi(tag = "Chainhooks")]
#[delete("/v1/chainhooks/bitcoin/<predicate_uuid>", format = "application/json")]
fn handle_delete_bitcoin_predicate(
    predicate_uuid: String,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "DELETE /v1/chainhooks/stacks/<predicate_uuid>"));

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::DeregisterBitcoinPredicate(predicate_uuid));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

pub fn load_predicates_from_redis(
    config: &crate::config::Config,
    ctx: &Context,
) -> Result<Vec<ChainhookSpecification>, String> {
    let redis_uri = config.expected_api_database_uri();
    let client = redis::Client::open(redis_uri.clone())
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?;
    let mut redis_con = client
        .get_connection()
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?;
    let chainhooks_to_load: Vec<String> = redis_con
        .scan_match("chainhook:*:*:*")
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?
        .into_iter()
        .collect();

    let mut predicates = vec![];
    for key in chainhooks_to_load.iter() {
        let chainhook = match redis_con.hget::<_, _, String>(key, "specification") {
            Ok(spec) => match ChainhookSpecification::deserialize_specification(&spec, key) {
                Ok(spec) => spec,
                Err(e) => {
                    error!(
                        ctx.expect_logger(),
                        "unable to load chainhook associated with key {}: {}",
                        key,
                        e.to_string()
                    );
                    continue;
                }
            },
            Err(e) => {
                error!(
                    ctx.expect_logger(),
                    "unable to load chainhook associated with key {}: {}",
                    key,
                    e.to_string()
                );
                continue;
            }
        };
        predicates.push(chainhook);
    }
    Ok(predicates)
}
