use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::{mpsc::Sender, Arc, Mutex},
};

use chainhook_sdk::{
    chainhooks::types::{ChainhookFullSpecification, ChainhookSpecification},
    observer::ObserverCommand,
    utils::Context,
};
use rocket::config::{self, Config, LogLevel};
use rocket::serde::json::{json, Json, Value as JsonValue};
use rocket::State;
use std::error::Error;

use super::observers::{
    find_all_observers, find_observer_with_uuid, open_readonly_observers_db_conn, ObserverReport,
};

pub async fn start_predicate_api_server(
    port: u16,
    observers_db_dir_path: PathBuf,
    observer_commands_tx: Sender<ObserverCommand>,
    ctx: Context,
) -> Result<(), Box<dyn Error>> {
    let log_level = LogLevel::Off;

    let mut shutdown_config = config::Shutdown::default();
    shutdown_config.ctrlc = false;
    shutdown_config.grace = 1;
    shutdown_config.mercy = 1;

    let control_config = Config {
        port,
        workers: 1,
        address: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        keep_alive: 5,
        temp_dir: std::env::temp_dir().into(),
        log_level,
        cli_colors: false,
        shutdown: shutdown_config,
        ..Config::default()
    };

    let routes = routes![
        handle_ping,
        handle_get_predicates,
        handle_get_predicate,
        handle_create_predicate,
        handle_delete_bitcoin_predicate,
    ];

    let background_job_tx_mutex = Arc::new(Mutex::new(observer_commands_tx.clone()));

    let ctx_cloned = ctx.clone();

    let ignite = rocket::custom(control_config)
        .manage(background_job_tx_mutex)
        .manage(observers_db_dir_path)
        .manage(ctx_cloned)
        .mount("/", routes)
        .ignite()
        .await?;

    let _ = std::thread::spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });
    Ok(())
}

#[get("/ping")]
fn handle_ping(ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| info!(logger, "Handling HTTP GET /ping"));
    Json(json!({
        "status": 200,
        "result": "chainhook service up and running",
    }))
}

#[get("/v1/observers", format = "application/json")]
fn handle_get_predicates(
    observers_db_dir_path: &State<PathBuf>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| info!(logger, "Handling HTTP GET /v1/observers"));
    match open_readonly_observers_db_conn(observers_db_dir_path, ctx) {
        Ok(mut db_conn) => {
            let observers = find_all_observers(&mut db_conn, &ctx);
            let serialized_predicates = observers
                .iter()
                .map(|(p, s)| serialized_predicate_with_status(p, s))
                .collect::<Vec<_>>();

            Json(json!({
                "status": 200,
                "result": serialized_predicates
            }))
        }
        Err(e) => Json(json!({
            "status": 500,
            "message": e,
        })),
    }
}

#[post("/v1/observers", format = "application/json", data = "<predicate>")]
fn handle_create_predicate(
    predicate: Json<ChainhookFullSpecification>,
    observers_db_dir_path: &State<PathBuf>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| info!(logger, "Handling HTTP POST /v1/observers"));
    let predicate = predicate.into_inner();
    if let Err(e) = predicate.validate() {
        return Json(json!({
            "status": 422,
            "error": e,
        }));
    }

    let predicate_uuid = predicate.get_uuid().to_string();

    if let Ok(mut predicates_db_conn) = open_readonly_observers_db_conn(observers_db_dir_path, ctx)
    {
        let key: String = format!("{}", ChainhookSpecification::bitcoin_key(&predicate_uuid));
        match find_observer_with_uuid(&key, &mut predicates_db_conn, &ctx) {
            Some(_) => {
                return Json(json!({
                    "status": 409,
                    "error": "Predicate uuid already in use",
                }))
            }
            _ => {}
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
        "result": predicate_uuid,
    }))
}

#[get("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_get_predicate(
    predicate_uuid: String,
    observers_db_dir_path: &State<PathBuf>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| info!(logger, "Handling HTTP GET /v1/observers/{}", predicate_uuid));

    match open_readonly_observers_db_conn(observers_db_dir_path, ctx) {
        Ok(mut predicates_db_conn) => {
            let key: String = format!("{}", ChainhookSpecification::bitcoin_key(&predicate_uuid));
            let entry = match find_observer_with_uuid(&key, &mut predicates_db_conn, &ctx) {
                Some((ChainhookSpecification::Bitcoin(spec), report)) => json!({
                    "chain": "bitcoin",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                    "status": report,
                    "enabled": spec.enabled,
                }),
                _ => {
                    return Json(json!({
                        "status": 404,
                    }))
                }
            };
            Json(json!({
                "status": 200,
                "result": entry
            }))
        }
        Err(e) => Json(json!({
            "status": 500,
            "message": e,
        })),
    }
}

#[delete("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_delete_bitcoin_predicate(
    predicate_uuid: String,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| {
        info!(
            logger,
            "Handling HTTP DELETE /v1/observers/{}", predicate_uuid
        )
    });

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

fn serialized_predicate_with_status(
    predicate: &ChainhookSpecification,
    report: &ObserverReport,
) -> JsonValue {
    match (predicate, report) {
        (ChainhookSpecification::Stacks(spec), report) => json!({
            "chain": "stacks",
            "uuid": spec.uuid,
            "network": spec.network,
            "predicate": spec.predicate,
            "status": report,
            "enabled": spec.enabled,
        }),
        (ChainhookSpecification::Bitcoin(spec), report) => json!({
            "chain": "bitcoin",
            "uuid": spec.uuid,
            "network": spec.network,
            "predicate": spec.predicate,
            "status": report,
            "enabled": spec.enabled,
        }),
    }
}
