mod http;

use crate::chainhooks::bitcoin::{
    evaluate_bitcoin_chainhooks_on_chain_event, handle_bitcoin_hook_action,
    BitcoinChainhookOccurrence, BitcoinChainhookOccurrencePayload, BitcoinTriggerChainhook,
};
use crate::chainhooks::stacks::{
    evaluate_stacks_chainhooks_on_chain_event, handle_stacks_hook_action,
    StacksChainhookOccurrence, StacksChainhookOccurrencePayload,
};
use crate::chainhooks::types::{
    ChainhookConfig, ChainhookFullSpecification, ChainhookSpecification,
};

#[cfg(feature = "ordinals")]
use crate::hord::{
    db::open_readwrite_hord_dbs, new_traversals_lazy_cache,
    revert_hord_db_with_augmented_bitcoin_block, update_hord_db_and_augment_bitcoin_block,
    HordConfig,
};
use crate::indexer::bitcoin::{
    download_and_parse_block_with_retry, standardize_bitcoin_block, BitcoinBlockFullBreakdown,
};
use crate::indexer::{Indexer, IndexerConfig};
use crate::utils::{send_request, Context};

use bitcoincore_rpc::bitcoin::{BlockHash, Txid};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use chainhook_types::{
    BitcoinBlockData, BitcoinBlockSignaling, BitcoinChainEvent, BitcoinChainUpdatedWithBlocksData,
    BitcoinChainUpdatedWithReorgData, BitcoinNetwork, BlockIdentifier, BlockchainEvent,
    StacksChainEvent, StacksNetwork, StacksNodeConfig, TransactionIdentifier,
};
use hiro_system_kit;
use hiro_system_kit::slog;
use reqwest::Client as HttpClient;
use rocket::config::{self, Config, LogLevel};
use rocket::data::{Limits, ToByteUnit};
use rocket::serde::Deserialize;
use rocket::Shutdown;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::str;
use std::str::FromStr;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "zeromq")]
use zeromq::{Socket, SocketRecv};

pub const DEFAULT_INGESTION_PORT: u16 = 20445;

#[derive(Deserialize)]
pub struct NewTransaction {
    pub txid: String,
    pub status: String,
    pub raw_result: String,
    pub raw_tx: String,
}

#[derive(Clone, Debug)]
pub enum Event {
    BitcoinChainEvent(BitcoinChainEvent),
    StacksChainEvent(StacksChainEvent),
}

// TODO(lgalabru): Support for GRPC?
#[derive(Deserialize, Debug, Clone)]
pub enum EventHandler {
    WebHook(String),
}

impl EventHandler {
    async fn propagate_stacks_event(&self, stacks_event: &StacksChainEvent) {
        match self {
            EventHandler::WebHook(host) => {
                let path = "chain-events/stacks";
                let url = format!("{}/{}", host, path);
                let body = rocket::serde::json::serde_json::to_vec(&stacks_event).unwrap_or(vec![]);
                let http_client = HttpClient::builder()
                    .timeout(Duration::from_secs(20))
                    .build()
                    .expect("Unable to build http client");
                let _ = http_client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await;
                // TODO(lgalabru): handle response errors
            }
        }
    }

    async fn propagate_bitcoin_event(&self, bitcoin_event: &BitcoinChainEvent) {
        match self {
            EventHandler::WebHook(host) => {
                let path = "chain-events/bitcoin";
                let url = format!("{}/{}", host, path);
                let body =
                    rocket::serde::json::serde_json::to_vec(&bitcoin_event).unwrap_or(vec![]);
                let http_client = HttpClient::builder()
                    .timeout(Duration::from_secs(20))
                    .build()
                    .expect("Unable to build http client");
                let _res = http_client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await;
                // TODO(lgalabru): handle response errors
            }
        }
    }

    async fn notify_bitcoin_transaction_proxied(&self) {}
}

#[derive(Debug, Clone)]
pub struct EventObserverConfig {
    pub chainhook_config: Option<ChainhookConfig>,
    pub bitcoin_rpc_proxy_enabled: bool,
    pub event_handlers: Vec<EventHandler>,
    pub ingestion_port: u16,
    pub bitcoind_rpc_username: String,
    pub bitcoind_rpc_password: String,
    pub bitcoind_rpc_url: String,
    pub bitcoin_block_signaling: BitcoinBlockSignaling,
    pub display_logs: bool,
    pub cache_path: String,
    pub bitcoin_network: BitcoinNetwork,
    pub stacks_network: StacksNetwork,
    #[cfg(feature = "ordinals")]
    pub hord_config: Option<HordConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct EventObserverConfigOverrides {
    pub ingestion_port: Option<u16>,
    pub bitcoind_rpc_username: Option<String>,
    pub bitcoind_rpc_password: Option<String>,
    pub bitcoind_rpc_url: Option<String>,
    pub bitcoind_zmq_url: Option<String>,
    pub stacks_node_rpc_url: Option<String>,
    pub display_logs: Option<bool>,
    pub cache_path: Option<String>,
    pub bitcoin_network: Option<String>,
    pub stacks_network: Option<String>,
}

impl EventObserverConfig {
    pub fn get_cache_path_buf(&self) -> PathBuf {
        let mut path_buf = PathBuf::new();
        path_buf.push(&self.cache_path);
        path_buf
    }

    pub fn get_bitcoin_config(&self) -> BitcoinConfig {
        let bitcoin_config = BitcoinConfig {
            username: self.bitcoind_rpc_username.clone(),
            password: self.bitcoind_rpc_password.clone(),
            rpc_url: self.bitcoind_rpc_url.clone(),
            network: self.bitcoin_network.clone(),
            bitcoin_block_signaling: self.bitcoin_block_signaling.clone(),
        };
        bitcoin_config
    }

    pub fn get_stacks_node_config(&self) -> &StacksNodeConfig {
        match self.bitcoin_block_signaling {
            BitcoinBlockSignaling::Stacks(ref config) => config,
            _ => unreachable!(),
        }
    }

    pub fn new_using_overrides(
        overrides: Option<&EventObserverConfigOverrides>,
    ) -> Result<EventObserverConfig, String> {
        let bitcoin_network =
            if let Some(network) = overrides.and_then(|c| c.bitcoin_network.as_ref()) {
                BitcoinNetwork::from_str(network)?
            } else {
                BitcoinNetwork::Regtest
            };

        let stacks_network =
            if let Some(network) = overrides.and_then(|c| c.stacks_network.as_ref()) {
                StacksNetwork::from_str(network)?
            } else {
                StacksNetwork::Devnet
            };

        let config = EventObserverConfig {
            bitcoin_rpc_proxy_enabled: false,
            event_handlers: vec![],
            chainhook_config: None,
            ingestion_port: overrides
                .and_then(|c| c.ingestion_port)
                .unwrap_or(DEFAULT_INGESTION_PORT),
            bitcoind_rpc_username: overrides
                .and_then(|c| c.bitcoind_rpc_username.clone())
                .unwrap_or("devnet".to_string()),
            bitcoind_rpc_password: overrides
                .and_then(|c| c.bitcoind_rpc_password.clone())
                .unwrap_or("devnet".to_string()),
            bitcoind_rpc_url: overrides
                .and_then(|c| c.bitcoind_rpc_url.clone())
                .unwrap_or("http://localhost:18443".to_string()),
            bitcoin_block_signaling: overrides
                .and_then(|c| match c.bitcoind_zmq_url.as_ref() {
                    Some(url) => Some(BitcoinBlockSignaling::ZeroMQ(url.clone())),
                    None => Some(BitcoinBlockSignaling::Stacks(
                        StacksNodeConfig::default_localhost(
                            overrides
                                .and_then(|c| c.ingestion_port)
                                .unwrap_or(DEFAULT_INGESTION_PORT),
                        ),
                    )),
                })
                .unwrap_or(BitcoinBlockSignaling::Stacks(
                    StacksNodeConfig::default_localhost(
                        overrides
                            .and_then(|c| c.ingestion_port)
                            .unwrap_or(DEFAULT_INGESTION_PORT),
                    ),
                )),
            display_logs: overrides.and_then(|c| c.display_logs).unwrap_or(false),
            cache_path: overrides
                .and_then(|c| c.cache_path.clone())
                .unwrap_or("cache".to_string()),
            bitcoin_network,
            stacks_network,
            #[cfg(feature = "ordinals")]
            hord_config: None,
        };
        Ok(config)
    }
}

#[derive(Deserialize, Debug)]
pub struct ContractReadonlyCall {
    pub okay: bool,
    pub result: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObserverCommand {
    ProcessBitcoinBlock(BitcoinBlockFullBreakdown),
    CacheBitcoinBlock(BitcoinBlockData),
    PropagateBitcoinChainEvent(BlockchainEvent),
    PropagateStacksChainEvent(StacksChainEvent),
    PropagateStacksMempoolEvent(StacksChainMempoolEvent),
    RegisterPredicate(ChainhookFullSpecification),
    EnablePredicate(ChainhookSpecification),
    DeregisterBitcoinPredicate(String),
    DeregisterStacksPredicate(String),
    NotifyBitcoinTransactionProxied,
    Terminate,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StacksChainMempoolEvent {
    TransactionsAdmitted(Vec<MempoolAdmissionData>),
    TransactionDropped(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MempoolAdmissionData {
    pub tx_data: String,
    pub tx_description: String,
}

#[derive(Clone, Debug)]
pub struct PredicateEvaluationReport {
    pub predicates_evaluated: BTreeMap<String, BTreeSet<BlockIdentifier>>,
    pub predicates_triggered: BTreeMap<String, BTreeSet<BlockIdentifier>>,
}

impl PredicateEvaluationReport {
    pub fn new() -> PredicateEvaluationReport {
        PredicateEvaluationReport {
            predicates_evaluated: BTreeMap::new(),
            predicates_triggered: BTreeMap::new(),
        }
    }

    pub fn track_evaluation(&mut self, uuid: &str, block_identifier: &BlockIdentifier) {
        self.predicates_evaluated
            .entry(uuid.to_string())
            .and_modify(|e| {
                e.insert(block_identifier.clone());
            })
            .or_insert_with(|| {
                let mut set = BTreeSet::new();
                set.insert(block_identifier.clone());
                set
            });
    }

    pub fn track_trigger(&mut self, uuid: &str, blocks: &Vec<&BlockIdentifier>) {
        for block_id in blocks.into_iter() {
            self.predicates_triggered
                .entry(uuid.to_string())
                .and_modify(|e| {
                    e.insert((*block_id).clone());
                })
                .or_insert_with(|| {
                    let mut set = BTreeSet::new();
                    set.insert((*block_id).clone());
                    set
                });
        }
    }
}

#[derive(Clone, Debug)]
pub enum ObserverEvent {
    Error(String),
    Fatal(String),
    Info(String),
    BitcoinChainEvent((BitcoinChainEvent, PredicateEvaluationReport)),
    StacksChainEvent((StacksChainEvent, PredicateEvaluationReport)),
    NotifyBitcoinTransactionProxied,
    PredicateRegistered(ChainhookSpecification),
    PredicateDeregistered(ChainhookSpecification),
    PredicateEnabled(ChainhookSpecification),
    BitcoinPredicateTriggered(BitcoinChainhookOccurrencePayload),
    StacksPredicateTriggered(StacksChainhookOccurrencePayload),
    PredicatesTriggered(usize),
    Terminate,
    StacksChainMempoolEvent(StacksChainMempoolEvent),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// JSONRPC Request
pub struct BitcoinRPCRequest {
    /// The name of the RPC call
    pub method: String,
    /// Parameters to the RPC call
    pub params: serde_json::Value,
    /// Identifier for this Request, which should appear in the response
    pub id: serde_json::Value,
    /// jsonrpc field, MUST be "2.0"
    pub jsonrpc: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct BitcoinConfig {
    pub username: String,
    pub password: String,
    pub rpc_url: String,
    pub network: BitcoinNetwork,
    pub bitcoin_block_signaling: BitcoinBlockSignaling,
}

#[derive(Debug, Clone)]
pub struct ChainhookStore {
    pub predicates: ChainhookConfig,
}

impl ChainhookStore {
    pub fn new() -> Self {
        Self {
            predicates: ChainhookConfig {
                stacks_chainhooks: vec![],
                bitcoin_chainhooks: vec![],
            },
        }
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct ReorgMetrics {
    timestamp: i64,
    applied_blocks: usize,
    rolled_back_blocks: usize,
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct ChainMetrics {
    pub tip_height: u64,
    pub last_reorg: Option<ReorgMetrics>,
    pub last_block_ingestion_at: u128,
    pub registered_predicates: usize,
    pub deregistered_predicates: usize,
}

impl ChainMetrics {
    pub fn deregister_prediate(&mut self) {
        self.registered_predicates -= 1;
        self.deregistered_predicates += 1;
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct ObserverMetrics {
    pub bitcoin: ChainMetrics,
    pub stacks: ChainMetrics,
}

pub async fn start_event_observer(
    mut config: EventObserverConfig,
    observer_commands_tx: Sender<ObserverCommand>,
    observer_commands_rx: Receiver<ObserverCommand>,
    observer_events_tx: Option<crossbeam_channel::Sender<ObserverEvent>>,
    ctx: Context,
) -> Result<(), Box<dyn Error>> {
    let indexer_config = IndexerConfig {
        bitcoind_rpc_url: config.bitcoind_rpc_url.clone(),
        bitcoind_rpc_username: config.bitcoind_rpc_username.clone(),
        bitcoind_rpc_password: config.bitcoind_rpc_password.clone(),
        stacks_network: StacksNetwork::Devnet,
        bitcoin_network: BitcoinNetwork::Regtest,
        bitcoin_block_signaling: config.bitcoin_block_signaling.clone(),
    };

    let indexer = Indexer::new(indexer_config.clone());

    let log_level = if config.display_logs {
        if cfg!(feature = "cli") {
            LogLevel::Critical
        } else {
            LogLevel::Debug
        }
    } else {
        LogLevel::Off
    };

    let ingestion_port = config.get_stacks_node_config().ingestion_port;
    let bitcoin_rpc_proxy_enabled = config.bitcoin_rpc_proxy_enabled;
    let bitcoin_config = config.get_bitcoin_config();

    let mut chainhook_store = ChainhookStore::new();
    // If authorization not required, we create a default ChainhookConfig
    if let Some(ref mut initial_chainhook_config) = config.chainhook_config {
        chainhook_store
            .predicates
            .stacks_chainhooks
            .append(&mut initial_chainhook_config.stacks_chainhooks);
        chainhook_store
            .predicates
            .bitcoin_chainhooks
            .append(&mut initial_chainhook_config.bitcoin_chainhooks);
    }

    let indexer_rw_lock = Arc::new(RwLock::new(indexer));

    let background_job_tx_mutex = Arc::new(Mutex::new(observer_commands_tx.clone()));

    let observer_metrics = ObserverMetrics {
        bitcoin: ChainMetrics {
            registered_predicates: chainhook_store.predicates.bitcoin_chainhooks.len(),
            ..Default::default()
        },
        stacks: ChainMetrics {
            registered_predicates: chainhook_store.predicates.stacks_chainhooks.len(),
            ..Default::default()
        },
    };
    let observer_metrics_rw_lock = Arc::new(RwLock::new(observer_metrics));

    let limits = Limits::default().limit("json", 20.megabytes());
    let mut shutdown_config = config::Shutdown::default();
    shutdown_config.ctrlc = false;
    shutdown_config.grace = 0;
    shutdown_config.mercy = 0;

    let ingestion_config = Config {
        port: ingestion_port,
        workers: 3,
        address: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        keep_alive: 5,
        temp_dir: std::env::temp_dir().into(),
        log_level: log_level.clone(),
        cli_colors: false,
        limits,
        shutdown: shutdown_config,
        ..Config::default()
    };

    let mut routes = rocket::routes![
        http::handle_ping,
        http::handle_new_bitcoin_block,
        http::handle_new_stacks_block,
        http::handle_new_microblocks,
        http::handle_new_mempool_tx,
        http::handle_drop_mempool_tx,
        http::handle_new_attachement,
        http::handle_mined_block,
        http::handle_mined_microblock,
    ];

    if bitcoin_rpc_proxy_enabled {
        routes.append(&mut routes![http::handle_bitcoin_rpc_call]);
        routes.append(&mut routes![http::handle_bitcoin_wallet_rpc_call]);
    }

    let ctx_cloned = ctx.clone();
    let ignite = rocket::custom(ingestion_config)
        .manage(indexer_rw_lock)
        .manage(background_job_tx_mutex)
        .manage(bitcoin_config)
        .manage(ctx_cloned)
        .manage(observer_metrics_rw_lock.clone())
        .mount("/", routes)
        .ignite()
        .await?;
    let ingestion_shutdown = Some(ignite.shutdown());

    let _ = std::thread::spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });

    #[cfg(feature = "zeromq")]
    start_zeromq_runloop(&config, observer_commands_tx, &ctx);

    // This loop is used for handling background jobs, emitted by HTTP calls.
    start_observer_commands_handler(
        config,
        chainhook_store,
        observer_commands_rx,
        observer_events_tx,
        ingestion_shutdown,
        observer_metrics_rw_lock.clone(),
        ctx,
    )
    .await
}

pub fn get_bitcoin_proof(
    bitcoin_client_rpc: &Client,
    transaction_identifier: &TransactionIdentifier,
    block_identifier: &BlockIdentifier,
) -> Result<String, String> {
    let txid =
        Txid::from_str(&transaction_identifier.get_hash_bytes_str()).expect("unable to build txid");
    let block_hash =
        BlockHash::from_str(&block_identifier.hash[2..]).expect("unable to build block_hash");

    let res = bitcoin_client_rpc.get_tx_out_proof(&vec![txid], Some(&block_hash));
    match res {
        Ok(proof) => Ok(format!("0x{}", hex::encode(&proof))),
        Err(e) => Err(format!(
            "failed collecting proof for transaction {}: {}",
            transaction_identifier.hash,
            e.to_string()
        )),
    }
}

#[allow(unused_variables, unused_imports)]
pub fn start_zeromq_runloop(
    config: &EventObserverConfig,
    observer_commands_tx: Sender<ObserverCommand>,
    ctx: &Context,
) {
    #[cfg(feature = "zeromq")]
    {
        use crate::indexer::fork_scratch_pad::ForkScratchPad;

        if let BitcoinBlockSignaling::ZeroMQ(ref bitcoind_zmq_url) = config.bitcoin_block_signaling
        {
            let bitcoind_zmq_url = bitcoind_zmq_url.clone();
            let ctx_moved = ctx.clone();
            let bitcoin_config = config.get_bitcoin_config();

            hiro_system_kit::thread_named("Bitcoind zmq listener")
                .spawn(move || {
                    ctx_moved.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Waiting for ZMQ connection acknowledgment from bitcoind"
                        )
                    });

                    let _: Result<(), Box<dyn Error>> =
                        hiro_system_kit::nestable_block_on(async move {
                            let mut socket = zeromq::SubSocket::new();

                            socket
                                .connect(&bitcoind_zmq_url)
                                .await
                                .expect("Failed to connect");

                            socket.subscribe("").await?;
                            ctx_moved.try_log(|logger| {
                                slog::info!(logger, "Waiting for ZMQ messages from bitcoind")
                            });

                            let mut bitcoin_blocks_pool = ForkScratchPad::new();

                            loop {
                                let message = match socket.recv().await {
                                    Ok(message) => message,
                                    Err(e) => {
                                        ctx_moved.try_log(|logger| {
                                            slog::error!(
                                                logger,
                                                "Unable to receive ZMQ message: {}",
                                                e.to_string()
                                            )
                                        });
                                        continue;
                                    }
                                };
                                let block_hash = hex::encode(message.get(1).unwrap().to_vec());

                                let block = match download_and_parse_block_with_retry(
                                    &block_hash,
                                    &bitcoin_config,
                                    &ctx_moved,
                                )
                                .await
                                {
                                    Ok(block) => block,
                                    Err(e) => {
                                        ctx_moved.try_log(|logger| {
                                            slog::warn!(
                                                logger,
                                                "unable to download_and_parse_block: {}",
                                                e.to_string()
                                            )
                                        });
                                        continue;
                                    }
                                };

                                ctx_moved.try_log(|logger| {
                                    slog::info!(
                                        logger,
                                        "Bitcoin block #{} dispatched for processing",
                                        block.height
                                    )
                                });

                                let header = block.get_block_header();
                                let _ = observer_commands_tx
                                    .send(ObserverCommand::ProcessBitcoinBlock(block));

                                if let Ok(Some(event)) =
                                    bitcoin_blocks_pool.process_header(header, &ctx_moved)
                                {
                                    let _ = observer_commands_tx
                                        .send(ObserverCommand::PropagateBitcoinChainEvent(event));
                                }
                            }
                        });
                })
                .expect("unable to spawn thread");
        }
    }
}

pub fn pre_process_bitcoin_block() {}

pub fn apply_bitcoin_block() {}

pub fn rollback_bitcoin_block() {}

pub fn gather_proofs<'a>(
    trigger: &BitcoinTriggerChainhook<'a>,
    proofs: &mut HashMap<&'a TransactionIdentifier, String>,
    config: &EventObserverConfig,
    ctx: &Context,
) {
    let bitcoin_client_rpc = Client::new(
        &config.bitcoind_rpc_url,
        Auth::UserPass(
            config.bitcoind_rpc_username.to_string(),
            config.bitcoind_rpc_password.to_string(),
        ),
    )
    .expect("unable to build http client");

    for (transactions, block) in trigger.apply.iter() {
        for transaction in transactions.iter() {
            if !proofs.contains_key(&transaction.transaction_identifier) {
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Collecting proof for transaction {}",
                        transaction.transaction_identifier.hash
                    )
                });
                match get_bitcoin_proof(
                    &bitcoin_client_rpc,
                    &transaction.transaction_identifier,
                    &block.block_identifier,
                ) {
                    Ok(proof) => {
                        proofs.insert(&transaction.transaction_identifier, proof);
                    }
                    Err(e) => {
                        ctx.try_log(|logger| slog::error!(logger, "{e}"));
                    }
                }
            }
        }
    }
}

pub async fn start_observer_commands_handler(
    config: EventObserverConfig,
    mut chainhook_store: ChainhookStore,
    observer_commands_rx: Receiver<ObserverCommand>,
    observer_events_tx: Option<crossbeam_channel::Sender<ObserverEvent>>,
    ingestion_shutdown: Option<Shutdown>,
    observer_metrics: Arc<RwLock<ObserverMetrics>>,
    ctx: Context,
) -> Result<(), Box<dyn Error>> {
    let mut chainhooks_occurrences_tracker: HashMap<String, u64> = HashMap::new();
    let event_handlers = config.event_handlers.clone();
    let networks = (&config.bitcoin_network, &config.stacks_network);
    let mut bitcoin_block_store: HashMap<BlockIdentifier, BitcoinBlockData> = HashMap::new();
    #[cfg(feature = "ordinals")]
    let cache_size = config
        .hord_config
        .as_ref()
        .and_then(|c| Some(c.cache_size))
        .unwrap_or(0);

    #[cfg(feature = "ordinals")]
    let traversals_cache = Arc::new(new_traversals_lazy_cache(cache_size));

    loop {
        let command = match observer_commands_rx.recv() {
            Ok(cmd) => cmd,
            Err(e) => {
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::Error(format!("Channel error: {:?}", e)));
                }
                continue;
            }
        };
        match command {
            ObserverCommand::Terminate => {
                ctx.try_log(|logger| slog::info!(logger, "Handling Termination command"));
                if let Some(ingestion_shutdown) = ingestion_shutdown {
                    ingestion_shutdown.notify();
                }
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::Info("Terminating event observer".into()));
                    let _ = tx.send(ObserverEvent::Terminate);
                }
                break;
            }
            ObserverCommand::ProcessBitcoinBlock(mut block_data) => {
                let block_hash = block_data.hash.to_string();
                let new_block = loop {
                    match standardize_bitcoin_block(
                        block_data.clone(),
                        &config.bitcoin_network,
                        &ctx,
                    ) {
                        Ok(block) => break block,
                        Err((e, retry)) => {
                            ctx.try_log(|logger| {
                                slog::error!(logger, "Error standardizing block: {}", e)
                            });
                            if retry {
                                block_data = match download_and_parse_block_with_retry(
                                    &block_hash,
                                    &config.get_bitcoin_config(),
                                    &ctx,
                                )
                                .await
                                {
                                    Ok(block) => block,
                                    Err(e) => {
                                        ctx.try_log(|logger| {
                                            slog::warn!(
                                                logger,
                                                "unable to download_and_parse_block: {}",
                                                e.to_string()
                                            )
                                        });
                                        continue;
                                    }
                                };
                            }
                        }
                    };
                };
                match observer_metrics.write() {
                    Ok(mut metrics) => {
                        if new_block.block_identifier.index > metrics.bitcoin.tip_height {
                            metrics.bitcoin.tip_height = new_block.block_identifier.index;
                        }
                        metrics.bitcoin.last_block_ingestion_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Could not get current time in ms")
                            .as_millis()
                            .into();
                    }
                    Err(e) => ctx.try_log(|logger| {
                        slog::warn!(logger, "unable to acquire observer_metrics_rw_lock:{}", e)
                    }),
                };
                bitcoin_block_store.insert(new_block.block_identifier.clone(), new_block);
            }
            ObserverCommand::CacheBitcoinBlock(block) => {
                bitcoin_block_store.insert(block.block_identifier.clone(), block);
            }
            ObserverCommand::PropagateBitcoinChainEvent(blockchain_event) => {
                ctx.try_log(|logger| {
                    slog::info!(logger, "Handling PropagateBitcoinChainEvent command")
                });
                let mut confirmed_blocks = vec![];

                // Update Chain event before propagation
                let chain_event = match blockchain_event {
                    BlockchainEvent::BlockchainUpdatedWithHeaders(data) => {
                        let mut new_blocks = vec![];

                        for header in data.new_headers.iter() {
                            match bitcoin_block_store.get(&header.block_identifier) {
                                Some(block) => {
                                    new_blocks.push(block.clone());
                                }
                                None => {
                                    ctx.try_log(|logger| {
                                        slog::error!(
                                            logger,
                                            "Unable to retrieve bitcoin block {}",
                                            header.block_identifier
                                        )
                                    });
                                }
                            }
                        }

                        #[cfg(feature = "ordinals")]
                        {
                            if let Some(ref hord_config) = config.hord_config {
                                let (blocks_db, inscriptions_db_conn_rw) =
                                    match open_readwrite_hord_dbs(
                                        &config.get_cache_path_buf(),
                                        &ctx,
                                    ) {
                                        Ok(dbs) => dbs,
                                        Err(e) => {
                                            if let Some(ref tx) = observer_events_tx {
                                                let _ = tx.send(ObserverEvent::Error(format!(
                                                    "Channel error: {:?}",
                                                    e
                                                )));
                                            } else {
                                                ctx.try_log(|logger| {
                                                    slog::error!(
                                                        logger,
                                                        "Unable to open readwtite connection",
                                                    )
                                                });
                                            }
                                            continue;
                                        }
                                    };

                                for block in new_blocks.iter_mut() {
                                    if let Err(e) = update_hord_db_and_augment_bitcoin_block(
                                        block,
                                        &blocks_db,
                                        &inscriptions_db_conn_rw,
                                        true,
                                        &hord_config,
                                        &traversals_cache,
                                        &ctx,
                                    ) {
                                        ctx.try_log(|logger| {
                                            slog::error!(
                                                logger,
                                                "Unable to insert bitcoin block {} in hord_db: {e}",
                                                block.block_identifier.index
                                            )
                                        });
                                    }
                                }
                            }
                        };

                        for header in data.confirmed_headers.iter() {
                            match bitcoin_block_store.remove(&header.block_identifier) {
                                Some(block) => {
                                    confirmed_blocks.push(block);
                                }
                                None => {
                                    ctx.try_log(|logger| {
                                        slog::error!(
                                            logger,
                                            "Unable to retrieve confirmed bitcoin block {}",
                                            header.block_identifier
                                        )
                                    });
                                }
                            }
                        }

                        BitcoinChainEvent::ChainUpdatedWithBlocks(
                            BitcoinChainUpdatedWithBlocksData {
                                new_blocks,
                                confirmed_blocks: confirmed_blocks.clone(),
                            },
                        )
                    }
                    BlockchainEvent::BlockchainUpdatedWithReorg(data) => {
                        let mut blocks_to_apply = vec![];
                        let mut blocks_to_rollback = vec![];

                        let blocks_ids_to_rollback = data
                            .headers_to_rollback
                            .iter()
                            .map(|b| b.block_identifier.index.to_string())
                            .collect::<Vec<String>>();
                        let blocks_ids_to_apply = data
                            .headers_to_apply
                            .iter()
                            .map(|b| b.block_identifier.index.to_string())
                            .collect::<Vec<String>>();

                        ctx.try_log(|logger| {
                            slog::info!(logger, "Bitcoin reorg detected, will rollback blocks {} and apply blocks {}", blocks_ids_to_rollback.join(", "), blocks_ids_to_apply.join(", "))
                        });

                        #[cfg(feature = "ordinals")]
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Flushing traversals_cache ({} entries)",
                                traversals_cache.len()
                            )
                        });

                        #[cfg(feature = "ordinals")]
                        traversals_cache.clear();

                        #[cfg(feature = "ordinals")]
                        let (blocks_db, inscriptions_db_conn_rw) =
                            match open_readwrite_hord_dbs(&config.get_cache_path_buf(), &ctx) {
                                Ok(dbs) => dbs,
                                Err(e) => {
                                    if let Some(ref tx) = observer_events_tx {
                                        let _ = tx.send(ObserverEvent::Error(format!(
                                            "Channel error: {:?}",
                                            e
                                        )));
                                    } else {
                                        ctx.try_log(|logger| {
                                            slog::error!(
                                                logger,
                                                "Unable to open readwtite connection",
                                            )
                                        });
                                    }
                                    continue;
                                }
                            };

                        for header in data.headers_to_rollback.iter() {
                            match bitcoin_block_store.get(&header.block_identifier) {
                                Some(block) => {
                                    #[cfg(feature = "ordinals")]
                                    if let Some(ref _hord_config) = config.hord_config {
                                        if let Err(e) = revert_hord_db_with_augmented_bitcoin_block(
                                            block,
                                            &blocks_db,
                                            &inscriptions_db_conn_rw,
                                            &ctx,
                                        ) {
                                            ctx.try_log(|logger| {
                                                slog::error!(
                                                    logger,
                                                    "Unable to rollback bitcoin block {}: {e}",
                                                    header.block_identifier
                                                )
                                            });
                                        }
                                    }
                                    blocks_to_rollback.push(block.clone());
                                }
                                None => {
                                    ctx.try_log(|logger| {
                                        slog::error!(
                                            logger,
                                            "Unable to retrieve bitcoin block {}",
                                            header.block_identifier
                                        )
                                    });
                                }
                            }
                        }

                        for header in data.headers_to_apply.iter() {
                            match bitcoin_block_store.get_mut(&header.block_identifier) {
                                Some(block) => {
                                    #[cfg(feature = "ordinals")]
                                    if let Some(ref hord_config) = config.hord_config {
                                        if let Err(e) = update_hord_db_and_augment_bitcoin_block(
                                            block,
                                            &blocks_db,
                                            &inscriptions_db_conn_rw,
                                            true,
                                            &hord_config,
                                            &traversals_cache,
                                            &ctx,
                                        ) {
                                            ctx.try_log(|logger| {
                                                    slog::error!(
                                                        logger,
                                                        "Unable to apply bitcoin block {} with hord_db: {e}", block.block_identifier.index
                                                    )
                                                });
                                        }
                                    }
                                    blocks_to_apply.push(block.clone());
                                }
                                None => {
                                    ctx.try_log(|logger| {
                                        slog::error!(
                                            logger,
                                            "Unable to retrieve bitcoin block {}",
                                            header.block_identifier
                                        )
                                    });
                                }
                            }
                        }

                        for header in data.confirmed_headers.iter() {
                            match bitcoin_block_store.remove(&header.block_identifier) {
                                Some(block) => {
                                    confirmed_blocks.push(block);
                                }
                                None => {
                                    ctx.try_log(|logger| {
                                        slog::error!(
                                            logger,
                                            "Unable to retrieve confirmed bitcoin block {}",
                                            header.block_identifier
                                        )
                                    });
                                }
                            }
                        }

                        match blocks_to_apply
                            .iter()
                            .max_by_key(|b| b.block_identifier.index)
                        {
                            Some(highest_tip_block) => match observer_metrics.write() {
                                Ok(mut metrics) => {
                                    metrics.bitcoin.last_reorg = Some(ReorgMetrics {
                                        timestamp: highest_tip_block.timestamp.into(),
                                        applied_blocks: blocks_to_apply.len(),
                                        rolled_back_blocks: blocks_to_rollback.len(),
                                    });
                                }
                                Err(e) => ctx.try_log(|logger| {
                                    slog::warn!(
                                        logger,
                                        "unable to acquire observer_metrics_rw_lock:{}",
                                        e
                                    )
                                }),
                            },
                            None => {}
                        }

                        BitcoinChainEvent::ChainUpdatedWithReorg(BitcoinChainUpdatedWithReorgData {
                            blocks_to_apply,
                            blocks_to_rollback,
                            confirmed_blocks: confirmed_blocks.clone(),
                        })
                    }
                };

                for event_handler in event_handlers.iter() {
                    event_handler.propagate_bitcoin_event(&chain_event).await;
                }
                // process hooks
                let mut hooks_ids_to_deregister = vec![];
                let mut requests = vec![];
                let mut report = PredicateEvaluationReport::new();

                let bitcoin_chainhooks = chainhook_store
                    .predicates
                    .bitcoin_chainhooks
                    .iter()
                    .filter(|p| p.enabled)
                    .collect::<Vec<_>>();
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Evaluating {} bitcoin chainhooks registered",
                        bitcoin_chainhooks.len()
                    )
                });

                let (predicates_triggered, predicates_evaluated) =
                    evaluate_bitcoin_chainhooks_on_chain_event(
                        &chain_event,
                        &bitcoin_chainhooks,
                        &ctx,
                    );
                for (uuid, block_identifier) in predicates_evaluated.into_iter() {
                    report.track_evaluation(uuid, block_identifier);
                }
                for entry in predicates_triggered.iter() {
                    let blocks_ids = entry
                        .apply
                        .iter()
                        .map(|e| &e.1.block_identifier)
                        .collect::<Vec<&BlockIdentifier>>();
                    report.track_trigger(&entry.chainhook.uuid, &blocks_ids);
                }

                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "{} bitcoin chainhooks positive evaluations",
                        predicates_triggered.len()
                    )
                });

                let mut chainhooks_to_trigger = vec![];

                for trigger in predicates_triggered.into_iter() {
                    let mut total_occurrences: u64 = *chainhooks_occurrences_tracker
                        .get(&trigger.chainhook.uuid)
                        .unwrap_or(&0);
                    total_occurrences += 1;

                    let limit = trigger.chainhook.expire_after_occurrence.unwrap_or(0);
                    if limit == 0 || total_occurrences <= limit {
                        chainhooks_occurrences_tracker
                            .insert(trigger.chainhook.uuid.clone(), total_occurrences);
                        chainhooks_to_trigger.push(trigger);
                    } else {
                        hooks_ids_to_deregister.push(trigger.chainhook.uuid.clone());
                    }
                }

                let mut proofs = HashMap::new();
                for trigger in chainhooks_to_trigger.iter() {
                    if trigger.chainhook.include_proof {
                        gather_proofs(&trigger, &mut proofs, &config, &ctx);
                    }
                }

                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "{} bitcoin chainhooks will be triggered",
                        chainhooks_to_trigger.len()
                    )
                });

                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::PredicatesTriggered(
                        chainhooks_to_trigger.len(),
                    ));
                }
                for chainhook_to_trigger in chainhooks_to_trigger.into_iter() {
                    match handle_bitcoin_hook_action(chainhook_to_trigger, &proofs) {
                        Err(e) => {
                            ctx.try_log(|logger| {
                                slog::error!(logger, "unable to handle action {}", e)
                            });
                        }
                        Ok(BitcoinChainhookOccurrence::Http(request)) => {
                            requests.push(request);
                        }
                        Ok(BitcoinChainhookOccurrence::File(_path, _bytes)) => {
                            ctx.try_log(|logger| {
                                slog::info!(logger, "Writing to disk not supported in server mode")
                            })
                        }
                        Ok(BitcoinChainhookOccurrence::Data(payload)) => {
                            if let Some(ref tx) = observer_events_tx {
                                let _ = tx.send(ObserverEvent::BitcoinPredicateTriggered(payload));
                            }
                        }
                    }
                }
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "{} bitcoin chainhooks to deregister",
                        hooks_ids_to_deregister.len()
                    )
                });

                for hook_uuid in hooks_ids_to_deregister.iter() {
                    if let Some(chainhook) = chainhook_store
                        .predicates
                        .deregister_bitcoin_hook(hook_uuid.clone())
                    {
                        if let Some(ref tx) = observer_events_tx {
                            let _ = tx.send(ObserverEvent::PredicateDeregistered(
                                ChainhookSpecification::Bitcoin(chainhook),
                            ));
                        }

                        match observer_metrics.write() {
                            Ok(mut metrics) => metrics.bitcoin.deregister_prediate(),
                            Err(e) => ctx.try_log(|logger| {
                                slog::warn!(
                                    logger,
                                    "unable to acquire observer_metrics_rw_lock:{}",
                                    e
                                )
                            }),
                        }
                    }
                }

                for request in requests.into_iter() {
                    let _ = send_request(request, 3, 1, &ctx).await;
                }

                #[cfg(feature = "ordinals")]
                for block in confirmed_blocks.into_iter() {
                    if block.block_identifier.index % 24 == 0 {
                        ctx.try_log(|logger| {
                            slog::info!(
                                logger,
                                "Flushing traversals_cache ({} entries)",
                                traversals_cache.len()
                            )
                        });
                        traversals_cache.clear();
                    }
                }

                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::BitcoinChainEvent((chain_event, report)));
                }
            }
            ObserverCommand::PropagateStacksChainEvent(chain_event) => {
                ctx.try_log(|logger| {
                    slog::info!(logger, "Handling PropagateStacksChainEvent command")
                });
                for event_handler in event_handlers.iter() {
                    event_handler.propagate_stacks_event(&chain_event).await;
                }
                let mut hooks_ids_to_deregister = vec![];
                let mut requests = vec![];
                let mut report = PredicateEvaluationReport::new();

                let stacks_chainhooks = chainhook_store
                    .predicates
                    .stacks_chainhooks
                    .iter()
                    .filter(|p| p.enabled)
                    .collect::<Vec<_>>();
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Evaluating {} stacks chainhooks registered",
                        stacks_chainhooks.len()
                    )
                });
                // track stacks chain metrics
                match &chain_event {
                    StacksChainEvent::ChainUpdatedWithBlocks(update) => {
                        match update
                            .new_blocks
                            .iter()
                            .max_by_key(|b| b.block.block_identifier.index)
                        {
                            Some(highest_tip_update) => match observer_metrics.write() {
                                Ok(mut metrics) => {
                                    if highest_tip_update.block.block_identifier.index
                                        > metrics.stacks.tip_height
                                    {
                                        metrics.stacks.tip_height =
                                            highest_tip_update.block.block_identifier.index;
                                    }
                                    metrics.stacks.last_block_ingestion_at = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("Could not get current time in ms")
                                        .as_millis()
                                        .into();
                                }
                                Err(e) => ctx.try_log(|logger| {
                                    slog::warn!(
                                        logger,
                                        "unable to acquire observer_metrics_rw_lock:{}",
                                        e
                                    )
                                }),
                            },
                            None => {}
                        }
                    }
                    StacksChainEvent::ChainUpdatedWithReorg(update) => {
                        match update
                            .blocks_to_apply
                            .iter()
                            .max_by_key(|b| b.block.block_identifier.index)
                        {
                            Some(highest_tip_update) => match observer_metrics.write() {
                                Ok(mut metrics) => {
                                    metrics.stacks.last_reorg = Some(ReorgMetrics {
                                        timestamp: highest_tip_update.block.timestamp.into(),
                                        applied_blocks: update.blocks_to_apply.len(),
                                        rolled_back_blocks: update.blocks_to_rollback.len(),
                                    });
                                }
                                Err(e) => ctx.try_log(|logger| {
                                    slog::warn!(
                                        logger,
                                        "unable to acquire observer_metrics_rw_lock:{}",
                                        e
                                    )
                                }),
                            },
                            None => {}
                        }
                    }
                    _ => {}
                }

                // process hooks
                let (predicates_triggered, predicates_evaluated) =
                    evaluate_stacks_chainhooks_on_chain_event(
                        &chain_event,
                        stacks_chainhooks,
                        &ctx,
                    );
                for (uuid, block_identifier) in predicates_evaluated.into_iter() {
                    report.track_evaluation(uuid, block_identifier);
                }
                for entry in predicates_triggered.iter() {
                    let blocks_ids = entry
                        .apply
                        .iter()
                        .map(|e| e.1.get_identifier())
                        .collect::<Vec<&BlockIdentifier>>();
                    report.track_trigger(&entry.chainhook.uuid, &blocks_ids);
                }
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "{} stacks chainhooks positive evaluations",
                        predicates_triggered.len()
                    )
                });

                let mut chainhooks_to_trigger = vec![];

                for trigger in predicates_triggered.into_iter() {
                    let mut total_occurrences: u64 = *chainhooks_occurrences_tracker
                        .get(&trigger.chainhook.uuid)
                        .unwrap_or(&0);
                    total_occurrences += 1;

                    let limit = trigger.chainhook.expire_after_occurrence.unwrap_or(0);
                    if limit == 0 || total_occurrences <= limit {
                        chainhooks_occurrences_tracker
                            .insert(trigger.chainhook.uuid.clone(), total_occurrences);
                        chainhooks_to_trigger.push(trigger);
                    } else {
                        hooks_ids_to_deregister.push(trigger.chainhook.uuid.clone());
                    }
                }

                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::PredicatesTriggered(
                        chainhooks_to_trigger.len(),
                    ));
                }
                let proofs = HashMap::new();
                for chainhook_to_trigger in chainhooks_to_trigger.into_iter() {
                    match handle_stacks_hook_action(chainhook_to_trigger, &proofs, &ctx) {
                        Err(e) => {
                            ctx.try_log(|logger| {
                                slog::error!(logger, "unable to handle action {}", e)
                            });
                        }
                        Ok(StacksChainhookOccurrence::Http(request)) => {
                            requests.push(request);
                        }
                        Ok(StacksChainhookOccurrence::File(_path, _bytes)) => {
                            ctx.try_log(|logger| {
                                slog::info!(logger, "Writing to disk not supported in server mode")
                            })
                        }
                        Ok(StacksChainhookOccurrence::Data(payload)) => {
                            if let Some(ref tx) = observer_events_tx {
                                let _ = tx.send(ObserverEvent::StacksPredicateTriggered(payload));
                            }
                        }
                    }
                }

                for hook_uuid in hooks_ids_to_deregister.iter() {
                    if let Some(chainhook) = chainhook_store
                        .predicates
                        .deregister_stacks_hook(hook_uuid.clone())
                    {
                        if let Some(ref tx) = observer_events_tx {
                            let _ = tx.send(ObserverEvent::PredicateDeregistered(
                                ChainhookSpecification::Stacks(chainhook),
                            ));
                        }

                        match observer_metrics.write() {
                            Ok(mut metrics) => metrics.stacks.deregister_prediate(),
                            Err(e) => ctx.try_log(|logger| {
                                slog::warn!(
                                    logger,
                                    "unable to acquire observer_metrics_rw_lock:{}",
                                    e
                                )
                            }),
                        }
                    }
                }

                for request in requests.into_iter() {
                    // todo(lgalabru): collect responses for reporting
                    ctx.try_log(|logger| {
                        slog::info!(
                            logger,
                            "Dispatching request from stacks chainhook {:?}",
                            request
                        )
                    });
                    let _ = send_request(request, 3, 1, &ctx).await;
                }

                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::StacksChainEvent((chain_event, report)));
                }
            }
            ObserverCommand::PropagateStacksMempoolEvent(mempool_event) => {
                ctx.try_log(|logger| {
                    slog::debug!(logger, "Handling PropagateStacksMempoolEvent command")
                });
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::StacksChainMempoolEvent(mempool_event));
                }
            }
            ObserverCommand::NotifyBitcoinTransactionProxied => {
                ctx.try_log(|logger| {
                    slog::info!(logger, "Handling NotifyBitcoinTransactionProxied command")
                });
                for event_handler in event_handlers.iter() {
                    event_handler.notify_bitcoin_transaction_proxied().await;
                }
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::NotifyBitcoinTransactionProxied);
                }
            }
            ObserverCommand::RegisterPredicate(spec) => {
                ctx.try_log(|logger| slog::info!(logger, "Handling RegisterPredicate command"));

                let mut spec = match chainhook_store
                    .predicates
                    .register_full_specification(networks, spec)
                {
                    Ok(spec) => spec,
                    Err(e) => {
                        ctx.try_log(|logger| {
                            slog::error!(
                                logger,
                                "Unable to register new chainhook spec: {}",
                                e.to_string()
                            )
                        });
                        continue;
                    }
                };
                ctx.try_log(|logger| slog::info!(logger, "Registering chainhook {}", spec.uuid(),));
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::PredicateRegistered(spec.clone()));
                } else {
                    ctx.try_log(|logger| slog::info!(logger, "Enabling Predicate {}", spec.uuid()));
                    chainhook_store.predicates.enable_specification(&mut spec);
                }

                match observer_metrics.write() {
                    Ok(mut metrics) => match spec {
                        ChainhookSpecification::Bitcoin(_) => {
                            metrics.bitcoin.registered_predicates += 1
                        }
                        ChainhookSpecification::Stacks(_) => {
                            metrics.stacks.registered_predicates += 1
                        }
                    },
                    Err(e) => ctx.try_log(|logger| {
                        slog::warn!(logger, "unable to acquire observer_metrics_rw_lock:{}", e)
                    }),
                };
            }
            ObserverCommand::EnablePredicate(mut spec) => {
                ctx.try_log(|logger| slog::info!(logger, "Enabling Predicate {}", spec.uuid()));
                chainhook_store.predicates.enable_specification(&mut spec);
                if let Some(ref tx) = observer_events_tx {
                    let _ = tx.send(ObserverEvent::PredicateEnabled(spec));
                }
            }
            ObserverCommand::DeregisterStacksPredicate(hook_uuid) => {
                ctx.try_log(|logger| {
                    slog::info!(logger, "Handling DeregisterStacksPredicate command")
                });
                let hook = chainhook_store.predicates.deregister_stacks_hook(hook_uuid);
                if let (Some(tx), Some(hook)) = (&observer_events_tx, hook) {
                    let _ = tx.send(ObserverEvent::PredicateDeregistered(
                        ChainhookSpecification::Stacks(hook),
                    ));
                }

                match observer_metrics.write() {
                    Ok(mut metrics) => metrics.stacks.deregister_prediate(),
                    Err(e) => ctx.try_log(|logger| {
                        slog::warn!(logger, "unable to acquire observer_metrics_rw_lock:{}", e)
                    }),
                }
            }
            ObserverCommand::DeregisterBitcoinPredicate(hook_uuid) => {
                ctx.try_log(|logger| {
                    slog::info!(logger, "Handling DeregisterBitcoinPredicate command")
                });
                let hook = chainhook_store
                    .predicates
                    .deregister_bitcoin_hook(hook_uuid);
                if let (Some(tx), Some(hook)) = (&observer_events_tx, hook) {
                    let _ = tx.send(ObserverEvent::PredicateDeregistered(
                        ChainhookSpecification::Bitcoin(hook),
                    ));

                    match observer_metrics.write() {
                        Ok(mut metrics) => metrics.bitcoin.deregister_prediate(),
                        Err(e) => ctx.try_log(|logger| {
                            slog::warn!(logger, "unable to acquire observer_metrics_rw_lock:{}", e)
                        }),
                    }
                }
            }
        }
    }
    Ok(())
}
#[cfg(test)]
pub mod tests;
