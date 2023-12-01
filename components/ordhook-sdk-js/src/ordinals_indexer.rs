use core::panic;
use crossbeam_channel::Sender;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
  ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use ordhook::chainhook_sdk::chainhooks::bitcoin::BitcoinTransactionPayload;
use ordhook::chainhook_sdk::chainhooks::types::{
  BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification, BitcoinPredicateType,
  HookAction, OrdinalOperations,
};
use ordhook::chainhook_sdk::observer::DataHandlerEvent;
use ordhook::chainhook_sdk::utils::{BlockHeights, Context as OrdhookContext};
use ordhook::config::Config;
use ordhook::scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate;
use ordhook::service::Service;
use std::collections::BTreeMap;
use std::thread;

enum IndexerCommand {
  StreamBlocks,
  ReplayBlocks(Vec<u64>),
  SyncBlocks,
  DropBlocks(Vec<u64>),
  RewriteBlocks(Vec<u64>),
  Terminate,
}

type BlockJsHandler = ThreadsafeFunction<BitcoinTransactionPayload, ErrorStrategy::Fatal>;

#[allow(dead_code)]
enum CustomIndexerCommand {
  UpdateApplyCallback(BlockJsHandler),
  UpdateUndoCallback(BlockJsHandler),
  Terminate,
}

struct OrdinalsIndexingRunloop {
  pub command_tx: Sender<IndexerCommand>,
  pub custom_indexer_command_tx: Sender<CustomIndexerCommand>,
}

impl OrdinalsIndexingRunloop {
  pub fn new(ordhook_config: Config) -> Self {
    let (command_tx, command_rx) = crossbeam_channel::unbounded();
    let (custom_indexer_command_tx, custom_indexer_command_rx) = crossbeam_channel::unbounded();

    let logger = hiro_system_kit::log::setup_logger();
    let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
    let ctx = OrdhookContext {
      logger: Some(logger),
      tracer: false,
    };

    let mut service: Service = Service::new(ordhook_config, ctx);

    // Set-up the observer sidecar - used for augmenting the bitcoin blocks with
    // ordinals informations
    let observer_sidecar = service
      .set_up_observer_sidecar_runloop()
      .expect("unable to setup indexer");
    // Prepare internal predicate
    let (observer_config, payload_rx) = service
      .set_up_observer_config(vec![], true)
      .expect("unable to setup indexer");

    // Indexing thread
    thread::spawn(move || {
      let payload_rx = payload_rx.unwrap();

      let mut apply_callback: Option<BlockJsHandler> = None;

      let mut undo_callback: Option<BlockJsHandler> = None;

      loop {
        let mut sel = crossbeam_channel::Select::new();
        let payload_rx_sel = sel.recv(&payload_rx);
        let custom_indexer_command_rx_sel = sel.recv(&custom_indexer_command_rx);

        // The second operation will be selected because it becomes ready first.
        let oper = sel.select();
        match oper.index() {
          i if i == payload_rx_sel => match oper.recv(&payload_rx) {
            Ok(DataHandlerEvent::Process(payload)) => {
              if let Some(callback) = undo_callback.clone() {
                for to_rollback in payload.rollback.into_iter() {
                  loop {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    callback.call_with_return_value::<bool, _>(
                      to_rollback.clone(),
                      ThreadsafeFunctionCallMode::Blocking,
                      move |p| {
                        let _ = tx.send(p);
                        Ok(())
                      },
                    );
                    match rx.recv() {
                      Ok(true) => break,
                      Ok(false) => continue,
                      _ => panic!(),
                    }
                  }
                }
              }

              if let Some(ref callback) = apply_callback.clone() {
                for to_apply in payload.apply.into_iter() {
                  loop {
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    callback.call_with_return_value::<bool, _>(
                      to_apply.clone(),
                      ThreadsafeFunctionCallMode::Blocking,
                      move |p| {
                        let _ = tx.send(p);
                        Ok(())
                      },
                    );
                    match rx.recv() {
                      Ok(true) => break,
                      Ok(false) => continue,
                      _ => panic!(),
                    }
                  }
                }
              }
            }
            Ok(DataHandlerEvent::Terminate) => {
              break;
            }
            Err(e) => {
              println!("Error {}", e.to_string());
            }
          },
          i if i == custom_indexer_command_rx_sel => match oper.recv(&custom_indexer_command_rx) {
            Ok(CustomIndexerCommand::UpdateApplyCallback(callback)) => {
              apply_callback = Some(callback);
            }
            Ok(CustomIndexerCommand::UpdateUndoCallback(callback)) => {
              undo_callback = Some(callback);
            }
            Ok(CustomIndexerCommand::Terminate) => break,
            _ => {}
          },
          _ => unreachable!(),
        };
      }
    });

    // Processing thread
    thread::spawn(move || {
      loop {
        let cmd = match command_rx.recv() {
          Ok(cmd) => cmd,
          Err(e) => {
            panic!("Runloop error: {}", e.to_string());
          }
        };

        match cmd {
          IndexerCommand::StreamBlocks => {
            // We start the service as soon as the start() method is being called.
            let future = service.catch_up_with_chain_tip(false, true);
            let _ = hiro_system_kit::nestable_block_on(future).expect("unable to start indexer");
            let future = service.start_event_observer(observer_sidecar);
            let (command_tx, event_rx) =
              hiro_system_kit::nestable_block_on(future).expect("unable to start indexer");
            // Blocking call
            let _ = service.start_main_runloop(&command_tx, event_rx, None);
            break;
          }
          IndexerCommand::ReplayBlocks(blocks) => {
            let network = &service.config.network.bitcoin_network;
            let mut networks = BTreeMap::new();
            // Retrieve last block height known, and display it
            networks.insert(
              network.clone(),
              BitcoinChainhookNetworkSpecification {
                start_block: None,
                end_block: None,
                blocks: Some(blocks),
                expire_after_occurrence: None,
                include_proof: None,
                include_inputs: None,
                include_outputs: None,
                include_witness: None,
                predicate: BitcoinPredicateType::OrdinalsProtocol(
                  OrdinalOperations::InscriptionFeed,
                ),
                action: HookAction::Noop,
              },
            );
            let predicate_spec = BitcoinChainhookFullSpecification {
              uuid: "replay".to_string(),
              owner_uuid: None,
              name: "replay".to_string(),
              version: 1,
              networks,
            }
            .into_selected_network_specification(&network)
            .unwrap();

            let future = scan_bitcoin_chainstate_via_rpc_using_predicate(
              &predicate_spec,
              &service.config,
              Some(&observer_config),
              &service.ctx,
            );
            let _ = hiro_system_kit::nestable_block_on(future).expect("unable to start indexer");

            if let Some(tx) = observer_config.data_handler_tx {
              let _ = tx.send(DataHandlerEvent::Terminate);
            }
            break;
          }
          IndexerCommand::DropBlocks(blocks) => {
            println!("Will drop blocks {:?}", blocks);
          }
          IndexerCommand::RewriteBlocks(blocks) => {
            println!("Will rewrite blocks {:?}", blocks);
          }
          IndexerCommand::SyncBlocks => {
            println!("Will sync blocks");
          }
          IndexerCommand::Terminate => {
            if let Some(tx) = observer_config.data_handler_tx {
              let _ = tx.send(DataHandlerEvent::Terminate);
            }
            std::process::exit(0);
          }
        }
      }
    });

    Self {
      command_tx,
      custom_indexer_command_tx,
    }
  }
}

#[napi(object)]
pub struct OrdinalsIndexerConfig {
  pub bitcoin_rpc_url: Option<String>,
  pub bitcoin_rpc_username: Option<String>,
  pub bitcoin_rpc_password: Option<String>,
  pub working_dir: Option<String>,
  pub logs_enabled: Option<bool>,
}

impl OrdinalsIndexerConfig {
  pub fn default() -> OrdinalsIndexerConfig {
    OrdinalsIndexerConfig {
      bitcoin_rpc_url: Some("http://0.0.0.0:8332".to_string()),
      bitcoin_rpc_username: Some("devnet".to_string()),
      bitcoin_rpc_password: Some("devnet".to_string()),
      working_dir: Some("/tmp/ordinals".to_string()),
      logs_enabled: Some(true),
    }
  }
}

#[napi(js_name = "OrdinalsIndexer")]
pub struct OrdinalsIndexer {
  runloop: OrdinalsIndexingRunloop,
}

#[napi]
impl OrdinalsIndexer {
  #[napi(constructor)]
  pub fn new(config_overrides: Option<OrdinalsIndexerConfig>) -> Self {
    let mut config = Config::mainnet_default();

    if let Some(config_overrides) = config_overrides {
      if let Some(bitcoin_rpc_url) = config_overrides.bitcoin_rpc_url {
        config.network.bitcoind_rpc_url = bitcoin_rpc_url.clone();
      }
      if let Some(bitcoin_rpc_username) = config_overrides.bitcoin_rpc_username {
        config.network.bitcoind_rpc_username = bitcoin_rpc_username.clone();
      }
      if let Some(bitcoin_rpc_password) = config_overrides.bitcoin_rpc_password {
        config.network.bitcoind_rpc_password = bitcoin_rpc_password.clone();
      }
      if let Some(working_dir) = config_overrides.working_dir {
        config.storage.working_dir = working_dir.clone();
      }
      if let Some(logs_enabled) = config_overrides.logs_enabled {
        config.logs.chainhook_internals = logs_enabled.clone();
      }
      if let Some(logs_enabled) = config_overrides.logs_enabled {
        config.logs.ordinals_internals = logs_enabled;
      }
    }

    let runloop = OrdinalsIndexingRunloop::new(config);

    OrdinalsIndexer { runloop }
  }

  #[napi(
    js_name = "onBlock",
    ts_args_type = "callback: (block: any) => boolean"
  )]
  pub fn update_apply_block_callback(&self, apply_block_cb: JsFunction) {
    let tsfn: ThreadsafeFunction<BitcoinTransactionPayload, ErrorStrategy::Fatal> = apply_block_cb
      .create_threadsafe_function(0, |ctx| ctx.env.to_js_value(&ctx.value).map(|v| vec![v]))
      .unwrap();
    let _ = self
      .runloop
      .custom_indexer_command_tx
      .send(CustomIndexerCommand::UpdateApplyCallback(tsfn));
  }

  #[napi(
    js_name = "onBlockRollBack",
    ts_args_type = "callback: (block: any) => boolean"
  )]
  pub fn update_undo_block_callback(&self, undo_block_cb: JsFunction) {
    let tsfn: ThreadsafeFunction<BitcoinTransactionPayload, ErrorStrategy::Fatal> = undo_block_cb
      .create_threadsafe_function(
        0,
        |ctx: ThreadSafeCallContext<BitcoinTransactionPayload>| {
          ctx.env.to_js_value(&ctx.value).map(|v| vec![v])
        },
      )
      .unwrap();
    let _ = self
      .runloop
      .custom_indexer_command_tx
      .send(CustomIndexerCommand::UpdateUndoCallback(tsfn));
  }

  #[napi]
  pub fn stream_blocks(&self) {
    let _ = self.runloop.command_tx.send(IndexerCommand::StreamBlocks);
  }

  #[napi]
  pub fn replay_blocks(&self, blocks: Vec<i64>) {
    let blocks = blocks
      .into_iter()
      .map(|block| block as u64)
      .collect::<Vec<u64>>();
    let _ = self
      .runloop
      .command_tx
      .send(IndexerCommand::ReplayBlocks(blocks));
  }

  #[napi]
  pub fn replay_block_range(&self, start_block: i64, end_block: i64) {
    let range = BlockHeights::BlockRange(start_block as u64, end_block as u64);
    let blocks = range.get_sorted_entries().into_iter().collect();
    let _ = self
      .runloop
      .command_tx
      .send(IndexerCommand::ReplayBlocks(blocks));
  }

  #[napi]
  pub fn terminate(&self) {
    let _ = self.runloop.command_tx.send(IndexerCommand::Terminate);
  }
}
