use crate::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification,
    BitcoinChainhookSpecification, BitcoinPredicateType, ChainhookConfig,
    ChainhookFullSpecification, ChainhookSpecification, ExactMatchingRule, HookAction,
    OutputPredicate, StacksChainhookFullSpecification, StacksChainhookNetworkSpecification,
    StacksChainhookSpecification, StacksContractCallBasedPredicate, StacksPredicate,
};
use crate::indexer::tests::helpers::transactions::generate_test_tx_bitcoin_p2pkh_transfer;
use crate::indexer::tests::helpers::{
    accounts, bitcoin_blocks, stacks_blocks, transactions::generate_test_tx_stacks_contract_call,
};
use crate::observer::{
    start_observer_commands_handler, ApiKey, ChainhookStore, EventObserverConfig, ObserverCommand,
};
use crate::utils::{AbstractBlock, Context};
use chainhook_types::{
    BitcoinNetwork, BlockchainEvent, BlockchainUpdatedWithHeaders, StacksBlockUpdate,
    StacksChainEvent, StacksChainUpdatedWithBlocksData, StacksNetwork,
};
use hiro_system_kit;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};

use super::ObserverEvent;

fn generate_test_config() -> (EventObserverConfig, ChainhookStore) {
    let operators = HashSet::new();
    let config = EventObserverConfig {
        hooks_enabled: true,
        chainhook_config: Some(ChainhookConfig::new()),
        bitcoin_rpc_proxy_enabled: false,
        event_handlers: vec![],
        ingestion_port: 0,
        control_port: 0,
        bitcoin_node_username: "user".into(),
        bitcoin_node_password: "user".into(),
        bitcoin_node_rpc_url: "http://localhost:20443".into(),
        stacks_node_rpc_url: "http://localhost:18443".into(),
        operators,
        display_logs: false,
        cache_path: "cache".into(),
        bitcoin_network: BitcoinNetwork::Regtest,
        stacks_network: StacksNetwork::Devnet,
    };
    let mut entries = HashMap::new();
    entries.insert(ApiKey(None), ChainhookConfig::new());
    let chainhook_store = ChainhookStore { entries };
    (config, chainhook_store)
}

fn stacks_chainhook_contract_call(
    id: u8,
    contract_identifier: &str,
    expire_after_occurrence: Option<u64>,
    method: &str,
) -> StacksChainhookFullSpecification {
    let mut networks = BTreeMap::new();
    networks.insert(
        StacksNetwork::Devnet,
        StacksChainhookNetworkSpecification {
            start_block: None,
            end_block: None,
            expire_after_occurrence,
            capture_all_events: None,
            decode_clarity_values: Some(true),
            predicate: StacksPredicate::ContractCall(StacksContractCallBasedPredicate {
                contract_identifier: contract_identifier.to_string(),
                method: method.to_string(),
            }),
            action: HookAction::Noop,
        },
    );

    let spec = StacksChainhookFullSpecification {
        uuid: format!("{}", id),
        name: format!("Chainhook {}", id),
        owner_uuid: None,
        networks,
        version: 1,
    };
    spec
}

fn bitcoin_chainhook_p2pkh(
    id: u8,
    address: &str,
    expire_after_occurrence: Option<u64>,
) -> BitcoinChainhookFullSpecification {
    let mut networks = BTreeMap::new();
    networks.insert(
        BitcoinNetwork::Regtest,
        BitcoinChainhookNetworkSpecification {
            start_block: None,
            end_block: None,
            expire_after_occurrence,
            predicate: BitcoinPredicateType::Outputs(OutputPredicate::P2pkh(
                ExactMatchingRule::Equals(address.to_string()),
            )),
            action: HookAction::Noop,
        },
    );

    let spec = BitcoinChainhookFullSpecification {
        uuid: format!("{}", id),
        name: format!("Chainhook {}", id),
        owner_uuid: None,
        version: 1,
        networks,
    };
    spec
}

fn generate_and_register_new_stacks_chainhook(
    observer_commands_tx: &Sender<ObserverCommand>,
    observer_events_rx: &crossbeam_channel::Receiver<ObserverEvent>,
    id: u8,
    contract_name: &str,
    method: &str,
) -> StacksChainhookSpecification {
    let contract_identifier = format!("{}.{}", accounts::deployer_stx_address(), contract_name);
    let chainhook = stacks_chainhook_contract_call(id, &contract_identifier, None, method);
    let _ = observer_commands_tx.send(ObserverCommand::RegisterHook(
        ChainhookFullSpecification::Stacks(chainhook.clone()),
        ApiKey(None),
    ));
    let chainhook = chainhook
        .into_selected_network_specification(&StacksNetwork::Devnet)
        .unwrap();
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookRegistered(registered_chainhook)) => {
            assert_eq!(
                ChainhookSpecification::Stacks(chainhook.clone()),
                registered_chainhook
            );
            true
        }
        _ => false,
    });
    chainhook
}

fn generate_and_register_new_bitcoin_chainhook(
    observer_commands_tx: &Sender<ObserverCommand>,
    observer_events_rx: &crossbeam_channel::Receiver<ObserverEvent>,
    id: u8,
    p2pkh_address: &str,
    expire_after_occurrence: Option<u64>,
) -> BitcoinChainhookSpecification {
    let chainhook = bitcoin_chainhook_p2pkh(id, &p2pkh_address, expire_after_occurrence);
    let _ = observer_commands_tx.send(ObserverCommand::RegisterHook(
        ChainhookFullSpecification::Bitcoin(chainhook.clone()),
        ApiKey(None),
    ));
    let chainhook = chainhook
        .into_selected_network_specification(&BitcoinNetwork::Regtest)
        .unwrap();
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookRegistered(registered_chainhook)) => {
            assert_eq!(
                ChainhookSpecification::Bitcoin(chainhook.clone()),
                registered_chainhook
            );
            true
        }
        _ => false,
    });
    chainhook
}

#[test]
fn test_stacks_chainhook_register_deregister() {
    let (observer_commands_tx, observer_commands_rx) = channel();
    let (observer_events_tx, observer_events_rx) = crossbeam_channel::unbounded();

    let handle = std::thread::spawn(move || {
        let (config, chainhook_store) = generate_test_config();
        let _ = hiro_system_kit::nestable_block_on(start_observer_commands_handler(
            config,
            Arc::new(RwLock::new(chainhook_store)),
            observer_commands_rx,
            Some(observer_events_tx),
            None,
            None,
            Context::empty(),
        ));
    });

    // Create and register a new chainhook
    let chainhook = generate_and_register_new_stacks_chainhook(
        &observer_commands_tx,
        &observer_events_rx,
        1,
        "counter",
        "increment",
    );

    // Simulate a block that does not include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        0,
        &accounts::wallet_1_stx_address(),
        "counter",
        "decrement",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 1, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        1,
        &accounts::wallet_1_stx_address(),
        "counter",
        "increment",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 2, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainhookTriggered(payload)) => {
            assert_eq!(payload.apply.len(), 1);
            assert_eq!(payload.apply[0].transactions.len(), 1);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include 2 trigger
    let transactions = vec![
        generate_test_tx_stacks_contract_call(
            1,
            &accounts::wallet_1_stx_address(),
            "counter",
            "increment",
            vec!["u1"],
        ),
        generate_test_tx_stacks_contract_call(
            2,
            &accounts::wallet_2_stx_address(),
            "counter",
            "increment",
            vec!["u2"],
        ),
        generate_test_tx_stacks_contract_call(
            3,
            &accounts::wallet_3_stx_address(),
            "counter",
            "decrement",
            vec!["u2"],
        ),
    ];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 2, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainhookTriggered(payload)) => {
            assert_eq!(payload.apply.len(), 1);
            assert_eq!(payload.apply[0].transactions.len(), 2);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Deregister the hook
    let _ = observer_commands_tx.send(ObserverCommand::DeregisterStacksHook(
        chainhook.uuid.clone(),
        ApiKey(None),
    ));
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookDeregistered(deregistered_chainhook)) => {
            assert_eq!(
                ChainhookSpecification::Stacks(chainhook),
                deregistered_chainhook
            );
            true
        }
        _ => false,
    });

    // Simulate a block that does not include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        2,
        &accounts::wallet_1_stx_address(),
        "counter",
        "decrement",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 2, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        3,
        &accounts::wallet_1_stx_address(),
        "counter",
        "increment",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 3, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    let _ = observer_commands_tx.send(ObserverCommand::Terminate);
    handle.join().expect("unable to terminate thread");
}

#[test]
fn test_stacks_chainhook_auto_deregister() {
    let (observer_commands_tx, observer_commands_rx) = channel();
    let (observer_events_tx, observer_events_rx) = crossbeam_channel::unbounded();

    let handle = std::thread::spawn(move || {
        let (config, chainhook_store) = generate_test_config();
        let _ = hiro_system_kit::nestable_block_on(start_observer_commands_handler(
            config,
            Arc::new(RwLock::new(chainhook_store)),
            observer_commands_rx,
            Some(observer_events_tx),
            None,
            None,
            Context::empty(),
        ));
    });

    // Create and register a new chainhook
    let contract_identifier = format!("{}.{}", accounts::deployer_stx_address(), "counter");
    let chainhook = stacks_chainhook_contract_call(0, &contract_identifier, Some(1), "increment");

    let _ = observer_commands_tx.send(ObserverCommand::RegisterHook(
        ChainhookFullSpecification::Stacks(chainhook.clone()),
        ApiKey(None),
    ));
    let chainhook = chainhook
        .into_selected_network_specification(&StacksNetwork::Devnet)
        .unwrap();
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookRegistered(registered_chainhook)) => {
            assert_eq!(
                ChainhookSpecification::Stacks(chainhook.clone()),
                registered_chainhook
            );
            true
        }
        _ => false,
    });

    // Simulate a block that does not include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        0,
        &accounts::wallet_1_stx_address(),
        "counter",
        "decrement",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 1, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        1,
        &accounts::wallet_1_stx_address(),
        "counter",
        "increment",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 2, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainhookTriggered(_)) => {
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate another block that does include a trigger
    let transactions = vec![generate_test_tx_stacks_contract_call(
        3,
        &accounts::wallet_1_stx_address(),
        "counter",
        "increment",
        vec!["u1"],
    )];
    let chain_event = StacksChainEvent::ChainUpdatedWithBlocks(StacksChainUpdatedWithBlocksData {
        new_blocks: vec![StacksBlockUpdate::new(
            stacks_blocks::generate_test_stacks_block(0, 3, transactions, None).expect_block(),
        )],
        confirmed_blocks: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateStacksChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should signal that a hook was deregistered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookDeregistered(deregistered_hook)) => {
            assert_eq!(deregistered_hook.uuid(), chainhook.uuid);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::StacksChainEvent(_)) => {
            true
        }
        Ok(event) => {
            println!("Unexpected event: {:?}", event);
            false
        }
        Err(e) => {
            println!("Error: {:?}", e);
            false
        }
    });

    let _ = observer_commands_tx.send(ObserverCommand::Terminate);
    handle.join().expect("unable to terminate thread");
}

#[test]
fn test_bitcoin_chainhook_register_deregister() {
    let (observer_commands_tx, observer_commands_rx) = channel();
    let (observer_events_tx, observer_events_rx) = crossbeam_channel::unbounded();

    let handle = std::thread::spawn(move || {
        let (config, chainhook_store) = generate_test_config();
        let _ = hiro_system_kit::nestable_block_on(start_observer_commands_handler(
            config,
            Arc::new(RwLock::new(chainhook_store)),
            observer_commands_rx,
            Some(observer_events_tx),
            None,
            None,
            Context::empty(),
        ));
    });

    // Create and register a new chainhook (wallet_2 received some sats)
    let chainhook = generate_and_register_new_bitcoin_chainhook(
        &observer_commands_tx,
        &observer_events_rx,
        1,
        &accounts::wallet_2_btc_address(),
        None,
    );

    // Simulate a block that does not include a trigger (wallet_1 to wallet_3)
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        0,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_3_btc_address(),
        3,
    )];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 1, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        Ok(event) => {
            println!("Unexpected event: {:?}", event);
            false
        }
        Err(e) => {
            println!("Error: {:?}", e);
            false
        }
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger (wallet_1 to wallet_2)
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        0,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_2_btc_address(),
        3,
    )];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 2, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });

    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainhookTriggered(payload)) => {
            assert_eq!(payload.apply.len(), 1);
            assert_eq!(payload.apply[0].block.transactions.len(), 1);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger (wallet_1 to wallet_2)
    let transactions = vec![
        generate_test_tx_bitcoin_p2pkh_transfer(
            0,
            &accounts::wallet_1_btc_address(),
            &accounts::wallet_2_btc_address(),
            3,
        ),
        generate_test_tx_bitcoin_p2pkh_transfer(
            1,
            &accounts::wallet_3_btc_address(),
            &accounts::wallet_2_btc_address(),
            5,
        ),
        generate_test_tx_bitcoin_p2pkh_transfer(
            1,
            &accounts::wallet_3_btc_address(),
            &accounts::wallet_1_btc_address(),
            5,
        ),
    ];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 2, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });

    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainhookTriggered(payload)) => {
            assert_eq!(payload.apply.len(), 1);
            assert_eq!(payload.apply[0].block.transactions.len(), 2);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Deregister the hook
    let _ = observer_commands_tx.send(ObserverCommand::DeregisterBitcoinHook(
        chainhook.uuid.clone(),
        ApiKey(None),
    ));
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookDeregistered(deregistered_chainhook)) => {
            assert_eq!(
                ChainhookSpecification::Bitcoin(chainhook),
                deregistered_chainhook
            );
            true
        }
        _ => false,
    });

    // Simulate a block that does not include a trigger
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        2,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_3_btc_address(),
        1,
    )];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 2, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));

    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        3,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_2_btc_address(),
        1,
    )];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 3, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));
    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    let _ = observer_commands_tx.send(ObserverCommand::Terminate);
    handle.join().expect("unable to terminate thread");
}

#[test]
fn test_bitcoin_chainhook_auto_deregister() {
    let (observer_commands_tx, observer_commands_rx) = channel();
    let (observer_events_tx, observer_events_rx) = crossbeam_channel::unbounded();

    let handle = std::thread::spawn(move || {
        let (config, chainhook_store) = generate_test_config();
        let _ = hiro_system_kit::nestable_block_on(start_observer_commands_handler(
            config,
            Arc::new(RwLock::new(chainhook_store)),
            observer_commands_rx,
            Some(observer_events_tx),
            None,
            None,
            Context::empty(),
        ));
    });

    // Create and register a new chainhook (wallet_2 received some sats)
    let chainhook = generate_and_register_new_bitcoin_chainhook(
        &observer_commands_tx,
        &observer_events_rx,
        1,
        &accounts::wallet_2_btc_address(),
        Some(1),
    );

    // Simulate a block that does not include a trigger (wallet_1 to wallet_3)
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        0,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_3_btc_address(),
        3,
    )];
    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 1, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));

    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger (wallet_1 to wallet_2)
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        0,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_2_btc_address(),
        3,
    )];

    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 2, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));

    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 1);
            true
        }
        _ => false,
    });

    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainhookTriggered(_)) => {
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does not include a trigger
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        2,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_3_btc_address(),
        1,
    )];

    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 2, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));

    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    // Simulate a block that does include a trigger
    let transactions = vec![generate_test_tx_bitcoin_p2pkh_transfer(
        3,
        &accounts::wallet_1_btc_address(),
        &accounts::wallet_2_btc_address(),
        1,
    )];

    let block = bitcoin_blocks::generate_test_bitcoin_block(0, 3, transactions, None);
    let _ = observer_commands_tx.send(ObserverCommand::CacheBitcoinBlock(block.clone()));
    let chain_event = BlockchainEvent::BlockchainUpdatedWithHeaders(BlockchainUpdatedWithHeaders {
        new_headers: vec![block.get_header()],
        confirmed_headers: vec![],
    });
    let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(chain_event));

    // Should signal that no hook were triggered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HooksTriggered(len)) => {
            assert_eq!(len, 0);
            true
        }
        _ => false,
    });
    // Should signal that a hook was deregistered
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::HookDeregistered(deregistered_hook)) => {
            assert_eq!(deregistered_hook.uuid(), chainhook.uuid);
            true
        }
        _ => false,
    });

    // Should propagate block
    assert!(match observer_events_rx.recv() {
        Ok(ObserverEvent::BitcoinChainEvent(_)) => {
            true
        }
        _ => false,
    });

    let _ = observer_commands_tx.send(ObserverCommand::Terminate);
    handle.join().expect("unable to terminate thread");
}
