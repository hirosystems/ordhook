use super::types::{
    BitcoinChainhookSpecification, BitcoinPredicateType, ExactMatchingRule, HookAction,
    InputPredicate, MatchingRule, OrdinalOperations, OutputPredicate, StacksOperations,
};
use crate::utils::Context;

use bitcoincore_rpc::bitcoin::util::address::Payload;
use bitcoincore_rpc::bitcoin::Address;
use chainhook_types::{
    BitcoinBlockData, BitcoinChainEvent, BitcoinTransactionData, BlockIdentifier, OrdinalOperation,
    StacksBaseChainOperation, TransactionIdentifier,
};

use reqwest::{Client, Method};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use reqwest::RequestBuilder;

pub struct BitcoinTriggerChainhook<'a> {
    pub chainhook: &'a BitcoinChainhookSpecification,
    pub apply: Vec<(Vec<&'a BitcoinTransactionData>, &'a BitcoinBlockData)>,
    pub rollback: Vec<(Vec<&'a BitcoinTransactionData>, &'a BitcoinBlockData)>,
}

#[derive(Clone, Debug)]
pub struct BitcoinApplyTransactionPayload {
    pub block: BitcoinBlockData,
}

#[derive(Clone, Debug)]
pub struct BitcoinRollbackTransactionPayload {
    pub block: BitcoinBlockData,
}

#[derive(Clone, Debug)]
pub struct BitcoinChainhookPayload {
    pub uuid: String,
}

#[derive(Clone, Debug)]
pub struct BitcoinChainhookOccurrencePayload {
    pub apply: Vec<BitcoinApplyTransactionPayload>,
    pub rollback: Vec<BitcoinRollbackTransactionPayload>,
    pub chainhook: BitcoinChainhookPayload,
}

pub enum BitcoinChainhookOccurrence {
    Http(RequestBuilder),
    File(String, Vec<u8>),
    Data(BitcoinChainhookOccurrencePayload),
}

pub fn evaluate_bitcoin_chainhooks_on_chain_event<'a>(
    chain_event: &'a BitcoinChainEvent,
    active_chainhooks: &Vec<&'a BitcoinChainhookSpecification>,
    ctx: &Context,
) -> (
    Vec<BitcoinTriggerChainhook<'a>>,
    BTreeMap<&'a str, &'a BlockIdentifier>,
) {
    let mut evaluated_predicates = BTreeMap::new();
    let mut triggered_predicates = vec![];
    match chain_event {
        BitcoinChainEvent::ChainUpdatedWithBlocks(event) => {
            for chainhook in active_chainhooks.iter() {
                let mut apply = vec![];
                let rollback = vec![];

                for block in event.new_blocks.iter() {
                    evaluated_predicates.insert(chainhook.uuid.as_str(), &block.block_identifier);
                    let mut hits = vec![];
                    for tx in block.transactions.iter() {
                        if chainhook.predicate.evaluate_transaction_predicate(&tx, ctx) {
                            hits.push(tx);
                        }
                    }
                    if hits.len() > 0 {
                        apply.push((hits, block));
                    }
                }

                if !apply.is_empty() {
                    triggered_predicates.push(BitcoinTriggerChainhook {
                        chainhook,
                        apply,
                        rollback,
                    })
                }
            }
        }
        BitcoinChainEvent::ChainUpdatedWithReorg(event) => {
            for chainhook in active_chainhooks.iter() {
                let mut apply = vec![];
                let mut rollback = vec![];

                for block in event.blocks_to_rollback.iter() {
                    let mut hits = vec![];
                    for tx in block.transactions.iter() {
                        if chainhook.predicate.evaluate_transaction_predicate(&tx, ctx) {
                            hits.push(tx);
                        }
                    }
                    if hits.len() > 0 {
                        rollback.push((hits, block));
                    }
                }
                for block in event.blocks_to_apply.iter() {
                    evaluated_predicates.insert(chainhook.uuid.as_str(), &block.block_identifier);
                    let mut hits = vec![];
                    for tx in block.transactions.iter() {
                        if chainhook.predicate.evaluate_transaction_predicate(&tx, ctx) {
                            hits.push(tx);
                        }
                    }
                    if hits.len() > 0 {
                        apply.push((hits, block));
                    }
                }
                if !apply.is_empty() || !rollback.is_empty() {
                    triggered_predicates.push(BitcoinTriggerChainhook {
                        chainhook,
                        apply,
                        rollback,
                    })
                }
            }
        }
    }
    (triggered_predicates, evaluated_predicates)
}

pub fn serialize_bitcoin_payload_to_json<'a>(
    trigger: BitcoinTriggerChainhook<'a>,
    proofs: &HashMap<&'a TransactionIdentifier, String>,
) -> JsonValue {
    let predicate_spec = &trigger.chainhook;
    json!({
        "apply": trigger.apply.into_iter().map(|(transactions, block)| {
            json!({
                "block_identifier": block.block_identifier,
                "parent_block_identifier": block.parent_block_identifier,
                "timestamp": block.timestamp,
                "transactions": serialize_bitcoin_transactions_to_json(&predicate_spec, &transactions, proofs),
                "metadata": block.metadata,
            })
        }).collect::<Vec<_>>(),
        "rollback": trigger.rollback.into_iter().map(|(transactions, block)| {
            json!({
                "block_identifier": block.block_identifier,
                "parent_block_identifier": block.parent_block_identifier,
                "timestamp": block.timestamp,
                "transactions": serialize_bitcoin_transactions_to_json(&predicate_spec, &transactions, proofs),
                "metadata": block.metadata,
            })
        }).collect::<Vec<_>>(),
        "chainhook": {
            "uuid": trigger.chainhook.uuid,
            "predicate": trigger.chainhook.predicate,
            "is_streaming_blocks": trigger.chainhook.enabled
        }
    })
}

pub fn serialize_bitcoin_transactions_to_json<'a>(
    predicate_spec: &BitcoinChainhookSpecification,
    transactions: &Vec<&BitcoinTransactionData>,
    proofs: &HashMap<&'a TransactionIdentifier, String>,
) -> Vec<JsonValue> {
    transactions
        .into_iter()
        .map(|transaction| {
            let mut metadata = serde_json::Map::new();
            if predicate_spec.include_inputs {
                metadata.insert(
                    "inputs".into(),
                    json!(transaction
                        .metadata
                        .inputs
                        .iter()
                        .map(|input| {
                            json!({
                                "txin": input.previous_output.txid.hash.to_string(),
                                "vout": input.previous_output.vout,
                                "sequence": input.sequence,
                            })
                        })
                        .collect::<Vec<_>>()),
                );
            }
            if predicate_spec.include_outputs {
                metadata.insert("outputs".into(), json!(transaction.metadata.outputs));
            }
            if !transaction.metadata.stacks_operations.is_empty() {
                metadata.insert(
                    "stacks_operations".into(),
                    json!(transaction.metadata.stacks_operations),
                );
            }
            if !transaction.metadata.ordinal_operations.is_empty() {
                metadata.insert(
                    "ordinal_operations".into(),
                    json!(transaction.metadata.ordinal_operations),
                );
            }
            metadata.insert(
                "proof".into(),
                json!(proofs.get(&transaction.transaction_identifier)),
            );
            json!({
                "transaction_identifier": transaction.transaction_identifier,
                "operations": transaction.operations,
                "metadata": metadata
            })
        })
        .collect::<Vec<_>>()
}

pub fn handle_bitcoin_hook_action<'a>(
    trigger: BitcoinTriggerChainhook<'a>,
    proofs: &HashMap<&'a TransactionIdentifier, String>,
) -> Result<BitcoinChainhookOccurrence, String> {
    match &trigger.chainhook.action {
        HookAction::HttpPost(http) => {
            let client = Client::builder()
                .build()
                .map_err(|e| format!("unable to build http client: {}", e.to_string()))?;
            let host = format!("{}", http.url);
            let method = Method::POST;
            let body = serde_json::to_vec(&serialize_bitcoin_payload_to_json(trigger, proofs))
                .map_err(|e| format!("unable to serialize payload {}", e.to_string()))?;
            Ok(BitcoinChainhookOccurrence::Http(
                client
                    .request(method, &host)
                    .header("Content-Type", "application/json")
                    .header("Authorization", http.authorization_header.clone())
                    .body(body),
            ))
        }
        HookAction::FileAppend(disk) => {
            let bytes = serde_json::to_vec(&serialize_bitcoin_payload_to_json(trigger, proofs))
                .map_err(|e| format!("unable to serialize payload {}", e.to_string()))?;
            Ok(BitcoinChainhookOccurrence::File(
                disk.path.to_string(),
                bytes,
            ))
        }
        HookAction::Noop => Ok(BitcoinChainhookOccurrence::Data(
            BitcoinChainhookOccurrencePayload {
                apply: trigger
                    .apply
                    .into_iter()
                    .map(|(transactions, block)| {
                        let mut block = block.clone();
                        block.transactions = transactions
                            .into_iter()
                            .map(|t| t.clone())
                            .collect::<Vec<_>>();
                        BitcoinApplyTransactionPayload { block }
                    })
                    .collect::<Vec<_>>(),
                rollback: trigger
                    .rollback
                    .into_iter()
                    .map(|(transactions, block)| {
                        let mut block = block.clone();
                        block.transactions = transactions
                            .into_iter()
                            .map(|t| t.clone())
                            .collect::<Vec<_>>();
                        BitcoinRollbackTransactionPayload { block }
                    })
                    .collect::<Vec<_>>(),
                chainhook: BitcoinChainhookPayload {
                    uuid: trigger.chainhook.uuid.clone(),
                },
            },
        )),
    }
}

impl BitcoinPredicateType {
    pub fn evaluate_transaction_predicate(
        &self,
        tx: &BitcoinTransactionData,
        _ctx: &Context,
    ) -> bool {
        // TODO(lgalabru): follow-up on this implementation
        match &self {
            BitcoinPredicateType::Block => true,
            BitcoinPredicateType::Txid(ExactMatchingRule::Equals(txid)) => {
                tx.transaction_identifier.hash.eq(txid)
            }
            BitcoinPredicateType::Outputs(OutputPredicate::OpReturn(MatchingRule::Equals(
                hex_bytes,
            ))) => {
                for output in tx.metadata.outputs.iter() {
                    if output.script_pubkey.eq(hex_bytes) {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Outputs(OutputPredicate::OpReturn(MatchingRule::StartsWith(
                hex_bytes,
            ))) => {
                for output in tx.metadata.outputs.iter() {
                    if output.script_pubkey.starts_with(hex_bytes) {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Outputs(OutputPredicate::OpReturn(MatchingRule::EndsWith(
                hex_bytes,
            ))) => {
                for output in tx.metadata.outputs.iter() {
                    if output.script_pubkey.ends_with(hex_bytes) {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Outputs(OutputPredicate::P2pkh(ExactMatchingRule::Equals(
                encoded_address,
            )))
            | BitcoinPredicateType::Outputs(OutputPredicate::P2sh(ExactMatchingRule::Equals(
                encoded_address,
            ))) => {
                let address = match Address::from_str(encoded_address) {
                    Ok(address) => address,
                    Err(_) => return false,
                };
                let address_bytes = hex::encode(address.script_pubkey().as_bytes());
                for output in tx.metadata.outputs.iter() {
                    if output.script_pubkey[2..] == address_bytes {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Outputs(OutputPredicate::P2wpkh(ExactMatchingRule::Equals(
                encoded_address,
            )))
            | BitcoinPredicateType::Outputs(OutputPredicate::P2wsh(ExactMatchingRule::Equals(
                encoded_address,
            ))) => {
                let address = match Address::from_str(encoded_address) {
                    Ok(address) => match address.payload {
                        Payload::WitnessProgram {
                            version: _,
                            program: _,
                        } => address,
                        _ => return false,
                    },
                    Err(_) => return false,
                };
                let address_bytes = hex::encode(address.script_pubkey().as_bytes());
                for output in tx.metadata.outputs.iter() {
                    if output.script_pubkey[2..] == address_bytes {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Inputs(InputPredicate::Txid(predicate)) => {
                // TODO(lgalabru): add support for transaction chainhing, if enabled
                for input in tx.metadata.inputs.iter() {
                    if input.previous_output.txid.hash.eq(&predicate.txid)
                        && input.previous_output.vout.eq(&predicate.vout)
                    {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::Inputs(InputPredicate::WitnessScript(_)) => {
                // TODO(lgalabru)
                unimplemented!()
            }
            BitcoinPredicateType::StacksProtocol(StacksOperations::StackerRewarded) => {
                for op in tx.metadata.stacks_operations.iter() {
                    if let StacksBaseChainOperation::BlockCommitted(_) = op {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::StacksProtocol(StacksOperations::BlockCommitted) => {
                for op in tx.metadata.stacks_operations.iter() {
                    if let StacksBaseChainOperation::BlockCommitted(_) = op {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::StacksProtocol(StacksOperations::LeaderRegistered) => {
                for op in tx.metadata.stacks_operations.iter() {
                    if let StacksBaseChainOperation::LeaderRegistered(_) = op {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::StacksProtocol(StacksOperations::StxTransferred) => {
                for op in tx.metadata.stacks_operations.iter() {
                    if let StacksBaseChainOperation::StxTransferred(_) = op {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::StacksProtocol(StacksOperations::StxLocked) => {
                for op in tx.metadata.stacks_operations.iter() {
                    if let StacksBaseChainOperation::StxLocked(_) = op {
                        return true;
                    }
                }
                false
            }
            BitcoinPredicateType::OrdinalsProtocol(OrdinalOperations::InscriptionFeed) => {
                for op in tx.metadata.ordinal_operations.iter() {
                    if let OrdinalOperation::InscriptionRevealed(_) = op {
                        return true;
                    }
                    if let OrdinalOperation::InscriptionTransferred(_) = op {
                        return true;
                    }
                }
                false
            }
        }
    }
}
