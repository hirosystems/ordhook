use std::collections::BTreeMap;

use super::{BitcoinBlockData, BitcoinTransactionData, StacksBlockData, StacksTransactionData};
use serde_json::Value as JsonValue;

pub struct ProcessedStacksTransaction {
    tx: StacksTransactionData,
    metadata: BTreeMap<String, JsonValue>,
}

pub struct ProcessedStacksBlock {
    tx: StacksBlockData,
    metadata: BTreeMap<String, JsonValue>,
}

pub struct ProcessedBitcoinTransaction {
    tx: BitcoinTransactionData,
    metadata: BTreeMap<String, JsonValue>,
}

pub struct ProcessedBitcoinBlock {
    tx: BitcoinBlockData,
    metadata: BTreeMap<String, JsonValue>,
}

pub enum ProcessingContext {
    Scanning,
    Streaming,
}

pub trait BitcoinProtocolProcessor {
    fn register(&mut self);
    fn process_block(
        &mut self,
        block: &mut ProcessedBitcoinBlock,
        processing_context: ProcessingContext,
    );
    fn process_transaction(
        &mut self,
        transaction: &mut ProcessedBitcoinTransaction,
        processing_context: ProcessingContext,
    );
}

pub fn run_processor<P>(mut p: P)
where
    P: BitcoinProtocolProcessor,
{
    p.register();
}
