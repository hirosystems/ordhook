use chainhook_sdk::bitcoincore_rpc_json::bitcoin::Txid;
use chainhook_sdk::indexer::bitcoin::BitcoinTransactionFullBreakdown;
use chainhook_sdk::indexer::bitcoin::{standardize_bitcoin_block, BitcoinBlockFullBreakdown};
use chainhook_sdk::types::{
    BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, OrdinalInscriptionCurseType,
    OrdinalInscriptionNumber, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    OrdinalOperation,
};
use chainhook_sdk::utils::Context;
use serde_json::json;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::ord::envelope::{Envelope, ParsedEnvelope, RawEnvelope};
use crate::ord::inscription::Inscription;
use crate::ord::inscription_id::InscriptionId;
use {chainhook_sdk::bitcoincore_rpc::bitcoin::Witness, std::str};

pub fn parse_inscriptions_from_witness(
    input_index: usize,
    witness_bytes: Vec<Vec<u8>>,
    txid: &str,
) -> Option<Vec<OrdinalInscriptionRevealData>> {
    let witness = Witness::from_slice(&witness_bytes);
    let tapscript = witness.tapscript()?;
    let envelopes: Vec<Envelope<Inscription>> = RawEnvelope::from_tapscript(tapscript, input_index)
        .ok()?
        .into_iter()
        .map(|e| ParsedEnvelope::from(e))
        .collect();
    let mut inscriptions = vec![];
    for envelope in envelopes.into_iter() {
        let curse_type = if envelope.payload.unrecognized_even_field {
            Some(OrdinalInscriptionCurseType::UnrecognizedEvenField)
        } else if envelope.payload.duplicate_field {
            Some(OrdinalInscriptionCurseType::DuplicateField)
        } else if envelope.payload.incomplete_field {
            Some(OrdinalInscriptionCurseType::IncompleteField)
        } else if envelope.input != 0 {
            Some(OrdinalInscriptionCurseType::NotInFirstInput)
        } else if envelope.offset != 0 {
            Some(OrdinalInscriptionCurseType::NotAtOffsetZero)
        } else if envelope.payload.pointer.is_some() {
            Some(OrdinalInscriptionCurseType::Pointer)
        } else if envelope.pushnum {
            Some(OrdinalInscriptionCurseType::Pushnum)
        } else if envelope.stutter {
            Some(OrdinalInscriptionCurseType::Stutter)
        } else {
            None
        };

        let inscription_id = InscriptionId {
            txid: Txid::from_str(txid).unwrap(),
            index: input_index as u32,
        };

        let no_content_bytes = vec![];
        let inscription_content_bytes = envelope.payload.body().take().unwrap_or(&no_content_bytes);
        let mut content_bytes = "0x".to_string();
        content_bytes.push_str(&hex::encode(&inscription_content_bytes));

        let parent = envelope.payload.parent().and_then(|i| Some(i.to_string()));
        let delegate = envelope
            .payload
            .delegate()
            .and_then(|i| Some(i.to_string()));
        let metaprotocol = envelope
            .payload
            .metaprotocol()
            .and_then(|p| Some(p.to_string()));
        let metadata = envelope.payload.metadata().and_then(|m| Some(json!(m)));

        let reveal_data = OrdinalInscriptionRevealData {
            content_type: envelope
                .payload
                .content_type()
                .unwrap_or("unknown")
                .to_string(),
            content_bytes,
            content_length: inscription_content_bytes.len(),
            inscription_id: inscription_id.to_string(),
            inscription_input_index: input_index,
            tx_index: 0,
            inscription_output_value: 0,
            inscription_pointer: envelope.payload.pointer().unwrap_or(0),
            inscription_fee: 0,
            inscription_number: OrdinalInscriptionNumber::zero(),
            inscriber_address: None,
            parent,
            delegate,
            metaprotocol,
            metadata,
            ordinal_number: 0,
            ordinal_block_height: 0,
            ordinal_offset: 0,
            transfers_pre_inscription: 0,
            satpoint_post_inscription: format!(""),
            curse_type,
        };
        inscriptions.push(reveal_data);
    }
    Some(inscriptions)
}

pub fn parse_inscriptions_from_standardized_tx(
    tx: &BitcoinTransactionData,
    _ctx: &Context,
) -> Vec<OrdinalOperation> {
    let mut operations = vec![];
    for (input_index, input) in tx.metadata.inputs.iter().enumerate() {
        let witness_bytes: Vec<Vec<u8>> = input
            .witness
            .iter()
            .map(|w| hex::decode(&w[2..]).unwrap())
            .collect();

        if let Some(inscriptions) = parse_inscriptions_from_witness(
            input_index,
            witness_bytes,
            tx.transaction_identifier.get_hash_bytes_str(),
        ) {
            for inscription in inscriptions.into_iter() {
                operations.push(OrdinalOperation::InscriptionRevealed(inscription));
            }
        }
    }
    operations
}

pub fn parse_inscriptions_in_raw_tx(
    tx: &BitcoinTransactionFullBreakdown,
    _ctx: &Context,
) -> Vec<OrdinalOperation> {
    let mut operations = vec![];
    for (input_index, input) in tx.vin.iter().enumerate() {
        if let Some(ref witness_data) = input.txinwitness {
            let witness_bytes: Vec<Vec<u8>> = witness_data
                .iter()
                .map(|w| hex::decode(w).unwrap())
                .collect();

            if let Some(inscriptions) =
                parse_inscriptions_from_witness(input_index, witness_bytes, &tx.txid)
            {
                for inscription in inscriptions.into_iter() {
                    operations.push(OrdinalOperation::InscriptionRevealed(inscription));
                }
            }
        }
    }
    operations
}

// #[test]
// fn test_ordinal_inscription_parsing() {
//     let bytes = hex::decode("208737bc46923c3e64c7e6768c0346879468bf3aba795a5f5f56efca288f50ed2aac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c9948656c6c6f2030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030300a68").unwrap();

//     let script = Script::from(bytes);
//     let parser = InscriptionParser {
//         instructions: script.instructions().peekable(),
//     };

//     let inscription = match parser.parse_script() {
//         Ok(inscription) => inscription,
//         Err(_) => panic!(),
//     };

//     println!("{:?}", inscription);
// }

pub fn parse_inscriptions_and_standardize_block(
    raw_block: BitcoinBlockFullBreakdown,
    network: &BitcoinNetwork,
    ctx: &Context,
) -> Result<BitcoinBlockData, (String, bool)> {
    let mut ordinal_operations = BTreeMap::new();

    for tx in raw_block.tx.iter() {
        ordinal_operations.insert(tx.txid.to_string(), parse_inscriptions_in_raw_tx(&tx, ctx));
    }

    let mut block = standardize_bitcoin_block(raw_block, network, ctx)?;

    for tx in block.transactions.iter_mut() {
        if let Some(ordinal_operations) =
            ordinal_operations.remove(tx.transaction_identifier.get_hash_bytes_str())
        {
            tx.metadata.ordinal_operations = ordinal_operations;
        }
    }
    Ok(block)
}

pub fn parse_inscriptions_in_standardized_block(block: &mut BitcoinBlockData, ctx: &Context) {
    for tx in block.transactions.iter_mut() {
        tx.metadata.ordinal_operations = parse_inscriptions_from_standardized_tx(tx, ctx);
    }
}

pub fn get_inscriptions_revealed_in_block(
    block: &BitcoinBlockData,
) -> Vec<&OrdinalInscriptionRevealData> {
    let mut ops = vec![];
    for tx in block.transactions.iter() {
        for op in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionRevealed(op) = op {
                ops.push(op);
            }
        }
    }
    ops
}

pub fn get_inscriptions_transferred_in_block(
    block: &BitcoinBlockData,
) -> Vec<&OrdinalInscriptionTransferData> {
    let mut ops = vec![];
    for tx in block.transactions.iter() {
        for op in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionTransferred(op) = op {
                ops.push(op);
            }
        }
    }
    ops
}
