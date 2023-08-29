use std::collections::BTreeMap;
use std::str::FromStr;

use chainhook_sdk::bitcoincore_rpc_json::bitcoin::hashes::hex::FromHex;
use chainhook_sdk::bitcoincore_rpc_json::bitcoin::Txid;
use chainhook_sdk::indexer::bitcoin::{standardize_bitcoin_block, BitcoinBlockFullBreakdown};
use chainhook_sdk::types::{
    BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, OrdinalInscriptionCurseType,
    OrdinalInscriptionRevealData, OrdinalInscriptionTransferData, OrdinalOperation,
};
use chainhook_sdk::utils::Context;
use chainhook_sdk::{
    bitcoincore_rpc::bitcoin::Transaction, indexer::bitcoin::BitcoinTransactionFullBreakdown,
};

use crate::ord::inscription_id::InscriptionId;
use {
    chainhook_sdk::bitcoincore_rpc::bitcoin::{
        blockdata::{
            opcodes,
            script::{self, Instruction, Instructions},
        },
        util::taproot::TAPROOT_ANNEX_PREFIX,
        Script, Witness,
    },
    std::{iter::Peekable, str},
};

const PROTOCOL_ID: &[u8] = b"ord";

const BODY_TAG: &[u8] = &[];
const CONTENT_TYPE_TAG: &[u8] = &[1];

#[derive(Debug, PartialEq, Clone)]
pub struct Inscription {
    pub body: Option<Vec<u8>>,
    pub content_type: Option<Vec<u8>>,
    pub curse: Option<OrdinalInscriptionCurseType>,
}

impl Inscription {
    pub fn from_transaction(tx: &Transaction) -> Option<Inscription> {
        InscriptionParser::parse(&tx.input.get(0)?.witness).ok()
    }

    pub(crate) fn body(&self) -> Option<&[u8]> {
        Some(self.body.as_ref()?)
    }

    pub(crate) fn content_type(&self) -> Option<&str> {
        str::from_utf8(self.content_type.as_ref()?).ok()
    }
}

#[derive(Debug, PartialEq)]
pub enum InscriptionError {
    EmptyWitness,
    InvalidInscription,
    KeyPathSpend,
    NoInscription,
    Script(script::Error),
    UnrecognizedEvenField,
}

type Result<T, E = InscriptionError> = std::result::Result<T, E>;

pub struct InscriptionParser<'a> {
    pub instructions: Peekable<Instructions<'a>>,
}

impl<'a> InscriptionParser<'a> {
    pub fn parse(witness: &Witness) -> Result<Inscription> {
        if witness.is_empty() {
            return Err(InscriptionError::EmptyWitness);
        }

        if witness.len() == 1 {
            return Err(InscriptionError::KeyPathSpend);
        }

        let annex = witness
            .last()
            .and_then(|element| element.first().map(|byte| *byte == TAPROOT_ANNEX_PREFIX))
            .unwrap_or(false);

        if witness.len() == 2 && annex {
            return Err(InscriptionError::KeyPathSpend);
        }

        let script = witness
            .iter()
            .nth(if annex {
                witness.len() - 1
            } else {
                witness.len() - 2
            })
            .unwrap();

        InscriptionParser {
            instructions: Script::from(Vec::from(script)).instructions().peekable(),
        }
        .parse_script()
    }

    pub fn parse_script(mut self) -> Result<Inscription> {
        loop {
            let next = self.advance()?;

            if next == Instruction::PushBytes(&[]) {
                if let Some(inscription) = self.parse_inscription()? {
                    return Ok(inscription);
                }
            }
        }
    }

    fn advance(&mut self) -> Result<Instruction<'a>> {
        self.instructions
            .next()
            .ok_or(InscriptionError::NoInscription)?
            .map_err(InscriptionError::Script)
    }

    fn parse_inscription(&mut self) -> Result<Option<Inscription>> {
        if self.advance()? == Instruction::Op(opcodes::all::OP_IF) {
            if !self.accept(Instruction::PushBytes(PROTOCOL_ID))? {
                return Err(InscriptionError::NoInscription);
            }

            let mut fields = BTreeMap::new();

            loop {
                match self.advance()? {
                    Instruction::PushBytes(BODY_TAG) => {
                        let mut body = Vec::new();
                        while !self.accept(Instruction::Op(opcodes::all::OP_ENDIF))? {
                            body.extend_from_slice(self.expect_push()?);
                        }
                        fields.insert(BODY_TAG, body);
                        break;
                    }
                    Instruction::PushBytes(tag) => {
                        if fields.contains_key(tag) {
                            return Err(InscriptionError::InvalidInscription);
                        }
                        fields.insert(tag, self.expect_push()?.to_vec());
                    }
                    Instruction::Op(opcodes::all::OP_ENDIF) => break,
                    _ => return Err(InscriptionError::InvalidInscription),
                }
            }

            let body = fields.remove(BODY_TAG);
            let content_type = fields.remove(CONTENT_TYPE_TAG);

            for tag in fields.keys() {
                if let Some(lsb) = tag.first() {
                    if lsb % 2 == 0 {
                        return Ok(Some(Inscription {
                            body,
                            content_type,
                            curse: Some(OrdinalInscriptionCurseType::Tag(*lsb)),
                        }));
                    }
                }
            }

            return Ok(Some(Inscription {
                body,
                content_type,
                curse: None,
            }));
        }

        Ok(None)
    }

    fn expect_push(&mut self) -> Result<&'a [u8]> {
        match self.advance()? {
            Instruction::PushBytes(bytes) => Ok(bytes),
            _ => Err(InscriptionError::InvalidInscription),
        }
    }

    fn accept(&mut self, instruction: Instruction) -> Result<bool> {
        match self.instructions.peek() {
            Some(Ok(next)) => {
                if *next == instruction {
                    self.advance()?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(Err(err)) => Err(InscriptionError::Script(*err)),
            None => Ok(false),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum Media {
    Audio,
    Iframe,
    Image,
    Pdf,
    Text,
    Unknown,
    Video,
}

impl Media {
    const TABLE: &'static [(&'static str, Media, &'static [&'static str])] = &[
        ("application/json", Media::Text, &["json"]),
        ("application/pdf", Media::Pdf, &["pdf"]),
        ("application/pgp-signature", Media::Text, &["asc"]),
        ("application/yaml", Media::Text, &["yaml", "yml"]),
        ("audio/flac", Media::Audio, &["flac"]),
        ("audio/mpeg", Media::Audio, &["mp3"]),
        ("audio/wav", Media::Audio, &["wav"]),
        ("image/apng", Media::Image, &["apng"]),
        ("image/avif", Media::Image, &[]),
        ("image/gif", Media::Image, &["gif"]),
        ("image/jpeg", Media::Image, &["jpg", "jpeg"]),
        ("image/png", Media::Image, &["png"]),
        ("image/svg+xml", Media::Iframe, &["svg"]),
        ("image/webp", Media::Image, &["webp"]),
        ("model/stl", Media::Unknown, &["stl"]),
        ("text/html;charset=utf-8", Media::Iframe, &["html"]),
        ("text/plain;charset=utf-8", Media::Text, &["txt"]),
        ("video/mp4", Media::Video, &["mp4"]),
        ("video/webm", Media::Video, &["webm"]),
    ];
}

impl FromStr for Media {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for entry in Self::TABLE {
            if entry.0 == s {
                return Ok(entry.1);
            }
        }

        Err("unknown content type: {s}".to_string())
    }
}

pub fn parse_inscriptions_from_witness(
    input_index: usize,
    witness_bytes: Vec<Vec<u8>>,
    txid: &str,
) -> Option<OrdinalOperation> {
    let witness = Witness::from_vec(witness_bytes.clone());
    let mut inscription = match InscriptionParser::parse(&witness) {
        Ok(inscription) => inscription,
        Err(_e) => {
            let mut cursed_inscription = None;
            for bytes in witness_bytes.iter() {
                let script = Script::from(bytes.to_vec());
                let parser = InscriptionParser {
                    instructions: script.instructions().peekable(),
                };

                let mut inscription = match parser.parse_script() {
                    Ok(inscription) => inscription,
                    Err(_) => continue,
                };
                inscription.curse = Some(OrdinalInscriptionCurseType::P2wsh);
                cursed_inscription = Some(inscription);
                break;
            }
            match cursed_inscription {
                Some(inscription) => inscription,
                None => return None,
            }
        }
    };

    let inscription_id = InscriptionId {
        txid: Txid::from_hex(txid).unwrap(),
        index: input_index as u32,
    };

    if input_index > 0 {
        inscription.curse = Some(OrdinalInscriptionCurseType::Batch);
    }

    let no_content_bytes = vec![];
    let inscription_content_bytes = inscription.body().take().unwrap_or(&no_content_bytes);
    let mut content_bytes = "0x".to_string();
    content_bytes.push_str(&hex::encode(&inscription_content_bytes));

    let payload = OrdinalInscriptionRevealData {
        content_type: inscription.content_type().unwrap_or("unknown").to_string(),
        content_bytes,
        content_length: inscription_content_bytes.len(),
        inscription_id: inscription_id.to_string(),
        inscription_input_index: input_index,
        tx_index: 0,
        inscription_output_value: 0,
        inscription_fee: 0,
        inscription_number: 0,
        inscriber_address: None,
        ordinal_number: 0,
        ordinal_block_height: 0,
        ordinal_offset: 0,
        transfers_pre_inscription: 0,
        satpoint_post_inscription: format!(""),
        curse_type: inscription.curse.take(),
    };

    Some(OrdinalOperation::InscriptionRevealed(payload))
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

        if let Some(operation) = parse_inscriptions_from_witness(
            input_index,
            witness_bytes,
            tx.transaction_identifier.get_hash_bytes_str(),
        ) {
            operations.push(operation);
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

            if let Some(operation) =
                parse_inscriptions_from_witness(input_index, witness_bytes, &tx.txid)
            {
                operations.push(operation);
            }
        }
    }
    operations
}

#[test]
fn test_ordinal_inscription_parsing() {
    let bytes = hex::decode("208737bc46923c3e64c7e6768c0346879468bf3aba795a5f5f56efca288f50ed2aac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c9948656c6c6f2030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030300a68").unwrap();

    let script = Script::from(bytes);
    let parser = InscriptionParser {
        instructions: script.instructions().peekable(),
    };

    let inscription = match parser.parse_script() {
        Ok(inscription) => inscription,
        Err(_) => panic!(),
    };

    println!("{:?}", inscription);
}

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
