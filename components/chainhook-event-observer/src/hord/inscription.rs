use std::collections::BTreeMap;
use std::str::FromStr;

use bitcoincore_rpc::bitcoin::Transaction;
use {
    bitcoincore_rpc::bitcoin::{
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
    body: Option<Vec<u8>>,
    content_type: Option<Vec<u8>>,
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
                        return Err(InscriptionError::UnrecognizedEvenField);
                    }
                }
            }

            return Ok(Some(Inscription { body, content_type }));
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
