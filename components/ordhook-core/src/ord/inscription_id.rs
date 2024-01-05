use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use chainhook_sdk::bitcoin::Txid;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::deserialize_from_str::DeserializeFromStr;

#[derive(Debug, PartialEq, Copy, Clone, Hash, Eq)]
pub struct InscriptionId {
    pub(crate) txid: Txid,
    pub(crate) index: u32,
}

impl<'de> Deserialize<'de> for InscriptionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(DeserializeFromStr::deserialize(deserializer)?.0)
    }
}

impl Serialize for InscriptionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl Display for InscriptionId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}i{}", self.txid, self.index)
    }
}

#[derive(Debug)]
pub enum ParseError {
    Character(char),
    Length(usize),
    Separator(char),
    Txid(chainhook_sdk::bitcoin::hashes::hex::HexToArrayError),
    Index(std::num::ParseIntError),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Character(c) => write!(f, "invalid character: '{c}'"),
            Self::Length(len) => write!(f, "invalid length: {len}"),
            Self::Separator(c) => write!(f, "invalid seprator: `{c}`"),
            Self::Txid(err) => write!(f, "invalid txid: {err}"),
            Self::Index(err) => write!(f, "invalid index: {err}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl FromStr for InscriptionId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(char) = s.chars().find(|char| !char.is_ascii()) {
            return Err(ParseError::Character(char));
        }

        const TXID_LEN: usize = 64;
        const MIN_LEN: usize = TXID_LEN + 2;

        if s.len() < MIN_LEN {
            return Err(ParseError::Length(s.len()));
        }

        let txid = &s[..TXID_LEN];

        let separator = s.chars().nth(TXID_LEN).unwrap();

        if separator != 'i' {
            return Err(ParseError::Separator(separator));
        }

        let vout = &s[TXID_LEN + 1..];

        Ok(Self {
            txid: txid.parse().map_err(ParseError::Txid)?,
            index: vout.parse().map_err(ParseError::Index)?,
        })
    }
}

impl From<Txid> for InscriptionId {
    fn from(txid: Txid) -> Self {
        Self { txid, index: 0 }
    }
}

#[cfg(test)]
mod tests {

    macro_rules! assert_matches {
    ($expression:expr, $( $pattern:pat_param )|+ $( if $guard:expr )? $(,)?) => {
      match $expression {
        $( $pattern )|+ $( if $guard )? => {}
        left => panic!(
          "assertion failed: (left ~= right)\n  left: `{:?}`\n right: `{}`",
          left,
          stringify!($($pattern)|+ $(if $guard)?)
        ),
      }
    }
  }

    use super::*;

    fn txid(n: u64) -> Txid {
        let hex = format!("{n:x}");

        if hex.is_empty() || hex.len() > 1 {
            panic!();
        }

        hex.repeat(64).parse().unwrap()
    }

    pub(crate) fn inscription_id(n: u32) -> InscriptionId {
        let hex = format!("{n:x}");

        if hex.is_empty() || hex.len() > 1 {
            panic!();
        }

        format!("{}i{n}", hex.repeat(64)).parse().unwrap()
    }

    #[test]
    fn display() {
        assert_eq!(
            inscription_id(1).to_string(),
            "1111111111111111111111111111111111111111111111111111111111111111i1",
        );
        assert_eq!(
            InscriptionId {
                txid: txid(1),
                index: 0,
            }
            .to_string(),
            "1111111111111111111111111111111111111111111111111111111111111111i0",
        );
        assert_eq!(
            InscriptionId {
                txid: txid(1),
                index: 0xFFFFFFFF,
            }
            .to_string(),
            "1111111111111111111111111111111111111111111111111111111111111111i4294967295",
        );
    }

    #[test]
    fn from_str() {
        assert_eq!(
            "1111111111111111111111111111111111111111111111111111111111111111i1"
                .parse::<InscriptionId>()
                .unwrap(),
            inscription_id(1),
        );
        assert_eq!(
            "1111111111111111111111111111111111111111111111111111111111111111i4294967295"
                .parse::<InscriptionId>()
                .unwrap(),
            InscriptionId {
                txid: txid(1),
                index: 0xFFFFFFFF,
            },
        );
        assert_eq!(
            "1111111111111111111111111111111111111111111111111111111111111111i4294967295"
                .parse::<InscriptionId>()
                .unwrap(),
            InscriptionId {
                txid: txid(1),
                index: 0xFFFFFFFF,
            },
        );
    }

    #[test]
    fn from_str_bad_character() {
        assert_matches!(
            "→".parse::<InscriptionId>(),
            Err(ParseError::Character('→')),
        );
    }

    #[test]
    fn from_str_bad_length() {
        assert_matches!("foo".parse::<InscriptionId>(), Err(ParseError::Length(3)));
    }

    #[test]
    fn from_str_bad_separator() {
        assert_matches!(
            "0000000000000000000000000000000000000000000000000000000000000000x0"
                .parse::<InscriptionId>(),
            Err(ParseError::Separator('x')),
        );
    }

    #[test]
    fn from_str_bad_index() {
        assert_matches!(
            "0000000000000000000000000000000000000000000000000000000000000000ifoo"
                .parse::<InscriptionId>(),
            Err(ParseError::Index(_)),
        );
    }

    #[test]
    fn from_str_bad_txid() {
        assert_matches!(
            "x000000000000000000000000000000000000000000000000000000000000000i0"
                .parse::<InscriptionId>(),
            Err(ParseError::Txid(_)),
        );
    }
}
