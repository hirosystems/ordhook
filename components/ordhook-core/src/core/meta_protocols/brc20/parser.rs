use chainhook_sdk::types::OrdinalInscriptionRevealData;
use regex::Regex;

use crate::ord::inscription::Inscription;
use crate::ord::media::{Language, Media};

pub struct Brc20TokenDeployData {
    tick: String,
    max: f64,
    lim: f64,
    dec: u64,
}

pub struct Brc20BalanceData {
    tick: String,
    amt: f64,
}

pub enum Brc20Operation {
    Deploy(Brc20TokenDeployData),
    Mint(Brc20BalanceData),
    Transfer(Brc20BalanceData),
}

#[derive(Deserialize)]
struct Brc20DeployJson {
    p: String,
    op: String,
    tick: String,
    max: String,
    lim: Option<String>,
    dec: Option<String>
}

#[derive(Deserialize)]
struct Brc20MintOrTransferJson {
    p: String,
    op: String,
    tick: String,
    amt: String,
}

lazy_static! {
    pub static ref NUMERIC_FLOAT_REGEX: Regex =
        Regex::new(r#"^(([0-9]+)|([0-9]*\.?[0-9]+))$"#.into()).unwrap();
    pub static ref NUMERIC_INT_REGEX: Regex =
        Regex::new(r#"^([0-9]+)$"#.into()).unwrap();
}

fn parse_float_numeric_value(n: String, max_decimals: u64) -> Option<f64> {
    if NUMERIC_FLOAT_REGEX.is_match(&n) {
        if n.contains('.') {
            if n.split('.').nth(1).map_or(0, |s| s.chars().count()) as u64 > max_decimals {
                return None
            }
        } else if max_decimals > 0 {
            return None
        }
        match n.parse::<f64>() {
            Ok(parsed) => {
                if parsed > u64::MAX as f64 {
                    return None;
                }
                return Some(parsed);
            },
            _ => return None
        };
    }
    None
}

fn parse_int_numeric_value(n: String) -> Option<u64> {
    if NUMERIC_INT_REGEX.is_match(&n) {
        match n.parse::<u64>() {
            Ok(parsed) => {
                if parsed > u64::MAX {
                    return None;
                }
                return Some(parsed);
            },
            _ => return None
        };
    }
    None
}

/// Attempts to parse an `Inscription` into a BRC20 operation by following the rules explained in
/// https://layer1.gitbook.io/layer1-foundation/protocols/brc-20/indexing
/// TODO: Move to return `Result` only instead of `Option`
pub fn parse_brc20_operation(
    inscription: Inscription,
    reveal: OrdinalInscriptionRevealData
) -> Result<Option<Brc20Operation>, String> {
    match inscription.media() {
        Media::Code(Language::Json) | Media::Text => {},
        _ => return Ok(None)
    };
    if reveal.inscriber_address.is_none() || reveal.inscription_number.classic < 0 {
        return Ok(None);
    }
    let Some(inscription_body) = inscription.body() else {
        return Ok(None);
    };
    match serde_json::from_slice::<Brc20DeployJson>(inscription_body) {
        Ok(json) => {
            if json.p != "brc-20" || json.op != "deploy" || json.tick.len() != 4 {
                return Ok(None)
            }
            let mut deploy = Brc20TokenDeployData {
                tick: json.tick,
                max: 0.0,
                lim: 0.0,
                dec: 18,
            };
            if let Some(dec) = json.dec {
                if let Some(parsed_dec) = parse_int_numeric_value(dec) {
                    if parsed_dec > 18 {
                        return Ok(None);
                    }
                    deploy.dec = parsed_dec;
                }
            }
            if let Some(parsed_max) = parse_float_numeric_value(json.max, deploy.dec) {
                if parsed_max == 0.0 {
                    return Ok(None);
                }
                deploy.max = parsed_max;
            }
            if let Some(lim) = json.lim {
                if let Some(parsed_lim) = parse_float_numeric_value(lim, deploy.dec) {
                    if parsed_lim == 0.0 {
                        return Ok(None);
                    }
                    deploy.lim = parsed_lim;
                }
            } else {
                deploy.lim = deploy.max;
            }
            return Ok(Some(Brc20Operation::Deploy(deploy)));
        },
        Err(_) => match serde_json::from_slice::<Brc20MintOrTransferJson>(inscription_body) {
            Ok(json) => {
                if json.p != "brc-20" || json.tick.len() != 4 {
                    return Ok(None)
                }
                let op_str = json.op.as_str();
                match op_str {
                    "mint" | "transfer" => {
                        // TODO: Get the token from DB to check actual deployed `dec` value for decimal validation.
                        if let Some(parsed_amt) = parse_float_numeric_value(json.amt, 18) {
                            if parsed_amt == 0.0 {
                                return Ok(None);
                            }
                            match op_str {
                                "mint" => {
                                    return Ok(Some(Brc20Operation::Mint(Brc20BalanceData {
                                        tick: json.tick,
                                        amt: parsed_amt,
                                    })));
                                },
                                "transfer" => {
                                    return Ok(Some(Brc20Operation::Transfer(Brc20BalanceData {
                                        tick: json.tick,
                                        amt: parsed_amt,
                                    })));
                                },
                                _ => return Ok(None)
                            }
                        }
                    },
                    _ => return Ok(None)
                }
            },
            Err(_) => return Ok(None)
        }
    };
    return Ok(None);
}