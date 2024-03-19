use regex::Regex;

use crate::ord::inscription::Inscription;
use crate::ord::media::{Language, Media};

#[derive(PartialEq, Debug, Clone)]
pub struct Brc20TokenDeployData {
    pub tick: String,
    pub max: f64,
    pub lim: f64,
    pub dec: u64,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Brc20BalanceData {
    pub tick: String,
    // alksdjalskd
    pub amt: String,
}

impl Brc20BalanceData {
    pub fn float_amt(&self) -> f64 {
        self.amt.parse::<f64>().unwrap()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParsedBrc20Operation {
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
    dec: Option<String>,
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
    pub static ref NUMERIC_INT_REGEX: Regex = Regex::new(r#"^([0-9]+)$"#.into()).unwrap();
}

pub fn amt_has_valid_decimals(amt: &str, max_decimals: u64) -> bool {
    if amt.contains('.') {
        if amt.split('.').nth(1).map_or(0, |s| s.chars().count()) as u64 > max_decimals {
            return false;
        }
    } else if max_decimals > 0 {
        return false;
    }
    true
}

fn parse_float_numeric_value(n: &str, max_decimals: u64) -> Option<f64> {
    if NUMERIC_FLOAT_REGEX.is_match(&n) {
        if !amt_has_valid_decimals(n, max_decimals) {
            return None;
        }
        match n.parse::<f64>() {
            Ok(parsed) => {
                if parsed > u64::MAX as f64 {
                    return None;
                }
                return Some(parsed);
            }
            _ => return None,
        };
    }
    None
}

fn parse_int_numeric_value(n: &str) -> Option<u64> {
    if NUMERIC_INT_REGEX.is_match(&n) {
        match n.parse::<u64>() {
            Ok(parsed) => {
                if parsed > u64::MAX {
                    return None;
                }
                return Some(parsed);
            }
            _ => return None,
        };
    }
    None
}

/// Attempts to parse an `Inscription` into a BRC20 operation by following the rules explained in
/// https://layer1.gitbook.io/layer1-foundation/protocols/brc-20/indexing
pub fn parse_brc20_operation(
    inscription: &Inscription,
) -> Result<Option<ParsedBrc20Operation>, String> {
    match inscription.media() {
        Media::Code(Language::Json) | Media::Text => {}
        _ => return Ok(None),
    };
    let Some(inscription_body) = inscription.body() else {
        return Ok(None);
    };
    match serde_json::from_slice::<Brc20DeployJson>(inscription_body) {
        Ok(json) => {
            if json.p != "brc-20" || json.op != "deploy" || json.tick.len() != 4 {
                return Ok(None);
            }
            let mut deploy = Brc20TokenDeployData {
                tick: json.tick.to_lowercase(),
                max: 0.0,
                lim: 0.0,
                dec: 18,
            };
            if let Some(dec) = json.dec {
                if let Some(parsed_dec) = parse_int_numeric_value(&dec) {
                    if parsed_dec > 18 {
                        return Ok(None);
                    }
                    deploy.dec = parsed_dec;
                }
            }
            if let Some(parsed_max) = parse_float_numeric_value(&json.max, deploy.dec) {
                if parsed_max == 0.0 {
                    return Ok(None);
                }
                deploy.max = parsed_max;
            }
            if let Some(lim) = json.lim {
                if let Some(parsed_lim) = parse_float_numeric_value(&lim, deploy.dec) {
                    if parsed_lim == 0.0 {
                        return Ok(None);
                    }
                    deploy.lim = parsed_lim;
                }
            } else {
                deploy.lim = deploy.max;
            }
            return Ok(Some(ParsedBrc20Operation::Deploy(deploy)));
        }
        Err(_) => match serde_json::from_slice::<Brc20MintOrTransferJson>(inscription_body) {
            Ok(json) => {
                if json.p != "brc-20" || json.tick.len() != 4 {
                    return Ok(None);
                }
                let op_str = json.op.as_str();
                match op_str {
                    "mint" | "transfer" => {
                        // TODO: Get the token from DB to check actual deployed `dec` value for decimal validation.
                        if let Some(parsed_amt) = parse_float_numeric_value(&json.amt, 18) {
                            if parsed_amt == 0.0 {
                                return Ok(None);
                            }
                            match op_str {
                                "mint" => {
                                    return Ok(Some(ParsedBrc20Operation::Mint(
                                        Brc20BalanceData {
                                            tick: json.tick.clone(),
                                            amt: json.amt.clone(),
                                        },
                                    )));
                                }
                                "transfer" => {
                                    return Ok(Some(ParsedBrc20Operation::Transfer(
                                        Brc20BalanceData {
                                            tick: json.tick.clone(),
                                            amt: json.amt,
                                        },
                                    )));
                                }
                                _ => return Ok(None),
                            }
                        }
                    }
                    _ => return Ok(None),
                }
            }
            Err(_) => return Ok(None),
        },
    };
    return Ok(None);
}

#[cfg(test)]
mod test {
    use super::{parse_brc20_operation, ParsedBrc20Operation};
    use crate::ord::inscription::Inscription;
    use chainhook_sdk::types::{OrdinalInscriptionNumber, OrdinalInscriptionRevealData};
    use serde::Serialize;
    use serde_json::json;
    use test_case::test_case;

    struct Brc20InscriptionBuilder {
        body: Option<Vec<u8>>,
        content_encoding: Option<Vec<u8>>,
        content_type: Option<Vec<u8>>,
    }

    impl Brc20InscriptionBuilder {
        fn new() -> Self {
            Brc20InscriptionBuilder {
                body: Some(json!({"p":"brc-20"}).to_string().as_bytes().to_vec()),
                content_encoding: Some("utf-8".as_bytes().to_vec()),
                content_type: Some("text/html".as_bytes().to_vec()),
            }
        }

        fn content_type(mut self, val: &str) -> Self {
            self.content_type = Some(val.as_bytes().to_vec());
            self
        }

        fn build(self) -> Inscription {
            Inscription {
                body: self.body,
                content_encoding: self.content_encoding,
                content_type: self.content_type,
                duplicate_field: false,
                incomplete_field: false,
                metadata: None,
                metaprotocol: None,
                parent: None,
                pointer: None,
                unrecognized_even_field: false,
                delegate: None,
            }
        }
    }

    struct Brc20RevealBuilder {
        content_bytes: String,
        content_type: String,
        inscription_number: OrdinalInscriptionNumber,
        inscriber_address: Option<String>,
    }

    impl Brc20RevealBuilder {
        fn new() -> Self {
            Brc20RevealBuilder {
                content_bytes: "".to_string(),
                content_type: "text/plain".to_string(),
                inscription_number: OrdinalInscriptionNumber {
                    classic: 0,
                    jubilee: 0,
                },
                inscriber_address: Some("".to_string()),
            }
        }

        fn content_type(mut self, val: &str) -> Self {
            self.content_type = val.to_string();
            self
        }

        fn inscription_number(mut self, val: i64) -> Self {
            self.inscription_number = OrdinalInscriptionNumber {
                classic: val,
                jubilee: 0,
            };
            self
        }

        fn build(self) -> OrdinalInscriptionRevealData {
            OrdinalInscriptionRevealData {
                content_bytes: self.content_bytes,
                content_type: self.content_type,
                content_length: 10,
                inscription_number: self.inscription_number,
                inscription_fee: 100,
                inscription_output_value: 10000,
                inscription_id: "".to_string(),
                inscription_input_index: 0,
                inscription_pointer: None,
                inscriber_address: self.inscriber_address,
                delegate: None,
                metaprotocol: None,
                metadata: None,
                parent: None,
                ordinal_number: 0,
                ordinal_block_height: 767430,
                ordinal_offset: 0,
                tx_index: 0,
                transfers_pre_inscription: 0,
                satpoint_post_inscription: "".to_string(),
                curse_type: None,
            }
        }
    }

    #[test_case(
        Brc20InscriptionBuilder::new().content_type("text/html").build()
        => is equal_to Ok(None); "with invalid content_type"
    )]
    #[test_case(
        Brc20InscriptionBuilder::new().build()
        => is equal_to Ok(None); "with invalid inscription number"
    )]
    fn test_brc20_parse(inscription: Inscription) -> Result<Option<ParsedBrc20Operation>, String> {
        parse_brc20_operation(&inscription)
    }
}
