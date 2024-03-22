use chainhook_sdk::types::{
    OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    OrdinalInscriptionTransferDestination,
};
use chainhook_sdk::utils::Context;
use rusqlite::Transaction;

use super::db::get_unsent_token_transfer_with_sender;
use super::Brc20TransferData;
use super::{
    db::{
        get_token, get_token_available_balance_for_address, get_token_minted_supply, token_exists,
    },
    parser::{amt_has_valid_decimals, ParsedBrc20Operation},
    Brc20BalanceData, Brc20Operation, Brc20TokenDeployData,
};

pub fn verify_brc20_operation(
    operation: &ParsedBrc20Operation,
    reveal: &OrdinalInscriptionRevealData,
    db_tx: &Transaction,
    ctx: &Context,
) -> Result<Brc20Operation, String> {
    let Some(inscriber_address) = reveal.inscriber_address.clone() else {
        return Err(format!("Invalid inscriber address"));
    };
    if inscriber_address.is_empty() {
        return Err(format!("Empty inscriber address"));
    }
    if reveal.inscription_number.classic < 0 {
        return Err(format!("Inscription is cursed"));
    }
    match operation {
        ParsedBrc20Operation::Deploy(data) => {
            if token_exists(&data, db_tx, ctx) {
                return Err(format!("Token {} already exists", &data.tick));
            }
            return Ok(Brc20Operation::TokenDeploy(Brc20TokenDeployData {
                tick: data.tick.clone(),
                max: data.max,
                lim: data.lim,
                dec: data.dec,
                address: inscriber_address,
            }));
        }
        ParsedBrc20Operation::Mint(data) => {
            let Some(token) = get_token(&data.tick, db_tx, ctx) else {
                return Err(format!(
                    "Token {} does not exist on mint attempt",
                    &data.tick
                ));
            };
            if data.float_amt() > token.lim {
                return Err(format!(
                    "Cannot mint more than {} tokens for {}, attempted to mint {}",
                    token.lim, token.tick, data.amt
                ));
            }
            if !amt_has_valid_decimals(&data.amt, token.dec) {
                return Err(format!(
                    "Invalid decimals in amt field for {} mint, attempting to mint {}",
                    token.tick, data.amt
                ));
            }
            let remaining_supply = token.max - get_token_minted_supply(&data.tick, db_tx, ctx);
            if remaining_supply == 0.0 {
                return Err(format!(
                    "No supply available for {} mint, attempted to mint {}, remaining {}",
                    token.tick, data.amt, remaining_supply
                ));
            }
            let real_mint_amt = data.float_amt().min(token.lim.min(remaining_supply));
            return Ok(Brc20Operation::TokenMint(Brc20BalanceData {
                tick: token.tick,
                amt: real_mint_amt,
                address: inscriber_address,
            }));
        }
        ParsedBrc20Operation::Transfer(data) => {
            let Some(token) = get_token(&data.tick, db_tx, ctx) else {
                return Err(format!(
                    "Token {} does not exist on transfer attempt",
                    &data.tick
                ));
            };
            if !amt_has_valid_decimals(&data.amt, token.dec) {
                return Err(format!(
                    "Invalid decimals in amt field for {} transfer, attempting to transfer {}",
                    token.tick, data.amt
                ));
            }
            let avail_balance = get_token_available_balance_for_address(
                &token.tick,
                &inscriber_address,
                db_tx,
                ctx,
            );
            if avail_balance < data.float_amt() {
                return Err(format!("Insufficient balance for {} transfer, attempting to transfer {}, only {} available", token.tick, data.amt, avail_balance));
            }
            return Ok(Brc20Operation::TokenTransfer(Brc20BalanceData {
                tick: token.tick,
                amt: data.float_amt(),
                address: inscriber_address,
            }));
        }
    };
}

pub fn verify_brc20_transfer(
    transfer: &OrdinalInscriptionTransferData,
    db_tx: &Transaction,
    ctx: &Context,
) -> Result<Brc20TransferData, String> {
    let Some((balance_delta, sender_address)) =
        get_unsent_token_transfer_with_sender(transfer.ordinal_number, db_tx, ctx)
    else {
        return Err(format!(
            "No BRC-20 transfer in ordinal {} or transfer already sent",
            transfer.ordinal_number
        ));
    };
    match &transfer.destination {
        OrdinalInscriptionTransferDestination::Transferred(receiver_address) => {
            return Ok(Brc20TransferData {
                tick: balance_delta.tick.clone(),
                amt: balance_delta.float_amt(),
                sender_address,
                receiver_address: receiver_address.to_string(),
            });
        }
        OrdinalInscriptionTransferDestination::SpentInFees => {
            return Ok(Brc20TransferData {
                tick: balance_delta.tick.clone(),
                amt: balance_delta.float_amt(),
                sender_address: sender_address.clone(),
                receiver_address: sender_address, // Return to sender
            });
        }
        OrdinalInscriptionTransferDestination::Burnt(_) => {
            return Ok(Brc20TransferData {
                tick: balance_delta.tick.clone(),
                amt: balance_delta.float_amt(),
                sender_address,
                receiver_address: "".to_string(),
            });
        }
    };
}

#[cfg(test)]
mod test {
    use chainhook_sdk::{
        types::{BlockIdentifier, OrdinalInscriptionNumber, OrdinalInscriptionRevealData},
        utils::Context,
    };
    use test_case::test_case;

    use crate::core::meta_protocols::brc20::{
        db::{initialize_brc20_db, insert_token, insert_token_mint},
        parser::{ParsedBrc20BalanceData, ParsedBrc20Operation, ParsedBrc20TokenDeployData},
        Brc20BalanceData, Brc20Operation, Brc20TokenDeployData,
    };

    use super::verify_brc20_operation;

    struct Brc20RevealBuilder {
        inscription_number: OrdinalInscriptionNumber,
        inscriber_address: Option<String>,
    }

    impl Brc20RevealBuilder {
        fn new() -> Self {
            Brc20RevealBuilder {
                inscription_number: OrdinalInscriptionNumber {
                    classic: 0,
                    jubilee: 0,
                },
                inscriber_address: Some("324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()),
            }
        }

        fn inscription_number(mut self, val: i64) -> Self {
            self.inscription_number = OrdinalInscriptionNumber {
                classic: val,
                jubilee: 0,
            };
            self
        }

        fn inscriber_address(mut self, val: Option<String>) -> Self {
            self.inscriber_address = val;
            self
        }

        fn build(self) -> OrdinalInscriptionRevealData {
            OrdinalInscriptionRevealData {
                content_bytes: "".to_string(),
                content_type: "text/plain".to_string(),
                content_length: 10,
                inscription_number: self.inscription_number,
                inscription_fee: 100,
                inscription_output_value: 10000,
                inscription_id:
                    "9bb2314d666ae0b1db8161cb373fcc1381681f71445c4e0335aa80ea9c37fcddi0".to_string(),
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
                satpoint_post_inscription:
                    "9bb2314d666ae0b1db8161cb373fcc1381681f71445c4e0335aa80ea9c37fcdd:0:0"
                        .to_string(),
                curse_type: None,
            }
        }
    }

    fn get_test_ctx() -> Context {
        let logger = hiro_system_kit::log::setup_logger();
        let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
        Context {
            logger: Some(logger),
            tracer: false,
        }
    }

    #[test_case(
        ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            max: 21000000.0,
            lim: 1000.0,
            dec: 18,
        }),
        Brc20RevealBuilder::new().inscriber_address(None).build()
        => Err("Invalid inscriber address".to_string()); "with invalid address"
    )]
    #[test_case(
        ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            max: 21000000.0,
            lim: 1000.0,
            dec: 18,
        }),
        Brc20RevealBuilder::new().inscriber_address(Some("".to_string())).build()
        => Err("Empty inscriber address".to_string()); "with empty address"
    )]
    #[test_case(
        ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            max: 21000000.0,
            lim: 1000.0,
            dec: 18,
        }),
        Brc20RevealBuilder::new().inscription_number(-1).build()
        => Err("Inscription is cursed".to_string()); "with cursed inscription"
    )]
    #[test_case(
        ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            max: 21000000.0,
            lim: 1000.0,
            dec: 18,
        }),
        Brc20RevealBuilder::new().build()
        => Ok(
            Brc20Operation::TokenDeploy(Brc20TokenDeployData {
                tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            })
        ); "with deploy"
    )]
    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Token pepe does not exist on mint attempt".to_string()); "with mint non existing token"
    )]
    #[test_case(
        ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Token pepe does not exist on transfer attempt".to_string()); "with transfer non existing token"
    )]
    fn test_brc20_verify_for_empty_db(
        op: ParsedBrc20Operation,
        reveal: OrdinalInscriptionRevealData,
    ) -> Result<Brc20Operation, String> {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        verify_brc20_operation(&op, &reveal, &tx, &ctx)
    }

    #[test_case(
        ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            max: 21000000.0,
            lim: 1000.0,
            dec: 18,
        }),
        Brc20RevealBuilder::new().build()
        => Err("Token pepe already exists".to_string()); "with deploy existing token"
    )]
    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Ok(Brc20Operation::TokenMint(Brc20BalanceData {
            tick: "pepe".to_string(),
            amt: 1000.0,
            address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()
        })); "with mint"
    )]
    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "10000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Cannot mint more than 1000 tokens for pepe, attempted to mint 10000.0".to_string()); "with mint over lim"
    )]
    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "100.000000000000000000000".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Invalid decimals in amt field for pepe mint, attempting to mint 100.000000000000000000000".to_string()); "with mint invalid decimals"
    )]
    #[test_case(
        ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "100.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Insufficient balance for pepe transfer, attempting to transfer 100.0, only 0 available".to_string()); "with transfer on zero balance"
    )]
    #[test_case(
        ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "100.000000000000000000000".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("Invalid decimals in amt field for pepe transfer, attempting to transfer 100.000000000000000000000".to_string()); "with transfer invalid decimals"
    )]
    fn test_brc20_verify_for_existing_token(
        op: ParsedBrc20Operation,
        reveal: OrdinalInscriptionRevealData,
    ) -> Result<Brc20Operation, String> {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        insert_token(
            &Brc20TokenDeployData {
                tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &reveal,
            &BlockIdentifier {
                index: 835727,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            &tx,
            &ctx,
        );
        verify_brc20_operation(&op, &reveal, &tx, &ctx)
    }

    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Err("No supply available for pepe mint, attempted to mint 1000.0, remaining 0".to_string()); "with mint on no more supply"
    )]
    fn test_brc20_verify_for_minted_out_token(
        op: ParsedBrc20Operation,
        reveal: OrdinalInscriptionRevealData,
    ) -> Result<Brc20Operation, String> {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        insert_token(
            &Brc20TokenDeployData {
                tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &reveal,
            &BlockIdentifier {
                index: 835727,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            &tx,
            &ctx,
        );
        insert_token_mint(
            &Brc20BalanceData {
                tick: "pepe".to_string(),
                amt: 21000000.0, // For testing
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &reveal,
            &BlockIdentifier {
                index: 835727,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            &tx,
            &ctx,
        );
        verify_brc20_operation(&op, &reveal, &tx, &ctx)
    }

    #[test_case(
        ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000.0".to_string(),
        }),
        Brc20RevealBuilder::new().build()
        => Ok(Brc20Operation::TokenMint(Brc20BalanceData {
            tick: "pepe".to_string(),
            amt: 500.0,
            address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()
        })); "with mint on low supply"
    )]
    fn test_brc20_verify_for_almost_minted_out_token(
        op: ParsedBrc20Operation,
        reveal: OrdinalInscriptionRevealData,
    ) -> Result<Brc20Operation, String> {
        let ctx = get_test_ctx();
        let mut conn = initialize_brc20_db(None, &ctx);
        let tx = conn.transaction().unwrap();
        insert_token(
            &Brc20TokenDeployData {
                tick: "pepe".to_string(),
                max: 21000000.0,
                lim: 1000.0,
                dec: 18,
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &reveal,
            &BlockIdentifier {
                index: 835727,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            &tx,
            &ctx,
        );
        insert_token_mint(
            &Brc20BalanceData {
                tick: "pepe".to_string(),
                amt: 21000000.0 - 500.0, // For testing
                address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
            },
            &reveal,
            &BlockIdentifier {
                index: 835727,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            },
            &tx,
            &ctx,
        );
        verify_brc20_operation(&op, &reveal, &tx, &ctx)
    }
}
