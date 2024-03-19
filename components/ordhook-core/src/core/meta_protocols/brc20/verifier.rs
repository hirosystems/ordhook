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
    if reveal.inscription_number.classic < 0 {
        return Err(format!("Inscription is cursed"));
    }
    match operation {
        ParsedBrc20Operation::Deploy(data) => {
            if token_exists(&data, db_tx, ctx) {
                return Err(format!("Token already exists {}", &data.tick));
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
            let real_mint_amt = data
                .float_amt()
                .max(token.max - get_token_minted_supply(&data.tick, db_tx, ctx));
            if real_mint_amt == 0.0 {
                return Err(format!(
                    "No supply available for mint {}, attempted to mint {}",
                    token.tick, data.amt
                ));
            }
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
        OrdinalInscriptionTransferDestination::Burnt(dest) => {
            // TODO: What is "dest" in this case?
            return Ok(Brc20TransferData {
                tick: balance_delta.tick.clone(),
                amt: balance_delta.float_amt(),
                sender_address,
                receiver_address: dest.to_string(),
            });
        }
    };
}
