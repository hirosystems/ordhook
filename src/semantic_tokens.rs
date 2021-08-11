
use lsp_types::{
    Range,
    SemanticToken,
    SemanticTokenModifier,
    SemanticTokenType,
    SemanticTokens,
    SemanticTokensEdit,
    SemanticTokensParams,
    SemanticTokensRangeResult
};

use String;

use std::vec;

use std::fs::read_to_string;

use std::io::ErrorKind;

/*
use chrono::{

    Local,
    Utc

}; 
 */

use chrono::prelude::*;

pub fn get_supported_token_types() -> Vec<SemanticTokenType>
{

    let mut token_types = vec!();

    token_types.push(SemanticTokenType::TYPE);

    token_types.push(SemanticTokenType::FUNCTION);

    token_types.push(SemanticTokenType::KEYWORD);
    
    /*token_types.push(SemanticTokenType::new("block-height"));

    token_types.push(SemanticTokenType::new("burn-block-height"));

    token_types.push(SemanticTokenType::new("contract-caller"));

    token_types.push(SemanticTokenType::new("false"));

    token_types.push(SemanticTokenType::new("is-in-regtest"));

    token_types.push(SemanticTokenType::new("none"));

    token_types.push(SemanticTokenType::new("stx-liquid-supply"));

    token_types.push(SemanticTokenType::new("true"));

    token_types.push(SemanticTokenType::new("tx-sender"));*/

    token_types

}

pub fn get_supported_token_modifiers() -> Vec<SemanticTokenModifier>
{

    vec![]

}

pub fn try_get_SemanticTokenType(input: &str) -> Option<SemanticTokenType>
{

    match input
    {

        //Types

        "tuple" |
        "list" |
        "response" |
        "optional" |
        "buff" |
        "string-ascii" |
        "string-utf8" |
        "principal" |
        "bool" |
        "int" |
        "uint" => {

            return Some(SemanticTokenType::TYPE);

        },

        //Functions

        "-" | // (subtract)
        "*" | //(multiply)
        "/" |//(divide)
        "+" | //(add)
        "<" | //(less than)
        "<=" | //(less than or equal)
        ">" | //(greater than)
        ">=" | //(greater than or equal)
        "and" |
        "append" |
        "as-contract" |
        "as-max-len?" |
        "asserts!" |
        "at-block" |
        "begin" |
        "concat" |
        "contract-call?" |
        "contract-of" |
        "default-to" |
        "define-constant" |
        "define-data-var" |
        "define-fungible-token" |
        "define-map" |
        "define-non-fungible-token" |
        "define-private" |
        "define-public" |
        "define-read-only" |
        "define-trait" |
        "element-at" |
        "err" |
        "filter" |
        "fold" |
        "ft-burn?" |
        "ft-get-balance" |
        "ft-get-supply" |
        "ft-mint?" |
        "ft-transfer?" |
        "get" |
        "get-block-info?" |
        "hash160" |
        "if" |
        "impl-trait" |
        "index-of" |
        "is-eq" |
        "is-err" |
        "is-none" |
        "is-ok" |
        "is-some" |
        "keccak256" |
        "len" |
        "let" |
        "list" |
        "log2" |
        "map" |
        "map-delete" |
        "map-get?" |
        "map-insert" |
        "map-set" |
        "match" |
        "merge" |
        "mod" |
        "nft-burn?" |
        "nft-get-owner?" |
        "nft-mint?" |
        "nft-transfer?" |
        "not" |
        "ok" |
        "or" |
        "pow" |
        "principal-of?" |
        "print" |
        "secp256k1-recover?" |
        "secp256k1-verify" |
        "sha256" |
        "sha512" |
        "sha512/256" |
        "some" |
        "sqrti" |
        "stx-burn?" |
        "stx-get-balance" |
        "stx-transfer?" |
        "to-int" |
        "to-uint" |
        "try!" |
        "tuple" |
        "unwrap-err-panic" |
        "unwrap-err!" |
        "unwrap-panic" |
        "unwrap!" |
        "use-trait" |
        "var-get" |
        "var-set" |
        "xor" => {

            return Some(SemanticTokenType::FUNCTION);

        },

        //Keywords

        "block-height" |
        "burn-block-height" |
        "contract-caller" |
        "false" |
        "is-in-regtest" |
        "none" |
        "stx-liquid-supply" |
        "true" |
        "tx-sender" =>
        {

            return Some(SemanticTokenType::KEYWORD);

        },
        _ => {

            None

        }

    }

}

pub struct SemanticTokensBuilder
{

    id: String,
    previous_line: u32,
    previous_char: u32,
    data: Vec<SemanticToken>

}

impl SemanticTokensBuilder
{

    pub fn new(id: String) -> Self
    {

        SemanticTokensBuilder{ id, previous_line: 0, previous_char: 0, data: Default::default() }

    }
     
    pub fn new_utc_date_id() -> Self
    {

        SemanticTokensBuilder{ id: Utc::now().to_string(), previous_line: 0, previous_char: 0, data: Default::default() }

    }

    pub fn new_local_date_id() -> Self
    {

        SemanticTokensBuilder{ id: Local::now().to_string(), previous_line: 0, previous_char: 0, data: Default::default() }

    }

    pub fn push(&mut self, line: u32, char: u32, length: u32, token_type: u32, token_modifiers: u32)
    {

        let mut pushLine = line;

        let mut pushChar = char;

        if self.data.len() > 0
        {

            pushLine -= self.previous_line;

            if pushLine == 0
            {

                pushChar -= self.previous_line;

            }

        }

        let sm_token = SemanticToken{
            
            delta_line: pushLine,
            delta_start: pushChar,
            length: length,
            token_type: token_type,
            token_modifiers_bitset: token_modifiers

        };

        self.data.push(sm_token);

        self.previous_line = line;

        self.previous_char = char;

    }

    pub fn push_strs(&mut self, line: u32, char: u32, length: u32, token_type: &SemanticTokenType, token_modifiers: Option<Vec<&str>>, supported_token_types: &Vec<SemanticTokenType>)
    {

        let mut index: u32 = 0;

        //let mut found_index: i32 = -1;

        let mut found_index: u32 = 0;

        let mut has_been_found = false;

        for supported_token_type in supported_token_types.iter()
        {

            index += 1;

            if token_type == supported_token_type
            {

                found_index = index;

                has_been_found = true;

            }

        }

        if !has_been_found //found_index == -1
        {

            return;

        }
        
        self.push(line, char, length, found_index, 0);

    }

    pub fn push_range(&mut self, range: Range, token_index: u32, modifier_bitset: u32)
    {

        let mut pushLine = range.start.line;

        let mut pushChar= range.start.character;

        if self.data.len() > 0
        {

            pushLine -= self.previous_line;

            if pushLine == 0
            {

                pushChar -= self.previous_line;

            }

        }

        //there are no multiline tokens

        let token_len = range.end.character - range.start.character;

        let sm_token = SemanticToken{
            
            delta_line: pushLine,
            delta_start: pushChar,
            length: token_len,
            token_type: token_index,
            token_modifiers_bitset: modifier_bitset

        };

        self.data.push(sm_token);

        self.previous_line = range.start.line;

        self.previous_char = range.start.character;

    }

    pub fn build(self) -> SemanticTokens
    {

        SemanticTokens 
        {

            result_id: Some(self.id),
            data: self.data

        }

    }

}

/*
pub struct ParsedToken
{

    line: u32,
    startCharacter: u32,
    length: u32,
    tokenType: String,
    tokenModifiers: Vec<String>

}
*/

pub fn parse_text(text: &String) -> Vec::<String>
{

    let mut parsedTokens = Vec::<String>::new();

    //let lines = Vec::<String>::new();

    //Tokenisation

    let mut currentToken = String::new();

    for line in text.lines()
    {

        /* 
        if line.is_empty()
        {

            continue;

        }
        */
        
        let mut is_string = false;

        //let mut is_start_of = false;

        for currentChar in line.chars()
        {

            if currentChar.is_whitespace() && !currentToken.is_empty() && !is_string
            {

                completeToken(&mut parsedTokens, &mut currentToken);

                continue;

            }

            match currentChar 
            {

                '(' | ')' => {

                    if !is_string
                    {

                        continue;

                    }

                }
                '\"' => {

                    is_string = !is_string;

                }
                ':' => {

                    if !is_string && !currentToken.is_empty()
                    {

                        completeToken(&mut parsedTokens, &mut currentToken);

                    }

                    currentToken.push(currentChar);

                    completeToken(&mut parsedTokens, &mut currentToken);

                    continue;

                }

                //operator detection

                '+' => {

                    if !is_string && !currentToken.is_empty()
                    {

                        completeToken(&mut parsedTokens, &mut currentToken);

                    }

                }
                //'-' => {
                //}
                '*' => {

                    if !is_string && !currentToken.is_empty()
                    {

                        completeToken(&mut parsedTokens, &mut currentToken);

                    }

                }
                '/' => {

                    if !is_string && !currentToken.is_empty()
                    {

                        completeToken(&mut parsedTokens, &mut currentToken);

                    }

                }
                /*'>' => {
                }
                '<' => {
                }*/
                '=' => {

                    if !currentToken.is_empty()
                    {
                        
                        if !is_string //&& 
                        {

                            completeToken(&mut parsedTokens, &mut currentToken);

                        }

                    }

                }
                _ => {

                }

            }

            currentToken.push(currentChar);

            //decide if the token should be cloned into paredtokens

        }

        if !currentToken.is_empty()
        {

            completeToken(&mut parsedTokens, &mut currentToken);

            continue;
            
        }

    }

    parsedTokens

}

fn completeToken(parsedTokens: &mut Vec::<String>, currentToken:  &mut String)
{

    parsedTokens.push(currentToken.clone());

    currentToken.clear();

}

pub fn semantic_tokens_full(params: &SemanticTokensParams) -> tower_lsp::jsonrpc::Result<Option<SemanticTokensRangeResult>>
{

    /*
    match read_to_string( params.url.into())
    {

        Ok()

    }  
    */

    //let file_string;
    
    match read_to_string( params.text_document.uri.as_str())
    {

        Ok(res) => {

            let tokens = parse_text(&res);



        }
        Err(err) => {

            let mut err_result = tower_lsp::jsonrpc::Error::internal_error();

            let mut error_kind = "";

            match err.kind() {
                ErrorKind::NotFound => error_kind = "NotFound",
                ErrorKind::PermissionDenied => error_kind = "PermissionDenied",
                ErrorKind::ConnectionRefused => error_kind = "ConnectionRefused",
                ErrorKind::ConnectionReset => error_kind = "ConnectionReset",
                ErrorKind::ConnectionAborted => error_kind = "ConnectionAborted",
                ErrorKind::NotConnected => error_kind = "NotConnected",
                ErrorKind::AddrInUse => error_kind = "AddrInUse",
                ErrorKind::AddrNotAvailable => error_kind = "AddrNotAvailable",
                ErrorKind::BrokenPipe => error_kind = "BrokenPipe",
                ErrorKind::AlreadyExists => error_kind = "AlreadyExists",
                ErrorKind::WouldBlock => error_kind = "WouldBlock",
                ErrorKind::InvalidInput => error_kind = "InvalidInput",
                ErrorKind::InvalidData => error_kind = "InvalidData",
                ErrorKind::TimedOut => error_kind = "TimedOut",
                ErrorKind::WriteZero => error_kind = "WriteZero",
                ErrorKind::Interrupted => error_kind = "Interrupted",
                ErrorKind::Other => error_kind = "Other",
                ErrorKind::UnexpectedEof => error_kind = "UnexpectedEof",
                _ => {}
            }

            //err_result.data.

            err_result.message = error_kind.to_string();

            return Err(err_result);

        }

    }



    tower_lsp::jsonrpc::Result::Ok(None)

}