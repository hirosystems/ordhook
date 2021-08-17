
use lsp_types::{Range, SemanticToken, SemanticTokenModifier, SemanticTokenType, SemanticTokens, SemanticTokensDeltaParams, SemanticTokensEdit, SemanticTokensFullDeltaResult, SemanticTokensParams, SemanticTokensRangeParams, SemanticTokensRangeResult, SemanticTokensResult, Url};

use String;

use std::path::PathBuf;
use std::vec;

use std::fs::read_to_string;

use std::io::ErrorKind;

use chrono::prelude::*;

pub fn get_supported_token_types() -> Vec<SemanticTokenType>
{

    let mut token_types = vec!();

    token_types.push(SemanticTokenType::TYPE);

    token_types.push(SemanticTokenType::FUNCTION);

    token_types.push(SemanticTokenType::KEYWORD);

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
        "list" => { //warning: unreachable pattern - Also a type...

            return Some(SemanticTokenType::FUNCTION);

        }, 
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
        "tuple" => { //warning: unreachable pattern  - Also a type...

            return Some(SemanticTokenType::FUNCTION);

        }, 
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

//builds the semantic tokens

//partially inspired from https://github.com/rust-analyzer/rust-analyzer/blob/master/crates/rust-analyzer/src/semantic_tokens.rs
//and https://github.com/microsoft/vscode-languageserver-node/blob/f425af9de46a0187adb78ec8a46b9b2ce80c5412/server/src/sematicTokens.proposed.ts#L45

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

    pub fn push(&mut self, line: u32, char_index: u32, length: u32, token_type: u32, token_modifiers: u32)
    {

        let mut pushLine = line;

        let mut pushChar = char_index;

        if self.data.len() > 0
        {

            pushLine -= self.previous_line;

            if pushLine == 0
            {

                pushChar -= self.previous_char;

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

        self.previous_char = char_index;

    }

    pub fn push_strs(&mut self, line: u32, char_index: u32, length: u32, token_type: &SemanticTokenType, token_modifiers: Option<Vec<&str>>, supported_token_types: &Vec<SemanticTokenType>)
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
        
        self.push(line, char_index, length, found_index, 0);

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

                pushChar -= self.previous_char;

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

pub fn parse_text(text: &String, path_as_string: &String) -> SemanticTokensBuilder //Vec::<SemanticToken>
{

    let token_types = get_supported_token_types();

    let mut parsedTokens = SemanticTokensBuilder::new(path_as_string.clone()); //Vec::<SemanticToken>::new();

    //let lines = Vec::<String>::new();

    //Tokenisation

    let mut currentToken = String::new();

    let mut start_char_index: Option::<u32> = None;

    let mut previous_char: Option::<char>; // = None; 

    let mut line_no: u32 = 0;
    
    let mut char_index: u32 = 0;

    for line in text.lines()
    {

        //check that we've looked at our last token on our previous line

        if !currentToken.is_empty()
        {
        
            completeToken(line_no.clone(), &mut currentToken, &mut start_char_index, &token_types, &mut parsedTokens);
        
        }

        //you want to count the line before checking whether or not you want to continue 

        line_no += 1;

        if line.is_empty()
        {

            continue;

        }

        previous_char = None;

        
        let mut is_string = false;

        //let mut is_start_of = false;

        char_index = 0;

        for currentChar in line.chars()
        {

            //char_index += 1;

            let is_whitespace = currentChar.is_whitespace();

            if is_whitespace && !currentToken.is_empty() && !is_string
            {

                //process token once we've reached whitespace

                completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);

                previous_char = Some(currentChar);

                char_index += 1;

                continue;

            }
            else if is_whitespace
            {

                previous_char = Some(currentChar);

                char_index += 1;

                continue;

            }

            match currentChar 
            {

                '(' | ')' => {

                    //if is not string continue

                    if !is_string
                    {

                        previous_char = Some(currentChar);

                        char_index += 1;

                        continue;
                        
                    }

                }
                '{' | '}' => {

                    //if is not string continue

                    if !is_string
                    {

                        previous_char = Some(currentChar);

                        char_index += 1;

                        continue;
                        
                    }

                }
                '\"' => {

                    match previous_char {
                        Some(res) => {

                            //is this quote charachter escaped?

                            if res == '\\'
                            {

                                is_string = !is_string;

                                previous_char = Some(currentChar);

                                char_index += 1;

                                continue;

                            }

                        }
                        None =>
                        {

                        }
                        
                    }

                    previous_char = Some(currentChar);

                }
                ':' => {
                    
                    if !is_string
                    {

                        previous_char = Some(currentChar);

                        char_index += 1;

                        continue;

                    }

                }

                //operator detection

                '+' => {

                    if is_string //&& !currentToken.is_empty()
                    {

                        //completeToken(line_no.clone(),  &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);
                        
                        char_index += 1;

                        continue;

                    }

                }
                '-' => {
                    
                    if is_string && !currentToken.is_empty()
                    {

                        //completeToken(line_no.clone(),  &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);
                        
                        char_index += 1;

                        continue;

                    }

                }
                '*' => {

                    if is_string //&& !currentToken.is_empty()
                    {

                        //completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);
                        
                        char_index += 1;

                        continue;

                    }

                }
                '/' => {

                    if is_string //&& !currentToken.is_empty()
                    {

                        //completeToken(line_no.clone(),  &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);

                        char_index += 1;

                        continue;

                    }

                }
                '>' => {

                    if is_string
                    {

                        char_index += 1;

                        continue;

                    }


                }
                '<' => {

                    if is_string
                    {

                        char_index += 1;

                        continue;

                    }


                }
                '=' => {

                    if currentToken.is_empty()
                    {
                        
                        if is_string //&& 
                        {

                            //completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);

                            char_index += 1;

                            continue;

                        }
                        else
                        {

                            match previous_char
                            {

                                Some(res ) =>
                                {

                                    if res == '<' || res == '>'
                                    {

                                        previous_char = Some(currentChar);

                                        currentToken.push(currentChar);

                                        completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);

                                    }

                                }
                                None =>
                                {

                                }

                            }

                        }

                    }

                }
                //comments
                ';' => {

                    if !is_string
                    {

                        match previous_char {
                            Some(res) => {
    
                                //is comment?
    
                                if res == ';'
                                {
    
                                    //previous_char = Some(currentChar);
    
                                    //don't save the previous char as we're going to a different line
    
                                    currentToken.clear();

                                    start_char_index = None;
    
                                    break; // skip the rest of the lkine
    
                                }
    
                            }
                            None =>
                            {
    
                            }
                            
                        }   

                    }

                }
                _ => {

                }

            }

            previous_char = Some(currentChar);

            currentToken.push(currentChar);

            if start_char_index == None
            {

                start_char_index = Some(char_index);

            }

            //decide if the token should be cloned into paredtokens

            char_index += 1;

        }

        if !currentToken.is_empty() && !is_string
        {

            completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);

            continue;
            
        }

    }

    //check if theres still one remaining token to be processed

    if !currentToken.is_empty()
    {
            
        completeToken(line_no.clone(), &mut currentToken, &mut start_char_index,&token_types, &mut parsedTokens);
            
    }

    parsedTokens

}

fn completeToken(line: u32, currentToken: &mut String, start_char_index: &mut Option::<u32>, tokenTypes: &Vec<SemanticTokenType>, builder: &mut SemanticTokensBuilder) //, parsedTokens: &mut Vec::<SemanticToken>)
{

    let sci: u32;

    match start_char_index
    {

        Some(res) =>
        {

            sci = *res;

        }
        None =>
        {

            return;

        }

    }

    let token_type_option = try_get_SemanticTokenType(currentToken.as_str());

    match token_type_option //token_type_index
    {

        Some(res) =>
        {

            //make line an index

            builder.push_strs(line - 1, sci, currentToken.chars().count() as u32, &res, None, tokenTypes)

        }
        None => {

        }

    }

    currentToken.clear();

    *start_char_index = None;

}


fn read_file_path(uri: &Url) -> Result<(PathBuf, String), tower_lsp::jsonrpc::Error>
{

    let path;

    match uri.to_file_path() //params.text_document.uri.to_file_path()
    {

        Ok(res) =>
        {

            //file_string = res.to

            path = res;

        }
        Err(err) =>
        {

            let mut err_result = tower_lsp::jsonrpc::Error::internal_error();

            err_result.message = "Error opening file".to_owned();

            return Err(err_result);

        }

    }

    //tower_lsp::jsonrpc::

    let path_as_string = path.clone().into_os_string().into_string().unwrap();

    Ok((path, path_as_string))

}

fn tokenise_full(path: (PathBuf, String)) -> Result<SemanticTokens, tower_lsp::jsonrpc::Error>
{

    match read_to_string(path.0) //.as_str())
    {

        Ok(res) => {

            let tokens = parse_text(&res, &path.1); //&path_as_string);

            //use the path as the id

            let sts = tokens.build();

            return Ok(sts);

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

}

pub fn semantic_tokens_full(params: SemanticTokensParams) -> tower_lsp::jsonrpc::Result<Option<SemanticTokensResult>>
{

    let path = read_file_path(&params.text_document.uri)?;

    Ok(Some(tokenise_full(path)?.into()))

}

pub fn semantic_tokens_full_delta(params: SemanticTokensDeltaParams) -> tower_lsp::jsonrpc::Result<Option<SemanticTokensFullDeltaResult>>
{

    let path = read_file_path(&params.text_document.uri)?;

    Ok(Some(tokenise_full(path)?.into()))

}

pub fn semantic_tokens_range(params: SemanticTokensRangeParams) -> tower_lsp::jsonrpc::Result<Option<SemanticTokensRangeResult>>
{

    let path = read_file_path(&params.text_document.uri)?;

    Ok(Some(tokenise_full(path)?.into()))

}

