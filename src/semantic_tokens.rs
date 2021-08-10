
use lsp_types::{
    Range,
    SemanticToken,
    SemanticTokenModifier,
    SemanticTokenType,
    SemanticTokens,
    SemanticTokensEdit
};

use String;

use std::vec;

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
    
    token_types.push(SemanticTokenType::new("block-height"));

    token_types.push(SemanticTokenType::new("burn-block-height"));

    token_types.push(SemanticTokenType::new("contract-caller"));

    token_types.push(SemanticTokenType::new("false"));

    token_types.push(SemanticTokenType::new("is-in-regtest"));

    token_types.push(SemanticTokenType::new("none"));

    token_types.push(SemanticTokenType::new("stx-liquid-supply"));

    token_types.push(SemanticTokenType::new("true"));

    token_types.push(SemanticTokenType::new("tx-sender"));

    token_types

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

    pub fn push_strs(&mut self, line: u32, char: u32, length: u32, token_type: &str, token_modifiers: Option<Vec<&str>>, supported_token_types: &Vec<SemanticTokenType>)
    {

        let mut index: u32 = 0;

        //let mut found_index: i32 = -1;

        let mut found_index: u32 = 0;

        let mut has_been_found = false;

        for supported_token_type in supported_token_types.iter()
        {

            index += 1;

            if token_type == supported_token_type.as_str()
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

pub fn parse_text(text: &String)
{

    let mut parsedTokens = Vec::<String>::new();

    let lines = Vec::<String>::new();

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

}

fn completeToken(parsedTokens: &mut Vec::<String>, currentToken:  &mut String)
{

    parsedTokens.push(currentToken.clone());

    currentToken.clear();

}
