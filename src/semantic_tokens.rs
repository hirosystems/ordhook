
use lsp_types::{
    Range,
    SemanticToken,
    SemanticTokenModifier,
    SemanticTokenType,
    SemanticTokens,
    SemanticTokensEdit
};

use String;

/*
use chrono::{

    Local,
    Utc

}; 
 */

use chrono::prelude::*;

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

    pub fn push(&mut self, range: Range, token_index: u32, modifier_bitset: u32)
    {

        let mut push_line = range.start.line;

        let mut push_char= range.start.character;

        if self.data.len() > 0
        {

            push_line -= self.previous_line;

            if push_line == 0
            {

                push_char -= self.previous_line;

            }

        }

        //there are no multiline tokens

        let token_len = range.end.character - range.start.character;

        let sm_token = SemanticToken{
            
            delta_line: push_line,
            delta_start: push_char,
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
