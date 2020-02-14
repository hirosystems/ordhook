use std::cmp;
use std::convert::TryInto;
use crate::clarity::util::hash::hex_bytes;
use regex::{Regex, Captures};
use crate::clarity::util::c32::c32_address_decode;
use crate::clarity::ast::errors::{ParseResult, ParseErrors, ParseError};
use crate::clarity::errors::{InterpreterResult as Result};
use crate::clarity::representations::{PreSymbolicExpression, PreSymbolicExpressionType, ContractName, ClarityName};
use crate::clarity::types::{Value, PrincipalData, TraitIdentifier, QualifiedContractIdentifier};

pub const CONTRACT_MIN_NAME_LENGTH : usize = 5;
pub const CONTRACT_MAX_NAME_LENGTH : usize = 40;

pub enum LexItem {
    LeftParen,
    RightParen,
    LiteralValue(usize, Value),
    SugaredContractIdentifier(usize, ContractName),
    SugaredFieldIdentifier(usize, ContractName, ClarityName),
    FieldIdentifier(usize, TraitIdentifier),
    TraitReference(usize, ClarityName),
    Variable(String),
    Whitespace
}

#[derive(Debug)]
enum TokenType {
    LParens, RParens, Whitespace,
    StringLiteral, HexStringLiteral,
    UIntLiteral, IntLiteral, QuoteLiteral,
    Variable, TraitReferenceLiteral, PrincipalLiteral,
    SugaredContractIdentifierLiteral,
    FullyQualifiedContractIdentifierLiteral,
    SugaredFieldIdentifierLiteral,
    FullyQualifiedFieldIdentifierLiteral,
}

struct LexMatcher {
    matcher: Regex,
    handler: TokenType
}

enum LexContext {
    ExpectNothing,
    ExpectClosing
}

impl LexMatcher {
    fn new(regex_str: &str, handles: TokenType) -> LexMatcher {
        LexMatcher {
            matcher: Regex::new(&format!("^{}", regex_str)).unwrap(),
            handler: handles
        }
    }
}

fn get_value_or_err(input: &str, captures: Captures) -> ParseResult<String> {
    let matched = captures.name("value").ok_or(
        ParseError::new(ParseErrors::FailedCapturingInput))?;
    Ok(input[matched.start()..matched.end()].to_string())
}

fn get_lines_at(input: &str) -> Vec<usize> {
    let mut out: Vec<_> = input.match_indices("\n")
        .map(|(ix, _)| ix)
        .collect();
    out.reverse();
    out
}

pub fn lex(input: &str) -> ParseResult<Vec<(LexItem, u32, u32)>> {
    // Aaron: I'd like these to be static, but that'd require using
    //    lazy_static (or just hand implementing that), and I'm not convinced
    //    it's worth either (1) an extern macro, or (2) the complexity of hand implementing.

    let lex_matchers: &[LexMatcher] = &[
        LexMatcher::new(r##""(?P<value>((\\")|([[ -~]&&[^"]]))*)""##, TokenType::StringLiteral),
        LexMatcher::new(";;[ -~]*", TokenType::Whitespace), // ;; comments.
        LexMatcher::new("[\n]+", TokenType::Whitespace),
        LexMatcher::new("[ \t]+", TokenType::Whitespace),
        LexMatcher::new("[(]", TokenType::LParens),
        LexMatcher::new("[)]", TokenType::RParens),
        LexMatcher::new("<(?P<value>([[:word:]]|[-])+)>", TokenType::TraitReferenceLiteral),
        LexMatcher::new("0x(?P<value>[[:xdigit:]]+)", TokenType::HexStringLiteral),
        LexMatcher::new("u(?P<value>[[:digit:]]+)", TokenType::UIntLiteral),
        LexMatcher::new("(?P<value>-?[[:digit:]]+)", TokenType::IntLiteral),
        LexMatcher::new("'(?P<value>true|false)", TokenType::QuoteLiteral),
        LexMatcher::new(&format!(r#"'(?P<value>[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{{28,41}}(\.)([[:alnum:]]|[-]){{{},{}}}(\.)([[:alnum:]]|[-]){{1,64}})"#, 
            CONTRACT_MIN_NAME_LENGTH, CONTRACT_MAX_NAME_LENGTH), 
            TokenType::FullyQualifiedFieldIdentifierLiteral),
        LexMatcher::new(&format!(r#"(?P<value>(\.)([[:alnum:]]|[-]){{{},{}}}(\.)([[:alnum:]]|[-]){{1,64}})"#, 
            CONTRACT_MIN_NAME_LENGTH, CONTRACT_MAX_NAME_LENGTH), TokenType::SugaredFieldIdentifierLiteral),
        LexMatcher::new(&format!(r#"'(?P<value>[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{{28,41}}(\.)([[:alnum:]]|[-]){{{},{}}})"#, 
            CONTRACT_MIN_NAME_LENGTH, CONTRACT_MAX_NAME_LENGTH), TokenType::FullyQualifiedContractIdentifierLiteral),
        LexMatcher::new(&format!(r#"(?P<value>(\.)([[:alnum:]]|[-]){{{},{}}})"#, 
            CONTRACT_MIN_NAME_LENGTH, CONTRACT_MAX_NAME_LENGTH), TokenType::SugaredContractIdentifierLiteral),
        LexMatcher::new("'(?P<value>[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{28,41})", TokenType::PrincipalLiteral),
        LexMatcher::new("(?P<value>([[:word:]]|[-!?+<>=/*])+)", TokenType::Variable),
    ];

    let mut context = LexContext::ExpectNothing;

    let mut line_indices = get_lines_at(input);
    let mut next_line_break = line_indices.pop();
    let mut current_line: u32 = 1;

    let mut result = Vec::new();
    let mut munch_index = 0;
    let mut column_pos: u32 = 1;
    let mut did_match = true;
    while did_match && munch_index < input.len() {
        if let Some(next_line_ix) = next_line_break {
            if munch_index > next_line_ix {
                next_line_break = line_indices.pop();
                column_pos = 1;
                current_line = current_line.checked_add(1)
                    .ok_or(ParseError::new(ParseErrors::ProgramTooLarge))?;
            }
        }

        did_match = false;
        let current_slice = &input[munch_index..];
        for matcher in lex_matchers.iter() {
            if let Some(captures) = matcher.matcher.captures(current_slice) {
                let whole_match = captures.get(0).unwrap();
                assert_eq!(whole_match.start(), 0);
                munch_index += whole_match.end();

                match context {
                    LexContext::ExpectNothing => Ok(()),
                    LexContext::ExpectClosing => {
                        // expect the next lexed item to be something that typically
                        // "closes" an atom -- i.e., whitespace or a right-parens.
                        // this prevents an atom like 1234abc from getting split into "1234" and "abc"
                        match matcher.handler {
                            TokenType::RParens => Ok(()),
                            TokenType::Whitespace => Ok(()),
                            _ => Err(ParseError::new(ParseErrors::SeparatorExpected(current_slice[..whole_match.end()].to_string())))
                        }
                    }
                }?;

                // default to expect a closing
                context = LexContext::ExpectClosing;

                let token = match matcher.handler {
                    TokenType::LParens => { 
                        context = LexContext::ExpectNothing;
                        Ok(LexItem::LeftParen)
                    },
                    TokenType::RParens => {
                        Ok(LexItem::RightParen)
                    },
                    TokenType::Whitespace => {
                        context = LexContext::ExpectNothing;
                        Ok(LexItem::Whitespace)
                    },
                    TokenType::Variable => {
                        let value = get_value_or_err(current_slice, captures)?;
                        if value.contains("#") {
                            Err(ParseError::new(ParseErrors::IllegalVariableName(value)))
                        } else {
                            Ok(LexItem::Variable(value))
                        }
                    },
                    TokenType::QuoteLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match str_value.as_str() {
                            "true" => Ok(Value::Bool(true)),
                            "false" => Ok(Value::Bool(false)),
                            _ => Err(ParseError::new(ParseErrors::UnknownQuotedValue(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::UIntLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match u128::from_str_radix(&str_value, 10) {
                            Ok(parsed) => Ok(Value::UInt(parsed)),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingIntValue(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::IntLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match i128::from_str_radix(&str_value, 10) {
                            Ok(parsed) => Ok(Value::Int(parsed)),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingIntValue(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::FullyQualifiedContractIdentifierLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match PrincipalData::parse_qualified_contract_principal(&str_value) {
                            Ok(parsed) => Ok(Value::Principal(parsed)),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingPrincipal(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::SugaredContractIdentifierLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match str_value[1..].to_string().try_into() {
                            Ok(parsed) => Ok(parsed),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingPrincipal(str_value.clone())))
                        }?;
                        Ok(LexItem::SugaredContractIdentifier(str_value.len(), value))
                    },
                    TokenType::FullyQualifiedFieldIdentifierLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match TraitIdentifier::parse_fully_qualified(&str_value) {
                            Ok(parsed) => Ok(parsed),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingField(str_value.clone())))
                        }?;
                        Ok(LexItem::FieldIdentifier(str_value.len(), value))
                    },
                    TokenType::SugaredFieldIdentifierLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let (contract_name, field_name) = match TraitIdentifier::parse_sugared_syntax(&str_value) {
                            Ok((contract_name, field_name)) => Ok((contract_name, field_name)),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingField(str_value.clone())))
                        }?;
                        Ok(LexItem::SugaredFieldIdentifier(str_value.len(), contract_name, field_name))
                    },
                    TokenType::PrincipalLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let value = match PrincipalData::parse_standard_principal(&str_value) {
                            Ok(parsed) => Ok(Value::Principal(PrincipalData::Standard(parsed))),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingPrincipal(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::TraitReferenceLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let data = str_value.clone().try_into()
                            .map_err(|_| { ParseError::new(ParseErrors::IllegalVariableName(str_value.to_string())) })?;
                        Ok(LexItem::TraitReference(str_value.len(), data))
                    },
                    TokenType::HexStringLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let byte_vec = hex_bytes(&str_value)
                            .map_err(|x| { ParseError::new(ParseErrors::FailedParsingHexValue(str_value.clone(), x.to_string())) })?;
                        let value = match Value::buff_from(byte_vec) {
                            Ok(parsed) => Ok(parsed),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingBuffer(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                    TokenType::StringLiteral => {
                        let str_value = get_value_or_err(current_slice, captures)?;
                        let quote_unescaped = str_value.replace("\\\"","\"");
                        let slash_unescaped = quote_unescaped.replace("\\\\","\\");
                        let byte_vec = slash_unescaped.as_bytes().to_vec();
                        let value = match Value::buff_from(byte_vec) {
                            Ok(parsed) => Ok(parsed),
                            Err(_e) => Err(ParseError::new(ParseErrors::FailedParsingBuffer(str_value.clone())))
                        }?;
                        Ok(LexItem::LiteralValue(str_value.len(), value))
                    },
                }?;

                result.push((token, current_line, column_pos));
                column_pos += whole_match.end() as u32;
                did_match = true;
                break;
            }
        }
    }

    if munch_index == input.len() {
        Ok(result)
    } else {
        Err(ParseError::new(ParseErrors::FailedParsingRemainder(input[munch_index..].to_string())))
    }
}

pub fn parse_lexed(mut input: Vec<(LexItem, u32, u32)>) -> ParseResult<Vec<PreSymbolicExpression>> {
    let mut parse_stack = Vec::new();

    let mut output_list = Vec::new();

    for (item, line_pos, column_pos) in input.drain(..) {
        match item {
            LexItem::LeftParen => {
                // start new list.
                let new_list = Vec::new();
                parse_stack.push((new_list, line_pos, column_pos));
            },
            LexItem::RightParen => {
                // end current list.
                if let Some((value, start_line, start_column)) = parse_stack.pop() {
                    let mut pre_expr = PreSymbolicExpression::list(value.into_boxed_slice());
                    pre_expr.set_span(start_line, start_column, line_pos, column_pos);
                    match parse_stack.last_mut() {
                        None => {
                            // no open lists on stack, add current to result.
                            output_list.push(pre_expr)
                        },
                        Some((ref mut list, _, _)) => {
                            list.push(pre_expr);
                        }
                    };
                } else {
                    return Err(ParseError::new(ParseErrors::ClosingParenthesisUnexpected))
                }
            },
            LexItem::Variable(value) => {
                let end_column = column_pos + (value.len() as u32) - 1;
                let value = value.clone().try_into()
                    .map_err(|_| { ParseError::new(ParseErrors::IllegalVariableName(value.to_string())) })?;
                let mut pre_expr = PreSymbolicExpression::atom(value);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            },
            LexItem::LiteralValue(length, value) => {
                let mut end_column = column_pos + (length as u32);
                // Avoid underflows on cases like empty strings
                if length > 0 {
                    end_column = end_column - 1;
                }
                let mut pre_expr = PreSymbolicExpression::atom_value(value);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            },
            LexItem::SugaredContractIdentifier(length, value) => {
                let mut end_column = column_pos + (length as u32);
                // Avoid underflows on cases like empty strings
                if length > 0 {
                    end_column = end_column - 1;
                }
                let mut pre_expr = PreSymbolicExpression::sugared_contract_identifier(value);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            },
            LexItem::SugaredFieldIdentifier(length, contract_name, name) => {
                let mut end_column = column_pos + (length as u32);
                // Avoid underflows on cases like empty strings
                if length > 0 {
                    end_column = end_column - 1;
                }
                let mut pre_expr = PreSymbolicExpression::sugared_field_identifier(contract_name, name);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            },
            LexItem::FieldIdentifier(length, trait_identifier) => {
                let mut end_column = column_pos + (length as u32);
                // Avoid underflows on cases like empty strings
                if length > 0 {
                    end_column = end_column - 1;
                }
                let mut pre_expr = PreSymbolicExpression::field_identifier(trait_identifier);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            },
            LexItem::TraitReference(length, value) => {
                let end_column = column_pos + (value.len() as u32) - 1;
                let value = value.clone().try_into()
                    .map_err(|_| { ParseError::new(ParseErrors::IllegalVariableName(value.to_string())) })?;
                let mut pre_expr = PreSymbolicExpression::trait_reference(value);
                pre_expr.set_span(line_pos, column_pos, line_pos, end_column);

                match parse_stack.last_mut() {
                    None => output_list.push(pre_expr),
                    Some((ref mut list, _, _)) => list.push(pre_expr)
                };
            }
            LexItem::Whitespace => ()
        };
    }

    // check unfinished stack:
    if parse_stack.len() > 0 {
        Err(ParseError::new(ParseErrors::ClosingParenthesisExpected))
    } else {
        Ok(output_list)
    }
}

pub fn parse(input: &str) -> ParseResult<Vec<PreSymbolicExpression>> {
    let lexed = lex(input)?;
    parse_lexed(lexed)
}
