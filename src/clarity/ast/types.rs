use crate::clarity::representations::{SymbolicExpression, PreSymbolicExpression};
use crate::clarity::ast::errors::{ParseResult};
use crate::clarity::types::{QualifiedContractIdentifier};
use serde::{Serialize, Deserialize};

pub trait BuildASTPass {
    fn run_pass(contract_ast: &mut ContractAST) -> ParseResult<()>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractAST {
    pub contract_identifier: QualifiedContractIdentifier,
    pub pre_expressions: Vec<PreSymbolicExpression>,
    pub expressions: Vec<SymbolicExpression>,
}

impl ContractAST {
    pub fn new(contract_identifier: QualifiedContractIdentifier, pre_expressions: Vec<PreSymbolicExpression>) -> ContractAST {
        ContractAST {
            contract_identifier,
            pre_expressions,
            expressions: Vec::new()
        }
    }
}

