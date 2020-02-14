pub mod types;
pub mod errors;
pub mod definition_sorter;
pub mod trait_checker;
pub mod type_checker;
pub mod read_only_checker;
pub mod analysis_db;
pub mod contract_interface_builder;

pub use self::types::{ContractAnalysis, AnalysisPass};
use crate::clarity::representations::{SymbolicExpression};
use crate::clarity::types::{TypeSignature, QualifiedContractIdentifier};

pub use self::errors::{CheckResult, CheckError, CheckErrors};
pub use self::analysis_db::{AnalysisDatabase};

use self::definition_sorter::DefinitionSorter;
use self::read_only_checker::ReadOnlyChecker;
use self::trait_checker::{PreTypeCheckingTraitChecker, PostTypeCheckingTraitChecker};
use self::type_checker::TypeChecker;

// Legacy function
// The analysis is not just checking type.
pub fn type_check(contract_identifier: &QualifiedContractIdentifier, 
                  expressions: &mut [SymbolicExpression],
                  analysis_db: &mut AnalysisDatabase, 
                  insert_contract: bool) -> CheckResult<ContractAnalysis> {
    run_analysis(&contract_identifier, expressions, analysis_db, insert_contract)
}

pub fn run_analysis(contract_identifier: &QualifiedContractIdentifier, 
                    expressions: &mut [SymbolicExpression],
                    analysis_db: &mut AnalysisDatabase, 
                    save_contract: bool) -> CheckResult<ContractAnalysis> {

    analysis_db.execute(|db| {
        let mut contract_analysis = ContractAnalysis::new(contract_identifier.clone(), expressions.to_vec());
        DefinitionSorter::run_pass(&mut contract_analysis, db)?;
        PreTypeCheckingTraitChecker::run_pass(&mut contract_analysis, db)?;
        ReadOnlyChecker::run_pass(&mut contract_analysis, db)?;
        TypeChecker::run_pass(&mut contract_analysis, db)?;
        PostTypeCheckingTraitChecker::run_pass(&mut contract_analysis, db)?;
        if save_contract {
            db.insert_contract(&contract_identifier, &contract_analysis)?;
        }
        Ok(contract_analysis)
    })
}

#[cfg(test)]
mod tests;


