use clarity_repl::clarity::analysis::ContractAnalysis;
use clarity_repl::clarity::ast::ContractAST;
use clarity_repl::repl::{Session, SessionSettings};
use tokio;

use serde_json::Value;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{async_trait, LanguageServer, LspService, Client, Server};

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::io::Read;
use sha2::Digest;
use clarity_repl::{repl, clarity};
use clarity_repl::clarity::functions::NativeFunctions;
use clarity_repl::clarity::functions::define::DefineFunctions;    
use clarity_repl::clarity::variables::NativeVariables;
use clarity_repl::clarity::types::BlockInfoProperty;
use clarity_repl::clarity::docs::{
    make_api_reference, 
    make_define_reference, 
    make_keyword_reference};
use clarity_repl::clarity::analysis::AnalysisDatabase;
use clarity_repl::clarity::types::{QualifiedContractIdentifier, StandardPrincipalData};
use clarity_repl::clarity::{ast, analysis};
use clarity_repl::clarity::costs::LimitedCostTracker;

use crate::clarinet::load_config_files;

#[derive(Debug)]
enum Symbol {
    PublicFunction,
    ReadonlyFunction,
    PrivateFunction,
    ImportedTrait,
    LocalVariable,
    Constant,
    DataMap,
    DataVar,
    FungibleToken,
    NonFungibleToken,
}


#[derive(Debug)]
pub struct ContractState {
    analysis: ContractAnalysis,
    cached_completion_items: Vec<CompletionItem>,
    // TODO(lgalabru)
    // hash: Vec<u8>,
    // session: Session,
    // symbols: HashMap<String, Symbol>,
}

#[derive(Debug)]
pub struct ClarityLanguageBackend {
    clarinet_toml: RwLock<Option<(PathBuf, SessionSettings)>>,
    native_functions: Vec<CompletionItem>,
    contracts: RwLock<HashMap<Url, ContractState>>,
    client: Client,
}

impl ClarityLanguageBackend {

    pub fn new(client: Client) -> Self {
        Self {
            clarinet_toml: RwLock::new(None),
            contracts: RwLock::new(HashMap::new()),
            client,
            native_functions: Self::build_default_native_keywords_list()
        }
    }

    pub fn run_project_analysis(&self) {
        let clarinet_toml_reader = self.clarinet_toml.read().unwrap();
        if let Some((ref path, ref settings)) = *clarinet_toml_reader {
            let mut session = repl::Session::new(settings.clone());
            match session.check() {
                Err(message) => {}
                Ok(contracts_analysis) => {
                    for (analysis, code, path) in contracts_analysis.into_iter() {

                        let mut cached_completion_items = vec![];
                        for (name, _signature) in analysis.public_function_types.iter() {
                            cached_completion_items.push(CompletionItem {
                                label: name.to_string(),
                                kind: Some(CompletionItemKind::Module),
                                detail: None,
                                documentation: None,
                                deprecated: None,
                                preselect: None,
                                sort_text: None,
                                filter_text: None,
                                insert_text: Some(name.to_string()),
                                insert_text_format: Some(InsertTextFormat::PlainText),
                                insert_text_mode: None,
                                text_edit: None,
                                additional_text_edits: None,
                                command: None,
                                commit_characters: None,
                                data: None,
                                tags: None,
                            });
                        } 

                        let contract_url = Url::from_file_path(path).unwrap();
                        let contract_state = ContractState {
                            analysis,
                            cached_completion_items,
                        };
                        let mut contracts_writer = self.contracts.write().unwrap();
                        contracts_writer.insert(contract_url, contract_state);
                    }
                }
            };
        }

        // Retrieve Clarinet.toml
        // Retrieve settings/Development.toml
        // Deploy the contracts and feed self.contracts with analysis
        
        // Craft one VM per contract, that we we will be cloning over and over.
        // REPL: add readonly mode
        // When Clarinet.toml or underlying contracts are getting updated, we re-construct these VMs.
    }
    
    pub fn run_atomic_analysis(&self, contract_url: Url) {
        // From path, retrieve the dependencies, recursively.
    }

    pub fn build_default_native_keywords_list() -> Vec<CompletionItem> {
        let native_functions: Vec<CompletionItem> = NativeFunctions::ALL
            .iter()
            .map(|func| {
                let api = make_api_reference(&func);
                CompletionItem {
                    label: api.name.to_string(),
                    kind: Some(CompletionItemKind::Function),
                    detail: Some(api.name.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: api.description.to_string(),
                    })),
                    deprecated: None,
                    preselect: None,
                    sort_text: None,
                    filter_text: None,
                    insert_text: Some(api.snippet.clone()),
                    insert_text_format: Some(InsertTextFormat::Snippet),
                    insert_text_mode: None,
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
                    commit_characters: None,
                    data: None,
                    tags: None,
                }})
            .collect();
        
        let define_functions: Vec<CompletionItem> = DefineFunctions::ALL
            .iter()
            .map(|func| {
                let api = make_define_reference(&func);
                CompletionItem {
                    label: api.name.to_string(),
                    kind: Some(CompletionItemKind::Class),
                    detail: Some(api.name.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: api.description.to_string(),
                    })),
                    deprecated: None,
                    preselect: None,
                    sort_text: None,
                    filter_text: None,
                    insert_text: Some(api.snippet.clone()),
                    insert_text_format: Some(InsertTextFormat::Snippet),
                    insert_text_mode: None,
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
                    commit_characters: None,
                    data: None,
                    tags: None,
                }})
            .collect();

        let native_variables: Vec<CompletionItem> = NativeVariables::ALL
            .iter()
            .map(|var| {
                let api = make_keyword_reference(&var);
                CompletionItem {
                    label: api.name.to_string(),
                    kind: Some(CompletionItemKind::Field),
                    detail: Some(api.name.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: api.description.to_string(),
                    })),
                    deprecated: None,
                    preselect: None,
                    sort_text: None,
                    filter_text: None,
                    insert_text: Some(api.snippet.to_string()),
                    insert_text_format: Some(InsertTextFormat::PlainText),
                    insert_text_mode: None,
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
                    commit_characters: None,
                    data: None,
                    tags: None,
                }})
            .collect();

        let block_properties: Vec<CompletionItem> = BlockInfoProperty::ALL_NAMES
            .to_vec()
            .iter()
            .map(|func| {
                CompletionItem::new_simple(func.to_string(), "".to_string())})
            .collect();

        let items = vec![
                native_functions, 
                define_functions, 
                native_variables, 
                block_properties
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<CompletionItem>>();
        items
    }
}

#[async_trait]
impl LanguageServer for ClarityLanguageBackend {

    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        if let Some(root_uri) = params.root_uri {
            let mut clarinet_toml_writer = self.clarinet_toml.write().unwrap();

            let settings = load_config_files(&root_uri, "Development".into()).unwrap();
            let mut url = root_uri.to_file_path().unwrap();
            url.set_file_name("Clarinet.toml");

            *clarinet_toml_writer = Some((url, settings));
        }
        
        Ok(InitializeResult {
            server_info: None,
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::Full,
                )),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: None,
                    all_commit_characters: None,
                    work_done_progress_options: Default::default(),
                }),
                type_definition_provider: None,
                hover_provider: Some(HoverProviderCapability::Simple(false)),
                declaration_provider: Some(DeclarationCapability::Simple(false)),
                ..ServerCapabilities::default()
            },
        })
    }

    async fn initialized(&self, params: InitializedParams) {
        self.run_project_analysis();
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn execute_command(
        &self,
        _: ExecuteCommandParams,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let mut keywords = self.native_functions.clone();
        let contract_uri = params.text_document_position.text_document.uri;

        let mut user_defined_keywords = {
            let contracts_reader = self.contracts.read().unwrap();
            match contracts_reader.get(&contract_uri) {
                Some(entry) => entry.cached_completion_items.clone(),
                _ => vec![]
            }
        };
        
        keywords.append(&mut user_defined_keywords);
        Ok(Some(CompletionResponse::from(keywords)))
    }

    async fn did_open(&self, _: DidOpenTextDocumentParams) {}

    async fn did_change(&self, _: DidChangeTextDocumentParams) {}

    async fn did_save(&self,  params: DidSaveTextDocumentParams) {
        
        let tx_sender = StandardPrincipalData::transient();
        let mut clarity_interpreter = repl::ClarityInterpreter::new(tx_sender);
        // let mut clarity_interpreter = repl::ClarityInterpreter::new();

        // When Clarinet is detected, we should get the name of the contracts from Paper.toml instead.
        let uri = format!("{:?}", params.text_document.uri);
        let file_path = params.text_document.uri.to_file_path()
            .expect("Unable to locate file");

        let contract = fs::read_to_string(file_path)
            .expect("Unable to read file");
        
        let contract_identifier = clarity::types::QualifiedContractIdentifier::transient();

        let mut contract_ast = match clarity_interpreter.build_ast(contract_identifier.clone(), contract.clone()) {
            Ok(res) => res,
            Err((_, Some(parsing_diag))) => {
                let range = match parsing_diag.spans.len() {
                    0 => Range::default(),
                    _ => Range {
                        start: Position {
                            line: parsing_diag.spans[0].start_line - 1,
                            character: parsing_diag.spans[0].start_column,
                        },
                        end: Position {
                            line: parsing_diag.spans[0].end_line - 1,
                            character: parsing_diag.spans[0].end_column,
                        },
                    }
                };
                let diag = Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::Error),
                    code: None,
                    code_description: None,
                    source: Some("clarity".to_string()),
                    message: parsing_diag.message,
                    related_information: None,
                    tags: None,
                    data: None,
                }; 
                self.client.publish_diagnostics(params.text_document.uri, vec![diag], None).await;
                return
            },
            _ => {
                println!("Error returned without diagnotic");
                return
            }
        };

        let diags = match clarity_interpreter.run_analysis(contract_identifier.clone(), &mut contract_ast) {
            Ok(_) => vec![],
            Err((_, Some(analysis_diag))) => {
                let range = match analysis_diag.spans.len() {
                    0 => Range::default(),
                    _ => Range {
                        start: Position {
                            line: analysis_diag.spans[0].start_line - 1,
                            character: analysis_diag.spans[0].start_column,
                        },
                        end: Position {
                            line: analysis_diag.spans[0].end_line - 1,
                            character: analysis_diag.spans[0].end_column,
                        },
                    }
                };
                let diag = Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::Error),
                    code: None,
                    code_description: None,
                    source: Some("clarity".to_string()),
                    message: analysis_diag.message,
                    related_information: None,
                    tags: None,
                    data: None,
                }; 
                vec![diag]
            },
            _ => {
                println!("Error returned without diagnotic");
                return
            }
        };        

        self.client.publish_diagnostics(params.text_document.uri, diags, None).await;
    }

    async fn did_close(&self, _: DidCloseTextDocumentParams) {}

    // fn symbol(&self, params: WorkspaceSymbolParams) -> Self::SymbolFuture {
    //     Box::new(future::ok(None))
    // }

    // fn goto_declaration(&self, _: TextDocumentPositionParams) -> Self::DeclarationFuture {
    //     Box::new(future::ok(None))
    // }

    // fn goto_definition(&self, _: TextDocumentPositionParams) -> Self::DefinitionFuture {
    //     Box::new(future::ok(None))
    // }

    // fn goto_type_definition(&self, _: TextDocumentPositionParams) -> Self::TypeDefinitionFuture {
    //     Box::new(future::ok(None))
    // }

    // fn hover(&self, _: TextDocumentPositionParams) -> Self::HoverFuture {
    //     // todo(ludo): to implement
    //     let result = Hover {
    //         contents: HoverContents::Scalar(MarkedString::String("".to_string())),
    //         range: None,
    //     };
    //     Box::new(future::ok(None))
    // }

    // fn document_highlight(&self, _: TextDocumentPositionParams) -> Self::HighlightFuture {
    //     Box::new(future::ok(None))
    // }
}
