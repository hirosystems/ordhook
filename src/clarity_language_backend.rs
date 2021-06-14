use tokio;

use serde_json::Value;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{async_trait, LanguageServer, LspService, Client, Server};

use std::collections::HashMap;
use std::fs;

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
use clarity_repl::repl;

#[derive(Debug)]
pub struct ClarityLanguageBackend {
    tracked_documents: HashMap<String, String>,
    client: Client,
}

impl ClarityLanguageBackend {

    pub fn new(client: Client) -> Self {
        Self {
            tracked_documents: HashMap::new(),
            client,
        }
    }
}

#[async_trait]
impl LanguageServer for ClarityLanguageBackend {

    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            server_info: None,
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::Full,
                )),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: None,
                    work_done_progress_options: Default::default(),
                }),
                type_definition_provider: None,
                hover_provider: Some(HoverProviderCapability::Simple(false)),
                declaration_provider: Some(false),
                ..ServerCapabilities::default()
            },
        })
    }

    async fn initialized(&self, _: InitializedParams) {
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

    async fn completion(&self, _: CompletionParams) -> Result<Option<CompletionResponse>> {

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
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
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
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
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
                    text_edit: None,
                    additional_text_edits: None,
                    command: None,
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
                block_properties]
            .into_iter()
            .flatten()
            .collect::<Vec<CompletionItem>>();

        let result = CompletionResponse::from(items);
        Ok(Some(result))
    }

    async fn did_open(&self, _: DidOpenTextDocumentParams) {}

    async fn did_change(&self, _: DidChangeTextDocumentParams) {}

    async fn did_save(&self,  params: DidSaveTextDocumentParams) {
        let tx_sender = StandardPrincipalData::transient();
        let mut clarity_interpreter = repl::ClarityInterpreter::new(tx_sender);

        // When Clarinet is detected, we should get the name of the contracts from Clarinet.toml instead.
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
                            line: parsing_diag.spans[0].start_line as u64 - 1,
                            character: parsing_diag.spans[0].start_column as u64,
                        },
                        end: Position {
                            line: parsing_diag.spans[0].end_line as u64 - 1,
                            character: parsing_diag.spans[0].end_column as u64,
                        },
                    }
                };
                let diag = Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::Error),
                    code: None,
                    source: Some("clarity".to_string()),
                    message: parsing_diag.message,
                    related_information: None,
                    tags: None,
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
                            line: analysis_diag.spans[0].start_line as u64 - 1,
                            character: analysis_diag.spans[0].start_column as u64,
                        },
                        end: Position {
                            line: analysis_diag.spans[0].end_line as u64 - 1,
                            character: analysis_diag.spans[0].end_column as u64,
                        },
                    }
                };
                let diag = Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::Error),
                    code: None,
                    source: Some("clarity".to_string()),
                    message: analysis_diag.message,
                    related_information: None,
                    tags: None,
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

pub fn load_session() -> Result<(), String> {
    let mut settings = repl::SessionSettings::default();

    let root_path = env::current_dir().unwrap();
    let mut project_config_path = root_path.clone();
    project_config_path.push("Clarinet.toml");

    let mut chain_config_path = root_path.clone();
    chain_config_path.push("settings");
    chain_config_path.push("Development.toml");

    let project_config = MainConfig::from_path(&project_config_path);
    let chain_config = ChainConfig::from_path(&chain_config_path);

    let mut deployer_address = None;
    let mut initial_deployer = None;

    for (name, account) in chain_config.accounts.iter() {
        let account = repl::settings::Account {
            name: name.clone(),
            balance: account.balance,
            address: account.address.clone(),
            mnemonic: account.mnemonic.clone(),
            derivation: account.derivation.clone(),
        };
        if name == "deployer" {
            initial_deployer = Some(account.clone());
            deployer_address = Some(account.address.clone());
        }
        settings
            .initial_accounts
            .push(account);
    }

    for (name, config) in project_config.ordered_contracts().iter() {
        let mut contract_path = root_path.clone();
        contract_path.push(&config.path);

        let code = match fs::read_to_string(&contract_path) {
            Ok(code) => code,
            Err(err) => {
                return Err(format!("Error: unable to read {:?}: {}", contract_path, err))
            }
        };

        settings
            .initial_contracts
            .push(repl::settings::InitialContract {
                code: code,
                name: Some(name.clone()),
                deployer: deployer_address.clone(),
            });
    }
    settings.initial_deployer = initial_deployer;

    let mut session = repl::Session::new(settings);
    Ok(session)
}
