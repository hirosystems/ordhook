use tokio;

use serde_json::Value;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{async_trait, LanguageServer, LspService, Client, Server};

use std::collections::HashMap;
use std::fs;

use super::clarity::functions::{
    NativeFunctions, 
    DefineFunctions, 
    NativeVariables, 
    BlockInfoProperty};

use super::clarity::docs::{
    make_api_reference, 
    make_define_reference, 
    make_keyword_reference};

use super::clarity::analysis::AnalysisDatabase;
use super::clarity::types::QualifiedContractIdentifier;
use super::clarity::{ast, analysis};
use super::clarity::costs::LimitedCostTracker;

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
        
        let uri = format!("{:?}", params.text_document.uri);
        let file_path = params.text_document.uri.to_file_path()
            .expect("Unable to locate file");

        let contract = fs::read_to_string(file_path)
            .expect("Unable to read file");
        
        let contract_identifier = QualifiedContractIdentifier::transient();
        let mut contract_ast = match ast::build_ast(&contract_identifier, &contract, &mut ()) {
            Ok(res) => res,
            Err(parse_error) => {
                let range = match parse_error.diagnostic.spans.len() {
                    0 => Range::default(),
                    _ => Range {
                        start: Position {
                            line: parse_error.diagnostic.spans[0].start_line as u64 - 1,
                            character: parse_error.diagnostic.spans[0].start_column as u64,
                        },
                        end: Position {
                            line: parse_error.diagnostic.spans[0].end_line as u64 - 1,
                            character: parse_error.diagnostic.spans[0].end_column as u64,
                        },
                    }
                };
                let diag = Diagnostic {
                    /// The range at which the message applies.
                    range,

                    /// The diagnostic's severity. Can be omitted. If omitted it is up to the
                    /// client to interpret diagnostics as error, warning, info or hint.
                    severity: Some(DiagnosticSeverity::Error),

                    /// The diagnostic's code. Can be omitted.
                    code: None,

                    /// A human-readable string describing the source of this
                    /// diagnostic, e.g. 'typescript' or 'super lint'.
                    source: Some("clarity".to_string()),

                    /// The diagnostic's message.
                    message: parse_error.diagnostic.message,

                    /// An array of related diagnostic information, e.g. when symbol-names within
                    /// a scope collide all definitions can be marked via this property.
                    related_information: None,

                    /// Additional metadata about the diagnostic.
                    tags: None,
                }; 
                self.client.publish_diagnostics(params.text_document.uri, vec![diag], None).await;
                return
            }
        };

        let mut db = AnalysisDatabase::new();
        let result = analysis::run_analysis(
            &contract_identifier, 
            &mut contract_ast.expressions,
            &mut db, 
            false,
            LimitedCostTracker::new_max_limit());
    
        let raw_output = format!("{:?}", result);

        let diags = match result {
            Ok(_) => vec![],
            Err((check_error, cost_tracker)) => {
                let range = match check_error.diagnostic.spans.len() {
                    0 => Range::default(),
                    _ => Range {
                        start: Position {
                            line: check_error.diagnostic.spans[0].start_line as u64 - 1,
                            character: check_error.diagnostic.spans[0].start_column as u64,
                        },
                        end: Position {
                            line: check_error.diagnostic.spans[0].end_line as u64 - 1,
                            character: check_error.diagnostic.spans[0].end_column as u64,
                        },
                    }
                };
                let diag = Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::Error),
                    code: None,
                    source: Some("clarity".to_string()),
                    message: check_error.diagnostic.message,
                    related_information: None,
                    tags: None,
                }; 
                vec![diag]
            },
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
