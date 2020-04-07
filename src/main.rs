#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

#[macro_use] extern crate serde_json;

use async_trait::async_trait;
use tokio;

mod clarity;

use serde_json::Value;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{LanguageServer, LspService, Client, Server};
use tower_lsp::lsp_types::{Diagnostic, Range, Position, DiagnosticSeverity};

use std::collections::HashMap;
use std::fs;

use clarity::functions::{NativeFunctions, DefineFunctions, NativeVariables, BlockInfoProperty};

#[derive(Debug, Default)]
struct Backend {
    tracked_documents: HashMap<String, String>,
}

impl Backend {

    pub fn new() -> Self {
        Self {
            tracked_documents: HashMap::new(),
        }
    }
}

#[async_trait]
impl LanguageServer for Backend {

    fn initialize(&self, _: &Client, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            server_info: None,
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::Full,
                )),
                hover_provider: Some(true),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(true),
                    trigger_characters: None,
                    work_done_progress_options: Default::default(),
                }),
                type_definition_provider: Some(TypeDefinitionProviderCapability::Simple(true)),
                declaration_provider: Some(true),
                ..ServerCapabilities::default()
            },
        })
    }

    async fn initialized(&self, client: &Client, _: InitializedParams) {
        client.log_message(MessageType::Info, "server initialized 1");
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn execute_command(
        &self,
        client: &Client,
        _: ExecuteCommandParams,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    async fn completion(&self, _: CompletionParams) -> Result<Option<CompletionResponse>> {
        use clarity::docs::{make_api_reference, make_define_reference, make_keyword_reference};

        let native_functions: Vec<CompletionItem> = NativeFunctions::ALL.iter().map(|func| {
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
            }
        }).collect();
        
        let define_functions: Vec<CompletionItem> = DefineFunctions::ALL.iter().map(|func| {
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
            }
        }).collect();

        let native_variables: Vec<CompletionItem> = NativeVariables::ALL.iter().map(|var| {
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
            }
        }).collect();

        let block_properties: Vec<CompletionItem> = BlockInfoProperty::ALL_NAMES.to_vec().iter().map(|func| {
            CompletionItem::new_simple(func.to_string(), "".to_string())
        }).collect();

        let items = vec![native_functions, define_functions, native_variables, block_properties];
        let items = items.into_iter().flatten().collect::<Vec<CompletionItem>>();

        let result = CompletionResponse::from(items);
        Ok(Some(result))
    }

    async fn did_open(&self, client: &Client, _: DidOpenTextDocumentParams) {}

    async fn did_change(&self, client: &Client, _: DidChangeTextDocumentParams) {}

    async fn did_save(&self, client: &Client, params: DidSaveTextDocumentParams) {
        use clarity::analysis::AnalysisDatabase;
        use clarity::types::QualifiedContractIdentifier;
        use clarity::ast;
        use clarity::analysis;
        
        let uri = format!("{:?}", params.text_document.uri);
        let file_path = params.text_document.uri.to_file_path().unwrap();

        let contract = fs::read_to_string(file_path)
            .expect("Something went wrong reading the file");
        
        let contract_identifier = QualifiedContractIdentifier::transient();
        let mut contract_ast = ast::build_ast(&contract_identifier, &contract).unwrap();

        let mut db = AnalysisDatabase::new();
        let result = analysis::run_analysis(
            &contract_identifier, 
            &mut contract_ast.expressions,
            &mut db, 
            false);
    
        let raw_output = format!("{:?}", result);

        let diags = match result {
            Ok(_) => vec![],
            Err(check_error) => {
                let diag = Diagnostic {
                    /// The range at which the message applies.
                    range: Range {
                        start: Position {
                            line: check_error.diagnostic.spans[0].start_line as u64,
                            character: check_error.diagnostic.spans[0].start_column as u64,
                        },
                        end: Position {
                            line: check_error.diagnostic.spans[0].end_line as u64,
                            character: check_error.diagnostic.spans[0].end_column as u64,
                        },
                    },

                    /// The diagnostic's severity. Can be omitted. If omitted it is up to the
                    /// client to interpret diagnostics as error, warning, info or hint.
                    severity: Some(DiagnosticSeverity::Error),

                    /// The diagnostic's code. Can be omitted.
                    code: None,

                    /// A human-readable string describing the source of this
                    /// diagnostic, e.g. 'typescript' or 'super lint'.
                    source: Some("clarity".to_string()),

                    /// The diagnostic's message.
                    message: check_error.diagnostic.message,

                    /// An array of related diagnostic information, e.g. when symbol-names within
                    /// a scope collide all definitions can be marked via this property.
                    related_information: None,

                    /// Additional metadata about the diagnostic.
                    tags: None,
                }; 
                vec![diag]
            },
        };        

        client.publish_diagnostics(params.text_document.uri, diags, None);
    }

    async fn did_close(&self, client: &Client, _: DidCloseTextDocumentParams) {}

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

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, messages) = LspService::new(Backend::default());
    Server::new(stdin, stdout)
        .interleave(messages)
        .serve(service)
        .await;
}
