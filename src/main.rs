#[macro_use] extern crate serde_json;

mod clarity;

use futures::future;
use jsonrpc_core::{BoxFuture, Result};
use serde_json::Value;
use tower_lsp::lsp_types::request::GotoDefinitionResponse;
use tower_lsp::lsp_types::*;
use tower_lsp::{LanguageServer, LspService, Printer, Server};
use tower_lsp::lsp_types::{Diagnostic, Range, Position, DiagnosticSeverity};

use std::collections::HashMap;
use std::fs;


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

impl LanguageServer for Backend {
    type ShutdownFuture = BoxFuture<()>;
    type SymbolFuture = BoxFuture<Option<Vec<SymbolInformation>>>;
    type ExecuteFuture = BoxFuture<Option<Value>>;
    type CompletionFuture = BoxFuture<Option<CompletionResponse>>;
    type HoverFuture = BoxFuture<Option<Hover>>;
    type DeclarationFuture = BoxFuture<Option<GotoDefinitionResponse>>;
    type DefinitionFuture = BoxFuture<Option<GotoDefinitionResponse>>;
    type TypeDefinitionFuture = BoxFuture<Option<GotoDefinitionResponse>>;
    type HighlightFuture = BoxFuture<Option<Vec<DocumentHighlight>>>;

    fn initialize(&self, _: &Printer, _: InitializeParams) -> Result<InitializeResult> {
        let res = InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(TextDocumentSyncKind::Full)),
                hover_provider: Some(true),
                completion_provider: None,
                signature_help_provider: None,
                definition_provider: None,
                type_definition_provider: None,
                implementation_provider: None,        
                references_provider: None,
                document_highlight_provider: None,
                document_symbol_provider: None,
                workspace_symbol_provider: None,
                code_action_provider: None,
                code_lens_provider: None,
                document_formatting_provider: Some(true),
                document_range_formatting_provider: None,
                document_on_type_formatting_provider: None,
                rename_provider: None,
                document_link_provider: None,
                color_provider: None,
                folding_range_provider: None,
                declaration_provider: Some(true),
                execute_command_provider: None,
                workspace: None,
                experimental: None,
                // selection_range_provider: None,
                // semantic_highlighting: None,
                // call_hierarchy_provider: None,
            },
            server_info: None,
        };

        Ok(res)
    }

    fn initialized(&self, printer: &Printer, _: InitializedParams) {
        printer.log_message(MessageType::Info, "server initialized 1");
    }

    fn shutdown(&self) -> Self::ShutdownFuture {
        Box::new(future::ok(()))
    }

    fn symbol(&self, _: WorkspaceSymbolParams) -> Self::SymbolFuture {
        Box::new(future::ok(None))
    }

    fn execute_command(&self, printer: &Printer, _: ExecuteCommandParams) -> Self::ExecuteFuture {
        printer.log_message(MessageType::Info, "execute_command!");
        Box::new(future::ok(None))
    }

    fn completion(&self, params: CompletionParams) -> Self::CompletionFuture {
        let result = CompletionResponse::from(vec![CompletionItem::new_simple("Label".to_string(), "Lorem ipsum dolor sit amet".to_string())]);
        Box::new(future::ok(None))
    }

    fn goto_declaration(&self, _: TextDocumentPositionParams) -> Self::DeclarationFuture {
        Box::new(future::ok(None))
    }

    fn goto_definition(&self, _: TextDocumentPositionParams) -> Self::DefinitionFuture {
        Box::new(future::ok(None))
    }

    fn goto_type_definition(&self, _: TextDocumentPositionParams) -> Self::TypeDefinitionFuture {
        Box::new(future::ok(None))
    }

    fn hover(&self, _: TextDocumentPositionParams) -> Self::HoverFuture {
        let result = Hover {
            contents: HoverContents::Scalar(MarkedString::String("Lorem ipsum dolor sit amet".to_string())),
            range: None,
        };
        Box::new(future::ok(None))
    }

    fn document_highlight(&self, _: TextDocumentPositionParams) -> Self::HighlightFuture {
        Box::new(future::ok(None))
    }

    fn did_open(&self, printer: &Printer, params: DidOpenTextDocumentParams) {
        printer.log_message(MessageType::Info, format!("Did opens: {:?}", params));
    }

    fn did_change(&self, printer: &Printer, params: DidChangeTextDocumentParams) {
        printer.log_message(MessageType::Info, format!("Did change: {:?}", params));
    }

    fn did_save(&self, printer: &Printer, params: DidSaveTextDocumentParams) {

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
        
        printer.publish_diagnostics(params.text_document.uri, diags, None);

        printer.log_message(MessageType::Info, raw_output);
    }

}

fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, messages) = LspService::new(Backend::new());
    let handle = service.close_handle();
    let server = Server::new(stdin, stdout)
        .interleave(messages)
        .serve(service);

    tokio::run(handle.run_until_exit(server));
}
