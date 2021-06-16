#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

#[macro_use] extern crate lazy_static;

extern crate serde;

#[macro_use]
extern crate serde_derive;

mod clarity_language_backend;
mod clarinet;

use clarity_language_backend::ClarityLanguageBackend;
use tokio;
use tower_lsp::{LspService, Server};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, messages) = LspService::new(|client| ClarityLanguageBackend::new(client));
    Server::new(stdin, stdout)
        .interleave(messages)
        .serve(service)
        .await;
}
