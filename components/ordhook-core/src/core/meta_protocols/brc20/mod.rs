pub mod parser;
pub mod db;
pub mod verifier;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Brc20TokenDeployData {
    pub tick: String,
    pub max: f64,
    pub lim: f64,
    pub dec: u64,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Brc20BalanceData {
    pub tick: String,
    pub amt: f64,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Brc20TransferData {
    pub tick: String,
    pub amt: f64,
    pub sender_address: String,
    pub receiver_address: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Brc20Operation {
    TokenDeploy(Brc20TokenDeployData),
    TokenMint(Brc20BalanceData),
    TokenTransfer(Brc20BalanceData),
    TokenTransferSend(Brc20TransferData),
}