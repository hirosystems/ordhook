use chainhook_sdk::{types::{OrdinalInscriptionNumber, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData, OrdinalInscriptionTransferDestination}, utils::Context};

pub fn get_test_ctx() -> Context {
    let logger = hiro_system_kit::log::setup_logger();
    let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
    Context {
        logger: Some(logger),
        tracer: false,
    }
}

pub struct Brc20RevealBuilder {
    pub inscription_number: OrdinalInscriptionNumber,
    pub inscriber_address: Option<String>,
    pub inscription_id: String,
    pub ordinal_number: u64,
    pub parent: Option<String>,
}

impl Brc20RevealBuilder {
    pub fn new() -> Self {
        Brc20RevealBuilder {
            inscription_number: OrdinalInscriptionNumber {
                classic: 0,
                jubilee: 0,
            },
            inscriber_address: Some("324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()),
            inscription_id:
                "9bb2314d666ae0b1db8161cb373fcc1381681f71445c4e0335aa80ea9c37fcddi0".to_string(),
            ordinal_number: 0,
            parent: None,
        }
    }

    pub fn inscription_number(mut self, val: i64) -> Self {
        self.inscription_number = OrdinalInscriptionNumber {
            classic: val,
            jubilee: val,
        };
        self
    }

    pub fn inscriber_address(mut self, val: Option<String>) -> Self {
        self.inscriber_address = val;
        self
    }

    pub fn inscription_id(mut self, val: &str) -> Self {
        self.inscription_id = val.to_string();
        self
    }

    pub fn ordinal_number(mut self, val: u64) -> Self {
        self.ordinal_number = val;
        self
    }

    pub fn parent(mut self, val: Option<String>) -> Self {
        self.parent = val;
        self
    }

    pub fn build(self) -> OrdinalInscriptionRevealData {
        OrdinalInscriptionRevealData {
            content_bytes: "".to_string(),
            content_type: "text/plain".to_string(),
            content_length: 10,
            inscription_number: self.inscription_number,
            inscription_fee: 100,
            inscription_output_value: 10000,
            inscription_id: self.inscription_id,
            inscription_input_index: 0,
            inscription_pointer: None,
            inscriber_address: self.inscriber_address,
            delegate: None,
            metaprotocol: None,
            metadata: None,
            parent: self.parent,
            ordinal_number: self.ordinal_number,
            ordinal_block_height: 767430,
            ordinal_offset: 0,
            tx_index: 0,
            transfers_pre_inscription: 0,
            satpoint_post_inscription:
                "9bb2314d666ae0b1db8161cb373fcc1381681f71445c4e0335aa80ea9c37fcdd:0:0"
                    .to_string(),
            curse_type: None,
        }
    }
}

pub struct Brc20TransferBuilder {
    pub ordinal_number: u64,
    pub destination: OrdinalInscriptionTransferDestination,
}

impl Brc20TransferBuilder {
    pub fn new() -> Self {
        Brc20TransferBuilder {
            ordinal_number: 0,
            destination: OrdinalInscriptionTransferDestination::Transferred(
                "bc1pls75sfwullhygkmqap344f5cqf97qz95lvle6fvddm0tpz2l5ffslgq3m0".to_string(),
            ),
        }
    }

    pub fn ordinal_number(mut self, val: u64) -> Self {
        self.ordinal_number = val;
        self
    }

    pub fn destination(mut self, val: OrdinalInscriptionTransferDestination) -> Self {
        self.destination = val;
        self
    }

    pub fn build(self) -> OrdinalInscriptionTransferData {
        OrdinalInscriptionTransferData {
            ordinal_number: self.ordinal_number,
            destination: self.destination,
            satpoint_pre_transfer: "".to_string(),
            satpoint_post_transfer: "".to_string(),
            post_transfer_output_value: Some(500),
            tx_index: 0,
        }
    }
}
