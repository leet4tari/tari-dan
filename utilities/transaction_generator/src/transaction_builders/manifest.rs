//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashMap, fs, path::Path};

use tari_common::configuration::Network;
use tari_crypto::ristretto::RistrettoSecretKey;
use tari_engine_types::TemplateAddress;
use tari_transaction::Transaction;
use tari_transaction_manifest::ManifestValue;

use crate::BoxedTransactionBuilder;

pub fn builder<P: AsRef<Path>>(
    signer_secret_key: RistrettoSecretKey,
    network: Network,
    manifest: P,
    globals: HashMap<String, ManifestValue>,
    templates: HashMap<String, TemplateAddress>,
) -> anyhow::Result<BoxedTransactionBuilder> {
    let contents = fs::read_to_string(manifest).unwrap();
    let instructions = tari_transaction_manifest::parse_manifest(&contents, globals, templates)?;
    Ok(Box::new(move |_| {
        Transaction::builder()
            .for_network(network.as_byte())
            .with_fee_instructions(instructions.fee_instructions.clone())
            .with_instructions(instructions.instructions.clone())
            .with_authorized_seal_signer()
            .build_and_seal(&signer_secret_key)
    }))
}
