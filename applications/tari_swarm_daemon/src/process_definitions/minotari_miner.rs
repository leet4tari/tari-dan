//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::str::FromStr;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use minotari_node_grpc_client::grpc;
use tari_common_types::tari_address::TariAddress;
use tokio::process::Command;

use crate::process_definitions::{ProcessContext, ProcessDefinition};

#[derive(Debug, Default)]
pub struct MinotariMiner;

impl MinotariMiner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProcessDefinition for MinotariMiner {
    async fn get_command(&self, context: ProcessContext<'_>) -> anyhow::Result<Command> {
        let mut command = Command::new(context.bin());
        let base_node = context
            .minotari_nodes()
            .next()
            .ok_or_else(|| anyhow!("Base nodes should be started before the miner"))?;

        let wallet = context
            .minotari_wallets()
            .next()
            .ok_or_else(|| anyhow!("Wallets should be started before the miner"))?;

        let base_node_grpc_port = base_node.instance().allocated_ports().expect("grpc");
        let mut wallet_client = wallet.connect_client().await?;

        let grpc::GetAddressResponse { one_sided_address, .. } =
            wallet_client.get_address(grpc::Empty {}).await?.into_inner();

        let wallet_payment_address =
            TariAddress::from_bytes(&one_sided_address).expect("Invalid public key returned from console wallet");

        let max_blocks = context
            .get_setting("max_blocks")
            .map(u64::from_str)
            .transpose()
            .context("max_blocks is not a u64")?
            .unwrap_or(10);

        command
            .envs(context.environment())
            .arg("-b")
            .arg(context.base_path())
            .arg("--network")
            .arg(context.network().to_string())
            .arg("--non-interactive")
            .arg(format!("--max-blocks={max_blocks}"))
            .arg(format!("-pminer.wallet_payment_address={wallet_payment_address}"))
            .arg(format!(
                "-pminer.base_node_grpc_address=/ip4/127.0.0.1/tcp/{base_node_grpc_port}",
            ))
            .arg("-pminer.num_mining_threads=1");

        Ok(command)
    }
}
