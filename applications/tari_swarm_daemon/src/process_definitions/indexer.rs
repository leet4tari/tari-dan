//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::path::PathBuf;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::process::Command;

use crate::process_definitions::{ProcessContext, ProcessDefinition};

#[derive(Debug, Default)]
pub struct Indexer;

impl Indexer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ProcessDefinition for Indexer {
    async fn get_command(&self, mut context: ProcessContext<'_>) -> anyhow::Result<Command> {
        let mut command = Command::new(context.bin());
        let jrpc_port = context.get_free_port("jrpc").await?;
        let web_ui_port = context.get_free_port("web").await?;
        let listen_ip = context.listen_ip();

        let json_rpc_public_address = format!("{listen_ip}:{jrpc_port}");
        let json_rpc_address = format!("{listen_ip}:{jrpc_port}");
        let web_ui_address = format!("{listen_ip}:{web_ui_port}");

        let base_node = context
            .minotari_nodes()
            .next()
            .ok_or_else(|| anyhow!("Base nodes should be started before validator nodes"))?;

        let base_node_grpc_url = base_node
            .instance()
            .allocated_ports()
            .get("grpc")
            .map(|port| format!("http://127.0.0.1:{port}"))
            .ok_or_else(|| anyhow!("grpc port not found for base node"))?;

        command
            .envs(context.environment())
            .arg("-b")
            .arg(context.base_path())
            .arg("--network")
            .arg(context.network().to_string())
            .arg(format!("-pindexer.base_node_grpc_url={base_node_grpc_url}"))
            .arg(format!("-pindexer.json_rpc_address={json_rpc_address}"))
            .arg(format!("-pindexer.http_ui_address={web_ui_address}"))
            .arg(format!("-pindexer.ui_connect_address={json_rpc_public_address}"))
            .arg("-pindexer.base_layer_scanning_interval=1");

        Ok(command)
    }

    fn get_relative_data_path(&self) -> Option<PathBuf> {
        Some("data".into())
    }
}
