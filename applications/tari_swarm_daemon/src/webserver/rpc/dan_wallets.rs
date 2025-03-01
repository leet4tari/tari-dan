//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{config::InstanceType, process_manager::InstanceId, webserver::context::HandlerContext};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDanWalletsRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDanWalletsResponse {
    pub nodes: Vec<DanWalletInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanWalletInfo {
    pub instance_id: InstanceId,
    pub name: String,
    pub web: String,
    pub jrpc: String,
    pub is_running: bool,
}

pub async fn list(
    context: &HandlerContext,
    _req: ListDanWalletsRequest,
) -> Result<ListDanWalletsResponse, anyhow::Error> {
    let instances = context.process_manager().list_wallet_daemons().await?;
    let public_ip = context.config().get_public_ip();

    let nodes = instances
        .into_iter()
        .map(|instance| {
            let web_port = instance.ports.get("web").ok_or_else(|| anyhow!("web port not found"))?;
            let json_rpc_port = instance
                .ports
                .get("jrpc")
                .ok_or_else(|| anyhow!("jrpc port not found"))?;
            let web = format!("http://{public_ip}:{web_port}");
            let jrpc = format!("http://{public_ip}:{json_rpc_port}");

            Ok(DanWalletInfo {
                instance_id: instance.id,
                name: instance.name,
                web,
                jrpc,
                is_running: instance.is_running,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    Ok(ListDanWalletsResponse { nodes })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletDaemonCreateRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletDaemonCreateResponse {
    pub instance_id: InstanceId,
}

pub async fn create(
    context: &HandlerContext,
    req: WalletDaemonCreateRequest,
) -> Result<WalletDaemonCreateResponse, anyhow::Error> {
    let instance_id = context
        .process_manager()
        .create_instance(req.name, InstanceType::TariWalletDaemon, HashMap::new())
        .await?;

    Ok(WalletDaemonCreateResponse { instance_id })
}
