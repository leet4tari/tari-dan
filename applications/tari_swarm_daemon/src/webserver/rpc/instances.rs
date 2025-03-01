//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashMap, path::PathBuf};

use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use serde::{Deserialize, Serialize};

use crate::{config::InstanceType, process_manager::InstanceId, webserver::context::HandlerContext};

#[derive(Debug, Clone, Deserialize)]
pub struct StartAllRequest {
    instance_type: Option<InstanceType>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartAllResponse {
    pub num_instances: u32,
}

pub async fn start_all(context: &HandlerContext, req: StartAllRequest) -> Result<StartAllResponse, anyhow::Error> {
    let instances = context.process_manager().list_instances(req.instance_type).await?;

    let num_instances = instances.len() as u32;
    for instance in instances {
        context.process_manager().start_instance(instance.id).await?;
    }

    Ok(StartAllResponse { num_instances })
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartInstanceRequest {
    pub by_name: Option<String>,
    pub by_id: Option<InstanceId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartInstanceResponse {
    pub success: bool,
}

pub async fn start(
    context: &HandlerContext,
    req: StartInstanceRequest,
) -> Result<StartInstanceResponse, anyhow::Error> {
    let instance = match (req.by_name, req.by_id) {
        (_, Some(id)) => context.process_manager().get_instance(id).await?,
        (Some(name), None) => context.process_manager().get_instance_by_name(name).await?,
        (None, None) => {
            return Err(JsonRpcError::new(
                JsonRpcErrorReason::InvalidParams,
                "Either `by_name` or `by_id` must be provided".to_string(),
                serde_json::Value::Null,
            )
            .into());
        },
    };

    let instance = instance.ok_or_else(|| {
        JsonRpcError::new(
            JsonRpcErrorReason::ApplicationError(404),
            "Instance not found".to_string(),
            serde_json::Value::Null,
        )
    })?;

    context.process_manager().start_instance(instance.id).await?;

    Ok(StartInstanceResponse { success: true })
}

#[derive(Debug, Clone, Deserialize)]
pub struct StopInstanceRequest {
    pub by_name: Option<String>,
    pub by_id: Option<InstanceId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StopInstanceResponse {
    pub success: bool,
}

pub async fn stop(context: &HandlerContext, req: StopInstanceRequest) -> Result<StopInstanceResponse, anyhow::Error> {
    let instance = match (req.by_name, req.by_id) {
        (_, Some(id)) => context.process_manager().get_instance(id).await?,
        (Some(name), None) => context.process_manager().get_instance_by_name(name).await?,
        (None, None) => {
            return Err(JsonRpcError::new(
                JsonRpcErrorReason::InvalidParams,
                "Either `by_name` or `by_id` must be provided".to_string(),
                serde_json::Value::Null,
            )
            .into());
        },
    };

    let instance = instance.ok_or_else(|| {
        JsonRpcError::new(
            JsonRpcErrorReason::ApplicationError(404),
            "Instance not found".to_string(),
            serde_json::Value::Null,
        )
    })?;

    context.process_manager().stop_instance(instance.id).await?;

    Ok(StopInstanceResponse { success: true })
}

#[derive(Debug, Clone, Deserialize)]
pub struct StopAllRequest {
    instance_type: Option<InstanceType>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StopAllResponse {
    pub num_instances: u32,
}

pub async fn stop_all(context: &HandlerContext, req: StopAllRequest) -> Result<StopAllResponse, anyhow::Error> {
    let instances = context.process_manager().list_instances(req.instance_type).await?;

    let num_instances = instances.len() as u32;
    for instance in instances {
        context.process_manager().stop_instance(instance.id).await?;
    }

    Ok(StopAllResponse { num_instances })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListInstancesRequest {
    pub by_type: Option<InstanceType>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListInstancesResponse {
    pub instances: Vec<InstanceInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceInfo {
    pub id: InstanceId,
    pub name: String,
    pub ports: HashMap<&'static str, u16>,
    pub base_path: PathBuf,
    pub instance_type: InstanceType,
    pub is_running: bool,
}

impl From<crate::process_manager::InstanceInfo> for InstanceInfo {
    fn from(value: crate::process_manager::InstanceInfo) -> Self {
        Self {
            id: value.id,
            name: value.name,
            ports: value.ports.into_ports(),
            base_path: value.base_path,
            instance_type: value.instance_type,
            is_running: value.is_running,
        }
    }
}

pub async fn list(context: &HandlerContext, req: ListInstancesRequest) -> Result<ListInstancesResponse, anyhow::Error> {
    let instances = context.process_manager().list_instances(req.by_type).await?;
    Ok(ListInstancesResponse {
        instances: instances.into_iter().map(Into::into).collect(),
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteInstanceDataRequest {
    pub by_name: Option<String>,
    pub by_id: Option<InstanceId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteInstanceDataResponse {
    pub success: bool,
}

pub async fn delete_data(
    context: &HandlerContext,
    req: DeleteInstanceDataRequest,
) -> Result<DeleteInstanceDataResponse, anyhow::Error> {
    let instance = match (req.by_name, req.by_id) {
        (_, Some(id)) => context.process_manager().get_instance(id).await?,
        (Some(name), None) => context.process_manager().get_instance_by_name(name).await?,
        (None, None) => {
            return Err(JsonRpcError::new(
                JsonRpcErrorReason::InvalidParams,
                "Either `by_name` or `by_id` must be provided".to_string(),
                serde_json::Value::Null,
            )
            .into());
        },
    };

    let instance = instance.ok_or_else(|| {
        JsonRpcError::new(
            JsonRpcErrorReason::ApplicationError(404),
            "Instance not found".to_string(),
            serde_json::Value::Null,
        )
    })?;

    context.process_manager().stop_instance(instance.id).await?;
    context.process_manager().delete_instance_data(instance.id).await?;

    Ok(DeleteInstanceDataResponse { success: true })
}
