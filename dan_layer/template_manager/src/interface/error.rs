//   Copyright 2023. The Tari Project
//
//   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//   following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//   disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//   following disclaimer in the documentation and/or other materials provided with the distribution.
//
//   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//   products derived from this software without specific prior written permission.
//
//   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//   INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//   USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::string::FromUtf8Error;

use serde_json;
use tari_common_types::types::FixedHashSizeError;
use tari_dan_common_types::{displayable::Displayable, optional::IsNotFoundError};
use tari_dan_engine::template::TemplateLoaderError;
use tari_dan_storage::{global::TemplateStatus, StorageError};
use tari_dan_storage_sqlite::error::SqliteStorageError;
use tari_epoch_manager::EpochManagerError;
use tari_template_lib::models::TemplateAddress;
use tari_validator_node_rpc::ValidatorNodeRpcClientError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TemplateManagerError {
    #[error("The template in the base layer is invalid")]
    InvalidBaseLayerTemplate,
    #[error("Internal service channel closed unexpectedly")]
    ChannelClosed,
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
    #[error("Storage error: {0}")]
    SqliteStorageError(#[from] SqliteStorageError),
    #[error("Template not found: {address}")]
    TemplateNotFound { address: TemplateAddress },
    #[error("Templates not found: {addresses:?}")]
    TemplatesNotFound { addresses: Vec<TemplateAddress> },
    #[error("The template is unavailable for use (status={})", .status.display())]
    TemplateUnavailable { status: Option<TemplateStatus> },
    #[error(transparent)]
    TemplateLoaderError(#[from] TemplateLoaderError),
    #[error("Unsupported template type")]
    UnsupportedTemplateType,
    #[error("The template is not valid UTF-8: {0}")]
    FlowJsonNotValidUtf8(#[from] FromUtf8Error),
    #[error("The flow was not valid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("The flow engine encountered an error: {0}")]
    FlowEngineError(#[from] tari_dan_engine::flow::FlowEngineError),
    #[error("FixedHashSizeError: {0}")]
    FixedHashSizeError(#[from] FixedHashSizeError),
    #[error("Epoch manager error: {0}")]
    EpochManager(#[from] EpochManagerError),
    #[error("Validator Node RPC client error: {0}")]
    ValidatorNodeRpcClient(#[from] ValidatorNodeRpcClientError),
}

impl IsNotFoundError for TemplateManagerError {
    fn is_not_found_error(&self) -> bool {
        match self {
            TemplateManagerError::TemplateNotFound { .. } => true,
            TemplateManagerError::SqliteStorageError(e) => e.is_not_found_error(),
            TemplateManagerError::StorageError(e) => e.is_not_found_error(),
            _ => false,
        }
    }
}
