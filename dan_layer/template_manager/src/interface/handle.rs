//  Copyright 2023. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use tari_common_types::types::PublicKey;
use tari_dan_common_types::Epoch;
use tari_dan_storage::global::TemplateStatus;
use tari_template_lib::models::TemplateAddress;
use tari_validator_node_client::types::TemplateAbi;
use tokio::sync::{mpsc, oneshot};

use super::{
    types::TemplateManagerRequest,
    Template,
    TemplateExecutable,
    TemplateManagerError,
    TemplateMetadata,
    TemplateQueryResult,
};
use crate::interface::types::TemplateChange;

#[derive(Debug, Clone)]
pub struct TemplateManagerHandle {
    request_tx: mpsc::Sender<TemplateManagerRequest>,
}

impl TemplateManagerHandle {
    pub fn new(request_tx: mpsc::Sender<TemplateManagerRequest>) -> Self {
        Self { request_tx }
    }

    pub async fn get_template(&self, address: TemplateAddress) -> Result<Template, TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::GetTemplate { address, reply: tx })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn load_template_abi(&self, address: TemplateAddress) -> Result<TemplateAbi, TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::LoadTemplateAbi { address, reply: tx })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn get_templates(&self, limit: usize) -> Result<Vec<TemplateMetadata>, TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::GetTemplates { limit, reply: tx })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn get_templates_by_addresses(
        &self,
        addresses: Vec<TemplateAddress>,
    ) -> Result<Vec<TemplateQueryResult>, TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::GetTemplatesByAddresses { addresses, reply: tx })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn enqueue_template_changes(
        &self,
        template_changes: Vec<TemplateChange>,
    ) -> Result<(), TemplateManagerError> {
        if template_changes.is_empty() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::EnqueueTemplateChanges {
                template_changes,
                reply: tx,
            })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn add_template(
        &self,
        author_public_key: PublicKey,
        template_address: TemplateAddress,
        template: TemplateExecutable,
        template_name: Option<String>,
        epoch: Epoch,
    ) -> Result<(), TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::AddTemplate {
                author_public_key,
                template_address,
                template,
                template_name,
                epoch,
                reply: tx,
            })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }

    pub async fn template_exists(
        &self,
        template_address: TemplateAddress,
        status: Option<TemplateStatus>,
    ) -> Result<bool, TemplateManagerError> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(TemplateManagerRequest::TemplateExists {
                address: template_address,
                status,
                reply: tx,
            })
            .await
            .map_err(|_| TemplateManagerError::ChannelClosed)?;
        rx.await.map_err(|_| TemplateManagerError::ChannelClosed)?
    }
}
