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

use tari_dan_common_types::{NodeAddressable, ToPeerId};
use tari_epoch_manager::base_layer::EpochManagerHandle;
use tari_shutdown::ShutdownSignal;
use tari_validator_node_rpc::client::TariValidatorNodeRpcClientFactory;
use tokio::{sync::mpsc, task::JoinHandle};

use super::{downloader::TemplateDownloadWorker, service::TemplateManagerService, TemplateManager};
use crate::interface::TemplateManagerHandle;

pub fn spawn<TAddr: NodeAddressable + ToPeerId + 'static>(
    manager: TemplateManager<TAddr>,
    epoch_manager: EpochManagerHandle<TAddr>,
    client_factory: TariValidatorNodeRpcClientFactory,
    shutdown: ShutdownSignal,
) -> (TemplateManagerHandle, JoinHandle<anyhow::Result<()>>) {
    let (tx_request, rx_request) = mpsc::channel(1);
    let handle = TemplateManagerHandle::new(tx_request);

    let (tx_download_queue, rx_download_queue) = mpsc::channel(1);
    let (tx_completed_downloads, rx_completed_downloads) = mpsc::channel(1);

    let join_handle = TemplateManagerService::spawn(
        rx_request,
        manager,
        epoch_manager,
        tx_download_queue,
        rx_completed_downloads,
        client_factory,
        shutdown,
    );
    TemplateDownloadWorker::new(rx_download_queue, tx_completed_downloads).spawn();
    (handle, join_handle)
}
