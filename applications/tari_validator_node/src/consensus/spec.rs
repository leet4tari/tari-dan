//    Copyright 2023 The Tari Project
//    SPDX-License-Identifier: BSD-3-Clause

#[cfg(not(feature = "metrics"))]
use tari_consensus::traits::hooks::NoopHooks;
use tari_consensus::traits::ConsensusSpec;
use tari_dan_app_utilities::transaction_executor::TariDanTransactionProcessor;
use tari_dan_common_types::PeerAddress;
use tari_epoch_manager::base_layer::EpochManagerHandle;
use tari_rpc_state_sync::RpcStateSyncClientProtocol;
use tari_state_store_sqlite::SqliteStateStore;
use tari_template_manager::implementation::TemplateManager;

#[cfg(feature = "metrics")]
use crate::consensus::metrics::PrometheusConsensusMetrics;
use crate::{
    consensus::{
        leader_selection::RoundRobinLeaderStrategy,
        signature_service::TariSignatureService,
        ConsensusTransactionValidator,
        TariDanBlockTransactionExecutor,
    },
    p2p::{
        services::messaging::{ConsensusInboundMessaging, ConsensusOutboundMessaging},
        NopLogger,
    },
};

#[derive(Clone)]
pub struct TariConsensusSpec;

impl ConsensusSpec for TariConsensusSpec {
    type Addr = PeerAddress;
    type EpochManager = EpochManagerHandle<Self::Addr>;
    #[cfg(not(feature = "metrics"))]
    type Hooks = NoopHooks;
    #[cfg(feature = "metrics")]
    type Hooks = PrometheusConsensusMetrics;
    type InboundMessaging = ConsensusInboundMessaging<NopLogger>;
    type LeaderStrategy = RoundRobinLeaderStrategy;
    type OutboundMessaging = ConsensusOutboundMessaging<NopLogger>;
    type SignatureService = TariSignatureService;
    type StateStore = SqliteStateStore<Self::Addr>;
    type SyncManager = RpcStateSyncClientProtocol<Self>;
    type TransactionExecutor = TariDanBlockTransactionExecutor<
        TariDanTransactionProcessor<TemplateManager<PeerAddress>>,
        ConsensusTransactionValidator,
    >;
}
