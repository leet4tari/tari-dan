//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::collections::HashMap;

use async_trait::async_trait;
use tari_base_node_client::types::BaseLayerConsensusConstants;
use tari_common_types::types::{FixedHash, PublicKey};
use tari_core::transactions::{tari_amount::MicroMinotari, transaction_components::ValidatorNodeRegistration};
use tari_dan_common_types::{
    committee::{Committee, CommitteeInfo},
    Epoch,
    NodeAddressable,
    ShardGroup,
    SubstateAddress,
};
use tari_dan_storage::global::models::ValidatorNode;
use tari_sidechain::EvictionProof;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    base_layer::types::EpochManagerRequest,
    error::EpochManagerError,
    traits::EpochManagerReader,
    EpochManagerEvent,
};

#[derive(Clone, Debug)]
pub struct EpochManagerHandle<TAddr> {
    tx_request: mpsc::Sender<EpochManagerRequest<TAddr>>,
    events: broadcast::Sender<EpochManagerEvent>,
}

impl<TAddr: NodeAddressable> EpochManagerHandle<TAddr> {
    pub fn new(
        tx_request: mpsc::Sender<EpochManagerRequest<TAddr>>,
        events: broadcast::Sender<EpochManagerEvent>,
    ) -> Self {
        Self { tx_request, events }
    }

    pub async fn add_block_hash(&self, block_height: u64, block_hash: FixedHash) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::AddBlockHash {
                block_height,
                block_hash,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn update_epoch(&self, block_height: u64, block_hash: FixedHash) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::UpdateEpoch {
                block_height,
                block_hash,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn get_base_layer_consensus_constants(&self) -> Result<BaseLayerConsensusConstants, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetBaseLayerConsensusConstants { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn last_registration_epoch(&self) -> Result<Option<Epoch>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::LastRegistrationEpoch { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn update_last_registration_epoch(&self, epoch: Epoch) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::UpdateLastRegistrationEpoch { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn add_validator_node_registration(
        &self,
        activation_epoch: Epoch,
        registration: ValidatorNodeRegistration,
        value_of_registration: MicroMinotari,
    ) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::AddValidatorNodeRegistration {
                activation_epoch,
                registration,
                value: value_of_registration,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn deactivate_validator_node(
        &self,
        public_key: PublicKey,
        deactivation_epoch: Epoch,
    ) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::DeactivateValidatorNode {
                public_key,
                deactivation_epoch,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn current_block_info(&self) -> Result<(u64, FixedHash), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::CurrentBlockInfo { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn notify_scanning_complete(&self) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::NotifyScanningComplete { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn get_fee_claim_public_key(&self) -> Result<Option<PublicKey>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetFeeClaimPublicKey { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn set_fee_claim_public_key(&self, public_key: PublicKey) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::SetFeeClaimPublicKey { public_key, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn get_committees(
        &self,
        epoch: Epoch,
    ) -> Result<HashMap<ShardGroup, Committee<TAddr>>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommittees { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    pub async fn get_random_committee_member(
        &self,
        epoch: Epoch,
        shard_group: Option<ShardGroup>,
        excluding: Vec<TAddr>,
    ) -> Result<ValidatorNode<TAddr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetRandomCommitteeMemberFromShardGroup {
                epoch,
                shard_group,
                excluding,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }
}

#[async_trait]
impl<TAddr: NodeAddressable> EpochManagerReader for EpochManagerHandle<TAddr> {
    type Addr = TAddr;

    fn subscribe(&self) -> broadcast::Receiver<EpochManagerEvent> {
        self.events.subscribe()
    }

    async fn wait_for_initial_scanning_to_complete(&self) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::WaitForInitialScanningToComplete { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_all_validator_nodes(&self, epoch: Epoch) -> Result<Vec<ValidatorNode<TAddr>>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetValidatorNodesPerEpoch { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_committees(
        &self,
        epoch: Epoch,
    ) -> Result<HashMap<ShardGroup, Committee<Self::Addr>>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommittees { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_committee_for_substate(
        &self,
        epoch: Epoch,
        substate_address: SubstateAddress,
    ) -> Result<Committee<Self::Addr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommitteeForSubstate {
                epoch,
                substate_address,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_validator_node(
        &self,
        epoch: Epoch,
        addr: &Self::Addr,
    ) -> Result<ValidatorNode<Self::Addr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetValidatorNode {
                epoch,
                addr: addr.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_validator_node_by_public_key(
        &self,
        epoch: Epoch,
        public_key: PublicKey,
    ) -> Result<ValidatorNode<Self::Addr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetValidatorNodeByPublicKey {
                epoch,
                public_key,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_many_validator_nodes(
        &self,
        query: Vec<(Epoch, PublicKey)>,
    ) -> Result<HashMap<(Epoch, PublicKey), ValidatorNode<Self::Addr>>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetManyValidatorNodes { query, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_our_validator_node(&self, epoch: Epoch) -> Result<ValidatorNode<Self::Addr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetOurValidatorNode { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_local_committee_info(&self, epoch: Epoch) -> Result<CommitteeInfo, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetLocalCommitteeInfo { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_committee_info_for_substate(
        &self,
        epoch: Epoch,
        substate_address: SubstateAddress,
    ) -> Result<CommitteeInfo, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommitteeInfo {
                epoch,
                substate_address,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        Ok(rx.await.map_err(|_| EpochManagerError::ReceiveError).unwrap().unwrap())
    }

    async fn get_committee_info_by_validator_address(
        &self,
        epoch: Epoch,
        address: &TAddr,
    ) -> Result<CommitteeInfo, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommitteeInfoByAddress {
                epoch,
                address: address.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn current_epoch(&self) -> Result<Epoch, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::CurrentEpoch { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn current_base_layer_block_info(&self) -> Result<(u64, FixedHash), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::CurrentBlockInfo { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_last_block_of_current_epoch(&self) -> Result<FixedHash, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetLastBlockOfTheEpoch { reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn is_last_block_of_epoch(&self, block_height: u64) -> Result<bool, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::IsLastBlockOfTheEpoch {
                block_height,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn is_epoch_active(&self, epoch: Epoch) -> Result<bool, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::IsEpochValid { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_num_committees(&self, epoch: Epoch) -> Result<u32, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetNumCommittees { epoch, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_committee_by_shard_group(
        &self,
        epoch: Epoch,
        shard_group: ShardGroup,
        limit: Option<usize>,
    ) -> Result<Committee<Self::Addr>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommitteeForShardGroup {
                epoch,
                shard_group,
                limit,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_committees_overlapping_shard_group(
        &self,
        epoch: Epoch,
        shard_group: ShardGroup,
    ) -> Result<HashMap<ShardGroup, Committee<Self::Addr>>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetCommitteesOverlappingShardGroup {
                epoch,
                shard_group,
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;

        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn get_base_layer_block_height(&self, hash: FixedHash) -> Result<Option<u64>, EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::GetBaseLayerBlockHeight { hash, reply: tx })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }

    async fn add_intent_to_evict_validator(&self, proof: EvictionProof) -> Result<(), EpochManagerError> {
        let (tx, rx) = oneshot::channel();
        self.tx_request
            .send(EpochManagerRequest::AddIntentToEvictValidator {
                proof: Box::new(proof),
                reply: tx,
            })
            .await
            .map_err(|_| EpochManagerError::SendError)?;
        rx.await.map_err(|_| EpochManagerError::ReceiveError)?
    }
}
