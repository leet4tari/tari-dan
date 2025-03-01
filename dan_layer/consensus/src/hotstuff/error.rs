//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use tari_common_types::types::{FixedHash, PublicKey};
use tari_crypto::ristretto::RistrettoPublicKey;
use tari_dan_common_types::{Epoch, NodeHeight, ShardGroup, VersionedSubstateIdError};
use tari_dan_storage::{
    consensus_models::{BlockError, BlockId, LeafBlock, LockedBlock, QcId, TransactionPoolError},
    StorageError,
};
use tari_epoch_manager::EpochManagerError;
use tari_state_tree::StateTreeError;
use tari_transaction::TransactionId;
use tokio::task::JoinError;

use crate::{
    hotstuff::substate_store::SubstateStoreError,
    traits::{InboundMessagingError, OutboundMessagingError},
};

#[derive(Debug, thiserror::Error)]
pub enum HotStuffError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
    #[error("State tree error: {0}")]
    StateTreeError(#[from] StateTreeError),
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
    #[error("Internal channel send error when {context}")]
    InternalChannelClosed { context: &'static str },
    #[error("Inbound messaging error: {0}")]
    InboundMessagingError(#[from] InboundMessagingError),
    #[error("Outbound messaging error: {0}")]
    OutboundMessagingError(#[from] OutboundMessagingError),
    #[error("Epoch {epoch} is not active. {details}")]
    EpochNotActive { epoch: Epoch, details: String },
    #[error("Not registered for current epoch {epoch}")]
    NotRegisteredForCurrentEpoch { epoch: Epoch },
    #[error("Received vote from non-committee member. Epoch: {epoch}, Sender: {sender}, {context}")]
    ReceivedVoteFromNonCommitteeMember {
        epoch: Epoch,
        sender: String,
        context: String,
    },
    #[error("Proposal validation error: {0}")]
    ProposalValidationError(#[from] ProposalValidationError),
    #[error("Decision mismatch for block {block_id} in pool {pool}")]
    DecisionMismatch { block_id: BlockId, pool: &'static str },
    #[error("Not the leader. {details}")]
    NotTheLeader { details: String },
    #[error("Epoch manager error: {0}")]
    EpochManagerError(anyhow::Error),
    #[error("State manager error: {0}")]
    StateManagerError(anyhow::Error),
    #[error("Invalid vote signature from {signer_public_key} (unauthenticated)")]
    InvalidVoteSignature { signer_public_key: String },
    #[error("Invalid vote {signer_public_key} (unauthenticated): {details}")]
    InvalidVote { signer_public_key: String, details: String },
    #[error("Transaction pool error: {0}")]
    TransactionPoolError(#[from] TransactionPoolError),
    #[error("Transaction {transaction_id} does not exist")]
    TransactionDoesNotExist { transaction_id: TransactionId },
    #[error(
        "Unable execute block {block_id} because the committee decided to ACCEPT transaction {transaction_id} but it \
         failed to execute locally: {reject_reason}"
    )]
    RejectedTransactionCommitDecision {
        block_id: BlockId,
        transaction_id: TransactionId,
        reject_reason: String,
    },
    #[error("Pacemaker channel dropped: {details}")]
    PacemakerChannelDropped { details: String },
    #[error(
        "Bad new view message: HighQC height {high_qc_height}, received new height {received_new_height}: {details}"
    )]
    BadNewViewMessage {
        high_qc_height: NodeHeight,
        received_new_height: NodeHeight,
        details: String,
    },
    #[error("BUG Invariant error occurred: {0}")]
    InvariantError(String),
    #[error("Sync error: {0}")]
    SyncError(anyhow::Error),
    #[error("Fallen behind: local_height={local_height}, qc_height={qc_height}")]
    FallenBehind {
        local_height: NodeHeight,
        qc_height: NodeHeight,
    },
    #[error("Transaction executor error: {0}")]
    TransactionExecutorError(String),
    #[error("Invalid sync request: {details}")]
    InvalidSyncRequest { details: String },
    #[error("Some input versions were not resolved at execution time: {0}")]
    VersionedSubstateIdError(#[from] VersionedSubstateIdError),
    #[error("Substate store error: {0}")]
    SubstateStoreError(#[from] SubstateStoreError),
    #[error(
        "Validator node omitted transaction pledges: remote_block={foreign_block}, transaction_id={transaction_id}, \
         is_prepare_phase={is_prepare_phase}"
    )]
    ForeignNodeOmittedTransactionPledges {
        foreign_block: LeafBlock,
        transaction_id: TransactionId,
        is_prepare_phase: bool,
    },
    #[error("Block building error: {0}")]
    BlockBuildingError(#[from] BlockError),
}

impl HotStuffError {
    pub fn validation_error(&self) -> Option<&ProposalValidationError> {
        match self {
            Self::ProposalValidationError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<EpochManagerError> for HotStuffError {
    fn from(err: EpochManagerError) -> Self {
        Self::EpochManagerError(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProposalValidationError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
    #[error("Node proposed by {proposed_by} with hash {hash} did not satisfy the safeNode predicate")]
    NotSafeBlock { proposed_by: String, hash: BlockId },
    #[error("Node proposed by {proposed_by} with hash {hash} is missing foreign index")]
    MissingForeignCounters { proposed_by: String, hash: BlockId },
    #[error("Node proposed by {proposed_by} with hash {hash} has invalid foreign counters: {details}")]
    InvalidForeignCounters {
        proposed_by: String,
        hash: BlockId,
        details: String,
    },
    #[error("Node proposed by {proposed_by} with hash {hash} is the genesis block")]
    ProposingGenesisBlock { proposed_by: String, hash: BlockId },
    #[error("Parent {parent_id} not found in block {block_id} proposed by {proposed_by}")]
    ParentNotFound {
        proposed_by: String,
        parent_id: BlockId,
        block_id: BlockId,
    },
    #[error("Justified block {justify_block} for proposed block {block_description} by {proposed_by} not found")]
    JustifyBlockNotFound {
        proposed_by: String,
        block_description: String,
        justify_block: LeafBlock,
    },
    #[error("QC in block {block_id} that was proposed by {proposed_by} is invalid: {details}")]
    JustifyBlockInvalid {
        proposed_by: String,
        block_id: BlockId,
        details: String,
    },
    #[error("Candidate block {candidate_block_height} is not higher than justify {justify_block_height}")]
    CandidateBlockNotHigherThanJustify {
        justify_block_height: NodeHeight,
        candidate_block_height: NodeHeight,
    },
    #[error(
        "Candidate block {candidate_block_height} is higher than max failures {max_failures}. Proposed by \
         {proposed_by}, justify block height {justify_block_height}"
    )]
    CandidateBlockHigherThanMaxFailures {
        proposed_by: String,
        justify_block_height: NodeHeight,
        candidate_block_height: NodeHeight,
        max_failures: usize,
    },
    #[error("Candidate block {candidate_block_height} does not extend justify block {justify_block_height}")]
    CandidateBlockDoesNotExtendJustify {
        justify_block_height: NodeHeight,
        candidate_block_height: NodeHeight,
    },
    #[error("Block {block_id} proposed by {proposed_by} is not the leader. Expect {expected_leader}")]
    NotLeader {
        proposed_by: String,
        expected_leader: String,
        block_id: BlockId,
    },
    #[error(
        "Block {candidate_block} justify proposed by {proposed_by} is less than the current locked {locked_block}"
    )]
    CandidateBlockNotHigherThanLockedBlock {
        proposed_by: String,
        locked_block: LockedBlock,
        candidate_block: LeafBlock,
    },
    #[error("Proposed block {block_id} {height} already has been processed")]
    BlockAlreadyProcessed { block_id: BlockId, height: NodeHeight },
    #[error("Proposed block {block_id} {height} doesn't have a signature")]
    MissingSignature { block_id: BlockId, height: NodeHeight },
    #[error("Proposed block {block_id} {height} has invalid signature")]
    InvalidSignature { block_id: BlockId, height: NodeHeight },
    #[error("QC has invalid signature: {qc}")]
    QcInvalidSignature { qc: QcId },
    #[error("QC has duplicate signature: {qc} by {validator}")]
    QcDuplicateSignature { qc: QcId, validator: PublicKey },
    #[error("Quorum was not reached on QC {qc}. {got} out of {required} signatures")]
    QuorumWasNotReached { qc: QcId, got: usize, required: usize },
    #[error("Invalid network in block {block_id}: expected {expected_network}, given {block_network}")]
    InvalidNetwork {
        expected_network: String,
        block_network: String,
        block_id: BlockId,
    },
    #[error("Invalid state merkle root for block {block_id}: calculated {calculated} but block has {from_block}")]
    InvalidStateMerkleRoot {
        block_id: BlockId,
        calculated: FixedHash,
        from_block: FixedHash,
    },
    #[error("Validator {validator} is not in the expected committee: {details}")]
    ValidatorNotInCommittee { validator: String, details: String },
    #[error("Base layer block hash for block with height {proposed} too high, current height {current}")]
    BlockHeightTooHigh { proposed: u64, current: u64 },
    #[error("Base layer block hash for block with height {proposed} too small, current height {current}")]
    BlockHeightTooSmall { proposed: u64, current: u64 },
    #[error("Base layer block hash ({hash}) is not known to the node")]
    BlockHashNotFound { hash: FixedHash },
    #[error("Base layer block height {height} does not match the real height {real_height}")]
    BlockHeightMismatch { height: u64, real_height: u64 },
    #[error("Base layer block with height {base_layer_block_height} is not the last block of the epoch")]
    NotLastBlockOfEpoch {
        block_id: BlockId,
        base_layer_block_height: u64,
    },
    #[error("Foreign node in {shard_group} submitted malformed BlockPledge for block {block_id}")]
    ForeignMalformedPledges { block_id: BlockId, shard_group: ShardGroup },
    #[error(
        "Foreign node in {shard_group} submitted invalid pledge for block {block}, transaction {transaction_id}: \
         {details}"
    )]
    ForeignInvalidPledge {
        block: LeafBlock,
        transaction_id: TransactionId,
        shard_group: ShardGroup,
        details: String,
    },
    #[error(
        "Foreign node in {shard_group} submitted invalid epoch for block {block_id}. Current epoch: {current_epoch}, \
         block epoch: {block_epoch}"
    )]
    ForeignInvalidEpoch {
        block_id: BlockId,
        shard_group: ShardGroup,
        current_epoch: Epoch,
        block_epoch: Epoch,
    },
    #[error(
        "Foreign node in {shard_group} submitted proposal {block_id} but justify QC justifies {justify_qc_block_id}"
    )]
    ForeignJustifyQcDoesNotJustifyProposal {
        block_id: BlockId,
        justify_qc_block_id: BlockId,
        shard_group: ShardGroup,
    },
    #[error(
        "Foreign node submitted an foreign proposal {block_id} that did not contain any transaction evidence for this \
         node"
    )]
    NoTransactionsInCommittee { block_id: BlockId },
    #[error("Foreign node submitted an foreign proposal {block_id} that did not contain a sidechain ID")]
    MissingSidechainId { block_id: BlockId },
    #[error("Foreign node submitted an foreign proposal {block_id} with an invalid sidechain ID: {reason}")]
    InvalidSidechainId { block_id: BlockId, reason: String },
    #[error(
        "Foreign node submitted an foreign proposal {block_id} with a mistmatched sidechain ID: expected \
         {expected_sidechain_id} but got {sidechain_id}"
    )]
    MismatchedSidechainId {
        block_id: BlockId,
        expected_sidechain_id: RistrettoPublicKey,
        sidechain_id: RistrettoPublicKey,
    },
    #[error("Invalid epoch in block {block_id}. Expected: {current_epoch}, given: {block_epoch}")]
    InvalidEpochInBlock {
        block_id: BlockId,
        block_epoch: Epoch,
        current_epoch: Epoch,
    },
    #[error("Dummy block {block_id} includes a signature")]
    DummyBlockWithSignature { block_id: BlockId },
    #[error("Dummy block {block_id} includes commands")]
    DummyBlockWithCommands { block_id: BlockId },
    #[error("Malformed block {block_id}: {details}")]
    MalformedBlock { block_id: BlockId, details: String },
    #[error("Block {block_id} is for a future epoch. Current epoch: {current_epoch}, block epoch: {block_epoch}")]
    FutureEpoch {
        block_id: BlockId,
        current_epoch: Epoch,
        block_epoch: Epoch,
    },
}
