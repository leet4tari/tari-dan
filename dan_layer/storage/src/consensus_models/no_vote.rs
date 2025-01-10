//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use tari_dan_common_types::ShardGroup;

use crate::consensus_models::{Decision, TransactionPoolStage};

#[derive(Debug, Clone, thiserror::Error)]
pub enum NoVoteReason {
    #[error("Already voted at this height. Not voting again.")]
    AlreadyVotedAtHeight,
    #[error("Stage disagreement. Expected: {expected:?}, Actual: {stage:?}")]
    StageDisagreement {
        expected: TransactionPoolStage,
        stage: TransactionPoolStage,
    },
    #[error("The transaction is not in the pool")]
    TransactionNotInPool,
    #[error("Decision disagreement. Local: {local:?}, Remote: {remote:?}")]
    DecisionDisagreement { local: Decision, remote: Decision },
    #[error("Fee disagreement")]
    FeeDisagreement,
    #[error("Leader fee disagreement")]
    LeaderFeeDisagreement,
    #[error("Total leader fee disagreement")]
    TotalLeaderFeeDisagreement,
    #[error("No leader fee")]
    NoLeaderFee,
    #[error("Local only proposed for multi shard")]
    LocalOnlyProposedForMultiShard,
    #[error("Multi shard proposed for local only")]
    MultiShardProposedForLocalOnly,
    #[error("Not all shard groups are prepared")]
    NotAllShardGroupsPrepared,
    #[error("Foreign proposal command in block missing")]
    ForeignProposalCommandInBlockMissing,
    #[error("Foreign proposal already proposed")]
    ForeignProposalAlreadyProposed,
    #[error("Foreign proposal not received")]
    ForeignProposalNotReceived,
    #[error("Foreign proposal already confirmed")]
    ForeignProposalAlreadyConfirmed,
    #[error("Foreign proposal processing failed")]
    ForeignProposalProcessingFailed,
    #[error("Mint confidential output unknown")]
    MintConfidentialOutputUnknown,
    #[error("Mint confidential output store failed")]
    MintConfidentialOutputStoreFailed,
    #[error("The node is not at the end of the epoch")]
    NotEndOfEpoch,
    #[error("The node is not at the end of the epoch and other commands are present")]
    EndOfEpochWithOtherCommands,
    #[error("The state Merkle root does not match")]
    StateMerkleRootMismatch,
    #[error("The command Merkle root does not match")]
    CommandMerkleRootMismatch,
    #[error("Not all foreign input pledges are present")]
    NotAllForeignInputPledges,
    #[error("Leader proposed to EVICT a node that should not be evicted")]
    ShouldNotEvictNode,
    #[error("Leader proposed to EVICT a node but node is already evicted")]
    NodeAlreadyEvicted,
    #[error("Leader proposed to evict a node but it is not permitted to suspend more than f nodes")]
    CannotEvictNodeBelowQuorumThreshold,
    #[error("Not all inputs and outputs are accepted")]
    NotAllInputsOutputsAccepted,
    #[error("Invalid evidence")]
    InvalidEvidence { reason: InvalidEvidenceReason },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum InvalidEvidenceReason {
    #[error("Expected evidence to contain {shard_group} but it was missing")]
    MissingInvolvedShardGroup { shard_group: ShardGroup },
    #[error("Evidence mismatched")]
    MismatchedEvidence,
    #[error("Not all shard groups prepared in evidence")]
    NotAllShardGroupsPrepared,
}

impl NoVoteReason {
    pub fn as_code_str(&self) -> &'static str {
        match self {
            Self::AlreadyVotedAtHeight => "ShouldNotVote",
            Self::StageDisagreement { .. } => "StageDisagreement",
            Self::TransactionNotInPool => "TransactionNotInPool",
            Self::DecisionDisagreement { .. } => "DecisionDisagreement",
            Self::FeeDisagreement => "FeeDisagreement",
            Self::LeaderFeeDisagreement => "LeaderFeeDisagreement",
            Self::NoLeaderFee => "NoLeaderFee",
            Self::LocalOnlyProposedForMultiShard => "LocalOnlyProposedForMultiShard",
            Self::MultiShardProposedForLocalOnly => "MultiShardProposedForLocalOnly",
            Self::NotAllShardGroupsPrepared => "NotAllShardGroupsPrepared",
            Self::ForeignProposalCommandInBlockMissing => "ForeignProposalCommandInBlockMissing",
            Self::ForeignProposalAlreadyProposed => "ForeignProposalAlreadyProposed",
            Self::ForeignProposalNotReceived => "ForeignProposalNotReceived",
            Self::ForeignProposalAlreadyConfirmed => "ForeignProposalAlreadyConfirmed",
            Self::ForeignProposalProcessingFailed => "ForeignProposalProcessingFailed",
            Self::MintConfidentialOutputUnknown => "MintConfidentialOutputUnknown",
            Self::MintConfidentialOutputStoreFailed => "MintConfidentialOutputStoreFailed",
            Self::NotEndOfEpoch => "NotEndOfEpoch",
            Self::EndOfEpochWithOtherCommands => "EndOfEpochWithOtherCommands",
            Self::TotalLeaderFeeDisagreement => "TotalLeaderFeeDisagreement",
            Self::StateMerkleRootMismatch => "StateMerkleRootMismatch",
            Self::CommandMerkleRootMismatch => "CommandMerkleRootMismatch",
            Self::NotAllForeignInputPledges => "NotAllForeignInputPledges",
            Self::NodeAlreadyEvicted => "NodeAlreadyEvicted",
            Self::ShouldNotEvictNode => "ShouldNotEvictNode",
            Self::CannotEvictNodeBelowQuorumThreshold => "CannotSuspendNodeBelowQuorumThreshold",
            Self::NotAllInputsOutputsAccepted => "NotAllInputsOutputsAccepted",
            Self::InvalidEvidence { .. } => "InvalidEvidence",
        }
    }
}
