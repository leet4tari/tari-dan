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

use std::{
    collections::BTreeSet,
    convert::{TryFrom, TryInto},
};

use anyhow::{anyhow, Context};
use tari_bor::{decode_exact, encode};
use tari_common_types::types::PublicKey;
use tari_consensus::messages::{
    ForeignProposalMessage,
    ForeignProposalNotificationMessage,
    ForeignProposalRequestMessage,
    FullBlock,
    HotstuffMessage,
    MissingTransactionsRequest,
    MissingTransactionsResponse,
    NewViewMessage,
    ProposalMessage,
    SyncRequestMessage,
    SyncResponseMessage,
    VoteMessage,
};
use tari_crypto::tari_utilities::ByteArray;
use tari_dan_common_types::{shard::Shard, Epoch, ExtraData, NodeHeight, ShardGroup, ValidatorMetadata};
use tari_dan_storage::{
    consensus_models,
    consensus_models::{
        AbortReason,
        BlockId,
        Command,
        Decision,
        EvictNodeAtom,
        Evidence,
        ForeignProposal,
        ForeignProposalAtom,
        HighQc,
        LeaderFee,
        MintConfidentialOutputAtom,
        QcId,
        QuorumCertificate,
        QuorumDecision,
        SubstateDestroyed,
        SubstateRecord,
        TransactionAtom,
    },
};
use tari_engine_types::substate::{SubstateId, SubstateValue};
use tari_transaction::TransactionId;

use crate::proto::{self};
// -------------------------------- HotstuffMessage -------------------------------- //

impl From<&HotstuffMessage> for proto::consensus::HotStuffMessage {
    fn from(source: &HotstuffMessage) -> Self {
        let message = match source {
            HotstuffMessage::NewView(msg) => proto::consensus::hot_stuff_message::Message::NewView(msg.into()),
            HotstuffMessage::Proposal(msg) => proto::consensus::hot_stuff_message::Message::Proposal(msg.into()),
            HotstuffMessage::ForeignProposal(msg) => {
                proto::consensus::hot_stuff_message::Message::ForeignProposal(msg.into())
            },
            HotstuffMessage::ForeignProposalNotification(msg) => {
                proto::consensus::hot_stuff_message::Message::ForeignProposalNotification(msg.into())
            },
            HotstuffMessage::ForeignProposalRequest(msg) => {
                proto::consensus::hot_stuff_message::Message::ForeignProposalRequest(msg.into())
            },
            HotstuffMessage::Vote(msg) => proto::consensus::hot_stuff_message::Message::Vote(msg.into()),
            HotstuffMessage::MissingTransactionsRequest(msg) => {
                proto::consensus::hot_stuff_message::Message::RequestMissingTransactions(msg.into())
            },
            HotstuffMessage::MissingTransactionsResponse(msg) => {
                proto::consensus::hot_stuff_message::Message::RequestedTransaction(msg.into())
            },
            HotstuffMessage::CatchUpSyncRequest(msg) => {
                proto::consensus::hot_stuff_message::Message::SyncRequest(msg.into())
            },
            HotstuffMessage::SyncResponse(msg) => {
                proto::consensus::hot_stuff_message::Message::SyncResponse(msg.into())
            },
        };
        Self { message: Some(message) }
    }
}

impl TryFrom<proto::consensus::HotStuffMessage> for HotstuffMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::HotStuffMessage) -> Result<Self, Self::Error> {
        let message = value.message.ok_or_else(|| anyhow!("Message is missing"))?;
        Ok(match message {
            proto::consensus::hot_stuff_message::Message::NewView(msg) => HotstuffMessage::NewView(msg.try_into()?),
            proto::consensus::hot_stuff_message::Message::Proposal(msg) => HotstuffMessage::Proposal(msg.try_into()?),
            proto::consensus::hot_stuff_message::Message::ForeignProposal(msg) => {
                HotstuffMessage::ForeignProposal(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::ForeignProposalNotification(msg) => {
                HotstuffMessage::ForeignProposalNotification(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::ForeignProposalRequest(msg) => {
                HotstuffMessage::ForeignProposalRequest(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::Vote(msg) => HotstuffMessage::Vote(msg.try_into()?),
            proto::consensus::hot_stuff_message::Message::RequestMissingTransactions(msg) => {
                HotstuffMessage::MissingTransactionsRequest(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::RequestedTransaction(msg) => {
                HotstuffMessage::MissingTransactionsResponse(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::SyncRequest(msg) => {
                HotstuffMessage::CatchUpSyncRequest(msg.try_into()?)
            },
            proto::consensus::hot_stuff_message::Message::SyncResponse(msg) => {
                HotstuffMessage::SyncResponse(msg.try_into()?)
            },
        })
    }
}

//---------------------------------- NewView --------------------------------------------//

impl From<&NewViewMessage> for proto::consensus::NewViewMessage {
    fn from(value: &NewViewMessage) -> Self {
        Self {
            high_qc: Some((&value.high_qc).into()),
            new_height: value.new_height.as_u64(),
            last_vote: value.last_vote.as_ref().map(|a| a.into()),
        }
    }
}

impl TryFrom<proto::consensus::NewViewMessage> for NewViewMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::NewViewMessage) -> Result<Self, Self::Error> {
        Ok(NewViewMessage {
            high_qc: value.high_qc.ok_or_else(|| anyhow!("High QC is missing"))?.try_into()?,
            new_height: value.new_height.into(),
            last_vote: value
                .last_vote
                .map(|a: proto::consensus::VoteMessage| a.try_into())
                .transpose()?,
        })
    }
}

//---------------------------------- ProposalMessage --------------------------------------------//

impl From<&ProposalMessage> for proto::consensus::ProposalMessage {
    fn from(value: &ProposalMessage) -> Self {
        Self {
            block: Some((&value.block).into()),
            foreign_proposals: value.foreign_proposals.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::consensus::ProposalMessage> for ProposalMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ProposalMessage) -> Result<Self, Self::Error> {
        Ok(ProposalMessage {
            block: value.block.ok_or_else(|| anyhow!("Block is missing"))?.try_into()?,
            foreign_proposals: value
                .foreign_proposals
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

// -------------------------------- ForeignProposalMessage -------------------------------- //

impl From<&ForeignProposalMessage> for proto::consensus::ForeignProposalMessage {
    fn from(value: &ForeignProposalMessage) -> Self {
        Self {
            proposal: Some(proto::consensus::ForeignProposal::from(value)),
        }
    }
}

impl TryFrom<proto::consensus::ForeignProposalMessage> for ForeignProposalMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ForeignProposalMessage) -> Result<Self, Self::Error> {
        let proposal = value.proposal.ok_or_else(|| anyhow!("Proposal is missing"))?;
        Ok(ForeignProposalMessage {
            block: proposal.block.ok_or_else(|| anyhow!("Block is missing"))?.try_into()?,
            justify_qc: proposal
                .justify_qc
                .ok_or_else(|| anyhow!("Justify QC is missing"))?
                .try_into()?,
            block_pledge: decode_exact(&proposal.encoded_block_pledge).context("Failed to decode block pledge")?,
        })
    }
}

impl From<&ForeignProposalMessage> for proto::consensus::ForeignProposal {
    fn from(value: &ForeignProposalMessage) -> Self {
        Self {
            block: Some(proto::consensus::Block::from(&value.block)),
            justify_qc: Some(proto::consensus::QuorumCertificate::from(&value.justify_qc)),
            encoded_block_pledge: encode(&value.block_pledge).expect("Failed to encode block pledge"),
        }
    }
}

impl From<&ForeignProposal> for proto::consensus::ForeignProposal {
    fn from(value: &ForeignProposal) -> Self {
        Self {
            block: Some(proto::consensus::Block::from(&value.block)),
            justify_qc: Some(proto::consensus::QuorumCertificate::from(&value.justify_qc)),
            encoded_block_pledge: encode(&value.block_pledge).expect("Failed to encode block pledge"),
        }
    }
}

impl TryFrom<proto::consensus::ForeignProposal> for ForeignProposal {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ForeignProposal) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.block.ok_or_else(|| anyhow!("Block is missing"))?.try_into()?,
            decode_exact(&value.encoded_block_pledge).context("Failed to decode block pledge")?,
            value
                .justify_qc
                .ok_or_else(|| anyhow!("Justify QC is missing"))?
                .try_into()?,
        ))
    }
}

// -------------------------------- ForeignProposalNotification -------------------------------- //

impl From<&ForeignProposalNotificationMessage> for proto::consensus::ForeignProposalNotification {
    fn from(value: &ForeignProposalNotificationMessage) -> Self {
        Self {
            block_id: value.block_id.as_bytes().to_vec(),
            epoch: value.epoch.as_u64(),
        }
    }
}

impl TryFrom<proto::consensus::ForeignProposalNotification> for ForeignProposalNotificationMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ForeignProposalNotification) -> Result<Self, Self::Error> {
        Ok(Self {
            block_id: BlockId::try_from(value.block_id)?,
            epoch: Epoch(value.epoch),
        })
    }
}

impl From<&ForeignProposalRequestMessage> for proto::consensus::ForeignProposalRequest {
    fn from(value: &ForeignProposalRequestMessage) -> Self {
        match value {
            ForeignProposalRequestMessage::ByBlockId {
                block_id,
                for_shard_group,
                epoch,
            } => Self {
                request: Some(proto::consensus::foreign_proposal_request::Request::ByBlockId(
                    proto::consensus::ForeignProposalRequestByBlockId {
                        block_id: block_id.as_bytes().to_vec(),
                        for_shard_group: for_shard_group.encode_as_u32(),
                        epoch: epoch.as_u64(),
                    },
                )),
            },
            ForeignProposalRequestMessage::ByTransactionId {
                transaction_id,
                for_shard_group,
                epoch,
            } => Self {
                request: Some(proto::consensus::foreign_proposal_request::Request::ByTransactionId(
                    proto::consensus::ForeignProposalRequestByTransactionId {
                        transaction_id: transaction_id.as_bytes().to_vec(),
                        for_shard_group: for_shard_group.encode_as_u32(),
                        epoch: epoch.as_u64(),
                    },
                )),
            },
        }
    }
}

impl TryFrom<proto::consensus::ForeignProposalRequest> for ForeignProposalRequestMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ForeignProposalRequest) -> Result<Self, Self::Error> {
        let request = value.request.ok_or_else(|| anyhow!("Request is missing"))?;
        Ok(match request {
            proto::consensus::foreign_proposal_request::Request::ByBlockId(by_block_id) => {
                ForeignProposalRequestMessage::ByBlockId {
                    block_id: BlockId::try_from(by_block_id.block_id)?,
                    for_shard_group: ShardGroup::decode_from_u32(by_block_id.for_shard_group)
                        .ok_or_else(|| anyhow!("Invalid ShardGroup"))?,
                    epoch: Epoch(by_block_id.epoch),
                }
            },
            proto::consensus::foreign_proposal_request::Request::ByTransactionId(by_transaction_id) => {
                ForeignProposalRequestMessage::ByTransactionId {
                    transaction_id: TransactionId::try_from(by_transaction_id.transaction_id)?,
                    for_shard_group: ShardGroup::decode_from_u32(by_transaction_id.for_shard_group)
                        .ok_or_else(|| anyhow!("Invalid ShardGroup"))?,
                    epoch: Epoch(by_transaction_id.epoch),
                }
            },
        })
    }
}

// -------------------------------- VoteMessage -------------------------------- //

impl From<&VoteMessage> for proto::consensus::VoteMessage {
    fn from(msg: &VoteMessage) -> Self {
        Self {
            epoch: msg.epoch.as_u64(),
            block_id: msg.block_id.as_bytes().to_vec(),
            block_height: msg.unverified_block_height.as_u64(),
            decision: i32::from(msg.decision.as_u8()),
            signature: Some((&msg.signature).into()),
        }
    }
}

impl TryFrom<proto::consensus::VoteMessage> for VoteMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::VoteMessage) -> Result<Self, Self::Error> {
        Ok(VoteMessage {
            epoch: Epoch(value.epoch),
            block_id: BlockId::try_from(value.block_id)?,
            unverified_block_height: NodeHeight(value.block_height),
            decision: QuorumDecision::from_u8(u8::try_from(value.decision)?)
                .ok_or_else(|| anyhow!("Invalid decision byte {}", value.decision))?,
            signature: value
                .signature
                .ok_or_else(|| anyhow!("Signature is missing"))?
                .try_into()?,
        })
    }
}

//---------------------------------- MissingTransactionsRequest --------------------------------------------//
impl From<&MissingTransactionsRequest> for proto::consensus::MissingTransactionsRequest {
    fn from(msg: &MissingTransactionsRequest) -> Self {
        Self {
            request_id: msg.request_id,
            epoch: msg.epoch.as_u64(),
            block_id: msg.block_id.as_bytes().to_vec(),
            transaction_ids: msg.transactions.iter().map(|tx_id| tx_id.as_bytes().to_vec()).collect(),
        }
    }
}

impl TryFrom<proto::consensus::MissingTransactionsRequest> for MissingTransactionsRequest {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::MissingTransactionsRequest) -> Result<Self, Self::Error> {
        Ok(MissingTransactionsRequest {
            request_id: value.request_id,
            epoch: Epoch(value.epoch),
            block_id: BlockId::try_from(value.block_id)?,
            transactions: value
                .transaction_ids
                .into_iter()
                .map(|tx_id| tx_id.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}
//---------------------------------- MissingTransactionsResponse --------------------------------------------//

impl From<&MissingTransactionsResponse> for proto::consensus::MissingTransactionsResponse {
    fn from(msg: &MissingTransactionsResponse) -> Self {
        Self {
            request_id: msg.request_id,
            epoch: msg.epoch.as_u64(),
            block_id: msg.block_id.as_bytes().to_vec(),
            transactions: msg.transactions.iter().map(|tx| tx.into()).collect(),
        }
    }
}

impl TryFrom<proto::consensus::MissingTransactionsResponse> for MissingTransactionsResponse {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::MissingTransactionsResponse) -> Result<Self, Self::Error> {
        Ok(MissingTransactionsResponse {
            request_id: value.request_id,
            epoch: Epoch(value.epoch),
            block_id: BlockId::try_from(value.block_id)?,
            transactions: value
                .transactions
                .into_iter()
                .map(|tx| tx.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<&consensus_models::BlockHeader> for proto::consensus::BlockHeader {
    fn from(value: &consensus_models::BlockHeader) -> Self {
        Self {
            network: value.network().as_byte().into(),
            height: value.height().as_u64(),
            epoch: value.epoch().as_u64(),
            shard_group: value.shard_group().encode_as_u32(),
            parent_id: value.parent().as_bytes().to_vec(),
            proposed_by: ByteArray::as_bytes(value.proposed_by()).to_vec(),
            state_merkle_root: value.state_merkle_root().as_slice().to_vec(),
            total_leader_fee: value.total_leader_fee(),
            foreign_indexes: encode(value.foreign_indexes()).unwrap(),
            signature: value.signature().map(Into::into),
            timestamp: value.timestamp(),
            base_layer_block_height: value.base_layer_block_height(),
            base_layer_block_hash: value.base_layer_block_hash().as_bytes().to_vec(),
            is_dummy: value.is_dummy(),
            extra_data: Some(value.extra_data().into()),
        }
    }
}

fn try_convert_proto_block_header(
    value: proto::consensus::BlockHeader,
    justify_id: QcId,
    commands: &BTreeSet<Command>,
) -> Result<consensus_models::BlockHeader, anyhow::Error> {
    let network = u8::try_from(value.network)
        .map_err(|_| anyhow!("Block conversion: Invalid network byte {}", value.network))?
        .try_into()?;

    let shard_group = ShardGroup::decode_from_u32(value.shard_group)
        .ok_or_else(|| anyhow!("Block shard_group ({}) is not a valid", value.shard_group))?;

    let proposed_by = PublicKey::from_canonical_bytes(&value.proposed_by)
        .map_err(|_| anyhow!("Block conversion: Invalid proposed_by"))?;

    let extra_data = value
        .extra_data
        .ok_or_else(|| anyhow!("ExtraData not provided"))?
        .try_into()?;

    if value.is_dummy {
        Ok(consensus_models::BlockHeader::dummy_block(
            network,
            value.parent_id.try_into()?,
            proposed_by,
            NodeHeight(value.height),
            justify_id,
            Epoch(value.epoch),
            shard_group,
            value.state_merkle_root.try_into()?,
            value.timestamp,
            value.base_layer_block_height,
            value.base_layer_block_hash.try_into()?,
        ))
    } else {
        // We calculate the BlockId and command MR locally from remote data. This means that they will
        // always be valid, therefore do not need to be explicitly validated.
        // If there were a mismatch (perhaps due modified data over the wire) the signature verification will fail.
        Ok(consensus_models::BlockHeader::create(
            network,
            value.parent_id.try_into()?,
            justify_id,
            NodeHeight(value.height),
            Epoch(value.epoch),
            shard_group,
            proposed_by,
            value.state_merkle_root.try_into()?,
            commands,
            value.total_leader_fee,
            decode_exact(&value.foreign_indexes)?,
            value.signature.map(TryInto::try_into).transpose()?,
            value.timestamp,
            value.base_layer_block_height,
            value.base_layer_block_hash.try_into()?,
            extra_data,
        )?)
    }
}

//---------------------------------- Block --------------------------------------------//

impl From<&consensus_models::Block> for proto::consensus::Block {
    fn from(value: &consensus_models::Block) -> Self {
        Self {
            header: Some(value.header().into()),
            justify: Some(value.justify().into()),
            commands: value.commands().iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::consensus::Block> for consensus_models::Block {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::Block) -> Result<Self, Self::Error> {
        let commands = value
            .commands
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;

        let justify = value
            .justify
            .ok_or_else(|| anyhow!("Block conversion: QC not provided"))?;
        let justify = consensus_models::QuorumCertificate::try_from(justify)?;

        let header = value.header.ok_or_else(|| anyhow!("BlockHeader not provided"))?;
        let header = try_convert_proto_block_header(header, *justify.id(), &commands)?;

        Ok(Self::new(header, justify, commands))
    }
}

//---------------------------------- Evidence --------------------------------------------//

impl From<&ExtraData> for proto::consensus::ExtraData {
    fn from(value: &ExtraData) -> Self {
        Self {
            encoded_extra_data: encode(value).unwrap(),
        }
    }
}

impl TryFrom<proto::consensus::ExtraData> for ExtraData {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ExtraData) -> Result<Self, Self::Error> {
        Ok(decode_exact(&value.encoded_extra_data)?)
    }
}

//---------------------------------- Command --------------------------------------------//

impl From<&Command> for proto::consensus::Command {
    fn from(value: &Command) -> Self {
        let command = match value {
            Command::LocalOnly(tx) => proto::consensus::command::Command::LocalOnly(tx.into()),
            Command::Prepare(tx) => proto::consensus::command::Command::Prepare(tx.into()),
            Command::LocalPrepare(tx) => proto::consensus::command::Command::LocalPrepare(tx.into()),
            Command::AllPrepare(tx) => proto::consensus::command::Command::AllPrepare(tx.into()),
            Command::SomePrepare(tx) => proto::consensus::command::Command::SomePrepare(tx.into()),
            Command::LocalAccept(tx) => proto::consensus::command::Command::LocalAccept(tx.into()),
            Command::AllAccept(tx) => proto::consensus::command::Command::AllAccept(tx.into()),
            Command::SomeAccept(tx) => proto::consensus::command::Command::SomeAccept(tx.into()),
            Command::ForeignProposal(foreign_proposal) => {
                proto::consensus::command::Command::ForeignProposal(foreign_proposal.into())
            },
            Command::MintConfidentialOutput(atom) => {
                proto::consensus::command::Command::MintConfidentialOutput(atom.into())
            },
            Command::EvictNode(atom) => proto::consensus::command::Command::EvictNode(atom.into()),
            Command::EndEpoch => proto::consensus::command::Command::EndEpoch(true),
        };

        Self { command: Some(command) }
    }
}

impl TryFrom<proto::consensus::Command> for Command {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::Command) -> Result<Self, Self::Error> {
        let command = value.command.ok_or_else(|| anyhow!("Command is missing"))?;
        Ok(match command {
            proto::consensus::command::Command::LocalOnly(tx) => Command::LocalOnly(tx.try_into()?),
            proto::consensus::command::Command::Prepare(tx) => Command::Prepare(tx.try_into()?),
            proto::consensus::command::Command::LocalPrepare(tx) => Command::LocalPrepare(tx.try_into()?),
            proto::consensus::command::Command::AllPrepare(tx) => Command::AllPrepare(tx.try_into()?),
            proto::consensus::command::Command::SomePrepare(tx) => Command::SomePrepare(tx.try_into()?),
            proto::consensus::command::Command::LocalAccept(tx) => Command::LocalAccept(tx.try_into()?),
            proto::consensus::command::Command::AllAccept(tx) => Command::AllAccept(tx.try_into()?),
            proto::consensus::command::Command::SomeAccept(tx) => Command::SomeAccept(tx.try_into()?),
            proto::consensus::command::Command::ForeignProposal(foreign_proposal) => {
                Command::ForeignProposal(foreign_proposal.try_into()?)
            },
            proto::consensus::command::Command::MintConfidentialOutput(atom) => {
                Command::MintConfidentialOutput(atom.try_into()?)
            },
            proto::consensus::command::Command::EvictNode(atom) => Command::EvictNode(atom.try_into()?),
            proto::consensus::command::Command::EndEpoch(_) => Command::EndEpoch,
        })
    }
}

//---------------------------------- TransactionAtom --------------------------------------------//

impl From<&TransactionAtom> for proto::consensus::TransactionAtom {
    fn from(value: &TransactionAtom) -> Self {
        Self {
            id: value.id.as_bytes().to_vec(),
            decision: Some(proto::consensus::Decision::from(value.decision)),
            evidence: Some((&value.evidence).into()),
            fee: value.transaction_fee,
            leader_fee: value.leader_fee.as_ref().map(|a| a.into()),
        }
    }
}

impl TryFrom<proto::consensus::TransactionAtom> for TransactionAtom {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::TransactionAtom) -> Result<Self, Self::Error> {
        let proto_decision = value.decision.ok_or(anyhow!("Decision is missing!"))?;
        Ok(TransactionAtom {
            id: TransactionId::try_from(value.id)?,
            decision: Decision::try_from(proto_decision)?,
            evidence: value
                .evidence
                .ok_or_else(|| anyhow!("evidence not provided"))?
                .try_into()?,
            transaction_fee: value.fee,
            leader_fee: value.leader_fee.map(TryInto::try_into).transpose()?,
        })
    }
}

// -------------------------------- BlockFee -------------------------------- //

impl From<&LeaderFee> for proto::consensus::LeaderFee {
    fn from(value: &LeaderFee) -> Self {
        Self {
            leader_fee: value.fee,
            global_exhaust_burn: value.global_exhaust_burn,
        }
    }
}

impl TryFrom<proto::consensus::LeaderFee> for LeaderFee {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::LeaderFee) -> Result<Self, Self::Error> {
        Ok(Self {
            fee: value.leader_fee,
            global_exhaust_burn: value.global_exhaust_burn,
        })
    }
}

// -------------------------------- ForeignProposalAtom -------------------------------- //

impl From<&ForeignProposalAtom> for proto::consensus::ForeignProposalAtom {
    fn from(value: &ForeignProposalAtom) -> Self {
        Self {
            block_id: value.block_id.as_bytes().to_vec(),
            shard_group: value.shard_group.encode_as_u32(),
        }
    }
}

impl TryFrom<proto::consensus::ForeignProposalAtom> for ForeignProposalAtom {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ForeignProposalAtom) -> Result<Self, Self::Error> {
        Ok(ForeignProposalAtom {
            block_id: BlockId::try_from(value.block_id)?,
            shard_group: ShardGroup::decode_from_u32(value.shard_group)
                .ok_or_else(|| anyhow!("Block shard_group ({}) is not a valid", value.shard_group))?,
        })
    }
}

// -------------------------------- MintConfidentialOutputAtom -------------------------------- //

impl From<&MintConfidentialOutputAtom> for proto::consensus::MintConfidentialOutputAtom {
    fn from(value: &MintConfidentialOutputAtom) -> Self {
        Self {
            commitment: value.commitment.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<proto::consensus::MintConfidentialOutputAtom> for MintConfidentialOutputAtom {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::MintConfidentialOutputAtom) -> Result<Self, Self::Error> {
        use tari_template_lib::models::UnclaimedConfidentialOutputAddress;
        Ok(Self {
            commitment: UnclaimedConfidentialOutputAddress::from_bytes(&value.commitment)?,
        })
    }
}

// -------------------------------- EvictNodeAtom -------------------------------- //

impl From<&EvictNodeAtom> for proto::consensus::EvictNodeAtom {
    fn from(value: &EvictNodeAtom) -> Self {
        Self {
            public_key: value.public_key.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<proto::consensus::EvictNodeAtom> for EvictNodeAtom {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::EvictNodeAtom) -> Result<Self, Self::Error> {
        Ok(Self {
            public_key: PublicKey::from_canonical_bytes(&value.public_key)
                .map_err(|e| anyhow!("EvictNodeAtom failed to decode public key: {e}"))?,
        })
    }
}

// -------------------------------- Decision -------------------------------- //

impl From<Decision> for proto::consensus::Decision {
    fn from(value: Decision) -> Self {
        proto::consensus::Decision {
            decision: Some(value.into()),
        }
    }
}

impl From<Decision> for proto::consensus::decision::Decision {
    fn from(value: Decision) -> Self {
        match value {
            Decision::Commit => Self::Commit(true),
            Decision::Abort(reason) => Self::Abort(proto::consensus::AbortReason::from(reason) as i32),
        }
    }
}

// -------------------------------- Abort reason -------------------------------- //
impl From<AbortReason> for proto::consensus::AbortReason {
    fn from(value: AbortReason) -> Self {
        match value {
            AbortReason::None => Self::None,
            AbortReason::TransactionAtomMustBeAbort => Self::TransactionAtomMustBeAbort,
            AbortReason::TransactionAtomMustBeCommit => Self::TransactionAtomMustBeCommit,
            AbortReason::InputLockConflict => Self::InputLockConflict,
            AbortReason::LockOutputsFailed => Self::LockOutputsFailed,
            AbortReason::LockInputsOutputsFailed => Self::LockInputsOutputsFailed,
            AbortReason::LockInputsFailed => Self::LockInputsFailed,
            AbortReason::InvalidTransaction => Self::InvalidTransaction,
            AbortReason::ExecutionFailure => Self::ExecutionFailure,
            AbortReason::OneOrMoreInputsNotFound => Self::OneOrMoreInputsNotFound,
            AbortReason::ForeignShardGroupDecidedToAbort => Self::ForeignShardGroupDecidedToAbort,
            AbortReason::ForeignPledgeInputConflict => Self::ForeignPledgeInputConflict,
            AbortReason::InsufficientFeesPaid => Self::InsufficientFeesPaid,
            AbortReason::EarlyAbort => Self::EarlyAbort,
        }
    }
}

impl From<proto::consensus::AbortReason> for AbortReason {
    fn from(proto_reason: proto::consensus::AbortReason) -> Self {
        match proto_reason {
            proto::consensus::AbortReason::None => Self::None,
            proto::consensus::AbortReason::TransactionAtomMustBeAbort => Self::TransactionAtomMustBeAbort,
            proto::consensus::AbortReason::TransactionAtomMustBeCommit => Self::TransactionAtomMustBeCommit,
            proto::consensus::AbortReason::InputLockConflict => Self::InputLockConflict,
            proto::consensus::AbortReason::LockInputsFailed => Self::LockInputsFailed,
            proto::consensus::AbortReason::LockOutputsFailed => Self::LockOutputsFailed,
            proto::consensus::AbortReason::LockInputsOutputsFailed => Self::LockInputsOutputsFailed,
            proto::consensus::AbortReason::InvalidTransaction => Self::InvalidTransaction,
            proto::consensus::AbortReason::ExecutionFailure => Self::ExecutionFailure,
            proto::consensus::AbortReason::OneOrMoreInputsNotFound => Self::OneOrMoreInputsNotFound,
            proto::consensus::AbortReason::ForeignShardGroupDecidedToAbort => Self::ForeignShardGroupDecidedToAbort,
            proto::consensus::AbortReason::ForeignPledgeInputConflict => Self::ForeignPledgeInputConflict,
            proto::consensus::AbortReason::InsufficientFeesPaid => Self::InsufficientFeesPaid,
            proto::consensus::AbortReason::EarlyAbort => Self::EarlyAbort,
        }
    }
}

impl TryFrom<proto::consensus::Decision> for Decision {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::Decision) -> Result<Self, Self::Error> {
        match value
            .decision
            .as_ref()
            .ok_or_else(|| anyhow!("Decision not provided"))?
        {
            proto::consensus::decision::Decision::Commit(_) => Ok(Decision::Commit),
            proto::consensus::decision::Decision::Abort(reason) => {
                let reason = proto::consensus::AbortReason::try_from(*reason)?;
                Ok(Decision::Abort(reason.into()))
            },
        }
    }
}

//---------------------------------- Evidence --------------------------------------------//

impl From<&Evidence> for proto::consensus::Evidence {
    fn from(value: &Evidence) -> Self {
        // TODO: we may want to write out the protobuf here
        Self {
            encoded_evidence: encode(value).unwrap(),
        }
    }
}

impl TryFrom<proto::consensus::Evidence> for Evidence {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::Evidence) -> Result<Self, Self::Error> {
        Ok(decode_exact(&value.encoded_evidence)?)
    }
}

// -------------------------------- QuorumCertificate -------------------------------- //

impl From<&QuorumCertificate> for proto::consensus::QuorumCertificate {
    fn from(source: &QuorumCertificate) -> Self {
        Self {
            header_hash: source.header_hash().as_bytes().to_vec(),
            parent_id: source.parent_id().as_bytes().to_vec(),
            block_height: source.block_height().as_u64(),
            epoch: source.epoch().as_u64(),
            shard_group: source.shard_group().encode_as_u32(),
            signatures: source.signatures().iter().map(Into::into).collect(),
            leaf_hashes: source.leaf_hashes().iter().map(|h| h.to_vec()).collect(),
            decision: i32::from(source.decision().as_u8()),
        }
    }
}

impl TryFrom<proto::consensus::QuorumCertificate> for QuorumCertificate {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::QuorumCertificate) -> Result<Self, Self::Error> {
        let shard_group = ShardGroup::decode_from_u32(value.shard_group)
            .ok_or_else(|| anyhow!("QC shard_group ({}) is not a valid", value.shard_group))?;
        Ok(Self::new(
            value.header_hash.try_into().context("header_hash")?,
            value.parent_id.try_into().context("parent_id")?,
            NodeHeight(value.block_height),
            Epoch(value.epoch),
            shard_group,
            value
                .signatures
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            value
                .leaf_hashes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            QuorumDecision::from_u8(u8::try_from(value.decision)?)
                .ok_or_else(|| anyhow!("Invalid Decision byte {}", value.decision))?,
        ))
    }
}

// -------------------------------- ValidatorMetadata -------------------------------- //

impl From<ValidatorMetadata> for proto::consensus::ValidatorMetadata {
    fn from(msg: ValidatorMetadata) -> Self {
        Self {
            public_key: msg.public_key.to_vec(),
            vn_shard_key: msg.vn_shard_key.as_bytes().to_vec(),
            signature: Some((&msg.signature).into()),
        }
    }
}

impl TryFrom<proto::consensus::ValidatorMetadata> for ValidatorMetadata {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::ValidatorMetadata) -> Result<Self, Self::Error> {
        Ok(ValidatorMetadata {
            public_key: ByteArray::from_canonical_bytes(&value.public_key).map_err(anyhow::Error::msg)?,
            vn_shard_key: value.vn_shard_key.try_into()?,
            signature: value
                .signature
                .map(TryFrom::try_from)
                .transpose()?
                .ok_or_else(|| anyhow!("ValidatorMetadata missing signature"))?,
        })
    }
}

// -------------------------------- Substate -------------------------------- //

impl TryFrom<proto::consensus::Substate> for SubstateRecord {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::Substate) -> Result<Self, Self::Error> {
        Ok(Self {
            substate_id: SubstateId::from_bytes(&value.substate_id)?,
            version: value.version,
            substate_value: Some(value.substate.as_slice())
                .filter(|d| !d.is_empty())
                .map(SubstateValue::from_bytes)
                .transpose()?,
            // TODO: Should we add this to the proto?
            state_hash: Default::default(),

            created_at_epoch: Epoch(value.created_epoch),
            created_by_transaction: value.created_transaction.try_into()?,
            created_justify: value.created_justify.try_into()?,
            created_block: value.created_block.try_into()?,
            created_height: NodeHeight(value.created_height),

            destroyed: value.destroyed.map(TryInto::try_into).transpose()?,
            created_by_shard: Shard::from(value.created_by_shard),
        })
    }
}

impl From<SubstateRecord> for proto::consensus::Substate {
    fn from(value: SubstateRecord) -> Self {
        Self {
            substate_id: value.substate_id.to_bytes(),
            version: value.version,
            substate: value.substate_value.as_ref().map(|s| s.to_bytes()).unwrap_or_default(),

            created_transaction: value.created_by_transaction.as_bytes().to_vec(),
            created_justify: value.created_justify.as_bytes().to_vec(),
            created_block: value.created_block.as_bytes().to_vec(),
            created_height: value.created_height.as_u64(),
            created_epoch: value.created_at_epoch.as_u64(),
            created_by_shard: value.created_by_shard.as_u32(),

            destroyed: value.destroyed.map(Into::into),
        }
    }
}

// -------------------------------- SubstateDestroyed -------------------------------- //
impl TryFrom<proto::consensus::SubstateDestroyed> for SubstateDestroyed {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::SubstateDestroyed) -> Result<Self, Self::Error> {
        Ok(Self {
            by_transaction: value.transaction.try_into()?,
            justify: value.justify.try_into()?,
            by_block: NodeHeight(value.block_height),
            at_epoch: value
                .epoch
                .map(Into::into)
                .ok_or_else(|| anyhow!("Epoch not provided"))?,
            by_shard: Shard::from(value.shard),
        })
    }
}

impl From<SubstateDestroyed> for proto::consensus::SubstateDestroyed {
    fn from(value: SubstateDestroyed) -> Self {
        Self {
            transaction: value.by_transaction.as_bytes().to_vec(),
            justify: value.justify.as_bytes().to_vec(),
            block_height: value.by_block.as_u64(),
            epoch: Some(value.at_epoch.into()),
            shard: value.by_shard.as_u32(),
        }
    }
}

// -------------------------------- SyncRequest -------------------------------- //

impl From<&SyncRequestMessage> for proto::consensus::SyncRequest {
    fn from(value: &SyncRequestMessage) -> Self {
        Self {
            high_qc: Some(proto::consensus::HighQc {
                block_id: value.high_qc.block_id.as_bytes().to_vec(),
                block_height: value.high_qc.block_height.as_u64(),
                epoch: value.high_qc.epoch.as_u64(),
                qc_id: value.high_qc.qc_id.as_bytes().to_vec(),
            }),
        }
    }
}

impl TryFrom<proto::consensus::SyncRequest> for SyncRequestMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::SyncRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            high_qc: value
                .high_qc
                .map(|value| {
                    Ok::<_, anyhow::Error>(HighQc {
                        block_id: BlockId::try_from(value.block_id)?,
                        block_height: NodeHeight(value.block_height),
                        epoch: Epoch(value.epoch),
                        qc_id: QcId::try_from(value.qc_id)?,
                    })
                })
                .transpose()?
                .ok_or_else(|| anyhow!("High QC not provided"))?,
        })
    }
}

// -------------------------------- SyncResponse -------------------------------- //

impl From<&SyncResponseMessage> for proto::consensus::SyncResponse {
    fn from(value: &SyncResponseMessage) -> Self {
        Self {
            epoch: value.epoch.as_u64(),
            blocks: value.blocks.iter().map(|block| block.into()).collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<proto::consensus::SyncResponse> for SyncResponseMessage {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::SyncResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: Epoch(value.epoch),
            blocks: value
                .blocks
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

// -------------------------------- FullBlock -------------------------------- //

impl From<&FullBlock> for proto::consensus::FullBlock {
    fn from(value: &FullBlock) -> Self {
        Self {
            block: Some((&value.block).into()),
            qcs: value.qcs.iter().map(Into::into).collect(),
            transactions: value.transactions.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::consensus::FullBlock> for FullBlock {
    type Error = anyhow::Error;

    fn try_from(value: proto::consensus::FullBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value.block.ok_or_else(|| anyhow!("Block is missing"))?.try_into()?,
            qcs: value.qcs.into_iter().map(TryInto::try_into).collect::<Result<_, _>>()?,
            transactions: value
                .transactions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}
