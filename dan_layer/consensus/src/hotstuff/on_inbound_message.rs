//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::collections::{BTreeMap, VecDeque};

use log::*;
use tari_dan_common_types::{Epoch, NodeAddressable, NodeHeight};

use crate::{
    hotstuff::error::HotStuffError,
    messages::HotstuffMessage,
    traits::{hooks::ConsensusHooks, ConsensusSpec, InboundMessaging},
};

const LOG_TARGET: &str = "tari::dan::consensus::hotstuff::inbound_messages";

type IncomingMessageResult<TAddr> = Result<Option<(TAddr, HotstuffMessage)>, HotStuffError>;

pub struct OnInboundMessage<TConsensusSpec: ConsensusSpec> {
    message_buffer: MessageBuffer<TConsensusSpec>,
    hooks: TConsensusSpec::Hooks,
}

impl<TConsensusSpec: ConsensusSpec> OnInboundMessage<TConsensusSpec> {
    pub fn new(inbound_messaging: TConsensusSpec::InboundMessaging, hooks: TConsensusSpec::Hooks) -> Self {
        Self {
            message_buffer: MessageBuffer::new(inbound_messaging),
            hooks,
        }
    }

    /// Returns the next message that is ready for consensus. The future returned from this function is cancel safe, and
    /// can be used with tokio::select! macro.
    pub async fn next_message(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
    ) -> Option<Result<(TConsensusSpec::Addr, HotstuffMessage), HotStuffError>> {
        // Then incoming messages for the current epoch/height
        let result = self.message_buffer.next(current_epoch, current_height).await;
        match result {
            Ok(Some((from, msg))) => {
                self.hooks.on_message_received(&msg);
                Some(Ok((from, msg)))
            },
            Ok(None) => {
                // Inbound messages terminated
                None
            },
            Err(err) => Some(Err(err)),
        }
    }

    /// Discards all buffered messages including ones queued up for processing and returns when complete.
    pub async fn discard(&mut self) {
        self.message_buffer.discard().await;
    }

    pub fn clear_buffer(&mut self) {
        self.message_buffer.clear_buffer();
    }
}

type EpochAndHeight = (Epoch, NodeHeight);
pub struct MessageBuffer<TConsensusSpec: ConsensusSpec> {
    buffer: BTreeMap<EpochAndHeight, VecDeque<(TConsensusSpec::Addr, HotstuffMessage)>>,
    inbound_messaging: TConsensusSpec::InboundMessaging,
}

impl<TConsensusSpec: ConsensusSpec> MessageBuffer<TConsensusSpec> {
    pub fn new(inbound_messaging: TConsensusSpec::InboundMessaging) -> Self {
        Self {
            buffer: BTreeMap::new(),
            inbound_messaging,
        }
    }

    pub async fn next(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
    ) -> IncomingMessageResult<TConsensusSpec::Addr> {
        // We listen for messages for the next view
        let next_height = current_height + NodeHeight(1);
        // Clear buffer with lower (epoch, heights)
        self.buffer = self.buffer.split_off(&(current_epoch, next_height));

        // Check if message is in the buffer
        if let Some(buffer) = self.buffer.get_mut(&(current_epoch, next_height)) {
            if let Some(msg_tuple) = buffer.pop_front() {
                return Ok(Some(msg_tuple));
            }
        }

        while let Some(result) = self.inbound_messaging.next_message().await {
            let (from, msg) = result?;

            // If we receive an FP that is greater than our current epoch, we buffer it
            if let HotstuffMessage::ForeignProposal(ref m) = msg {
                if m.justify_qc.epoch() > current_epoch {
                    self.push_to_buffer(m.justify_qc.epoch(), NodeHeight::zero(), from, msg);
                    continue;
                }
            }

            match msg_epoch_and_height(&msg) {
                // Discard old message
                Some((e, h)) if e < current_epoch || (e == current_epoch && h < next_height) => {
                    info!(target: LOG_TARGET, "🗑️ Discard message {} is for previous view {}/{}. Current view {}/{}", msg, e, h, current_epoch, next_height);
                    continue;
                },
                // Buffer message for future epoch/height
                Some((epoch, height)) if epoch == current_epoch && height > next_height => {
                    if msg.proposal().is_some() {
                        info!(target: LOG_TARGET, "🦴Proposal {msg} is for future view (Current view: {current_epoch}, {current_height})");
                    } else {
                        info!(target: LOG_TARGET, "🦴Message {msg} is for future view (Current view: {current_epoch}, {current_height})");
                    }
                    self.push_to_buffer(epoch, height, from, msg);
                    continue;
                },
                Some((epoch, height)) if epoch > current_epoch => {
                    warn!(target: LOG_TARGET, "⚠️ Message {msg} is for future epoch {epoch}. Current epoch {current_epoch}");
                    if matches!(&msg, HotstuffMessage::Vote(_)) {
                        // Buffer VOTE messages. As it does not contain a QC we can use to prove that a BFT-majority has
                        // reached the epoch
                        self.push_to_buffer(epoch, height, from, msg);
                        continue;
                    }
                    // Return the message, it will be validated and if valid, will kick consensus into sync
                    return Ok(Some((from, msg)));
                },
                // Height is irrelevant or current, return message
                _ => return Ok(Some((from, msg))),
            }
        }

        info!(
            target: LOG_TARGET,
            "Inbound messaging has terminated. Current view: {}/{}", current_epoch, next_height
        );
        // Inbound messaging has terminated
        Ok(None)
    }

    pub async fn discard(&mut self) {
        self.clear_buffer();
        while self.inbound_messaging.next_message().await.is_some() {}
    }

    pub fn clear_buffer(&mut self) {
        self.buffer.clear();
    }

    fn push_to_buffer(&mut self, epoch: Epoch, height: NodeHeight, from: TConsensusSpec::Addr, msg: HotstuffMessage) {
        self.buffer.entry((epoch, height)).or_default().push_back((from, msg));
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Needs sync: local height {local_height} is less than remote QC height {qc_height} from {from}")]
pub struct NeedsSync<TAddr: NodeAddressable> {
    pub from: TAddr,
    pub local_height: NodeHeight,
    pub qc_height: NodeHeight,
    pub remote_epoch: Epoch,
    pub local_epoch: Epoch,
}

fn msg_epoch_and_height(msg: &HotstuffMessage) -> Option<EpochAndHeight> {
    match msg {
        HotstuffMessage::Proposal(msg) => Some((msg.block.epoch(), msg.block.height())),
        // Votes for block v occur in view v + 1
        HotstuffMessage::Vote(msg) => Some((msg.epoch, msg.unverified_block_height.saturating_add(NodeHeight(1)))),
        _ => None,
    }
}
