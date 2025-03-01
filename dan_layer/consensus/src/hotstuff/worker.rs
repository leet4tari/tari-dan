//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    fmt::{Debug, Formatter},
    iter,
};

use log::*;
use tari_common_types::types::{FixedHash, PublicKey};
use tari_dan_common_types::{
    committee::{Committee, CommitteeInfo},
    optional::Optional,
    Epoch,
    NodeHeight,
    ShardGroup,
};
use tari_dan_storage::{
    consensus_models::{
        Block,
        BlockDiff,
        BurntUtxo,
        EpochCheckpoint,
        ForeignProposal,
        HighQc,
        LeafBlock,
        TransactionPool,
        TransactionRecord,
    },
    StateStore,
};
use tari_epoch_manager::{EpochManagerEvent, EpochManagerReader};
use tari_shutdown::ShutdownSignal;
use tari_state_tree::SPARSE_MERKLE_PLACEHOLDER_HASH;
use tari_transaction::{Transaction, TransactionId};
use tokio::sync::{broadcast, mpsc};

use super::{
    calculate_last_dummy_block,
    config::HotstuffConfig,
    on_receive_new_transaction::OnReceiveNewTransaction,
    ProposalValidationError,
};
use crate::{
    hotstuff::{
        error::HotStuffError,
        event::HotstuffEvent,
        on_catch_up_sync::OnCatchUpSync,
        on_catch_up_sync_request::OnSyncRequest,
        on_inbound_message::OnInboundMessage,
        on_message_validate::{MessageValidationResult, OnMessageValidate},
        on_next_sync_view::OnNextSyncViewHandler,
        on_propose::OnPropose,
        on_receive_foreign_proposal::OnReceiveForeignProposalHandler,
        on_receive_local_proposal::OnReceiveLocalProposalHandler,
        on_receive_new_view::OnReceiveNewViewHandler,
        on_receive_request_missing_transactions::OnReceiveRequestMissingTransactions,
        on_receive_vote::OnReceiveVoteHandler,
        pacemaker::PaceMaker,
        pacemaker_handle::PaceMakerHandle,
        transaction_manager::ConsensusTransactionManager,
        vote_collector::VoteCollector,
    },
    messages::{HotstuffMessage, ProposalMessage},
    tracing::TraceTimer,
    traits::{hooks::ConsensusHooks, ConsensusSpec, LeaderStrategy},
};

const LOG_TARGET: &str = "tari::dan::consensus::hotstuff::worker";

pub struct HotstuffWorker<TConsensusSpec: ConsensusSpec> {
    local_validator_addr: TConsensusSpec::Addr,

    config: HotstuffConfig,
    hooks: TConsensusSpec::Hooks,

    tx_events: broadcast::Sender<HotstuffEvent>,
    rx_new_transactions: mpsc::Receiver<(Transaction, usize)>,
    rx_missing_transactions: mpsc::UnboundedReceiver<Vec<TransactionId>>,

    on_inbound_message: OnInboundMessage<TConsensusSpec>,
    on_next_sync_view: OnNextSyncViewHandler<TConsensusSpec>,
    on_receive_local_proposal: OnReceiveLocalProposalHandler<TConsensusSpec>,
    on_receive_foreign_proposal: OnReceiveForeignProposalHandler<TConsensusSpec>,
    on_receive_vote: OnReceiveVoteHandler<TConsensusSpec>,
    on_receive_new_view: OnReceiveNewViewHandler<TConsensusSpec>,
    on_receive_request_missing_txs: OnReceiveRequestMissingTransactions<TConsensusSpec>,
    on_receive_new_transaction: OnReceiveNewTransaction<TConsensusSpec>,
    on_message_validate: OnMessageValidate<TConsensusSpec>,
    on_propose: OnPropose<TConsensusSpec>,
    on_sync_request: OnSyncRequest<TConsensusSpec>,
    on_catch_up_sync: OnCatchUpSync<TConsensusSpec>,

    state_store: TConsensusSpec::StateStore,
    leader_strategy: TConsensusSpec::LeaderStrategy,
    transaction_pool: TransactionPool<TConsensusSpec::StateStore>,

    epoch_manager: TConsensusSpec::EpochManager,
    pacemaker_worker: Option<PaceMaker>,
    pacemaker: PaceMakerHandle,
    shutdown: ShutdownSignal,
}
impl<TConsensusSpec: ConsensusSpec> HotstuffWorker<TConsensusSpec> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: HotstuffConfig,
        local_validator_addr: TConsensusSpec::Addr,
        inbound_messaging: TConsensusSpec::InboundMessaging,
        outbound_messaging: TConsensusSpec::OutboundMessaging,
        rx_new_transactions: mpsc::Receiver<(Transaction, usize)>,
        state_store: TConsensusSpec::StateStore,
        epoch_manager: TConsensusSpec::EpochManager,
        leader_strategy: TConsensusSpec::LeaderStrategy,
        signing_service: TConsensusSpec::SignatureService,
        transaction_pool: TransactionPool<TConsensusSpec::StateStore>,
        transaction_executor: TConsensusSpec::TransactionExecutor,
        tx_events: broadcast::Sender<HotstuffEvent>,
        hooks: TConsensusSpec::Hooks,
        shutdown: ShutdownSignal,
    ) -> Self {
        let (tx_missing_transactions, rx_missing_transactions) = mpsc::unbounded_channel();
        let pacemaker = PaceMaker::new(config.consensus_constants.pacemaker_block_time);
        let vote_receiver = VoteCollector::new(
            config.network,
            state_store.clone(),
            epoch_manager.clone(),
            signing_service.clone(),
        );
        let transaction_manager = ConsensusTransactionManager::new(transaction_executor.clone());

        Self {
            local_validator_addr: local_validator_addr.clone(),

            config: config.clone(),
            tx_events: tx_events.clone(),
            rx_new_transactions,
            rx_missing_transactions,

            on_inbound_message: OnInboundMessage::new(inbound_messaging, hooks.clone()),
            on_message_validate: OnMessageValidate::new(
                config.clone(),
                state_store.clone(),
                epoch_manager.clone(),
                pacemaker.clone_handle().current_view().clone(),
                leader_strategy.clone(),
                signing_service.clone(),
                outbound_messaging.clone(),
                tx_events.clone(),
            ),

            on_next_sync_view: OnNextSyncViewHandler::new(
                state_store.clone(),
                outbound_messaging.clone(),
                leader_strategy.clone(),
                pacemaker.clone_handle(),
            ),
            on_receive_local_proposal: OnReceiveLocalProposalHandler::new(
                state_store.clone(),
                epoch_manager.clone(),
                leader_strategy.clone(),
                pacemaker.clone_handle(),
                outbound_messaging.clone(),
                signing_service.clone(),
                transaction_pool.clone(),
                tx_events,
                transaction_manager.clone(),
                config.clone(),
                hooks.clone(),
            ),
            on_receive_foreign_proposal: OnReceiveForeignProposalHandler::new(
                state_store.clone(),
                epoch_manager.clone(),
                pacemaker.clone_handle(),
                outbound_messaging.clone(),
            ),
            on_receive_vote: OnReceiveVoteHandler::new(pacemaker.clone_handle(), vote_receiver.clone()),
            on_receive_new_view: OnReceiveNewViewHandler::new(
                local_validator_addr,
                state_store.clone(),
                leader_strategy.clone(),
                pacemaker.clone_handle(),
                vote_receiver,
            ),
            on_receive_request_missing_txs: OnReceiveRequestMissingTransactions::new(
                state_store.clone(),
                outbound_messaging.clone(),
            ),
            on_receive_new_transaction: OnReceiveNewTransaction::new(
                state_store.clone(),
                transaction_pool.clone(),
                transaction_executor.clone(),
                tx_missing_transactions,
            ),
            on_propose: OnPropose::new(
                config,
                state_store.clone(),
                epoch_manager.clone(),
                transaction_pool.clone(),
                transaction_manager,
                signing_service,
                outbound_messaging.clone(),
            ),

            on_sync_request: OnSyncRequest::new(state_store.clone(), outbound_messaging.clone()),
            on_catch_up_sync: OnCatchUpSync::new(state_store.clone(), pacemaker.clone_handle(), outbound_messaging),

            state_store,
            leader_strategy,
            epoch_manager,
            transaction_pool,

            pacemaker: pacemaker.clone_handle(),
            pacemaker_worker: Some(pacemaker),
            hooks,
            shutdown,
        }
    }

    pub fn pacemaker(&self) -> &PaceMakerHandle {
        &self.pacemaker
    }

    pub async fn start(&mut self) -> Result<(), HotStuffError> {
        let current_epoch = self.epoch_manager.current_epoch().await?;
        let local_committee_info = self.epoch_manager.get_local_committee_info(current_epoch).await?;

        self.create_genesis_block_if_required(current_epoch, local_committee_info.shard_group())?;

        // Resume pacemaker from the last epoch/height
        let (current_height, high_qc) = self.state_store.with_read_tx(|tx| {
            let leaf = LeafBlock::get(tx, current_epoch)?;
            let high_qc = HighQc::get(tx, leaf.epoch())?;
            Ok::<_, HotStuffError>((leaf.height(), high_qc))
        })?;

        info!(
            target: LOG_TARGET,
            "🚀 Pacemaker starting for epoch {}, height: {}, high_qc: {}",
            current_epoch,
            current_height,
            high_qc
        );

        self.pacemaker
            .start(current_epoch, current_height, high_qc.block_height())
            .await?;
        self.publish_event(HotstuffEvent::EpochChanged {
            epoch: current_epoch,
            registered_shard_group: Some(local_committee_info.shard_group()),
        });

        let local_committee = self.epoch_manager.get_local_committee(current_epoch).await?;
        self.run(local_committee_info, local_committee).await?;
        Ok(())
    }

    async fn run(
        &mut self,
        mut local_committee_info: CommitteeInfo,
        mut local_committee: Committee<TConsensusSpec::Addr>,
    ) -> Result<(), HotStuffError> {
        // Spawn pacemaker if not spawned already
        if let Some(pm) = self.pacemaker_worker.take() {
            pm.spawn();
        }

        let mut on_beat = self.pacemaker.get_on_beat();
        let mut on_force_beat = self.pacemaker.get_on_force_beat();
        let mut on_leader_timeout = self.pacemaker.get_on_leader_timeout();

        let mut epoch_manager_events = self.epoch_manager.subscribe();

        let mut prev_height = self.pacemaker.current_view().get_height();
        let current_epoch = self.pacemaker.current_view().get_epoch();
        self.request_initial_catch_up_sync(current_epoch).await?;
        let mut prev_epoch = current_epoch;
        let mut local_claim_public_key = self
            .epoch_manager
            .get_our_validator_node(current_epoch)
            .await?
            .fee_claim_public_key;

        loop {
            let current_height = self.pacemaker.current_view().get_height();
            let current_epoch = self.pacemaker.current_view().get_epoch();

            // Need to update local committee info if the epoch has changed
            if prev_epoch != current_epoch {
                local_committee_info = self.epoch_manager.get_local_committee_info(current_epoch).await?;
                local_committee = self.epoch_manager.get_local_committee(current_epoch).await?;
                local_claim_public_key = self
                    .epoch_manager
                    .get_our_validator_node(current_epoch)
                    .await?
                    .fee_claim_public_key;
                prev_epoch = current_epoch;
            }

            if current_height != prev_height {
                self.hooks.on_pacemaker_height_changed(current_height);
                prev_height = current_height;
            }

            debug!(
                target: LOG_TARGET,
                "🔥 {} Current height #{}",
                self.local_validator_addr,
                current_height.as_u64()
            );

            tokio::select! {
                Ok(event) = epoch_manager_events.recv() => {
                    self.on_epoch_manager_event(event).await?;
                },

                forced_height = on_force_beat.wait() => {
                    if let Err(e) = self.on_force_beat(current_epoch, forced_height, &local_committee_info, &local_committee, &local_claim_public_key).await {
                        self.on_failure("propose_if_leader", &e).await;
                        return Err(e);
                    }
                },

                _ = on_beat.wait() => {
                    if let Err(e) = self.on_beat(current_epoch,  &local_committee_info, &local_committee, &local_claim_public_key).await {
                        self.on_failure("on_beat", &e).await;
                        return Err(e);
                    }
                },

                Some((tx_id, pending)) = self.rx_new_transactions.recv() => {
                    if let Err(err) = self.on_new_transaction(tx_id, pending, current_epoch, current_height, &local_committee_info, &local_committee).await {
                        self.hooks.on_error(&err);
                        error!(target: LOG_TARGET, "🚨Error handling new transaction: {}", err);
                    }
                },

                Some(result) = self.on_inbound_message.next_message(current_epoch, current_height) => {
                    if let Err(e) = self.on_unvalidated_message(current_epoch, current_height, result, &local_committee_info, &local_committee).await {
                        self.on_failure("on_unvalidated_message", &e).await;
                        return Err(e);
                    }
                },

               // TODO: This channel is used to work around some design-flaws in missing transactions handling.
                //       We cannot simply call check_if_block_can_be_unparked in dispatch_hotstuff_message as that creates a cycle.
                //       One suggestion is to refactor consensus to emit events (kinda like libp2p does) and handle those events.
                //       This should be easy to reason about and avoid a large depth of async calls and "callback channels".
                Some(batch) = self.rx_missing_transactions.recv() => {
                    if let Err(err) = self.check_if_block_can_be_unparked(current_epoch, current_height, batch.iter(), &local_committee_info, &local_committee).await {
                        self.hooks.on_error(&err);
                        error!(target: LOG_TARGET, "🚨Error handling missing transaction: {}", err);
                    }
                },

                _ = on_leader_timeout.wait() => {
                    if let Err(e) = self.on_leader_timeout(current_epoch, current_height,  &local_committee).await {
                        self.on_failure("on_leader_timeout", &e).await;
                        return Err(e);
                    }
                },

                _ = self.shutdown.wait() => {
                    info!(target: LOG_TARGET, "💤 Shutting down");
                    break;
                }
            }
        }

        self.on_receive_new_view.clear_new_views();
        self.on_inbound_message.clear_buffer();
        // This only happens if we're shutting down.
        if let Err(err) = self.pacemaker.stop().await {
            debug!(target: LOG_TARGET, "Pacemaker channel dropped: {}", err);
        }

        Ok(())
    }

    async fn on_unvalidated_message(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
        result: Result<(TConsensusSpec::Addr, HotstuffMessage), HotStuffError>,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
    ) -> Result<(), HotStuffError> {
        let (from, msg) = result?;

        match self
            .on_message_validate
            .handle(current_height, local_committee_info, local_committee, from.clone(), msg)
            .await?
        {
            MessageValidationResult::Ready { from, message: msg } => {
                if let Err(e) = self
                    .dispatch_hotstuff_message(
                        current_epoch,
                        current_height,
                        from,
                        msg,
                        local_committee_info,
                        local_committee,
                    )
                    .await
                {
                    self.on_failure("on_unvalidated_message -> dispatch_hotstuff_message", &e)
                        .await;
                    return Err(e);
                }
                Ok(())
            },
            MessageValidationResult::ParkedProposal {
                epoch,
                missing_txs,
                block_id,
                ..
            } => {
                let mut request_from_address = from;
                if request_from_address == self.local_validator_addr {
                    // Edge case: If we're catching up, we could be the proposer but we no longer have
                    // the transaction (we deleted our database likely during development testing).
                    // In this case, request from another random VN.
                    // (TODO: not 100% reliable since we're just asking a single random committee member)
                    let mut local_committee = self.epoch_manager.get_local_committee(epoch).await?;

                    local_committee.shuffle();
                    match local_committee
                        .into_iter()
                        .find(|(addr, _)| *addr != self.local_validator_addr)
                    {
                        Some((addr, _)) => {
                            warn!(
                                target: LOG_TARGET,
                                "⚠️Requesting missing transactions from another validator {addr} because we are (presumably) catching up (local_peer_id = {})",
                                self.local_validator_addr,
                            );
                            request_from_address = addr;
                        },
                        None => {
                            warn!(
                                target: LOG_TARGET,
                                "❌NEVERHAPPEN: We're the only validator in the committee but we need to request missing transactions."
                            );
                            return Ok(());
                        },
                    }
                }

                self.on_message_validate
                    .request_missing_transactions(request_from_address, block_id, epoch, missing_txs)
                    .await?;
                Ok(())
            },
            MessageValidationResult::Discard => Ok(()),
            // In these cases, we want to propagate the error back to the state machine, to allow sync
            MessageValidationResult::Invalid {
                err: err @ HotStuffError::FallenBehind { .. },
                ..
            } |
            MessageValidationResult::Invalid {
                err: err @ HotStuffError::ProposalValidationError(ProposalValidationError::FutureEpoch { .. }),
                ..
            } => {
                self.hooks.on_error(&err);
                Err(err)
            },
            MessageValidationResult::Invalid { err, from, message } => {
                self.hooks.on_error(&err);
                error!(target: LOG_TARGET, "🚨 Invalid new message from {from}: {err} - {message}");
                Ok(())
            },
        }
    }

    async fn on_new_transaction(
        &mut self,
        transaction: Transaction,
        num_pending_txs: usize,
        current_epoch: Epoch,
        current_height: NodeHeight,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
    ) -> Result<(), HotStuffError> {
        let _timer = TraceTimer::info(LOG_TARGET, "on_new_transaction");
        let maybe_transaction = self.on_receive_new_transaction.try_sequence_transaction(
            current_epoch,
            TransactionRecord::new(transaction),
            local_committee_info,
        )?;

        let Some(transaction) = maybe_transaction else {
            return Ok(());
        };

        info!(
            target: LOG_TARGET,
            "🔥 new transaction ready for consensus: {} ({} pending)",
            transaction.id(),
            num_pending_txs,
        );

        self.hooks.on_transaction_ready(transaction.id());

        if self
            .check_if_block_can_be_unparked(
                current_epoch,
                current_height,
                iter::once(transaction.id()),
                local_committee_info,
                local_committee,
            )
            .await?
        {
            // No need to call on_beat, a block was unparked so on_beat will be called as needed
            return Ok(());
        }

        // There are num_pending_txs transactions in the queue. If we have no pending transactions, we'll propose now if
        // able.
        if num_pending_txs == 0 {
            self.pacemaker.beat();
        }

        Ok(())
    }

    /// Returns true if a block was unparked, otherwise false
    async fn check_if_block_can_be_unparked<
        'a,
        I: IntoIterator<Item = &'a TransactionId> + ExactSizeIterator + Clone,
    >(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
        transaction_ids: I,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
    ) -> Result<bool, HotStuffError> {
        let (local_proposals, foreign_proposals) = self
            .on_message_validate
            .update_local_parked_blocks(current_height, transaction_ids)?;

        let is_any_block_unparked = !local_proposals.is_empty() || !foreign_proposals.is_empty();

        for msg in foreign_proposals {
            if let Err(e) = self
                .on_receive_foreign_proposal
                .handle_received(msg, local_committee_info)
                .await
            {
                self.on_failure("check_if_block_can_be_unparked -> on_receive_foreign_proposal", &e)
                    .await;
                return Err(e);
            }
        }

        for msg in local_proposals {
            if let Err(e) = self
                .on_proposal_message(
                    current_epoch,
                    current_height,
                    local_committee_info,
                    local_committee,
                    msg,
                )
                .await
            {
                self.on_failure("check_if_block_can_be_unparked -> on_proposal_message", &e)
                    .await;
                return Err(e);
            }
        }

        Ok(is_any_block_unparked)
    }

    async fn on_epoch_manager_event(&mut self, event: EpochManagerEvent) -> Result<(), HotStuffError> {
        match event {
            EpochManagerEvent::EpochChanged {
                epoch,
                registered_shard_group,
            } => {
                if registered_shard_group.is_none() {
                    info!(
                        target: LOG_TARGET,
                        "💤 This validator is not registered for epoch {}. Going to sleep.", epoch
                    );
                    return Err(HotStuffError::NotRegisteredForCurrentEpoch { epoch });
                }
                info!(
                    target: LOG_TARGET,
                    "🌟 This validator is registered for epoch {}.", epoch
                );

                // Edge case: we have started a VN and have progressed a few epochs quickly and have no blocks in
                // previous epochs to update the current view. This only really applies when mining is
                // instant (localnet)
                // let leaf_block = self.state_store.with_read_tx(|tx| LeafBlock::get(tx))?;
                // if leaf_block.block_id.is_zero() {
                //     self.pacemaker.set_epoch(epoch).await?;
                // }

                // If we can propose a block end, let's not wait for the block time to do it
                // self.pacemaker.beat();
            },
        }

        Ok(())
    }

    async fn request_initial_catch_up_sync(&mut self, current_epoch: Epoch) -> Result<(), HotStuffError> {
        let mut committee = self.epoch_manager.get_local_committee(current_epoch).await?;
        committee.shuffle();
        for (member, _) in committee {
            if member != self.local_validator_addr {
                self.on_catch_up_sync.request_sync(current_epoch, member).await?;
                break;
            }
        }
        Ok(())
    }

    async fn on_failure(&mut self, context: &str, err: &HotStuffError) {
        self.hooks.on_error(err);
        self.publish_event(HotstuffEvent::Failure {
            message: err.to_string(),
        });
        error!(target: LOG_TARGET, "Error ({}): {}", context, err);
        if let Err(e) = self.pacemaker.stop().await {
            error!(target: LOG_TARGET, "Error while stopping pacemaker: {}", e);
        }
        self.on_receive_new_view.clear_new_views();
        self.on_inbound_message.clear_buffer();
    }

    /// Read and discard messages. This should be used only when consensus is inactive.
    pub async fn discard_messages(&mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.wait() => {
                    break;
                },
                _ = self.on_inbound_message.discard() => {},
                _ = self.rx_new_transactions.recv() => {}
            }
        }
    }

    async fn on_leader_timeout(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
        local_committee: &Committee<TConsensusSpec::Addr>,
    ) -> Result<(), HotStuffError> {
        self.hooks.on_leader_timeout(current_height);
        info!(target: LOG_TARGET, "⚠️ {} Leader failure: NEXTSYNCVIEW for epoch {} and current height {}", self.local_validator_addr, current_epoch, current_height);
        self.on_next_sync_view
            .handle(current_epoch, current_height, local_committee)
            .await?;
        self.publish_event(HotstuffEvent::LeaderTimeout { height: current_height });
        Ok(())
    }

    /// Called when it may be time to propose if this node is the leader for the current leaf block.
    async fn on_beat(
        &mut self,
        epoch: Epoch,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
        local_claim_public_key: &PublicKey,
    ) -> Result<(), HotStuffError> {
        let leaf_block = self.state_store.with_read_tx(|tx| LeafBlock::get(tx, epoch))?;
        let next_height = leaf_block.height() + NodeHeight(1);
        if !self
            .leader_strategy
            .is_leader(&self.local_validator_addr, local_committee, next_height)
        {
            debug!(
                target: LOG_TARGET,
                "🔥 [on_beat] {} Not leader for height ({})",
                self.local_validator_addr,
                next_height,
            );
            return Ok(());
        }

        info!(
            target: LOG_TARGET,
            "🔥 [on_beat] {} Local node is leader for height ({}), num local members: {}, {}",
            self.local_validator_addr,
            next_height,
            local_committee.len(),
            local_committee_info.shard_group()
        );

        let propose_now = self.state_store.with_read_tx(|tx| {
            // Propose quickly if there are UTXOs to mint or transactions to propose
            let propose_now = ForeignProposal::has_unconfirmed(tx, epoch)? ||
                BurntUtxo::has_unproposed(tx)? ||
                self.transaction_pool
                    .has_ready_or_pending_transaction_updates(tx, leaf_block.block_id())?;

            Ok::<_, HotStuffError>(propose_now)
        })?;

        if !propose_now {
            let current_epoch = self.epoch_manager.current_epoch().await?;
            // Propose quickly if we should end the epoch (i.e base layer epoch > pacemaker epoch)
            if current_epoch == epoch {
                info!(target: LOG_TARGET, "[on_beat] No transactions to propose. Waiting for a timeout.");
                return Ok(());
            }
        }

        self.propose_now(
            epoch,
            next_height,
            local_committee_info,
            local_committee,
            local_claim_public_key,
        )
        .await?;

        Ok(())
    }

    /// Called when the block time expires (forced_height == None) or when NEWVIEW quorum is reached (forced_height ==
    /// Some(_))
    async fn on_force_beat(
        &mut self,
        epoch: Epoch,
        forced_height: Option<NodeHeight>,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
        local_claim_public_key: &PublicKey,
    ) -> Result<(), HotStuffError> {
        let next_height = match forced_height {
            Some(height) => {
                debug!(target: LOG_TARGET, "🔥 [force_beat] {} forced {height}", self.local_validator_addr);
                height + NodeHeight(1)
            },
            None => {
                let leaf_block = self.state_store.with_read_tx(|tx| LeafBlock::get(tx, epoch))?;
                leaf_block.height() + NodeHeight(1)
            },
        };
        let is_leader = self
            .leader_strategy
            .is_leader(&self.local_validator_addr, local_committee, next_height);

        if !is_leader {
            debug!(
                target: LOG_TARGET,
                "🔥 [force_beat] {} Not leader for next height ({}), local_committee: {}",
                self.local_validator_addr,
                next_height,
                local_committee
                    .len(),
            );
            return Ok(());
        }

        info!(
            target: LOG_TARGET,
            "🔥 [force_beat] {} Local node is leader for next height ({}), local_committee: {}",
            self.local_validator_addr,
            next_height,
            local_committee
                .len(),
        );

        self.propose_now(
            epoch,
            next_height,
            local_committee_info,
            local_committee,
            local_claim_public_key,
        )
        .await?;

        Ok(())
    }

    async fn propose_now(
        &mut self,
        epoch: Epoch,
        next_height: NodeHeight,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
        local_claim_public_key: &PublicKey,
    ) -> Result<(), HotStuffError> {
        let mut leaf_block = self.state_store.with_read_tx(|tx| LeafBlock::get(tx, epoch))?;
        if next_height > leaf_block.height + NodeHeight(1) {
            let (high_qc, block) = self.state_store.with_read_tx(|tx| {
                let high_qc = HighQc::get(tx, epoch)?.get_quorum_certificate(tx)?;
                let block = leaf_block.get_block(tx)?;
                Ok::<_, HotStuffError>((high_qc, block))
            })?;

            info!(
                target: LOG_TARGET,
                "⚠️ Next height is {next_height} but leaf_block is {leaf_block} due to leader failure. Proposing with dummy blocks to fill the gap.",
            );

            if let Some(dummy) = calculate_last_dummy_block(
                leaf_block.height,
                next_height,
                self.config.network,
                epoch,
                block.shard_group(),
                *block.id(),
                &high_qc,
                *block.state_merkle_root(),
                &self.leader_strategy,
                local_committee,
                block.timestamp(),
                block.base_layer_block_height(),
                *block.base_layer_block_hash(),
            ) {
                leaf_block = dummy;
            }
        }

        let current_epoch = self.epoch_manager.current_epoch().await?;
        let propose_epoch_end = current_epoch > epoch;

        self.on_propose
            .handle(
                epoch,
                next_height,
                local_committee,
                *local_committee_info,
                local_claim_public_key,
                leaf_block,
                propose_epoch_end,
            )
            .await?;

        Ok(())
    }

    async fn dispatch_hotstuff_message(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
        from: TConsensusSpec::Addr,
        msg: HotstuffMessage,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
    ) -> Result<(), HotStuffError> {
        match msg {
            HotstuffMessage::NewView(message) => log_err(
                "on_receive_new_view",
                self.on_receive_new_view
                    .handle(
                        current_epoch,
                        current_height,
                        from,
                        message,
                        local_committee_info,
                        local_committee,
                    )
                    .await,
            ),
            HotstuffMessage::Proposal(msg) => log_err(
                "on_receive_local_proposal",
                self.on_proposal_message(
                    current_epoch,
                    current_height,
                    local_committee_info,
                    local_committee,
                    msg,
                )
                .await,
            ),
            HotstuffMessage::ForeignProposal(msg) => log_err(
                "on_receive_foreign_proposal (received)",
                self.on_receive_foreign_proposal
                    .handle_received(msg, local_committee_info)
                    .await,
            ),
            HotstuffMessage::ForeignProposalNotification(msg) => log_err(
                "on_receive_foreign_proposal (notification)",
                self.on_receive_foreign_proposal
                    .handle_notification_received(from, current_epoch, msg, local_committee_info)
                    .await,
            ),
            HotstuffMessage::ForeignProposalRequest(msg) => log_err(
                "on_receive_foreign_proposal (request)",
                self.on_receive_foreign_proposal.handle_requested(from, msg).await,
            ),
            HotstuffMessage::Vote(msg) => log_err(
                "on_receive_vote",
                self.on_receive_vote
                    .handle(from, current_epoch, msg, local_committee_info)
                    .await,
            ),
            HotstuffMessage::MissingTransactionsRequest(msg) => log_err(
                "on_receive_request_missing_transactions",
                self.on_receive_request_missing_txs.handle(from, msg).await,
            ),
            HotstuffMessage::MissingTransactionsResponse(msg) => log_err(
                "on_receive_new_transaction",
                self.on_receive_new_transaction
                    .process_requested(current_epoch, from, msg, local_committee_info)
                    .await,
            ),
            HotstuffMessage::CatchUpSyncRequest(msg) => {
                self.on_sync_request
                    .handle(from, *local_committee_info, current_epoch, msg);
                Ok(())
            },
            HotstuffMessage::SyncResponse(_) => {
                warn!(
                    target: LOG_TARGET,
                    "⚠️ Ignoring unrequested SyncResponse from {}",from
                );
                Ok(())
            },
        }
    }

    async fn on_proposal_message(
        &mut self,
        current_epoch: Epoch,
        current_height: NodeHeight,
        local_committee_info: &CommitteeInfo,
        local_committee: &Committee<TConsensusSpec::Addr>,
        msg: ProposalMessage,
    ) -> Result<(), HotStuffError> {
        let proposed_by = msg.block.proposed_by().clone();
        match log_err(
            "on_receive_local_proposal",
            self.on_receive_local_proposal
                .handle(current_epoch, local_committee_info, local_committee, msg)
                .await,
        ) {
            Ok(true) => Ok(()),
            Ok(false) => {
                // We decided NOVOTE, so we immediately send a NEWVIEW
                self.on_leader_timeout(current_epoch, current_height, local_committee)
                    .await
            },
            Err(err @ HotStuffError::ProposalValidationError(ProposalValidationError::JustifyBlockNotFound { .. })) => {
                let vn = self
                    .epoch_manager
                    .get_validator_node_by_public_key(current_epoch, proposed_by)
                    .await?;
                warn!(
                    target: LOG_TARGET,
                    "⚠️This node has fallen behind due to a missing justified block: {err}"
                );
                self.on_catch_up_sync.request_sync(current_epoch, vn.address).await?;
                Ok(())
            },
            Err(err) => Err(err),
        }
    }

    fn create_genesis_block_if_required(&self, epoch: Epoch, shard_group: ShardGroup) -> Result<(), HotStuffError> {
        self.state_store.with_write_tx(|tx| {
            let previous_epoch = epoch.saturating_sub(Epoch(1));
            let checkpoint = EpochCheckpoint::get(&**tx, previous_epoch).optional()?;
            let state_merkle_root = checkpoint
                .map(|cp| cp.compute_state_merkle_root())
                .transpose()?
                .unwrap_or(SPARSE_MERKLE_PLACEHOLDER_HASH);
            // The parent for genesis blocks refer to this zero block
            let mut zero_block = Block::zero_block(self.config.network, self.config.consensus_constants.num_preshards);
            if !zero_block.exists(&**tx)? {
                debug!(target: LOG_TARGET, "Creating zero block");
                zero_block.justify().insert(tx)?;
                zero_block.insert(tx)?;
                zero_block.set_as_justified(tx)?;
                zero_block.commit_diff(tx, BlockDiff::empty(*zero_block.id()))?;
            }

            let mut genesis = Block::genesis(
                self.config.network,
                epoch,
                shard_group,
                FixedHash::from(state_merkle_root.into_array()),
                self.config.sidechain_id.clone(),
            );
            if !genesis.exists(&**tx)? {
                info!(target: LOG_TARGET, "✨Creating genesis block {genesis}");
                genesis.justify().save(tx)?;
                genesis.insert(tx)?;
                genesis.set_as_justified(tx)?;
                genesis.as_locked_block().set(tx)?;
                genesis.as_leaf_block().set(tx)?;
                genesis.as_last_executed().set(tx)?;
                genesis.as_last_voted().set(tx)?;
                genesis.justify().as_high_qc().set(tx)?;
                genesis.commit_diff(tx, BlockDiff::empty(*genesis.id()))?;
            }

            Ok(())
        })
    }

    fn publish_event(&self, event: HotstuffEvent) {
        let _ignore = self.tx_events.send(event);
    }
}

impl<TConsensusSpec: ConsensusSpec> Debug for HotstuffWorker<TConsensusSpec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotstuffWorker")
            .field("validator_addr", &self.local_validator_addr)
            .field("epoch_manager", &"EpochManager")
            .field("pacemaker_handle", &self.pacemaker)
            .field("pacemaker", &"Pacemaker")
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

fn log_err<T>(context: &'static str, result: Result<T, HotStuffError>) -> Result<T, HotStuffError> {
    if let Err(ref e) = result {
        error!(target: LOG_TARGET, "Error while processing new hotstuff message ({context}): {e}");
    }
    result
}
