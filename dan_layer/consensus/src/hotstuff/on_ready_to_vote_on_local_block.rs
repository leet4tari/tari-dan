//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::num::NonZeroU64;

use log::*;
use tari_crypto::ristretto::RistrettoPublicKey;
use tari_dan_common_types::{
    committee::CommitteeInfo,
    displayable::Displayable,
    optional::Optional,
    shard::Shard,
    Epoch,
    ShardGroup,
    VersionedSubstateId,
};
use tari_dan_storage::{
    consensus_models::{
        AbortReason,
        Block,
        BlockDiff,
        BlockId,
        BlockTransactionExecution,
        Command,
        Decision,
        ForeignProposalAtom,
        ForeignProposalStatus,
        HighQc,
        InvalidEvidenceReason,
        LastExecuted,
        LastVoted,
        LockedBlock,
        MintConfidentialOutputAtom,
        NoVoteReason,
        PendingShardStateTreeDiff,
        QcId,
        QuorumDecision,
        SubstateChange,
        SubstateRecord,
        TransactionAtom,
        TransactionPool,
        TransactionPoolRecord,
        TransactionPoolStage,
        TransactionRecord,
        ValidBlock,
        ValidatorConsensusStats,
    },
    StateStore,
};
use tari_engine_types::{commit_result::RejectReason, substate::Substate};
use tokio::sync::broadcast;

use crate::{
    hotstuff::{
        apply_leader_fee_to_substate_store,
        block_change_set::{BlockDecision, ProposedBlockChangeSet},
        calculate_state_merkle_root,
        error::HotStuffError,
        event::HotstuffEvent,
        filter_diff_for_committee,
        foreign_proposal_processor::process_foreign_block,
        substate_store::{PendingSubstateStore, ShardedStateTree},
        transaction_manager::{
            ConsensusTransactionManager,
            EvidenceOrExecution,
            LocalPreparedTransaction,
            PledgedTransaction,
            PreparedTransaction,
            TransactionLockConflicts,
        },
        HotstuffConfig,
        ProposalValidationError,
    },
    tracing::TraceTimer,
    traits::{ConsensusSpec, WriteableSubstateStore},
};

const LOG_TARGET: &str = "tari::dan::consensus::hotstuff::on_ready_to_vote_on_local_block";

#[derive(Debug, Clone)]
pub struct OnReadyToVoteOnLocalBlock<TConsensusSpec: ConsensusSpec> {
    local_validator_pk: RistrettoPublicKey,
    config: HotstuffConfig,
    transaction_pool: TransactionPool<TConsensusSpec::StateStore>,
    tx_events: broadcast::Sender<HotstuffEvent>,
    transaction_manager: ConsensusTransactionManager<TConsensusSpec::TransactionExecutor, TConsensusSpec::StateStore>,
}

impl<TConsensusSpec> OnReadyToVoteOnLocalBlock<TConsensusSpec>
where TConsensusSpec: ConsensusSpec
{
    pub fn new(
        local_validator_pk: RistrettoPublicKey,
        config: HotstuffConfig,
        transaction_pool: TransactionPool<TConsensusSpec::StateStore>,
        tx_events: broadcast::Sender<HotstuffEvent>,
        transaction_manager: ConsensusTransactionManager<
            TConsensusSpec::TransactionExecutor,
            TConsensusSpec::StateStore,
        >,
    ) -> Self {
        Self {
            local_validator_pk,
            config,
            transaction_pool,
            tx_events,
            transaction_manager,
        }
    }

    pub fn handle(
        &mut self,
        tx: &mut <TConsensusSpec::StateStore as StateStore>::WriteTransaction<'_>,
        valid_block: &ValidBlock,
        local_committee_info: &CommitteeInfo,
        proposer_claim_public_key_bytes: [u8; 32],
        can_propose_epoch_end: bool,
        change_set: &mut ProposedBlockChangeSet,
    ) -> Result<BlockDecision, HotStuffError> {
        let _timer =
            TraceTimer::info(LOG_TARGET, "Decide on local block").with_iterations(valid_block.block().commands().len());
        debug!(
            target: LOG_TARGET,
            "🔥 LOCAL PROPOSAL READY: {}",
            valid_block,
        );

        if self.should_vote(tx, valid_block.block())? {
            let mut justified_block = valid_block.justify().get_block(&**tx)?;
            // This comes before decide so that all evidence can be in place before LocalPrepare and LocalAccept
            if !justified_block.is_justified() {
                self.process_newly_justified_block(
                    tx,
                    &justified_block,
                    *valid_block.justify().id(),
                    local_committee_info,
                    change_set,
                )?;
                justified_block.set_as_justified(tx)?;
            }

            self.decide_what_to_vote(
                tx,
                valid_block.block(),
                local_committee_info,
                proposer_claim_public_key_bytes,
                can_propose_epoch_end,
                change_set,
            )?;
        } else {
            change_set.no_vote(NoVoteReason::AlreadyVotedAtHeight);
        }

        let mut locked_blocks = Vec::new();
        let mut finalized_transactions = Vec::new();
        let mut end_of_epoch = None;
        let mut maybe_high_qc = None;
        let mut committed_blocks_with_evictions = Vec::new();

        if change_set.is_accept() {
            // Update nodes
            let high_qc = valid_block.block().update_nodes(
                tx,
                |tx, _prev_locked, block, _justify_qc| {
                    if !block.is_dummy() {
                        locked_blocks.push(block.clone());
                    }
                    self.on_lock_block(tx, block)
                },
                |tx, last_exec, commit_block| {
                    let committed = self.on_commit(tx, last_exec, &commit_block, local_committee_info)?;
                    if commit_block.is_epoch_end() {
                        end_of_epoch = Some(commit_block.epoch());
                    }
                    if commit_block.all_node_evictions().next().is_some() {
                        committed_blocks_with_evictions.push(commit_block);
                    }
                    if !committed.is_empty() {
                        finalized_transactions.push(committed);
                    }
                    Ok(())
                },
            )?;

            maybe_high_qc = Some(high_qc);

            valid_block.block().as_last_voted().set(tx)?;
        }

        let quorum_decision = change_set.quorum_decision();
        info!(
            target: LOG_TARGET,
            "✅ Saving changeset for Local block {} decision {:?}, change set: {}",
            valid_block.block(),
            quorum_decision,
            change_set
        );
        change_set.save(tx)?;

        let high_qc = maybe_high_qc
            .map(Ok)
            .unwrap_or_else(|| HighQc::get(&**tx, valid_block.epoch()))?;

        Ok(BlockDecision {
            quorum_decision,
            locked_blocks,
            finalized_transactions,
            end_of_epoch,
            high_qc,
            committed_blocks_with_evictions,
        })
    }

    fn process_newly_justified_block(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        new_leaf_block: &Block,
        justify_id: QcId,
        local_committee_info: &CommitteeInfo,
        change_set: &mut ProposedBlockChangeSet,
    ) -> Result<(), HotStuffError> {
        let timer = TraceTimer::info(LOG_TARGET, "Process newly justified block");
        let locked_block = LockedBlock::get(tx, new_leaf_block.epoch())?;
        info!(
            target: LOG_TARGET,
            "✅ New leaf block {} is justified. Updating evidence for transactions",
            new_leaf_block,
        );

        let mut num_applicable_commands = 0;
        let leaf = new_leaf_block.as_leaf_block();
        for cmd in new_leaf_block.commands() {
            if !cmd.is_local_prepare() && !cmd.is_local_accept() {
                continue;
            }

            num_applicable_commands += 1;

            let atom = cmd.transaction().expect("Command must be a transaction");

            let Some(mut pool_tx) = change_set
                .get_transaction(tx, &locked_block, &leaf, atom.id())
                .optional()?
            else {
                return Err(HotStuffError::InvariantError(format!(
                    "Transaction {} in newly justified block {} not found in the pool",
                    atom.id(),
                    leaf,
                )));
            };

            if cmd.is_local_prepare() {
                debug!(
                    target: LOG_TARGET,
                    "🔍 Updating evidence for LocalPrepare command in block {} for transaction {}",
                    leaf,
                    atom.id(),
                );
                pool_tx
                    .evidence_mut()
                    .add_shard_group(local_committee_info.shard_group())
                    .set_prepare_qc(justify_id);
            } else if cmd.is_local_accept() {
                pool_tx
                    .evidence_mut()
                    .add_shard_group(local_committee_info.shard_group())
                    .set_accept_qc(justify_id);
                debug!(
                    target: LOG_TARGET,
                    "🔍 Updating evidence for LocalAccept command in block {} for transaction {}. {:#}",
                    leaf,
                    atom.id(),
                    pool_tx.evidence()
                );
            } else {
                // Nothing
            }

            // Set readiness
            if !pool_tx.is_ready() && pool_tx.is_ready_for_pending_stage() {
                pool_tx.set_ready(true);
            }

            change_set.set_next_transaction_update(pool_tx)?;
        }

        timer.with_iterations(num_applicable_commands);

        Ok(())
    }

    /// if b_new .height > vheight && (b_new extends b_lock || b_new .justify.node.height > b_lock .height)
    ///
    /// If we have not previously voted on this block and the node extends the current locked node, then we vote
    fn should_vote(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
    ) -> Result<bool, ProposalValidationError> {
        let Some(last_voted) = LastVoted::get(tx).optional()? else {
            // Never voted, then validated.block.height() > last_voted.height (0)
            return Ok(true);
        };

        // if b_new .height > vheight And ...
        if block.height() <= last_voted.height {
            info!(
                target: LOG_TARGET,
                "❌ NOT voting on block {}. Block height is not greater than last voted height {}",
                block,
                last_voted.height,
            );
            return Ok(false);
        }

        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
    fn decide_what_to_vote(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        local_committee_info: &CommitteeInfo,
        proposer_claim_public_key_bytes: [u8; 32],
        can_propose_epoch_end: bool,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<(), HotStuffError> {
        // Store used for transactions that have inputs without specific versions.
        // It lives through the entire block so multiple transactions can be sequenced together in the same block
        let mut substate_store =
            PendingSubstateStore::new(tx, *block.parent(), self.config.consensus_constants.num_preshards);
        let mut total_leader_fee = 0;
        let locked_block = LockedBlock::get(tx, block.epoch())?;

        for cmd in block.commands() {
            match cmd {
                Command::LocalOnly(atom) => {
                    if let Some(reason) = self.evaluate_local_only_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        &mut substate_store,
                        proposed_block_change_set,
                        &mut total_leader_fee,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::Prepare(atom) => {
                    if let Some(reason) = self.evaluate_prepare_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        &mut substate_store,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::LocalPrepare(atom) => {
                    if let Some(reason) = self.evaluate_local_prepare_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::AllPrepare(atom) => {
                    // Execute here
                    if let Some(reason) = self.evaluate_all_prepare_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        &mut substate_store,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::SomePrepare(atom) => {
                    if let Some(reason) =
                        self.evaluate_some_prepare_command(tx, block, &locked_block, atom, proposed_block_change_set)?
                    {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::LocalAccept(atom) => {
                    if let Some(reason) = self.evaluate_local_accept_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::AllAccept(atom) => {
                    if let Some(reason) = self.evaluate_all_accept_command(
                        tx,
                        block,
                        &locked_block,
                        atom,
                        local_committee_info,
                        &mut substate_store,
                        proposed_block_change_set,
                        &mut total_leader_fee,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::SomeAccept(atom) => {
                    if let Some(reason) =
                        self.evaluate_some_accept_command(tx, block, &locked_block, atom, proposed_block_change_set)?
                    {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::ForeignProposal(fp_atom) => {
                    if let Some(reason) = self.evaluate_foreign_proposal_command(
                        tx,
                        block,
                        &locked_block,
                        fp_atom,
                        local_committee_info,
                        fp_atom.shard_group,
                        &mut substate_store,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }

                    continue;
                },
                Command::MintConfidentialOutput(atom) => {
                    if let Some(reason) = self.evaluate_mint_confidential_output_command(
                        tx,
                        atom,
                        local_committee_info,
                        &mut substate_store,
                        proposed_block_change_set,
                    )? {
                        proposed_block_change_set.no_vote(reason);
                        return Ok(());
                    }
                },
                Command::EvictNode(atom) => {
                    if ValidatorConsensusStats::is_node_evicted(tx, block.id(), &atom.public_key)? {
                        warn!(
                            target: LOG_TARGET,
                            "❌ NO VOTE: {}", NoVoteReason::NodeAlreadyEvicted
                        );

                        proposed_block_change_set.no_vote(NoVoteReason::NodeAlreadyEvicted);
                        return Ok(());
                    }

                    let num_evicted = ValidatorConsensusStats::count_number_evicted_nodes(tx, block.epoch())?;
                    let max_allowed_to_evict = u64::from(local_committee_info.quorum_threshold())
                        .saturating_sub(num_evicted)
                        .saturating_sub(proposed_block_change_set.num_evicted_nodes_this_block() as u64);
                    if max_allowed_to_evict == 0 {
                        warn!(
                            target: LOG_TARGET,
                            "❌ NO VOTE: {}", NoVoteReason::CannotEvictNodeBelowQuorumThreshold
                        );

                        proposed_block_change_set.no_vote(NoVoteReason::CannotEvictNodeBelowQuorumThreshold);
                        return Ok(());
                    }

                    let stats = ValidatorConsensusStats::get_by_public_key(tx, block.epoch(), &atom.public_key)?;
                    if stats.missed_proposals < self.config.consensus_constants.missed_proposal_evict_threshold {
                        warn!(
                            target: LOG_TARGET,
                            "❌ NO VOTE: {} (actual missed count: {}, threshold: {})", NoVoteReason::ShouldNotEvictNode, stats.missed_proposals, self.config.consensus_constants.missed_proposal_evict_threshold
                        );

                        proposed_block_change_set.no_vote(NoVoteReason::ShouldNotEvictNode);
                        return Ok(());
                    }

                    info!(
                        target: LOG_TARGET,
                        "💀 EVICTING node: {} with missed count {}",
                        atom.public_key,
                        stats.missed_proposals
                    );
                    proposed_block_change_set.add_evict_node(atom.public_key.clone());
                },
                Command::EndEpoch => {
                    if !can_propose_epoch_end {
                        warn!(
                            target: LOG_TARGET,
                            "❌ EpochEvent::End command received for block {} but it is not the next epoch",
                            block.id(),
                        );
                        proposed_block_change_set.no_vote(NoVoteReason::NotEndOfEpoch);
                        return Ok(());
                    }
                    if block.commands().len() > 1 {
                        warn!(
                            target: LOG_TARGET,
                            "❌ EpochEvent::End command in block {} but block contains other commands",
                            block.id()
                        );
                        proposed_block_change_set.no_vote(NoVoteReason::EndOfEpochWithOtherCommands);
                        return Ok(());
                    }

                    continue;
                },
            }
        }

        if total_leader_fee != block.total_leader_fee() {
            warn!(
                target: LOG_TARGET,
                "❌ Leader fee disagreement for block {}. Leader proposed {}, we calculated {}",
                block,
                block.total_leader_fee(),
                total_leader_fee
            );
            proposed_block_change_set.no_vote(NoVoteReason::TotalLeaderFeeDisagreement);
            return Ok(());
        }

        // Apply leader fee to substate store before we calculate the state root
        if total_leader_fee > 0 {
            let total_leader_fee_amt = total_leader_fee.try_into().map_err(|_| {
                HotStuffError::InvariantError(format!("Total leader fee {total_leader_fee} under/overflows Amount"))
            })?;
            apply_leader_fee_to_substate_store(
                &mut substate_store,
                proposer_claim_public_key_bytes,
                local_committee_info.shard_group().start(),
                local_committee_info.num_preshards(),
                total_leader_fee_amt,
            )?;
        }

        let pending = PendingShardStateTreeDiff::get_all_up_to_commit_block(tx, block.parent())?;
        let (expected_merkle_root, tree_diffs) = calculate_state_merkle_root(
            tx,
            block.shard_group(),
            pending,
            substate_store
                .diff()
                .iter()
                // Calculate for local shards only AND global shard
                .filter(|ch| block.shard_group().contains(&ch.shard()) || ch.shard() == Shard::global()),
        )?;
        if expected_merkle_root != *block.state_merkle_root() {
            warn!(
                target: LOG_TARGET,
                "❌ State Merkle root disagreement for block {}. Leader proposed {}, we calculated {}",
                block,
                block.state_merkle_root(),
                expected_merkle_root
            );
            let (diff, locks) = substate_store.into_parts();
            proposed_block_change_set
                .no_vote(NoVoteReason::StateMerkleRootMismatch)
                // These are set for debugging purposes but aren't actually committed
                .set_substate_changes(diff)
                .set_substate_locks(locks);
            return Ok(());
        }

        let (diff, locks) = substate_store.into_parts();
        proposed_block_change_set
            .set_substate_changes(diff)
            .set_state_tree_diffs(tree_diffs)
            .set_substate_locks(locks)
            .set_quorum_decision(QuorumDecision::Accept);

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_local_only_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
        total_leader_fee: &mut u64,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let _timer = TraceTimer::info(LOG_TARGET, "Evaluate LocalOnly command");
        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        if !tx_rec.current_stage().is_new() {
            warn!(
                target: LOG_TARGET,
                "❌ Stage disagreement for tx {} in block {}. Leader proposed LocalOnly, local stage is {}",
                tx_rec.transaction_id(),
                block,
                tx_rec.current_stage(),
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                stage: tx_rec.current_stage(),
                expected: TransactionPoolStage::New,
            }));
        }

        // TODO(perf): proposer shouldn't have to do this twice, esp. executing the transaction and locking
        let prepared = self
            .transaction_manager
            .prepare(substate_store, local_committee_info, block.epoch(), &tx_rec, block.id())
            .map_err(|e| HotStuffError::TransactionExecutorError(e.to_string()))?;

        match prepared {
            PreparedTransaction::LocalOnly(local) => {
                match *local {
                    LocalPreparedTransaction::Accept { execution, .. } => {
                        tx_rec
                            .set_local_decision(execution.decision())
                            .set_transaction_fee(execution.transaction_fee())
                            .set_evidence(execution.to_evidence(
                                local_committee_info.num_preshards(),
                                local_committee_info.num_committees(),
                            ));

                        info!(
                            target: LOG_TARGET,
                            "👨‍🔧 LocalOnly: Prepare for transaction {} ({}) in block {}",
                            tx_rec.transaction_id(),
                            tx_rec.current_decision(),
                            block,
                        );

                        // If the leader proposed to commit a transaction that we want to abort, we abstain from voting
                        if tx_rec.current_decision() != atom.decision {
                            // If we disagree with any local decision we abstain from voting
                            warn!(
                                target: LOG_TARGET,
                                "❌ Prepare decision disagreement for tx {} in block {}. Leader proposed {}, we decided {}",
                                tx_rec.transaction_id(),
                                block,
                                atom.decision,
                                tx_rec.current_decision()
                            );
                            return Ok(Some(NoVoteReason::DecisionDisagreement {
                                local: tx_rec.current_decision(),
                                remote: atom.decision,
                            }));
                        }

                        if tx_rec.transaction_fee() != atom.transaction_fee {
                            warn!(
                                target: LOG_TARGET,
                                "❌ LocalOnly transaction fee disagreement for block {}. Leader proposed {}, we calculated {}",
                                block,
                                atom.transaction_fee,
                                tx_rec.transaction_fee()
                            );
                            return Ok(Some(NoVoteReason::FeeDisagreement));
                        }

                        if tx_rec.current_decision().is_commit() {
                            if let Some(diff) = execution.result().finalize.accept() {
                                substate_store.put_diff(atom.id, diff)?;
                            }

                            if atom.leader_fee.is_none() {
                                warn!(
                                    target: LOG_TARGET,
                                    "❌ Leader fee for tx {} is None for LocalOnly command in block {}",
                                    atom.id,
                                    block,
                                );
                                return Ok(Some(NoVoteReason::NoLeaderFee));
                            }

                            let calculated_leader_fee = tx_rec.calculate_leader_fee(
                                NonZeroU64::new(1).expect("1 > 0"),
                                self.config.consensus_constants.fee_exhaust_divisor,
                            );
                            if calculated_leader_fee != *atom.leader_fee.as_ref().expect("None already checked") {
                                warn!(
                                    target: LOG_TARGET,
                                    "❌ LocalOnly leader fee disagreement for block {}. Leader proposed {}, we calculated {}",
                                    block,
                                    atom.leader_fee.as_ref().expect("None already checked"),
                                    calculated_leader_fee
                                );

                                return Ok(Some(NoVoteReason::LeaderFeeDisagreement));
                            }

                            *total_leader_fee += calculated_leader_fee.fee();
                        }

                        proposed_block_change_set.add_transaction_execution(execution)?;
                    },
                    LocalPreparedTransaction::EarlyAbort { execution } => {
                        if atom.decision.is_commit() {
                            warn!(
                                target: LOG_TARGET,
                                "❌ Failed to lock inputs/outputs for transaction {} but leader proposed COMMIT. Not voting for block {}",
                                tx_rec.transaction_id(),
                                block,
                            );
                            return Ok(Some(NoVoteReason::DecisionDisagreement {
                                local: Decision::Abort(AbortReason::LockInputsOutputsFailed),
                                remote: Decision::Commit,
                            }));
                        }

                        // They want to ABORT a successfully executed transaction because of a lock conflict, which
                        // we also have.
                        info!(
                            target: LOG_TARGET,
                            "⚠️ Proposer chose to ABORT and we chose to ABORT due to lock conflict for transaction {} in block {}",
                            block,
                            tx_rec.transaction_id(),
                        );
                        tx_rec
                            .set_local_decision(execution.decision())
                            .set_transaction_fee(execution.transaction_fee())
                            .set_evidence(execution.to_evidence(
                                local_committee_info.num_preshards(),
                                local_committee_info.num_committees(),
                            ));
                        proposed_block_change_set.add_transaction_execution(execution)?;
                    },
                }
            },
            PreparedTransaction::MultiShard(_) => {
                warn!(
                    target: LOG_TARGET,
                    "❌ transaction {} in block {} is not Local-Only but was proposed as LocalOnly",
                    atom.id(),
                    block,
                );
                return Ok(Some(NoVoteReason::LocalOnlyProposedForMultiShard));
            },
        }

        tx_rec.set_next_stage(TransactionPoolStage::LocalOnly)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;
        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_prepare_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let _timer = TraceTimer::info(LOG_TARGET, "Evaluate Prepare command");
        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        info!(
            target: LOG_TARGET,
            "👨‍🔧 PREPARE: Transaction {} in block {}",
            tx_rec.transaction_id(),
            block,
        );

        if !tx_rec.current_stage().is_new() {
            warn!(
                target: LOG_TARGET,
                "❌ Stage disagreement for tx {} in block {}. Leader proposed Prepare, local stage is {}",
                tx_rec.transaction_id(),
                block,
                tx_rec.current_stage(),
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                stage: tx_rec.current_stage(),
                expected: TransactionPoolStage::New,
            }));
        }

        let prepared = self
            .transaction_manager
            .prepare(substate_store, local_committee_info, block.epoch(), &tx_rec, block.id())
            .map_err(|e| HotStuffError::TransactionExecutorError(e.to_string()))?;

        if !prepared.is_involved(local_committee_info) {
            warn!(
                target: LOG_TARGET,
                "❓️ Not involved in prepared transaction {}", tx_rec.transaction_id(),
            );
        }

        match prepared {
            PreparedTransaction::LocalOnly(_) => {
                warn!(
                    target: LOG_TARGET,
                    "❌ transaction {} in block {} is Local-Only but was proposed as Prepare",
                    atom.id(),
                    block,
                );
                return Ok(Some(NoVoteReason::MultiShardProposedForLocalOnly));
            },
            PreparedTransaction::MultiShard(multishard) => {
                // TODO: Because on_propose does not process foreign proposals before proposing, the decision abort
                // reason may mismatch (e.g. ExecutionFailure != ForeignShardGroupDecidedToAbort)
                // This is why we use is_same_outcome here. We should try to process foreign proposals in
                // on_propose
                if !multishard.current_decision().is_same_outcome(atom.decision) {
                    warn!(
                        target: LOG_TARGET,
                        "❌ Leader proposed {} for transaction {} but we decided {} in block {}",
                        atom.decision,
                        atom.id,
                        multishard.current_decision(),
                        block,
                    );
                    return Ok(Some(NoVoteReason::DecisionDisagreement {
                        local: multishard.current_decision(),
                        remote: atom.decision,
                    }));
                }

                if !atom.evidence.contains(&local_committee_info.shard_group()) {
                    warn!(
                        target: LOG_TARGET,
                        "❌ Prepare command for transaction {} has invalid evidence {} missing local shard group {}
                    in {}",         tx_rec.transaction_id(),
                        atom.evidence,
                        local_committee_info.shard_group(),
                        block,
                    );
                    return Ok(Some(NoVoteReason::InvalidEvidence {
                        reason: InvalidEvidenceReason::MissingInvolvedShardGroup {
                            shard_group: local_committee_info.shard_group(),
                        },
                    }));
                };

                // TODO: this is kinda hacky - we may not be involved in the transaction after ABORT execution,
                // but this would be invalid so we ensure that we are added to evidence. Ideally, we wouldn't
                // sequence this transaction at all - investigate.
                tx_rec
                    .evidence_mut()
                    .add_shard_group(local_committee_info.shard_group());

                match multishard.into_evidence_or_execution() {
                    EvidenceOrExecution::Execution(execution) => {
                        debug!(
                            target: LOG_TARGET,
                            "👨‍🔧 PREPARE: Transaction {} in block {} is executed",
                            tx_rec.transaction_id(),
                            block,
                        );
                        // CASE: All inputs are local and outputs are foreign (i.e. the transaction is
                        // executed), or we're output-only and have received all pledges.
                        tx_rec.update_from_execution(
                            local_committee_info.num_preshards(),
                            local_committee_info.num_committees(),
                            &execution,
                        );
                        if execution.decision().is_commit() {
                            let involves_inputs = tx_rec.evidence().has_inputs(local_committee_info.shard_group());
                            if !involves_inputs {
                                // Output only
                                let num_involved_shard_groups = tx_rec.evidence().num_shard_groups();
                                let involved = NonZeroU64::new(num_involved_shard_groups as u64).ok_or_else(|| {
                                    HotStuffError::InvariantError("Number of involved shard groups is 0".to_string())
                                })?;
                                let leader_fee = tx_rec.calculate_leader_fee(
                                    involved,
                                    self.config.consensus_constants.fee_exhaust_divisor,
                                );
                                tx_rec.set_leader_fee(leader_fee);
                            }
                        }
                        proposed_block_change_set.add_transaction_execution(*execution)?;
                    },
                    EvidenceOrExecution::Evidence { evidence, .. } => {
                        debug!(
                            target: LOG_TARGET,
                            "👨‍🔧 PREPARE: Transaction {} in block {} is not executed. Using partial evidence.",
                            tx_rec.transaction_id(),
                            block,
                        );
                        // CASE: All local inputs were resolved. We need to continue with consensus to get the
                        // foreign inputs/outputs.
                        tx_rec.set_local_decision(Decision::Commit);
                        // Set partial evidence for local inputs using what we know.
                        tx_rec.set_evidence(evidence);
                    },
                }

                // Check the local evidence only since in this phase that is all that should be included.
                // Aside from data reduction, on_propose may propose a foreign proposal that updates the
                // evidence in the same block. However, because on_propose does not
                // include the evidence from the foreign proposal since it is processed here for the proposer,
                // foreign shard evidence will mismatch in this case.
                if atom.evidence.get(&local_committee_info.shard_group()) !=
                    tx_rec.evidence().get(&local_committee_info.shard_group())
                {
                    warn!(
                        target: LOG_TARGET,
                        "❌ Prepare command for transaction {} has invalid evidence in {}",
                        tx_rec.transaction_id(),
                        block,
                    );
                    warn!(
                        target: LOG_TARGET,
                        "❌ Prepare command evidence {} {}",
                        local_committee_info.shard_group(),
                        atom.evidence.get(&local_committee_info.shard_group()).display()
                    );
                    warn!(
                        target: LOG_TARGET,
                        "❌ local evidence {} {}",
                        local_committee_info.shard_group(),
                        tx_rec.evidence().get(&local_committee_info.shard_group()).display()
                    );
                    return Ok(Some(NoVoteReason::InvalidEvidence {
                        reason: InvalidEvidenceReason::MismatchedEvidence,
                    }));
                }
            },
        }

        tx_rec.set_next_stage(TransactionPoolStage::Prepared)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    fn evaluate_local_prepare_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        if !tx_rec.current_stage().is_prepared() {
            warn!(
                target: LOG_TARGET,
                "{} ❌ LocalPrepare Stage disagreement in block {} for transaction {}. Leader proposed LocalPrepare, but local stage is {}",
                self.local_validator_pk,
                block,
                tx_rec.transaction_id(),
                tx_rec.current_stage()
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                expected: TransactionPoolStage::Prepared,
                stage: tx_rec.current_stage(),
            }));
        }
        // We check that the leader decision is the same as our local decision.
        // We disregard the remote decision because not all validators may have received the foreign
        // LocalPrepared yet. We will never accept a decision disagreement for the Accept command.
        if tx_rec.current_local_decision() != atom.decision {
            warn!(
                target: LOG_TARGET,
                "❌ LocalPrepared decision disagreement for transaction {} in block {}. Leader proposed {}, we decided {}",
                tx_rec.transaction_id(),
                block,
                atom.decision,
                tx_rec.current_local_decision()
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: tx_rec.current_local_decision(),
                remote: atom.decision,
            }));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ LocalPrepared transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        if atom.evidence.get(&local_committee_info.shard_group()) !=
            tx_rec.evidence().get(&local_committee_info.shard_group())
        {
            warn!(
                target: LOG_TARGET,
                "❌ LocalPrepared evidence disagreement tx {} in block {}. Leader proposed {}, local {}",
                tx_rec.transaction_id(),
                block,
                atom.evidence,
                tx_rec.evidence()
            );
            return Ok(Some(NoVoteReason::InvalidEvidence {
                reason: InvalidEvidenceReason::MismatchedEvidence,
            }));
        }

        tx_rec.set_next_stage(TransactionPoolStage::LocalPrepared)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_all_prepare_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let _timer = TraceTimer::info(LOG_TARGET, "Evaluate AllPrepare command (execute)");
        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        if !tx_rec.current_stage().is_local_prepared() {
            warn!(
                target: LOG_TARGET,
                "{} ❌ Stage disagreement in block {} for transaction {}. Leader proposed AllPrepare, but current stage is {}",
                self.local_validator_pk,
                block,
                tx_rec.transaction_id(),
                tx_rec.current_stage()
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                expected: TransactionPoolStage::LocalPrepared,
                stage: tx_rec.current_stage(),
            }));
        }

        // If we've already decided to abort, we cannot change to commit in LocalPrepared phase so proposing AllPrepared
        // is invalid
        if tx_rec.current_decision().is_abort() && atom.decision.is_commit() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE AllPrepare: decision disagreement for transaction {} in block {}. Leader proposed {}, we decided {}",
                tx_rec.transaction_id(),
                block,
                atom.decision,
                tx_rec.current_decision()
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: tx_rec.current_decision(),
                remote: atom.decision,
            }));
        }

        if !tx_rec.evidence().all_input_shard_groups_prepared() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllPrepare disagreement for transaction {} in block {}. Leader proposed that all shard groups have prepared, but this is not the case",
                tx_rec.transaction_id(),
                block,
            );
            return Ok(Some(NoVoteReason::NotAllShardGroupsPrepared));
        }

        // TODO: on_propose does not process foreign proposals so we cannot rely on this check
        // if !atom.evidence.all_input_shard_groups_prepared() {
        //     warn!(
        //         target: LOG_TARGET,
        //         "❌ NO VOTE: AllPrepare disagreement for transaction {} in block {}. {}",
        //         tx_rec.transaction_id(),
        //         block,
        //         InvalidEvidenceReason::NotAllShardGroupsPrepared,
        //     );
        //     return Ok(Some(NoVoteReason::InvalidEvidence {
        //         reason: InvalidEvidenceReason::NotAllShardGroupsPrepared,
        //     }));
        // }

        let maybe_execution = if tx_rec.current_decision().is_commit() {
            // TODO: provide the current input locks to the executor, the executor must fail if a write lock is
            // requested for a read-locked substate.

            let transaction = tx_rec.get_transaction(tx)?;
            if !transaction.has_all_required_input_pledges(tx, local_committee_info)? {
                warn!(
                    target: LOG_TARGET,
                    "❌ NO VOTE AllPrepare: transaction {} in block {} has not received all foreign input pledges",
                    tx_rec.transaction_id(),
                    block,
                );
                return Ok(Some(NoVoteReason::NotAllForeignInputPledges));
            }
            let execution = self.execute_transaction(tx, block.id(), block.epoch(), transaction)?;
            let mut execution = execution.into_transaction_execution();

            // TODO: can we modify input locks at this point? For multi-shard input transactions, we locked all inputs
            // as Read due to lack of information. We now know what locks are necessary, and this
            // block has the correct evidence so this should be fine.
            tx_rec.update_from_execution(
                local_committee_info.num_preshards(),
                local_committee_info.num_committees(),
                &execution,
            );

            if execution.decision().is_commit() {
                // Lock all local outputs
                let local_outputs = execution
                    .resulting_outputs()
                    .iter()
                    .filter(|o| local_committee_info.includes_substate_id(o.substate_id()));
                let lock_status = substate_store.try_lock_all(*tx_rec.transaction_id(), local_outputs, false)?;
                if let Some(err) = lock_status.failures().first() {
                    if atom.decision.is_commit() {
                        // If we disagree with any local decision we abstain from voting
                        warn!(
                            target: LOG_TARGET,
                            "❌ NO VOTE LocalAccept: Lock failure: {} but leader decided COMMIT for tx {} in block {}. Leader proposed COMMIT, we decided ABORT",
                            err,
                            tx_rec.transaction_id(),
                            block,
                        );
                        return Ok(Some(NoVoteReason::DecisionDisagreement {
                            local: Decision::Abort(AbortReason::LockOutputsFailed),
                            remote: Decision::Commit,
                        }));
                    }

                    info!(
                        target: LOG_TARGET,
                        "⚠️ Failed to lock outputs for transaction {} in block {}. Error: {}",
                        tx_rec.transaction_id(),
                        block,
                        err
                    );

                    execution.set_abort_reason(RejectReason::FailedToLockOutputs(err.to_string()));

                    tx_rec.set_local_decision(Decision::Abort(AbortReason::LockOutputsFailed));
                    tx_rec.set_transaction_fee(0);
                    tx_rec.set_next_stage(TransactionPoolStage::AllPrepared)?;

                    proposed_block_change_set
                        .set_next_transaction_update(tx_rec)?
                        .add_transaction_execution(execution)?;

                    return Ok(None);
                }
            }

            Some(execution)
        } else {
            // If we already locally decided to abort, there is no purpose in executing the transaction
            None
        };

        // We check that the leader decision is the same as our local decision.
        if tx_rec.current_decision() != atom.decision {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE AllAccept: decision disagreement for transaction {} (after execute) in block {}. Leader proposed {}, we decided {}",
                tx_rec.transaction_id(),
                block,
                atom.decision,
                tx_rec.current_decision()
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: tx_rec.current_decision(),
                remote: atom.decision,
            }));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE AllAccept: transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ AllPrepare transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        if !tx_rec.evidence().eq_pledges(&atom.evidence) {
            warn!(
                target: LOG_TARGET,
                "❌ AllPrepare evidence disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.evidence,
                tx_rec.evidence()
            );
            return Ok(Some(NoVoteReason::InvalidEvidence {
                reason: InvalidEvidenceReason::MismatchedEvidence,
            }));
        }

        // maybe_execution is only None if the transaction is not committed
        if let Some(execution) = maybe_execution {
            proposed_block_change_set.add_transaction_execution(execution)?;
        }

        tx_rec.set_next_stage(TransactionPoolStage::AllPrepared)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    fn evaluate_some_prepare_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        if atom.decision.is_commit() {
            warn!(
                target: LOG_TARGET,
                "❌ SomePrepare command received for block {} but requires that the transaction is ABORT",
                block.id(),
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: Decision::Abort(AbortReason::TransactionAtomMustBeAbort),
                remote: Decision::Commit,
            }));
        }

        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        // If the local node would decide SomePrepare too, we should have already ABORTed due to foreign prepare abort
        // or local input lock conflict
        if tx_rec.current_decision().is_commit() {
            warn!(
                target: LOG_TARGET,
                "❌ SomePrepare decision disagreement for transaction {} in block {}. Leader proposed ABORT, we decided COMMIT",
                tx_rec.transaction_id(),
                block,
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: Decision::Commit,
                remote: atom.decision,
            }));
        }

        if !tx_rec.current_stage().is_local_prepared() {
            warn!(
                target: LOG_TARGET,
                "{} ❌ Stage disagreement in block {} for transaction {}. Leader proposed SomePrepare, but local stage is {}",
                self.local_validator_pk,
                block,
                tx_rec.transaction_id(),
                tx_rec.current_stage()
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                expected: TransactionPoolStage::LocalPrepared,
                stage: tx_rec.current_stage(),
            }));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ SomePrepare transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        tx_rec.set_next_stage(TransactionPoolStage::SomePrepared)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_local_accept_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        // We check that the leader decision is the same as our local decision.
        // We disregard the remote decision because not all validators may have received the foreign
        // LocalPrepared yet. We will never accept a decision disagreement for the Accept command.
        if tx_rec.current_decision() != atom.decision {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE LocalAccept: decision disagreement for transaction {} in block {}. Leader proposed {}, we decided {}",
                tx_rec.transaction_id(),
                block,
                atom.decision,
                tx_rec.current_decision()
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: tx_rec.current_decision(),
                remote: atom.decision,
            }));
        }

        let is_applicable_stage = match tx_rec.current_decision() {
            Decision::Commit => {
                // Commit. AllPrepared, or from Prepared directly if we are output-only (which only has Accept phase)
                tx_rec.current_stage().is_all_prepared() ||
                    (tx_rec.current_stage().is_prepared() &&
                        tx_rec
                            .evidence()
                            .is_committee_output_only(local_committee_info.shard_group()))
            },
            Decision::Abort(_) => {
                // Abort. AllPrepared (aborted after executing) or SomePrepared (aborted before executing). We allow the
                // transition from Prepared to LocalAccepted if ABORT, or we are output-only
                tx_rec.current_stage().is_all_prepared() ||
                    tx_rec.current_stage().is_some_prepared() ||
                    (tx_rec.current_stage().is_prepared() ||
                        tx_rec
                            .evidence()
                            .is_committee_output_only(local_committee_info.shard_group()))
            },
        };

        if !is_applicable_stage {
            let is_output_only = tx_rec
                .evidence()
                .is_committee_output_only(local_committee_info.shard_group());
            let reason = NoVoteReason::StageTransitionNotApplicable {
                current: tx_rec.current_stage(),
                next: TransactionPoolStage::LocalAccepted,
                is_output_only,
                decision: tx_rec.current_decision(),
            };

            warn!(target: LOG_TARGET, "❌ {reason} in {block}");
            return Ok(Some(reason));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE LocalAccept: transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        if atom.decision.is_commit() {
            let Some(ref leader_fee) = atom.leader_fee else {
                warn!(
                    target: LOG_TARGET,
                    "❌ NO VOTE: Leader fee in tx {} not set for LocalAccept command in block {}",
                    atom.id,
                    block,
                );
                return Ok(Some(NoVoteReason::NoLeaderFee));
            };

            // Check the leader fee in the local accept phase. The fee only applied (is added to the block fee) for
            // AllAccept
            let num_involved_shard_groups = tx_rec.evidence().num_shard_groups();
            let involved = NonZeroU64::new(num_involved_shard_groups as u64)
                .ok_or_else(|| HotStuffError::InvariantError("Number of involved shard groups is 0".to_string()))?;
            let calculated_leader_fee =
                tx_rec.calculate_leader_fee(involved, self.config.consensus_constants.fee_exhaust_divisor);
            if calculated_leader_fee != *leader_fee {
                warn!(
                    target: LOG_TARGET,
                    "❌ NO VOTE: LocalAccept leader fee disagreement for block {}. Leader proposed {}, we calculated {}",
                    block,
                    atom.leader_fee.as_ref().expect("None already checked"),
                    calculated_leader_fee
                );

                return Ok(Some(NoVoteReason::LeaderFeeDisagreement));
            }

            tx_rec.set_leader_fee(calculated_leader_fee);
        } else if atom.leader_fee.is_some() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Leader fee in tx {} is set for LocalAccept ABORT command in block {}",
                atom.id,
                block,
            );
            return Ok(Some(NoVoteReason::LeaderFeeDisagreement));
        } else {
            // Ok
        }

        // on_propose does not process foreign proposals, so the QC evidence may not match the evidence here.
        // We only check that the input/output pledges match
        if !tx_rec.evidence().eq_pledges(&atom.evidence) {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Evidence mismatch for LocalAccept transaction {} in block {}. Leader proposed evidence {}, but we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.evidence,
                tx_rec.evidence()
            );
            return Ok(Some(NoVoteReason::InvalidEvidence {
                reason: InvalidEvidenceReason::MismatchedEvidence,
            }));
        }

        tx_rec.set_next_stage(TransactionPoolStage::LocalAccepted)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate_all_accept_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        local_committee_info: &CommitteeInfo,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
        total_leader_fee: &mut u64,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        if atom.decision.is_abort() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept command received for block {} but requires that the transaction is COMMIT",
                block.id(),
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: Decision::Commit,
                remote: Decision::Abort(AbortReason::TransactionAtomMustBeCommit),
            }));
        }

        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ NO VOTE: Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        if !tx_rec.current_stage().is_local_accepted() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept Stage disagreement in block {} for transaction {}. Leader proposed AllAccept, but local stage is {}",
                block,
                tx_rec.transaction_id(),
                tx_rec.current_stage()
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                expected: TransactionPoolStage::LocalAccepted,
                stage: tx_rec.current_stage(),
            }));
        }

        if tx_rec.current_decision().is_abort() {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept decision disagreement for transaction {} in block {}. Leader proposed COMMIT, we decided ABORT",
                tx_rec.transaction_id(),
                block,
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: atom.decision,
                remote: Decision::Commit,
            }));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        let Some(ref leader_fee) = atom.leader_fee else {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Leader fee in tx {} not set for AllAccept command in block {}",
                atom.id,
                block,
            );
            return Ok(Some(NoVoteReason::NoLeaderFee));
        };

        let local_leader_fee = tx_rec.leader_fee().ok_or_else(|| {
            HotStuffError::InvariantError(format!(
                "evaluate_all_accept_command: Transaction {} has COMMIT decision and is at LocalAccepted stage but \
                 leader fee is missing",
                tx_rec.transaction_id()
            ))
        })?;

        if local_leader_fee != leader_fee {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Leader fee disagreement for tx {} in block {}. Leader proposed {}, we calculated {}",
                atom.id,
                block,
                leader_fee,
                local_leader_fee
            );
            return Ok(Some(NoVoteReason::LeaderFeeDisagreement));
        }

        // TODO: investigate, this fails sometimes
        // if !tx_rec.evidence().all_objects_accepted() {
        //     warn!(
        //         target: LOG_TARGET,
        //         "❌ NO VOTE: AllAccept disagreement for transaction {} in block {}. Leader proposed that all shard
        // groups have accepted the atom but locally this is not the case",         tx_rec.transaction_id(),
        //         block,
        //     );
        //     return Ok(Some(NoVoteReason::NotAllInputsOutputsAccepted));
        // }

        if !tx_rec.has_all_required_foreign_pledges(tx, local_committee_info)? {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept disagreement for transaction {} in block {}. Leader proposed that all foreign pledges have been received but locally this is not the case",
                tx_rec.transaction_id(),
                block,
            );
            return Ok(Some(NoVoteReason::NotAllForeignInputPledges));
        }

        // TODO: on_propose does not process foreign proposals so we cannot rely on this check
        // if !atom.evidence.all_shard_groups_accepted() {
        //     warn!(
        //         target: LOG_TARGET,
        //         "❌ NO VOTE: AllAccept disagreement for transaction {} in block {}. Leader proposed an atom which did
        // not indicate that all shard groups have accepted the transaction",         tx_rec.transaction_id(),
        //         block,
        //     );
        //     return Ok(Some(NoVoteReason::NotAllInputsOutputsAccepted));
        // }

        if *tx_rec.evidence() != atom.evidence {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: AllAccept disagreement for transaction {} in block {}. Leader proposed evidence {}, but we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.evidence,
                tx_rec.evidence()
            );
            return Ok(Some(NoVoteReason::InvalidEvidence {
                reason: InvalidEvidenceReason::MismatchedEvidence,
            }));
        }

        let execution = BlockTransactionExecution::get_pending_for_block(tx, tx_rec.transaction_id(), block.parent())
            .optional()?
            .ok_or_else(|| {
                HotStuffError::InvariantError(format!(
                    "evaluate_all_accept_command: Transaction {} has COMMIT decision but execution is missing",
                    tx_rec.transaction_id()
                ))
            })?;

        let diff = execution.result().finalize.accept().ok_or_else(|| {
            HotStuffError::InvariantError(format!(
                "evaluate_local_accept_command: Transaction {} has COMMIT decision but execution failed when proposing",
                tx_rec.transaction_id(),
            ))
        })?;

        *total_leader_fee += leader_fee.fee();

        substate_store.put_diff(
            *tx_rec.transaction_id(),
            &filter_diff_for_committee(local_committee_info, diff),
        )?;

        tx_rec.set_next_stage(TransactionPoolStage::AllAccepted)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    fn evaluate_some_accept_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block: &Block,
        locked_block: &LockedBlock,
        atom: &TransactionAtom,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        if atom.decision.is_commit() {
            warn!(
                target: LOG_TARGET,
                "❌ SomeAccept command received for block {} but requires that the atom is ABORT",
                block.id(),
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: Decision::Abort(AbortReason::TransactionAtomMustBeAbort),
                remote: Decision::Commit,
            }));
        }

        let Some(mut tx_rec) = proposed_block_change_set
            .get_transaction(tx, locked_block, &block.as_leaf_block(), atom.id())
            .optional()?
        else {
            warn!(
                target: LOG_TARGET,
                "⚠️ Local proposal received ({}) for transaction {} which is not in the pool. This is likely a previous transaction that has been re-proposed. Not voting on block.",
                block,
                atom.id(),
            );
            return Ok(Some(NoVoteReason::TransactionNotInPool));
        };

        if !tx_rec.current_stage().is_local_accepted() {
            warn!(
                target: LOG_TARGET,
                "{} ❌ Stage disagreement in block {} for transaction {}. Leader proposed SomeAccept, but local stage is {}",
                self.local_validator_pk,
                block,
                tx_rec.transaction_id(),
                tx_rec.current_stage()
            );
            return Ok(Some(NoVoteReason::StageDisagreement {
                expected: TransactionPoolStage::LocalAccepted,
                stage: tx_rec.current_stage(),
            }));
        }

        // We check that the leader decision is the same as our local decision (this will change to ABORT once we've
        // received the foreign LocalAccept).
        if tx_rec.current_decision().is_commit() {
            warn!(
                target: LOG_TARGET,
                "❌ SomeAccept decision disagreement for transaction {} in block {}. Leader proposed ABORT, we decided COMMIT",
                tx_rec.transaction_id(),
                block,
            );
            return Ok(Some(NoVoteReason::DecisionDisagreement {
                local: Decision::Commit,
                remote: atom.decision,
            }));
        }

        if tx_rec.transaction_fee() != atom.transaction_fee {
            warn!(
                target: LOG_TARGET,
                "❌ SomeAccept transaction fee disagreement tx {} in block {}. Leader proposed {}, we calculated {}",
                tx_rec.transaction_id(),
                block,
                atom.transaction_fee,
                tx_rec.transaction_fee()
            );
            return Ok(Some(NoVoteReason::FeeDisagreement));
        }

        tx_rec.set_next_stage(TransactionPoolStage::SomeAccepted)?;
        proposed_block_change_set.set_next_transaction_update(tx_rec)?;

        Ok(None)
    }

    fn evaluate_foreign_proposal_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        local_block: &Block,
        locked_block: &LockedBlock,
        fp_atom: &ForeignProposalAtom,
        local_committee_info: &CommitteeInfo,
        foreign_shard_group: ShardGroup,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        if proposed_block_change_set
            .proposed_foreign_proposals()
            .contains(&fp_atom.block_id)
        {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Foreign proposal {block_id} has already been proposed in this block.",
                block_id = fp_atom.block_id,
            );
            return Ok(Some(NoVoteReason::ForeignProposalAlreadyProposed));
        }

        let Some(fp) = fp_atom.get_proposal(tx).optional()? else {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Foreign proposal {block_id} has not been received.",
                block_id = fp_atom.block_id,
            );
            return Ok(Some(NoVoteReason::ForeignProposalNotReceived));
        };

        // Case: cannot re-propose if it is already committed
        // TODO: if this is already proposed we need to reject if it is already proposed in the current block's
        // commit->leaf chain Currently we allow it to be proposed again
        if matches!(fp.status(), ForeignProposalStatus::Confirmed) {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Foreign proposal {block_id} has status {status}.",
                block_id = fp_atom.block_id,
                status = fp.status(),
            );
            return Ok(Some(NoVoteReason::ForeignProposalAlreadyConfirmed));
        }

        if let Err(err) = process_foreign_block(
            tx,
            &local_block.as_leaf_block(),
            locked_block,
            &fp,
            local_committee_info,
            substate_store,
            proposed_block_change_set,
        ) {
            // TODO: split validation errors from HotStuff errors so that we can selectively crash or not vote
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Failed to process foreign proposal {foreign_block_id} from {shard_group} for local block {block} Error: {error}",
                block = local_block,
                foreign_block_id = fp_atom.block_id,
                error = err,
                shard_group = foreign_shard_group,
            );
            return Ok(Some(NoVoteReason::ForeignProposalProcessingFailed));
        }

        proposed_block_change_set.set_foreign_proposal_proposed_in(fp_atom.block_id);

        Ok(None)
    }

    fn evaluate_mint_confidential_output_command(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        atom: &MintConfidentialOutputAtom,
        local_committee_info: &CommitteeInfo,
        substate_store: &mut PendingSubstateStore<TConsensusSpec::StateStore>,
        proposed_block_change_set: &mut ProposedBlockChangeSet,
    ) -> Result<Option<NoVoteReason>, HotStuffError> {
        let Some(utxo) = atom.get(tx).optional()? else {
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: MintConfidentialOutputAtom for {} is not known.",
                atom.commitment
            );
            return Ok(Some(NoVoteReason::MintConfidentialOutputUnknown));
        };
        let id = VersionedSubstateId::new(utxo.commitment, 0);
        let shard = id.to_shard(local_committee_info.num_preshards());
        let change = SubstateChange::Up {
            id,
            shard,
            // N/A
            transaction_id: Default::default(),
            substate: Substate::new(0, utxo.output),
        };

        if let Err(err) = substate_store.put(change) {
            let err = err.ok_lock_failed()?;
            warn!(
                target: LOG_TARGET,
                "❌ NO VOTE: Failed to store mint confidential output for {}. Error: {}",
                atom.commitment,
                err
            );
            return Ok(Some(NoVoteReason::MintConfidentialOutputStoreFailed));
        }

        proposed_block_change_set.set_utxo_mint_proposed_in(utxo.commitment);

        Ok(None)
    }

    fn execute_transaction(
        &self,
        tx: &<TConsensusSpec::StateStore as StateStore>::ReadTransaction<'_>,
        block_id: &BlockId,
        current_epoch: Epoch,
        transaction: TransactionRecord,
    ) -> Result<BlockTransactionExecution, HotStuffError> {
        info!(
            target: LOG_TARGET,
            "👨‍🔧 DECIDE: Executing transaction {} in block {}",
            transaction.id(),
            block_id,
        );
        // Might have been executed already in on propose
        if let Some(execution) =
            BlockTransactionExecution::get_pending_for_block(tx, transaction.id(), block_id).optional()?
        {
            return Ok(execution);
        }

        let pledged = PledgedTransaction::load_pledges(tx, transaction)?;

        let executed = self
            .transaction_manager
            .execute(current_epoch, pledged)
            .map_err(|e| HotStuffError::TransactionExecutorError(e.to_string()))?;

        Ok(executed.into_execution().for_block(*block_id))
    }

    fn on_commit(
        &self,
        tx: &mut <TConsensusSpec::StateStore as StateStore>::WriteTransaction<'_>,
        last_executed: &LastExecuted,
        block: &Block,
        local_committee_info: &CommitteeInfo,
    ) -> Result<Vec<TransactionPoolRecord>, HotStuffError> {
        let committed_transactions = self.finalize_block(tx, block, local_committee_info)?;
        debug!(
            target: LOG_TARGET,
            "✅ COMMIT block {}, last executed height = {}",
            block,
            last_executed.height
        );
        self.publish_event(HotstuffEvent::BlockCommitted {
            epoch: block.epoch(),
            block_id: *block.id(),
            height: block.height(),
        });
        Ok(committed_transactions)
    }

    fn on_lock_block(
        &self,
        tx: &mut <TConsensusSpec::StateStore as StateStore>::WriteTransaction<'_>,
        new_locked_block: &Block,
    ) -> Result<(), HotStuffError> {
        info!(
            target: LOG_TARGET,
            "🔒️ LOCKED BLOCK: {}",
            new_locked_block,
        );

        // Release all locks for SomePrepare transactions since these can never be committed
        SubstateRecord::unlock_all(tx, new_locked_block.all_some_prepare().map(|t| &t.id))?;

        // Remove the chains that are no longer in this block's chain
        // This will also release any locks for blocks that no longer apply
        new_locked_block.remove_parallel_chains(tx)?;

        // This moves the stage update from pending to current for all transactions on the locked block
        self.transaction_pool
            .confirm_all_transitions(tx, &new_locked_block.as_locked_block())?;

        Ok(())
    }

    fn publish_event(&self, event: HotstuffEvent) {
        let _ignore = self.tx_events.send(event);
    }

    fn finalize_block(
        &self,
        tx: &mut <TConsensusSpec::StateStore as StateStore>::WriteTransaction<'_>,
        block: &Block,
        local_committee_info: &CommitteeInfo,
    ) -> Result<Vec<TransactionPoolRecord>, HotStuffError> {
        if block.is_dummy() {
            block.increment_leader_failure_count(
                tx,
                self.config.consensus_constants.missed_proposal_recovery_threshold,
            )?;

            // Nothing to do here for empty dummy blocks. Just mark the block as committed.
            block.commit_diff(tx, BlockDiff::empty(*block.id()))?;
            return Ok(vec![]);
        }

        let diff = block.get_diff(&**tx)?;
        info!(
            target: LOG_TARGET,
            "🌳 Committing block {} with {} substate change(s)", block, diff.len()
        );

        for atom in block.all_foreign_proposals() {
            // TODO: we need to keep these ATM to send them if a node needs to catch up
            atom.set_status(tx, ForeignProposalStatus::Confirmed)?;
        }

        for atom in block.all_confidential_output_mints() {
            atom.delete(tx)?;
        }

        for atom in block.all_node_evictions() {
            atom.mark_as_committed_in_epoch(tx, block.epoch())?;
        }

        // NOTE: this must happen before we commit the substate diff because the state transitions use this version
        let pending = block.remove_pending_tree_diff_and_return(tx)?;
        let mut state_tree = ShardedStateTree::new(tx);
        state_tree.commit_diffs(pending)?;
        let tx = state_tree.into_transaction();

        let local_diff = diff.into_filtered(local_committee_info);
        block.commit_diff(tx, local_diff)?;

        let finalized_transactions = self
            .transaction_pool
            .remove_all(tx, block.all_finalising_transactions_ids())?;

        // Whenever we commit a block that will result in an abort for a transaction, we can remove lock conflicts to
        // allow other "blocked" transactions to be proposed.
        TransactionLockConflicts::remove_for_transactions(tx, block.all_aborting_transaction_ids())?;

        if !finalized_transactions.is_empty() {
            // Remove locks for finalized transactions
            SubstateRecord::unlock_all(tx, finalized_transactions.iter().map(|t| t.transaction_id()))?;
            TransactionRecord::finalize_all(tx, *block.id(), &finalized_transactions)?;

            debug!(
                target: LOG_TARGET,
                "✅ {} transactions finalized",
                finalized_transactions.len(),
            );
        }

        let total_transaction_fee = block.calculate_total_transaction_fee();
        if total_transaction_fee > 0 {
            info!(
                target: LOG_TARGET,
                "🪙 Validator fee ({}, Total Fees Paid = {}) for block {}",
                block.total_leader_fee(),
                total_transaction_fee,
                block,
            );
        }

        block.justify().update_participation_shares(tx)?;
        block.clear_leader_failure_count(tx)?;

        Ok(finalized_transactions)
    }
}
