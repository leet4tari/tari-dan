//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::RangeInclusive,
};

use diesel::{
    query_builder::SqlQuery,
    sql_query,
    sql_types::{BigInt, Text},
    BoolExpressionMethods,
    ExpressionMethods,
    JoinOnDsl,
    NullableExpressionMethods,
    OptionalExtension,
    QueryDsl,
    QueryableByName,
    RunQueryDsl,
    SelectableHelper,
    SqliteConnection,
    TextExpressionMethods,
};
use indexmap::IndexMap;
use log::*;
use serde::{de::DeserializeOwned, Serialize};
use tari_common_types::types::{FixedHash, PublicKey};
use tari_dan_common_types::{
    optional::Optional,
    shard::Shard,
    Epoch,
    NodeAddressable,
    NodeHeight,
    ShardGroup,
    SubstateAddress,
    SubstateLockType,
    ToSubstateAddress,
    VersionedSubstateId,
    VersionedSubstateIdRef,
};
use tari_dan_storage::{
    consensus_models::{
        Block,
        BlockDiff,
        BlockId,
        BlockTransactionExecution,
        BurntUtxo,
        Command,
        EpochCheckpoint,
        ForeignProposal,
        ForeignProposalAtom,
        ForeignProposalStatus,
        ForeignReceiveCounters,
        ForeignSendCounters,
        HighQc,
        LastExecuted,
        LastProposed,
        LastSentVote,
        LastVoted,
        LeafBlock,
        LockedBlock,
        LockedSubstateValue,
        PendingShardStateTreeDiff,
        QcId,
        QuorumCertificate,
        StateTransition,
        StateTransitionId,
        SubstateChange,
        SubstateLock,
        SubstatePledges,
        SubstateRecord,
        TransactionPoolConfirmedStage,
        TransactionPoolRecord,
        TransactionPoolStage,
        TransactionRecord,
        ValidatorConsensusStats,
        Vote,
    },
    Ordering,
    StateStoreReadTransaction,
    StorageError,
};
use tari_engine_types::{substate::SubstateId, template_models::UnclaimedConfidentialOutputAddress};
use tari_state_tree::{Node, NodeKey, TreeNode, Version};
use tari_transaction::TransactionId;
use tari_utilities::{hex::Hex, ByteArray};

use crate::{
    error::SqliteStorageError,
    serialization::{deserialize_hex_try_from, deserialize_json, serialize_hex},
    sql_models,
    sqlite_transaction::SqliteTransaction,
};

const LOG_TARGET: &str = "tari::dan::storage::state_store_sqlite::reader";

pub struct SqliteStateStoreReadTransaction<'a, TAddr> {
    transaction: SqliteTransaction<'a>,
    _addr: PhantomData<TAddr>,
}

impl<'a, TAddr> SqliteStateStoreReadTransaction<'a, TAddr> {
    pub(crate) fn new(transaction: SqliteTransaction<'a>) -> Self {
        Self {
            transaction,
            _addr: PhantomData,
        }
    }

    pub(crate) fn connection(&self) -> &mut SqliteConnection {
        self.transaction.connection()
    }

    pub(crate) fn commit(self) -> Result<(), SqliteStorageError> {
        self.transaction.commit()
    }

    pub(crate) fn rollback(self) -> Result<(), SqliteStorageError> {
        self.transaction.rollback()
    }
}

impl<'a, TAddr: NodeAddressable + Serialize + DeserializeOwned + 'a> SqliteStateStoreReadTransaction<'a, TAddr> {
    pub(crate) fn get_transaction_atom_state_updates_between_blocks<'i, ITx>(
        &self,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
        transaction_ids: ITx,
    ) -> Result<IndexMap<String, sql_models::TransactionPoolStateUpdate>, SqliteStorageError>
    where
        ITx: Iterator<Item = &'i str> + ExactSizeIterator,
    {
        if transaction_ids.len() == 0 {
            return Ok(IndexMap::new());
        }

        // Blocks without commands may change pending transaction state because they justify a
        // block that proposes a change. So we cannot only use blocks that have commands.
        let applicable_block_ids = self.get_block_ids_between(from_block_id, to_block_id, 1000)?;

        debug!(
            target: LOG_TARGET,
            "get_transaction_atom_state_updates_between_blocks: from_block_id={}, to_block_id={}, len(applicable_block_ids)={}",
            from_block_id,
            to_block_id,
            applicable_block_ids.len());

        if applicable_block_ids.is_empty() {
            return Ok(IndexMap::new());
        }

        self.create_transaction_atom_updates_query(transaction_ids, applicable_block_ids.iter().map(|s| s.as_str()))
            .load_iter::<sql_models::TransactionPoolStateUpdate, _>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_get_many_ready",
                source: e,
            })?
            .map(|update| update.map(|u| (u.transaction_id.clone(), u)))
            .collect::<diesel::QueryResult<_>>()
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_get_many_ready",
                source: e,
            })
    }

    /// Creates a query to select the latest transaction pool state updates for the given transaction ids and block ids.
    /// If no transaction ids are provided, all updates for the given block ids are returned.
    /// WARNING: This method does not protect against SQL-injection, Be sure that the transaction ids and block ids
    /// strings are what they are meant to be.
    fn create_transaction_atom_updates_query<
        'i1,
        'i2,
        IBlk: Iterator<Item = &'i1 str> + ExactSizeIterator,
        ITx: Iterator<Item = &'i2 str> + ExactSizeIterator,
    >(
        &self,
        transaction_ids: ITx,
        block_ids: IBlk,
    ) -> SqlQuery {
        // Query all updates if the transaction_ids are empty
        let in_transactions = if transaction_ids.len() == 0 {
            String::new()
        } else {
            format!(
                "AND tpsu.transaction_id in ({})",
                self.sql_frag_for_in_statement(transaction_ids, TransactionId::byte_size() * 2)
            )
        };
        // Unfortunate hack. Binding array types in diesel is only supported for postgres.
        sql_query(format!(
            r#"
                 WITH RankedResults AS (
                    SELECT
                        tpsu.*,
                        ROW_NUMBER() OVER (PARTITION BY tpsu.transaction_id ORDER BY tpsu.block_height DESC) AS `rank`
                    FROM
                        transaction_pool_state_updates AS tpsu
                    WHERE
                        is_applied = 0  AND
                        tpsu.block_id in ({})
                    {}
                )
                SELECT
                    id,
                    block_id,
                    block_height,
                    transaction_id,
                    stage,
                    evidence,
                    is_ready,
                    local_decision,
                    transaction_fee,
                    leader_fee,
                    remote_decision,
                    is_applied,
                    created_at
                FROM
                    RankedResults
                WHERE
                    rank = 1;
                "#,
            self.sql_frag_for_in_statement(block_ids, BlockId::byte_size() * 2),
            in_transactions
        ))
    }

    fn sql_frag_for_in_statement<T: AsRef<str>, I: Iterator<Item = T> + ExactSizeIterator>(
        &self,
        values: I,
        item_size: usize,
    ) -> String {
        let len = values.len();
        let mut sql_frag = String::with_capacity((len * item_size + len * 3 + len).saturating_sub(1));
        for (i, value) in values.enumerate() {
            sql_frag.push('"');
            sql_frag.push_str(value.as_ref());
            sql_frag.push('"');
            if i < len - 1 {
                sql_frag.push(',');
            }
        }
        sql_frag
    }

    /// Returns the blocks from the start_block (inclusive) to the end_block (inclusive).
    /// It is the callers responsibility to ensure that start/end block IDs exist and start block is before end block.
    /// Failing this, the result if this function is undefined but typically results in an empty Vec or returning all
    /// block ids from genesis.
    fn get_block_ids_between(
        &self,
        start_block: &BlockId,
        end_block: &BlockId,
        limit: u64,
    ) -> Result<Vec<String>, SqliteStorageError> {
        debug!(target: LOG_TARGET, "get_block_ids_between: start: {start_block}, end: {end_block}");
        let block_ids = sql_query(
            r#"
            WITH RECURSIVE tree(bid, parent) AS (
                SELECT block_id, parent_block_id FROM blocks where block_id = ?
            UNION ALL
                SELECT block_id, parent_block_id
                FROM blocks JOIN tree ON
                    block_id = tree.parent
                    AND tree.bid != ?
                    AND tree.parent != '0000000000000000000000000000000000000000000000000000000000000000'
                LIMIT ?
            )
            SELECT bid FROM tree"#,
        )
        .bind::<Text, _>(serialize_hex(end_block))
        .bind::<Text, _>(serialize_hex(start_block))
        .bind::<BigInt, _>(limit as i64)
        .load_iter::<BlockIdSqlValue, _>(self.connection())
        .map_err(|e| SqliteStorageError::DieselError {
            operation: "get_block_ids_that_change_state_between",
            source: e,
        })?;

        block_ids
            .map(|b| {
                b.map(|b| b.bid).map_err(|e| SqliteStorageError::DieselError {
                    operation: "get_block_ids_that_change_state_between",
                    source: e,
                })
            })
            .collect()
    }

    pub(crate) fn get_block_ids_with_commands_between(
        &self,
        start_block: &BlockId,
        end_block: &BlockId,
    ) -> Result<Vec<String>, SqliteStorageError> {
        let block_ids = sql_query(
            r#"
            WITH RECURSIVE tree(bid, parent, is_dummy, command_count) AS (
                SELECT block_id, parent_block_id, is_dummy, command_count FROM blocks where block_id = ?
            UNION ALL
                SELECT block_id, parent_block_id, blocks.is_dummy, blocks.command_count
                FROM blocks JOIN tree ON
                    block_id = tree.parent
                    AND tree.bid != ?
                    AND tree.parent != '0000000000000000000000000000000000000000000000000000000000000000'
                LIMIT 1000
            )
            SELECT bid FROM tree where is_dummy = 0 AND command_count > 0"#,
        )
        .bind::<Text, _>(serialize_hex(end_block))
        .bind::<Text, _>(serialize_hex(start_block))
        .load_iter::<BlockIdSqlValue, _>(self.connection())
        .map_err(|e| SqliteStorageError::DieselError {
            operation: "get_block_ids_that_change_state_between",
            source: e,
        })?;

        block_ids
            .map(|b| {
                b.map(|b| b.bid).map_err(|e| SqliteStorageError::DieselError {
                    operation: "get_block_ids_that_change_state_between",
                    source: e,
                })
            })
            .collect()
    }

    /// Used in tests, therefore not used in consensus and not part of the trait
    pub fn transactions_count(&self) -> Result<u64, SqliteStorageError> {
        use crate::schema::transactions;

        let count = transactions::table
            .count()
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transactions_count",
                source: e,
            })?;

        Ok(count as u64)
    }

    pub(crate) fn get_commit_block(&self) -> Result<LeafBlock, StorageError> {
        use crate::schema::blocks;

        let (block_id, height, epoch) = blocks::table
            .select((blocks::block_id, blocks::height, blocks::epoch))
            .filter(blocks::is_committed.eq(true))
            .order_by(blocks::id.desc())
            .first::<(String, i64, i64)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "get_commit_block_id",
                source: e,
            })?;

        let block_id = deserialize_hex_try_from(&block_id)?;
        Ok(LeafBlock {
            block_id,
            height: NodeHeight(height as u64),
            epoch: Epoch(epoch as u64),
        })
    }

    pub fn substates_count(&self) -> Result<u64, SqliteStorageError> {
        use crate::schema::substates;

        let count = substates::table
            .count()
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_count",
                source: e,
            })?;

        Ok(count as u64)
    }

    pub fn blocks_get_tip(&self, epoch: Epoch, shard_group: ShardGroup) -> Result<Block, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let (block, qc) = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::epoch.eq(epoch.as_u64() as i64))
            .filter(blocks::shard_group.eq(shard_group.encode_as_u32() as i32))
            .order_by(blocks::height.desc())
            .first::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_tip",
                source: e,
            })?;

        let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
            operation: "blocks_get_tip",
            details: format!(
                "block {} references non-existent quorum certificate {}",
                block.block_id, block.qc_id
            ),
        })?;

        block.try_convert(qc)
    }

    fn get_current_locked_block(&self) -> Result<LockedBlock, StorageError> {
        use crate::schema::locked_block;

        let locked_block = locked_block::table
            .order_by(locked_block::id.desc())
            .first::<sql_models::LockedBlock>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "get_current_locked_block",
                source: e,
            })?;

        locked_block.try_into()
    }
}

impl<'tx, TAddr: NodeAddressable + Serialize + DeserializeOwned + 'tx> StateStoreReadTransaction
    for SqliteStateStoreReadTransaction<'tx, TAddr>
{
    type Addr = TAddr;

    fn last_sent_vote_get(&self) -> Result<LastSentVote, StorageError> {
        use crate::schema::last_sent_vote;

        let last_voted = last_sent_vote::table
            .order_by(last_sent_vote::id.desc())
            .first::<sql_models::LastSentVote>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "last_sent_vote_get",
                source: e,
            })?;

        last_voted.try_into()
    }

    fn last_voted_get(&self) -> Result<LastVoted, StorageError> {
        use crate::schema::last_voted;

        let last_voted = last_voted::table
            .order_by(last_voted::id.desc())
            .first::<sql_models::LastVoted>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "last_voted_get",
                source: e,
            })?;

        last_voted.try_into()
    }

    fn last_executed_get(&self) -> Result<LastExecuted, StorageError> {
        use crate::schema::last_executed;

        let last_executed = last_executed::table
            .order_by(last_executed::id.desc())
            .first::<sql_models::LastExecuted>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "last_executed_get",
                source: e,
            })?;

        last_executed.try_into()
    }

    fn last_proposed_get(&self) -> Result<LastProposed, StorageError> {
        use crate::schema::last_proposed;

        let last_proposed = last_proposed::table
            .order_by(last_proposed::id.desc())
            .first::<sql_models::LastProposed>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "last_proposed_get",
                source: e,
            })?;

        last_proposed.try_into()
    }

    fn locked_block_get(&self, epoch: Epoch) -> Result<LockedBlock, StorageError> {
        use crate::schema::locked_block;

        let locked_block = locked_block::table
            .filter(locked_block::epoch.eq(epoch.as_u64() as i64))
            .order_by(locked_block::id.desc())
            .first::<sql_models::LockedBlock>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "locked_block_get",
                source: e,
            })?;

        locked_block.try_into()
    }

    fn leaf_block_get(&self, epoch: Epoch) -> Result<LeafBlock, StorageError> {
        use crate::schema::leaf_blocks;

        let leaf_block = leaf_blocks::table
            .filter(leaf_blocks::epoch.eq(epoch.as_u64() as i64))
            .order_by(leaf_blocks::id.desc())
            .first::<sql_models::LeafBlock>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "leaf_block_get",
                source: e,
            })?;

        leaf_block.try_into()
    }

    fn high_qc_get(&self, epoch: Epoch) -> Result<HighQc, StorageError> {
        use crate::schema::high_qcs;

        let high_qc = high_qcs::table
            .filter(high_qcs::epoch.eq(epoch.as_u64() as i64))
            .order_by(high_qcs::id.desc())
            .first::<sql_models::HighQc>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "high_qc_get",
                source: e,
            })?;

        high_qc.try_into()
    }

    fn foreign_proposals_get_any<'a, I: IntoIterator<Item = &'a BlockId>>(
        &self,
        block_ids: I,
    ) -> Result<Vec<ForeignProposal>, StorageError> {
        use crate::schema::{foreign_proposals, quorum_certificates};

        let mut block_ids = block_ids.into_iter().peekable();
        if block_ids.peek().is_none() {
            return Ok(vec![]);
        }

        let foreign_proposals = foreign_proposals::table
            .left_join(quorum_certificates::table.on(foreign_proposals::justify_qc_id.eq(quorum_certificates::qc_id)))
            .filter(foreign_proposals::block_id.eq_any(block_ids.map(serialize_hex)))
            .get_results::<(sql_models::ForeignProposal, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_proposals_get_any",
                source: e,
            })?;

        foreign_proposals
            .into_iter()
            .map(|(proposal, qc)| {
                let justify_qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "foreign_proposals_get_any",
                    details: format!(
                        "foreign proposal {} references non-existent quorum certificate {}",
                        proposal.block_id, proposal.justify_qc_id
                    ),
                })?;
                proposal.try_convert(justify_qc)
            })
            .collect()
    }

    fn foreign_proposals_exists(&self, block_id: &BlockId) -> Result<bool, StorageError> {
        use crate::schema::foreign_proposals;

        let foreign_proposals = foreign_proposals::table
            .filter(foreign_proposals::block_id.eq(serialize_hex(block_id)))
            .count()
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_proposals_exists",
                source: e,
            })?;

        Ok(foreign_proposals > 0)
    }

    fn foreign_proposals_has_unconfirmed(&self, epoch: Epoch) -> Result<bool, StorageError> {
        use crate::schema::foreign_proposals;

        let foreign_proposals = foreign_proposals::table
            .filter(foreign_proposals::epoch.le(epoch.as_u64() as i64))
            .filter(
                foreign_proposals::status
                    .eq(ForeignProposalStatus::New.to_string())
                    .or(foreign_proposals::status.eq(ForeignProposalStatus::Proposed.to_string())),
            )
            .count()
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_proposals_has_unconfirmed",
                source: e,
            })?;

        Ok(foreign_proposals > 0)
    }

    fn foreign_proposals_get_all_new(
        &self,
        block_id: &BlockId,
        limit: usize,
    ) -> Result<Vec<ForeignProposal>, StorageError> {
        use crate::schema::{foreign_proposals, quorum_certificates};

        if !self.blocks_exists(block_id)? {
            return Err(StorageError::NotFound {
                item: "foreign_proposals_get_all_new: Block",
                key: block_id.to_string(),
            });
        }

        let locked = self.get_current_locked_block()?;
        let pending_block_ids = self.get_block_ids_with_commands_between(&locked.block_id, block_id)?;

        let foreign_proposals = foreign_proposals::table
            .left_join(quorum_certificates::table.on(foreign_proposals::justify_qc_id.eq(quorum_certificates::qc_id)))
            .filter(foreign_proposals::epoch.le(locked.epoch.as_u64() as i64))
            .filter(foreign_proposals::status.ne(ForeignProposalStatus::Confirmed.to_string()))
            .filter(foreign_proposals::status.ne(ForeignProposalStatus::Invalid.to_string()))
            .filter(
                foreign_proposals::proposed_in_block
                    .is_null()
                    .or(foreign_proposals::proposed_in_block
                        .ne_all(pending_block_ids)
                        .and(foreign_proposals::proposed_in_block_height.gt(locked.height.as_u64() as i64))),
            )
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .get_results::<(sql_models::ForeignProposal, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_proposals_get_all_new",
                source: e,
            })?;

        foreign_proposals
            .into_iter()
            .map(|(proposal, qc)| {
                let justify_qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "foreign_proposals_get_all_new",
                    details: format!(
                        "foreign proposal {} references non-existent quorum certificate {}",
                        proposal.block_id, proposal.justify_qc_id
                    ),
                })?;
                proposal.try_convert(justify_qc)
            })
            .collect()
    }

    fn foreign_proposal_get_all_pending(
        &self,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
    ) -> Result<Vec<ForeignProposalAtom>, StorageError> {
        use crate::schema::blocks;

        let blocks = self.get_block_ids_with_commands_between(from_block_id, to_block_id)?;

        let all_commands: Vec<String> = blocks::table
            .select(blocks::commands)
            .filter(blocks::command_count.gt(0)) // if there is no command, then there is definitely no foreign proposal command
            .filter(blocks::block_id.eq_any(blocks))
            .load::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_proposal_get_all",
                source: e,
            })?;
        let all_commands = all_commands
            .into_iter()
            .map(|commands| deserialize_json(commands.as_str()))
            .collect::<Result<Vec<Vec<Command>>, _>>()?;
        let all_commands = all_commands.into_iter().flatten().collect::<Vec<_>>();
        Ok(all_commands
            .into_iter()
            .filter_map(|command| command.foreign_proposal().cloned())
            .collect::<Vec<ForeignProposalAtom>>())
    }

    fn foreign_send_counters_get(&self, block_id: &BlockId) -> Result<ForeignSendCounters, StorageError> {
        use crate::schema::foreign_send_counters;

        let counter = foreign_send_counters::table
            .filter(foreign_send_counters::block_id.eq(serialize_hex(block_id)))
            .first::<sql_models::ForeignSendCounters>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_send_counters_get",
                source: e,
            })?;

        counter.try_into()
    }

    fn foreign_receive_counters_get(&self) -> Result<ForeignReceiveCounters, StorageError> {
        use crate::schema::foreign_receive_counters;

        let counter = foreign_receive_counters::table
            .order_by(foreign_receive_counters::id.desc())
            .first::<sql_models::ForeignReceiveCounters>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_receive_counters_get",
                source: e,
            })?;

        counter.try_into()
    }

    fn transactions_get(&self, tx_id: &TransactionId) -> Result<TransactionRecord, StorageError> {
        use crate::schema::transactions;

        let transaction = transactions::table
            .select(sql_models::Transaction::as_select())
            .filter(transactions::transaction_id.eq(serialize_hex(tx_id)))
            .first::<sql_models::Transaction>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transactions_get",
                source: e,
            })?;

        transaction.try_into()
    }

    fn transactions_exists(&self, tx_id: &TransactionId) -> Result<bool, StorageError> {
        use crate::schema::transactions;

        let exists = transactions::table
            .count()
            .filter(transactions::transaction_id.eq(serialize_hex(tx_id)))
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transactions_exists",
                source: e,
            })?;

        Ok(exists > 0)
    }

    fn transactions_get_any<'a, I: IntoIterator<Item = &'a TransactionId>>(
        &self,
        tx_ids: I,
    ) -> Result<Vec<TransactionRecord>, StorageError> {
        use crate::schema::transactions;

        let mut tx_ids = tx_ids.into_iter().map(serialize_hex).peekable();
        if tx_ids.peek().is_none() {
            return Ok(vec![]);
        }

        let transactions = transactions::table
            .filter(transactions::transaction_id.eq_any(tx_ids))
            .load::<sql_models::Transaction>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transactions_get_any",
                source: e,
            })?;

        transactions
            .into_iter()
            .map(|transaction| transaction.try_into())
            .collect()
    }

    fn transactions_get_paginated(
        &self,
        limit: u64,
        offset: u64,
        asc_desc_created_at: Option<Ordering>,
    ) -> Result<Vec<TransactionRecord>, StorageError> {
        use crate::schema::transactions;

        let mut query = transactions::table.into_boxed();

        if let Some(ordering) = asc_desc_created_at {
            match ordering {
                Ordering::Ascending => query = query.order_by(transactions::created_at.asc()),
                Ordering::Descending => query = query.order_by(transactions::created_at.desc()),
            }
        }

        let transactions = query
            .limit(limit as i64)
            .offset(offset as i64)
            .get_results::<sql_models::Transaction>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transactions_get_paginated",
                source: e,
            })?;

        transactions
            .into_iter()
            .map(|transaction| transaction.try_into())
            .collect()
    }

    fn transaction_executions_get(
        &self,
        tx_id: &TransactionId,
        block: &BlockId,
    ) -> Result<BlockTransactionExecution, StorageError> {
        use crate::schema::transaction_executions;

        let execution = transaction_executions::table
            .filter(transaction_executions::transaction_id.eq(serialize_hex(tx_id)))
            .filter(transaction_executions::block_id.eq(serialize_hex(block)))
            .first::<sql_models::TransactionExecution>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_executions_get",
                source: e,
            })?;

        execution.try_into()
    }

    fn transaction_executions_get_pending_for_block(
        &self,
        tx_id: &TransactionId,
        from_block_id: &BlockId,
    ) -> Result<BlockTransactionExecution, StorageError> {
        use crate::schema::{blocks, transaction_executions};

        if !self.blocks_exists(from_block_id)? {
            return Err(StorageError::QueryError {
                reason: format!(
                    "transaction_executions_get_pending_for_block: Block {} does not exist",
                    from_block_id
                ),
            });
        }

        let commit_block = self.get_commit_block()?;
        let block_ids = self.get_block_ids_between(commit_block.block_id(), from_block_id, 1000)?;
        let tx_id = serialize_hex(tx_id);

        let execution = transaction_executions::table
            .filter(transaction_executions::transaction_id.eq(&tx_id))
            .filter(transaction_executions::block_id.eq_any(block_ids))
            // Get last execution
            .order_by(transaction_executions::id.desc())
            .first::<sql_models::TransactionExecution>(self.connection())
            .optional()
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_executions_get_pending_for_block",
                source: e,
            })?;

        if let Some(execution) = execution {
            return execution.try_into();
        }

        // Otherwise look for executions after the commit block
        let execution = transaction_executions::table
            .select(transaction_executions::all_columns)
            .inner_join(
                blocks::table.on(transaction_executions::block_id
                    .eq(blocks::block_id)
                    .and(blocks::is_committed.eq(true))),
            )
            .filter(transaction_executions::transaction_id.eq(&tx_id))
            .order_by(transaction_executions::id.desc())
            .first::<sql_models::TransactionExecution>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_executions_get_pending_for_block",
                source: e,
            })?;

        execution.try_into()
    }

    fn blocks_get(&self, block_id: &BlockId) -> Result<Block, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let (block, qc) = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::block_id.eq(serialize_hex(block_id)))
            .first::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get",
                source: e,
            })?;

        let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
            operation: "blocks_get",
            details: format!(
                "block {} references non-existent quorum certificate {}",
                block_id, block.qc_id
            ),
        })?;

        block.try_convert(qc)
    }

    fn blocks_get_all_ids_by_height(&self, epoch: Epoch, height: NodeHeight) -> Result<Vec<BlockId>, StorageError> {
        use crate::schema::blocks;

        let block_ids = blocks::table
            .select(blocks::block_id)
            .filter(blocks::height.eq(height.as_u64() as i64))
            .filter(blocks::epoch.eq(epoch.as_u64() as i64))
            .get_results::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_all_ids_by_height",
                source: e,
            })?;

        block_ids
            .into_iter()
            .map(|block_id| deserialize_hex_try_from(&block_id))
            .collect()
    }

    fn blocks_get_genesis_for_epoch(&self, epoch: Epoch) -> Result<Block, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let (block, qc) = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::epoch.eq(epoch.as_u64() as i64))
            .filter(blocks::height.eq(0))
            .first::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_genesis_for_epoch",
                source: e,
            })?;

        let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
            operation: "blocks_get_genesis_for_epoch",
            details: format!(
                "block {} references non-existent quorum certificate {}",
                block.id, block.qc_id
            ),
        })?;

        block.try_convert(qc)
    }

    fn blocks_get_last_n_in_epoch(&self, n: usize, epoch: Epoch) -> Result<Vec<Block>, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let blocks = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::epoch.eq(epoch.as_u64() as i64))
            .filter(blocks::is_committed.eq(true))
            .order_by(blocks::height.desc())
            .limit(n as i64)
            .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_last_n_in_epoch",
                source: e,
            })?;

        blocks
            .into_iter()
            // Order from lowest to highest height
            .rev()
            .map(|(b, qc)| {
                qc.ok_or_else(|| StorageError::DataInconsistency {
                    details: format!(
                        "blocks_get_last_n_in_epoch: block {} references non-existent quorum certificate {}",
                        b.block_id, b.qc_id
                    ),
                })
                    .and_then(|qc| b.try_convert(qc))
            })
            .collect()
    }

    fn blocks_get_all_between(
        &self,
        epoch: Epoch,
        shard_group: ShardGroup,
        start_block_height: NodeHeight,
        end_block_height: NodeHeight,
        include_dummy_blocks: bool,
        limit: u64,
    ) -> Result<Vec<Block>, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        if start_block_height > end_block_height {
            return Err(StorageError::QueryError {
                reason: format!(
                    "Start block height {start_block_height} must be less than end block height {end_block_height}"
                ),
            });
        }

        let mut query = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .into_boxed();

        if !include_dummy_blocks {
            query = query.filter(blocks::is_dummy.eq(false));
        }

        let results = query
            .filter(blocks::epoch.eq(epoch.as_u64() as i64))
            .filter(blocks::shard_group.eq(shard_group.encode_as_u32() as i32))
            .filter(blocks::height.ge(start_block_height.as_u64() as i64))
            .filter(blocks::height.le(end_block_height.as_u64() as i64))
            .order_by(blocks::height.asc())
            .limit(limit as i64)
            .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_all_after_height",
                source: e,
            })?;

        results
            .into_iter()
            .map(|(block, qc)| {
                let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "blocks_all_after_height",
                    details: format!(
                        "block {} references non-existent quorum certificate {}",
                        block.block_id, block.qc_id
                    ),
                })?;

                block.try_convert(qc)
            })
            .collect()
    }

    fn blocks_exists(&self, block_id: &BlockId) -> Result<bool, StorageError> {
        use crate::schema::blocks;

        let count = blocks::table
            .filter(blocks::block_id.eq(serialize_hex(block_id)))
            .count()
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_exists",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn blocks_is_ancestor(&self, descendant: &BlockId, ancestor: &BlockId) -> Result<bool, StorageError> {
        if !self.blocks_exists(descendant)? {
            return Err(StorageError::QueryError {
                reason: format!("blocks_is_ancestor: descendant block {} does not exist", descendant),
            });
        }

        if !self.blocks_exists(ancestor)? {
            return Err(StorageError::QueryError {
                reason: format!("blocks_is_ancestor: ancestor block {} does not exist", ancestor),
            });
        }

        // TODO: this scans all the way to genesis for every query - can optimise though it's low priority for now
        let is_ancestor = sql_query(
            r#"
            WITH RECURSIVE tree(bid, parent) AS (
                  SELECT block_id, parent_block_id FROM blocks where block_id = ?
                UNION ALL
                  SELECT block_id, parent_block_id
                    FROM blocks JOIN tree ON block_id = tree.parent AND tree.bid != tree.parent -- stop recursing at zero block (or any self referencing block)
            )
            SELECT count(1) as "count" FROM tree WHERE bid = ? LIMIT 1
        "#,
        )
            .bind::<Text, _>(serialize_hex(descendant))
            // .bind::<Text, _>(serialize_hex(BlockId::genesis())) // stop recursing at zero block
            .bind::<Text, _>(serialize_hex(ancestor))
            .get_result::<Count>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_is_ancestor",
                source: e,
            })?;

        debug!(target: LOG_TARGET, "blocks_is_ancestor: is_ancestor: {}", is_ancestor.count);

        Ok(is_ancestor.count > 0)
    }

    fn blocks_get_ids_by_parent(&self, parent_id: &BlockId) -> Result<Vec<BlockId>, StorageError> {
        use crate::schema::blocks;

        let results = blocks::table
            .select(blocks::block_id)
            .filter(blocks::parent_block_id.eq(serialize_hex(parent_id)))
            .filter(blocks::block_id.ne(blocks::parent_block_id)) // Exclude the genesis block
            .get_results::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_by_parent",
                source: e,
            })?;

        results
            .into_iter()
            .map(|block_id| deserialize_hex_try_from(&block_id))
            .collect()
    }

    fn blocks_get_all_by_parent(&self, parent_id: &BlockId) -> Result<Vec<Block>, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let results = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::parent_block_id.eq(serialize_hex(parent_id)))
            .filter(blocks::block_id.ne(blocks::parent_block_id)) // Exclude the genesis block
            .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_by_parent",
                source: e,
            })?;

        results
            .into_iter()
            .map(|(block, qc)| {
                let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "blocks_get_by_parent",
                    details: format!(
                        "block {} references non-existent quorum certificate {}",
                        parent_id, block.qc_id
                    ),
                })?;

                block.try_convert(qc)
            })
            .collect()
    }

    fn blocks_get_parent_chain(&self, block_id: &BlockId, limit: usize) -> Result<Vec<Block>, StorageError> {
        if !self.blocks_exists(block_id)? {
            return Err(StorageError::QueryError {
                reason: format!("blocks_get_parent_chain: descendant block {} does not exist", block_id),
            });
        }
        let blocks = sql_query(
            r#"
            WITH RECURSIVE tree(bid, parent) AS (
                  SELECT block_id, parent_block_id FROM blocks where block_id = ?
                UNION ALL
                  SELECT block_id, parent_block_id
                    FROM blocks JOIN tree ON block_id = tree.parent AND tree.bid != tree.parent
                    LIMIT ?
            )
            SELECT blocks.*, quorum_certificates.* FROM tree
                INNER JOIN blocks ON blocks.block_id = tree.bid
                LEFT JOIN quorum_certificates ON blocks.qc_id = quorum_certificates.qc_id
                ORDER BY height desc
        "#,
        )
        .bind::<Text, _>(serialize_hex(block_id))
        .bind::<BigInt, _>(limit as i64)
        .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
        .map_err(|e| SqliteStorageError::DieselError {
            operation: "blocks_get_parent_chain",
            source: e,
        })?;

        blocks
            .into_iter()
            .map(|(b, qc)| {
                let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "blocks_get_by_parent",
                    details: format!(
                        "block {} references non-existent quorum certificate {}",
                        block_id, b.qc_id
                    ),
                })?;

                b.try_convert(qc)
            })
            .collect()
    }

    fn blocks_get_pending_transactions(&self, block_id: &BlockId) -> Result<Vec<TransactionId>, StorageError> {
        use crate::schema::missing_transactions;

        let txs = missing_transactions::table
            .select(missing_transactions::transaction_id)
            .filter(missing_transactions::block_id.eq(serialize_hex(block_id)))
            .get_results::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_missing_transactions",
                source: e,
            })?;
        txs.into_iter().map(|s| deserialize_hex_try_from(&s)).collect()
    }

    fn blocks_get_any_with_epoch_range(
        &self,
        epoch_range: RangeInclusive<Epoch>,
        validator_public_key: Option<&PublicKey>,
    ) -> Result<Vec<Block>, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let mut query = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .filter(blocks::epoch.between(epoch_range.start().as_u64() as i64, epoch_range.end().as_u64() as i64))
            .into_boxed();

        if let Some(vn) = validator_public_key {
            query = query.filter(blocks::proposed_by.eq(serialize_hex(vn.as_bytes())));
        }

        let blocks_and_qcs = query
            .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "validator_fees_get_any_with_epoch_range_for_validator",
                source: e,
            })?;

        blocks_and_qcs
            .into_iter()
            .map(|(block, qc)| {
                let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "blocks_get_by_parent",
                    details: format!(
                        "block {} references non-existent quorum certificate {}",
                        block.id, block.qc_id
                    ),
                })?;

                block.try_convert(qc)
            })
            .collect()
    }

    fn blocks_get_paginated(
        &self,
        limit: u64,
        offset: u64,
        filter_index: Option<usize>,
        filter: Option<String>,
        ordering_index: Option<usize>,
        ordering: Option<Ordering>,
    ) -> Result<Vec<Block>, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let mut query = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .into_boxed();

        query = match ordering {
            Some(Ordering::Ascending) => match ordering_index {
                Some(0) => query.order_by(blocks::block_id.asc()),
                Some(1) => query.order_by(blocks::epoch.asc()),
                Some(2) => query.order_by(blocks::epoch.asc()).then_order_by(blocks::height.asc()),
                Some(4) => query.order_by(blocks::command_count.asc()),
                Some(5) => query.order_by(blocks::total_leader_fee.asc()),
                Some(6) => query.order_by(blocks::block_time.asc()),
                Some(7) => query.order_by(blocks::created_at.asc()),
                Some(8) => query.order_by(blocks::proposed_by.asc()),
                _ => query.order_by(blocks::epoch.asc()).then_order_by(blocks::height.asc()),
            },
            _ => match ordering_index {
                Some(0) => query.order_by(blocks::block_id.desc()),
                Some(1) => query.order_by(blocks::epoch.desc()),
                Some(2) => query
                    .order_by(blocks::epoch.desc())
                    .then_order_by(blocks::height.desc()),
                Some(4) => query.order_by(blocks::command_count.desc()),
                Some(5) => query.order_by(blocks::total_leader_fee.desc()),
                Some(6) => query.order_by(blocks::block_time.desc()),
                Some(7) => query.order_by(blocks::created_at.desc()),
                Some(8) => query.order_by(blocks::proposed_by.desc()),
                _ => query
                    .order_by(blocks::epoch.desc())
                    .then_order_by(blocks::height.desc()),
            },
        };

        if let Some(filter) = filter {
            if !filter.is_empty() {
                if let Some(filter_index) = filter_index {
                    match filter_index {
                        0 => query = query.filter(blocks::block_id.like(format!("%{filter}%"))),
                        1 => {
                            query = query.filter(
                                blocks::epoch
                                    .eq(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        2 => {
                            query = query.filter(
                                blocks::height
                                    .eq(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        4 => {
                            query = query.filter(
                                blocks::command_count
                                    .ge(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        5 => {
                            query = query.filter(
                                blocks::total_leader_fee
                                    .ge(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        7 => query = query.filter(blocks::proposed_by.like(format!("%{filter}%"))),
                        _ => (),
                    }
                }
            }
        }

        let blocks = query
            .limit(limit as i64)
            .offset(offset as i64)
            .get_results::<(sql_models::Block, Option<sql_models::QuorumCertificate>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_paginated",
                source: e,
            })?;

        blocks
            .into_iter()
            .map(|(block, qc)| {
                let qc = qc.ok_or_else(|| SqliteStorageError::DbInconsistency {
                    operation: "blocks_get_paginated",
                    details: format!(
                        "block {} references non-existent quorum certificate {}",
                        block.id, block.qc_id
                    ),
                })?;

                block.try_convert(qc)
            })
            .collect()
    }

    fn blocks_get_count(&self) -> Result<i64, StorageError> {
        use crate::schema::{blocks, quorum_certificates};
        let count = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select(diesel::dsl::count(blocks::id))
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_get_count",
                source: e,
            })?;
        Ok(count)
    }

    fn filtered_blocks_get_count(
        &self,
        filter_index: Option<usize>,
        filter: Option<String>,
    ) -> Result<i64, StorageError> {
        use crate::schema::{blocks, quorum_certificates};

        let mut query = blocks::table
            .left_join(quorum_certificates::table.on(blocks::qc_id.eq(quorum_certificates::qc_id)))
            .select((blocks::all_columns, quorum_certificates::all_columns.nullable()))
            .into_boxed();

        if let Some(filter) = filter {
            if !filter.is_empty() {
                if let Some(filter_index) = filter_index {
                    match filter_index {
                        0 => query = query.filter(blocks::block_id.like(format!("%{filter}%"))),
                        1 => {
                            query = query.filter(
                                blocks::epoch
                                    .eq(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        2 => {
                            query = query.filter(
                                blocks::height
                                    .eq(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        4 => {
                            query = query.filter(
                                blocks::command_count
                                    .ge(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        5 => {
                            query = query.filter(
                                blocks::total_leader_fee
                                    .ge(filter.parse::<i64>().map_err(|_| StorageError::InvalidIntegerCast)?),
                            )
                        },
                        7 => query = query.filter(blocks::proposed_by.like(format!("%{filter}%"))),
                        _ => (),
                    }
                }
            }
        }

        let count = query
            .select(diesel::dsl::count(blocks::id))
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "filtered_blocks_get_count",
                source: e,
            })?;
        Ok(count)
    }

    fn blocks_max_height(&self) -> Result<NodeHeight, StorageError> {
        use crate::schema::blocks;

        let height = blocks::table
            .select(diesel::dsl::max(blocks::height))
            .first::<Option<i64>>(self.connection())
            .map(|height| NodeHeight(height.unwrap_or(0) as u64))
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "blocks_max_height",
                source: e,
            })?;

        Ok(height)
    }

    fn block_diffs_get(&self, block_id: &BlockId) -> Result<BlockDiff, StorageError> {
        use crate::schema::block_diffs;

        let block_diff = block_diffs::table
            .filter(block_diffs::block_id.eq(serialize_hex(block_id)))
            .get_results::<sql_models::BlockDiff>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "block_diffs_get",
                source: e,
            })?;

        sql_models::BlockDiff::try_load(*block_id, block_diff)
    }

    fn block_diffs_get_last_change_for_substate(
        &self,
        block_id: &BlockId,
        substate_id: &SubstateId,
    ) -> Result<SubstateChange, StorageError> {
        use crate::schema::block_diffs;
        if !Block::record_exists(self, block_id)? {
            return Err(StorageError::QueryError {
                reason: format!(
                    "block_diffs_get_last_change_for_substate: Block {} does not exist",
                    block_id
                ),
            });
        }

        let commit_block = self.get_commit_block()?;
        let block_ids = self.get_block_ids_with_commands_between(commit_block.block_id(), block_id)?;

        let diff = block_diffs::table
            .filter(block_diffs::block_id.eq_any(block_ids))
            .filter(block_diffs::substate_id.eq(substate_id.to_string()))
            .order_by(block_diffs::id.desc())
            .first::<sql_models::BlockDiff>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "block_diffs_get_last_change_for_substate",
                source: e,
            })?;

        sql_models::BlockDiff::try_convert_change(diff)
    }

    fn block_diffs_get_change_for_versioned_substate<'a, T: Into<VersionedSubstateIdRef<'a>>>(
        &self,
        block_id: &BlockId,
        substate_id: T,
    ) -> Result<SubstateChange, StorageError> {
        use crate::schema::block_diffs;
        if !Block::record_exists(self, block_id)? {
            return Err(StorageError::QueryError {
                reason: format!(
                    "block_diffs_get_change_for_versioned_substate: Block {} does not exist",
                    block_id
                ),
            });
        }

        let commit_block = self.get_commit_block()?;
        let block_ids = self.get_block_ids_with_commands_between(commit_block.block_id(), block_id)?;
        let substate_ref = substate_id.into();

        let diff = block_diffs::table
            .filter(block_diffs::block_id.eq_any(block_ids))
            .filter(block_diffs::substate_id.eq(substate_ref.substate_id.to_string()))
            .filter(block_diffs::version.eq(substate_ref.version as i32))
            .order_by(block_diffs::id.desc())
            .first::<sql_models::BlockDiff>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "block_diffs_get_change_for_versioned_substate",
                source: e,
            })?;

        sql_models::BlockDiff::try_convert_change(diff)
    }

    fn quorum_certificates_get(&self, qc_id: &QcId) -> Result<QuorumCertificate, StorageError> {
        use crate::schema::quorum_certificates;

        let qc_json = quorum_certificates::table
            .select(quorum_certificates::json)
            .filter(quorum_certificates::qc_id.eq(serialize_hex(qc_id)))
            .first::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "quorum_certificates_get",
                source: e,
            })?;

        deserialize_json(&qc_json)
    }

    fn quorum_certificates_get_all<'a, I>(&self, qc_ids: I) -> Result<Vec<QuorumCertificate>, StorageError>
    where
        I: IntoIterator<Item = &'a QcId>,
        I::IntoIter: ExactSizeIterator,
    {
        use crate::schema::quorum_certificates;

        let qc_ids = qc_ids.into_iter();
        let num_qcs = qc_ids.len();
        if num_qcs == 0 {
            return Ok(vec![]);
        }

        let qc_json = quorum_certificates::table
            .select(quorum_certificates::json)
            .filter(quorum_certificates::qc_id.eq_any(qc_ids.map(serialize_hex)))
            .get_results::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "quorum_certificates_get_all",
                source: e,
            })?;

        if qc_json.len() != num_qcs {
            return Err(SqliteStorageError::NotAllItemsFound {
                items: "QCs",
                operation: "quorum_certificates_get_all",
                details: format!(
                    "quorum_certificates_get_all: expected {} quorum certificates, got {}",
                    num_qcs,
                    qc_json.len()
                ),
            }
            .into());
        }

        qc_json.iter().map(|j| deserialize_json(j)).collect()
    }

    fn quorum_certificates_get_by_block_id(&self, block_id: &BlockId) -> Result<QuorumCertificate, StorageError> {
        use crate::schema::quorum_certificates;

        let qc_json = quorum_certificates::table
            .select(quorum_certificates::json)
            .filter(quorum_certificates::block_id.eq(serialize_hex(block_id)))
            .first::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "quorum_certificates_get_by_block_id",
                source: e,
            })?;

        deserialize_json(&qc_json)
    }

    fn transaction_pool_get_for_blocks(
        &self,
        from_block_id: &BlockId,
        to_block_id: &BlockId,
        transaction_id: &TransactionId,
    ) -> Result<TransactionPoolRecord, StorageError> {
        use crate::schema::transaction_pool;

        if !self.blocks_exists(from_block_id)? {
            return Err(StorageError::QueryError {
                reason: format!(
                    "transaction_pool_get_for_blocks: Block {} does not exist",
                    from_block_id
                ),
            });
        }

        if !self.blocks_exists(to_block_id)? {
            return Err(StorageError::QueryError {
                reason: format!("transaction_pool_get_for_blocks: Block {} does not exist", to_block_id),
            });
        }

        let transaction_id = serialize_hex(transaction_id);
        let mut updates = self.get_transaction_atom_state_updates_between_blocks(
            from_block_id,
            to_block_id,
            std::iter::once(transaction_id.as_str()),
        )?;

        debug!(
            target: LOG_TARGET,
            "transaction_pool_get: from_block_id={}, to_block_id={}, transaction_id={}, updates={} [{:?}]",
            from_block_id,
            to_block_id,
            transaction_id,
            updates.len(),
            updates.values().map(|v| v.id).collect::<Vec<_>>(),
        );

        let rec = transaction_pool::table
            .filter(transaction_pool::transaction_id.eq(&transaction_id))
            .first::<sql_models::TransactionPoolRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_get_for_blocks",
                source: e,
            })?;

        rec.try_convert(updates.swap_remove(&transaction_id))
    }

    fn transaction_pool_exists(&self, transaction_id: &TransactionId) -> Result<bool, StorageError> {
        use crate::schema::transaction_pool;

        let count = transaction_pool::table
            .count()
            .filter(transaction_pool::transaction_id.eq(serialize_hex(transaction_id)))
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_exists",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn transaction_pool_get_all(&self) -> Result<Vec<TransactionPoolRecord>, StorageError> {
        use crate::schema::{leaf_blocks, transaction_pool};
        let txs = transaction_pool::table
            .get_results::<sql_models::TransactionPoolRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_get_all",
                source: e,
            })?;

        // There may be no locked block, in which case we assume there are no updates either
        let mut updates = IndexMap::new();
        if let Some(locked) = self.get_current_locked_block().optional()? {
            let leaf_block = leaf_blocks::table
                .select(leaf_blocks::block_id)
                .order_by(leaf_blocks::id.desc())
                .first::<String>(self.connection())
                .map_err(|e| SqliteStorageError::DieselError {
                    operation: "leaf_block_get",
                    source: e,
                })?;
            let block_id = deserialize_hex_try_from(&leaf_block)?;

            updates = self.get_transaction_atom_state_updates_between_blocks(
                &locked.block_id,
                &block_id,
                txs.iter().map(|s| s.transaction_id.as_str()),
            )?;
        }

        txs.into_iter()
            .map(|tx| {
                let maybe_update = updates.swap_remove(&tx.transaction_id);
                tx.try_convert(maybe_update)
            })
            .collect()
    }

    fn transaction_pool_get_many_ready(
        &self,
        max_txs: usize,
        block_id: &BlockId,
    ) -> Result<Vec<TransactionPoolRecord>, StorageError> {
        use crate::schema::{lock_conflicts, transaction_pool};

        if !self.blocks_exists(block_id)? {
            return Err(StorageError::QueryError {
                reason: format!("transaction_pool_get_many_ready: block {block_id} does not exist"),
            });
        }

        let ready_txs = transaction_pool::table
            // Exclude new transactions
            .filter(transaction_pool::stage.ne(TransactionPoolStage::New.to_string()))
            .filter(transaction_pool::is_ready.eq(true))
            .order_by(transaction_pool::transaction_id.asc())
            .limit(max_txs as i64)
            .get_results::<sql_models::TransactionPoolRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_get_many_ready",
                source: e,
            })?;

        debug!(
            target: LOG_TARGET,
            "🛢️ transaction_pool_get_many_ready: block_id={}, in progress ready_txs={}, max={}",
            block_id,
            ready_txs.len(),
            max_txs
        );

        // Fetch all applicable block ids between the locked block and the given block
        let locked = self.get_current_locked_block()?;

        let mut updates = self.get_transaction_atom_state_updates_between_blocks(
            &locked.block_id,
            block_id,
            ready_txs.iter().map(|s| s.transaction_id.as_str()),
        )?;

        debug!(
            target: LOG_TARGET,
            "transaction_pool_get_many_ready: locked.block_id={}, leaf.block_id={}, len(ready_txs)={}, updates={}",
            locked.block_id,
            block_id,
            ready_txs.len(),
            updates.len()
        );
        let num_ready = ready_txs.len();
        let ready_txs = ready_txs
            .into_iter()
            .map(|rec| {
                let maybe_update = updates.swap_remove(&rec.transaction_id);
                rec.try_convert(maybe_update)
            })
            // Filter only Ok where is_ready == true (after update) or Err
            .filter(|result| result.as_ref().map_or(true, |rec| rec.is_ready()));

        // Prioritise already sequenced transactions, if there is still space, add transactions that are not previously
        // sequenced (new)
        let new_limit = max_txs.saturating_sub(num_ready);
        if new_limit == 0 {
            debug!(
                target: LOG_TARGET,
                "transaction_pool_get_many_ready: locked.block_id={}, leaf.block_id={}, len(ready_txs)={}, max={}",
                locked.block_id,
                block_id,
                num_ready,
                max_txs
            );

            return ready_txs.collect();
        }

        let new_txs = transaction_pool::table
                .filter(transaction_pool::stage.eq(TransactionPoolStage::New.to_string()))
                // Filter out any transactions that are in lock conflict
                .filter(transaction_pool::transaction_id.ne_all(lock_conflicts::table.select(lock_conflicts::transaction_id).filter(lock_conflicts::is_local_only.eq(false))))
                .order_by(transaction_pool::transaction_id.asc())
                .limit(new_limit as i64)
                .get_results::<sql_models::TransactionPoolRecord>(self.connection())
                .map_err(|e| SqliteStorageError::DieselError {
                    operation: "transaction_pool_get_many_ready",
                    source: e,
                })?;
        let mut updates = self.get_transaction_atom_state_updates_between_blocks(
            &locked.block_id,
            block_id,
            new_txs.iter().map(|s| s.transaction_id.as_str()),
        )?;

        debug!(
            target: LOG_TARGET,
            "🛢️ transaction_pool_get_many_ready: block_id={}, new ready_txs={}, total ready_txs={}, max={}, updates={}",
            block_id,
            new_txs.len(),
            num_ready + new_txs.len(),
            max_txs,
            updates.len(),
        );

        ready_txs
            .chain(
                new_txs
                .into_iter()
                .map(|rec| {
                    let maybe_update = updates.swap_remove(&rec.transaction_id);
                    rec.try_convert(maybe_update)
                })
                // Filter only Ok where is_ready == true (after update) or Err
                .filter(|result| result.as_ref().map_or(true, |rec| rec.is_ready())),
            )
            .collect()
    }

    fn transaction_pool_has_pending_state_updates(&self, block_id: &BlockId) -> Result<bool, StorageError> {
        use crate::schema::transaction_pool_state_updates;

        if !self.blocks_exists(block_id)? {
            return Err(StorageError::QueryError {
                reason: format!("transaction_pool_has_pending_state_updates: block {block_id} does not exist"),
            });
        }

        let commit_block = self.get_commit_block()?;
        let block_ids = self.get_block_ids_with_commands_between(commit_block.block_id(), block_id)?;

        let count = transaction_pool_state_updates::table
            .filter(transaction_pool_state_updates::is_applied.eq(false))
            .filter(transaction_pool_state_updates::block_id.eq_any(block_ids))
            .count()
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pool_has_pending_state_updates",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn transaction_pool_count(
        &self,
        stage: Option<TransactionPoolStage>,
        is_ready: Option<bool>,
        confirmed_stage: Option<Option<TransactionPoolConfirmedStage>>,
        skip_lock_conflicted: bool,
    ) -> Result<usize, StorageError> {
        use crate::schema::{lock_conflicts, transaction_pool};

        let mut query = transaction_pool::table.into_boxed();
        if let Some(stage) = stage {
            let stage_str = stage.to_string();
            query = query.filter(
                transaction_pool::pending_stage
                    .eq(stage_str.clone())
                    .or(transaction_pool::pending_stage
                        .is_null()
                        .and(transaction_pool::stage.eq(stage_str))),
            );
        }
        if let Some(is_ready) = is_ready {
            query = query.filter(transaction_pool::is_ready.eq(is_ready));
        }
        if skip_lock_conflicted {
            // Filter out any transactions that are in lock conflict
            query = query.filter(
                transaction_pool::transaction_id.ne_all(
                    lock_conflicts::table
                        .select(lock_conflicts::transaction_id)
                        .filter(lock_conflicts::is_local_only.eq(false)),
                ),
            )
        }

        match confirmed_stage {
            Some(Some(stage)) => {
                query = query.filter(transaction_pool::confirm_stage.eq(stage.to_string()));
            },
            Some(None) => {
                query = query.filter(transaction_pool::confirm_stage.is_null());
            },
            None => {},
        }

        let count =
            query
                .count()
                .get_result::<i64>(self.connection())
                .map_err(|e| SqliteStorageError::DieselError {
                    operation: "transaction_pool_count",
                    source: e,
                })?;

        Ok(count as usize)
    }

    fn transactions_fetch_involved_shards(
        &self,
        transaction_ids: HashSet<TransactionId>,
    ) -> Result<HashSet<SubstateAddress>, StorageError> {
        use crate::schema::transactions;

        let tx_ids = transaction_ids.into_iter().map(serialize_hex).collect::<Vec<_>>();

        let inputs_per_tx = transactions::table
            .select(transactions::resolved_inputs)
            .filter(transactions::transaction_id.eq_any(&tx_ids))
            .load::<Option<String>>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "transaction_pools_fetch_involved_shards",
                source: e,
            })?;

        if inputs_per_tx.len() != tx_ids.len() {
            return Err(SqliteStorageError::NotAllItemsFound {
                items: "Transactions",
                operation: "transactions_fetch_involved_shards",
                details: format!(
                    "transactions_fetch_involved_shards: expected {} transactions, got {}",
                    tx_ids.len(),
                    inputs_per_tx.len()
                ),
            }
            .into());
        }

        let shards = inputs_per_tx
            .into_iter()
            .filter_map(|inputs| {
                // a Result is very inconvenient with flat_map
                inputs.map(|inputs| {
                    deserialize_json::<HashSet<SubstateAddress>>(&inputs)
                        .expect("[transactions_fetch_involved_shards] Failed to deserialize involved shards")
                })
            })
            .flatten()
            .collect();

        Ok(shards)
    }

    fn votes_get_by_block_and_sender(
        &self,
        block_id: &BlockId,
        sender_leaf_hash: &FixedHash,
    ) -> Result<Vote, StorageError> {
        use crate::schema::votes;

        let vote = votes::table
            .filter(votes::block_id.eq(serialize_hex(block_id)))
            .filter(votes::sender_leaf_hash.eq(serialize_hex(sender_leaf_hash)))
            .first::<sql_models::Vote>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "votes_get",
                source: e,
            })?;

        Vote::try_from(vote)
    }

    fn votes_count_for_block(&self, block_id: &BlockId) -> Result<u64, StorageError> {
        use crate::schema::votes;

        let count = votes::table
            .filter(votes::block_id.eq(serialize_hex(block_id)))
            .count()
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "votes_count_for_block",
                source: e,
            })?;

        Ok(count as u64)
    }

    fn votes_get_for_block(&self, block_id: &BlockId) -> Result<Vec<Vote>, StorageError> {
        use crate::schema::votes;

        let votes = votes::table
            .filter(votes::block_id.eq(serialize_hex(block_id)))
            .get_results::<sql_models::Vote>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "votes_get_for_block",
                source: e,
            })?;

        votes.into_iter().map(Vote::try_from).collect()
    }

    fn substates_get(&self, address: &SubstateAddress) -> Result<SubstateRecord, StorageError> {
        use crate::schema::substates;

        let substate = substates::table
            .filter(substates::address.eq(serialize_hex(address)))
            .first::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get",
                source: e,
            })?;

        substate.try_into()
    }

    fn substates_get_any<'a, I: IntoIterator<Item = &'a VersionedSubstateIdRef<'a>>>(
        &self,
        substate_ids: I,
    ) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let mut substate_ids = substate_ids.into_iter().peekable();
        // NB: if we don't check this and substate_ids is empty, we'll return all substates!
        if substate_ids.peek().is_none() {
            return Ok(vec![]);
        }

        let mut query = substates::table.into_boxed();

        for id in substate_ids {
            let id_str = id.substate_id.to_string();
            query = query.or_filter(
                substates::substate_id
                    .eq(id_str)
                    .and(substates::version.eq(id.version() as i32)),
            );
        }

        let results = query
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_any",
                source: e,
            })?;

        results.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_any_max_version<'a, I>(&self, substate_ids: I) -> Result<Vec<SubstateRecord>, StorageError>
    where
        I: IntoIterator<Item = &'a SubstateId>,
        I::IntoIter: ExactSizeIterator,
    {
        use crate::schema::substates;
        #[derive(Debug, QueryableByName)]
        struct MaxVersionAndId {
            #[allow(dead_code)]
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
            max_version: Option<i32>,
            #[diesel(sql_type = diesel::sql_types::Integer)]
            id: i32,
        }

        let mut substate_ids = substate_ids.into_iter().peekable();
        if substate_ids.peek().is_none() {
            return Ok(Vec::new());
        }
        let frag = self.sql_frag_for_in_statement(substate_ids.map(|s| s.to_string()), 32);
        let max_versions_and_ids = sql_query(format!(
            r#"
                SELECT MAX(version) as max_version, id
                FROM substates
                WHERE substate_id in ({})
                GROUP BY substate_id"#,
            frag
        ))
        .get_results::<MaxVersionAndId>(self.connection())
        .map_err(|e| SqliteStorageError::DieselError {
            operation: "substates_get_any_max_version",
            source: e,
        })?;

        let results = substates::table
            .filter(substates::id.eq_any(max_versions_and_ids.iter().map(|m| m.id)))
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_any_max_version",
                source: e,
            })?;

        results.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_max_version_for_substate(&self, substate_id: &SubstateId) -> Result<(u32, bool), StorageError> {
        #[derive(Debug, QueryableByName)]
        struct MaxVersionAndDestroyed {
            #[allow(dead_code)]
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
            max_version: Option<i32>,
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
            destroyed_by_shard: Option<i32>,
        }

        let substate_id = substate_id.to_string();
        // Diesel GROUP BY support is limited
        let max_version_and_destroyed = sql_query(
            r#"
                SELECT MAX(version) as max_version, destroyed_by_shard
                FROM substates
                WHERE substate_id = ?
                GROUP BY substate_id"#,
        )
        .bind::<Text, _>(&substate_id)
        .get_result::<MaxVersionAndDestroyed>(self.connection())
        .map_err(|e| SqliteStorageError::DieselError {
            operation: "substates_get_max_version_for_substate",
            source: e,
        })?;

        let Some(max_version) = max_version_and_destroyed.max_version else {
            return Err(StorageError::NotFound {
                item: "Substate (substates_get_max_version_for_substate)",
                key: substate_id.to_string(),
            });
        };

        Ok((
            max_version as u32,
            max_version_and_destroyed.destroyed_by_shard.is_some(),
        ))
    }

    fn substates_any_exist<I: IntoIterator<Item = S>, S: Borrow<VersionedSubstateId>>(
        &self,
        addresses: I,
    ) -> Result<bool, StorageError> {
        use crate::schema::substates;

        let mut addresses = addresses.into_iter().peekable();
        if addresses.peek().is_none() {
            return Ok(false);
        }

        let count = substates::table
            .count()
            .filter(substates::address.eq_any(addresses.map(|v| v.borrow().to_substate_address()).map(serialize_hex)))
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_any",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn substates_exists_for_transaction(&self, transaction_id: &TransactionId) -> Result<bool, StorageError> {
        use crate::schema::substates;

        let transaction_id = serialize_hex(transaction_id);

        let count = substates::table
            .count()
            .filter(substates::created_by_transaction.eq(&transaction_id))
            .or_filter(substates::destroyed_by_transaction.eq(&transaction_id))
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_exists_for_transaction",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn substates_get_n_after(&self, n: usize, after: &SubstateAddress) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let start_id = substates::table
            .select(substates::id)
            .filter(substates::address.eq(after.to_string()))
            .get_result::<i32>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_n_after",
                source: e,
            })?;

        let substates = substates::table
            .filter(substates::id.gt(start_id))
            .limit(n as i64)
            .order_by(substates::id.asc())
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_n_after",
                source: e,
            })?;

        substates.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_many_within_range(
        &self,
        start: &SubstateAddress,
        end: &SubstateAddress,
        exclude: &[SubstateAddress],
    ) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let substates = substates::table
            .filter(substates::address.between(serialize_hex(start), serialize_hex(end)))
            .filter(substates::address.ne_all(exclude.iter().map(serialize_hex)))
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_many_within_range",
                source: e,
            })?;

        substates.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_many_by_created_transaction(
        &self,
        tx_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let substates = substates::table
            .filter(substates::created_by_transaction.eq(serialize_hex(tx_id)))
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_many_by_created_transaction",
                source: e,
            })?;

        substates.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_many_by_destroyed_transaction(
        &self,
        tx_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let substates = substates::table
            .filter(substates::destroyed_by_transaction.eq(serialize_hex(tx_id)))
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_many_by_destroyed_transaction",
                source: e,
            })?;

        substates.into_iter().map(TryInto::try_into).collect()
    }

    fn substates_get_all_for_transaction(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<Vec<SubstateRecord>, StorageError> {
        use crate::schema::substates;

        let transaction_id_hex = serialize_hex(transaction_id);

        let substates = substates::table
            .filter(
                substates::created_by_transaction
                    .eq(&transaction_id_hex)
                    .or(substates::destroyed_by_transaction.eq(Some(&transaction_id_hex))),
            )
            .order_by(substates::id.asc())
            .get_results::<sql_models::SubstateRecord>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substates_get_all_for_transaction",
                source: e,
            })?;

        let substates = substates
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(substates)
    }

    fn substate_locks_get_locked_substates_for_transaction(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<Vec<LockedSubstateValue>, StorageError> {
        use crate::schema::{substate_locks, substates};

        let recs = substate_locks::table
            .left_join(
                substates::table.on(substate_locks::substate_id
                    .eq(substates::substate_id)
                    .and(substate_locks::version.eq(substates::version))),
            )
            .filter(substate_locks::transaction_id.eq(serialize_hex(transaction_id)))
            .order_by(substate_locks::id.asc())
            .get_results::<(sql_models::SubstateLock, Option<sql_models::SubstateRecord>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substate_locks_get_value_for_transaction",
                source: e,
            })?;

        recs.into_iter()
            .map(|(lock, maybe_substate)| lock.try_into_locked_substate_value(maybe_substate))
            .collect()
    }

    fn substate_locks_has_any_write_locks_for_substates<'a, I: IntoIterator<Item = &'a SubstateId>>(
        &self,
        exclude_transaction_id: Option<&TransactionId>,
        substate_ids: I,
        exclude_local_only: bool,
    ) -> Result<Option<TransactionId>, StorageError> {
        use crate::schema::substate_locks;

        let mut substate_ids = substate_ids.into_iter().peekable();
        if substate_ids.peek().is_none() {
            return Ok(None);
        }
        let substate_ids = substate_ids.map(|id| id.to_string());
        let mut query = substate_locks::table
            .select(substate_locks::transaction_id)
            .filter(substate_locks::substate_id.eq_any(substate_ids))
            .filter(substate_locks::lock.eq(SubstateLockType::Write.as_str()))
            .into_boxed();

        if let Some(exclude_transaction_id) = exclude_transaction_id {
            query = query.filter(substate_locks::transaction_id.ne(serialize_hex(exclude_transaction_id)));
        }

        if exclude_local_only {
            query = query.filter(substate_locks::is_local_only.eq(false));
        }

        let transaction_id = query
            .limit(1)
            .get_result::<String>(self.connection())
            .optional()
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substate_locks_has_any_write_locks_for_substates",
                source: e,
            })?;

        transaction_id.map(|id| deserialize_hex_try_from(&id)).transpose()
    }

    fn substate_locks_get_latest_for_substate(&self, substate_id: &SubstateId) -> Result<SubstateLock, StorageError> {
        use crate::schema::substate_locks;

        // TODO: this may return an invalid lock if:
        // 1. the proposer links the parent block to the locked block instead of the previous tip
        // 2. if there are any inactive locks that were not removed from previous uncommitted blocks.

        let lock = substate_locks::table
            .filter(substate_locks::substate_id.eq(substate_id.to_string()))
            .order_by(substate_locks::id.desc())
            .first::<sql_models::SubstateLock>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "substate_locks_get_latest_for_substate",
                source: e,
            })?;

        lock.try_into_substate_lock()
    }

    fn pending_state_tree_diffs_get_all_up_to_commit_block(
        &self,
        block_id: &BlockId,
    ) -> Result<HashMap<Shard, Vec<PendingShardStateTreeDiff>>, StorageError> {
        use crate::schema::pending_state_tree_diffs;

        if !self.blocks_exists(block_id)? {
            return Err(StorageError::NotFound {
                item: "pending_state_tree_diffs_get_all_up_to_commit_block: Block",
                key: block_id.to_string(),
            });
        }

        // Get the last committed block
        let commit_block = self.get_commit_block()?;

        // Block may modify state with zero commands because it justifies a block that changes state
        let block_ids = self.get_block_ids_between(commit_block.block_id(), block_id, 1000)?;

        if block_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let diff_recs = pending_state_tree_diffs::table
            .filter(pending_state_tree_diffs::block_id.eq_any(block_ids))
            .order_by(pending_state_tree_diffs::block_height.asc())
            .then_order_by(pending_state_tree_diffs::id.asc())
            .get_results::<sql_models::PendingStateTreeDiff>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "pending_state_tree_diffs_get_all_pending",
                source: e,
            })?;

        let mut diffs = HashMap::new();
        for diff in diff_recs {
            let shard = Shard::from(diff.shard as u32);
            let diff = PendingShardStateTreeDiff::try_from(diff)?;
            diffs
                .entry(shard)
                .or_insert_with(Vec::new) //PendingStateTreeDiff::default)
                .push(diff);
        }

        Ok(diffs)
    }

    fn state_transitions_get_n_after(
        &self,
        n: usize,
        id: StateTransitionId,
        end_epoch: Epoch,
    ) -> Result<Vec<StateTransition>, StorageError> {
        use crate::schema::{state_transitions, substates};

        debug!(target: LOG_TARGET, "state_transitions_get_n_after: {id}, end_epoch:{end_epoch}");

        let transitions = state_transitions::table
            .left_join(substates::table.on(state_transitions::substate_address.eq(substates::address)))
            .select((state_transitions::all_columns, substates::all_columns.nullable()))
            .filter(state_transitions::seq.gt(id.seq() as i64))
            .filter(state_transitions::shard.eq(id.shard().as_u32() as i32))
            .filter(state_transitions::epoch.lt(end_epoch.as_u64() as i64))
            .limit(n as i64)
            .get_results::<(sql_models::StateTransition, Option<sql_models::SubstateRecord>)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "state_transitions_get_n_after",
                source: e,
            })?;

        transitions
            .into_iter()
            .map(|(t, s)| {
                let s = s.ok_or_else(|| StorageError::DataInconsistency {
                    details: format!("substate entry does not exist for transition {}", t.id),
                })?;

                t.try_convert(s)
            })
            .collect()
    }

    fn state_transitions_get_last_id(&self, shard: Shard) -> Result<StateTransitionId, StorageError> {
        use crate::schema::state_transitions;

        let (seq, epoch) = state_transitions::table
            .select((state_transitions::seq, state_transitions::epoch))
            .filter(state_transitions::shard.eq(shard.as_u32() as i32))
            .order_by(state_transitions::epoch.desc())
            .then_order_by(state_transitions::seq.desc())
            .first::<(i64, i64)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "state_transitions_get_last_id",
                source: e,
            })?;

        let epoch = Epoch(epoch as u64);
        let seq = seq as u64;

        Ok(StateTransitionId::new(epoch, shard, seq))
    }

    fn state_tree_nodes_get(&self, shard: Shard, key: &NodeKey) -> Result<Node<Version>, StorageError> {
        use crate::schema::state_tree;

        let node = state_tree::table
            .select(state_tree::node)
            .filter(state_tree::shard.eq(shard.as_u32() as i32))
            .filter(state_tree::key.eq(key.to_string()))
            .first::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "state_tree_nodes_get",
                source: e,
            })?;

        let node = serde_json::from_str::<TreeNode<Version>>(&node).map_err(|e| StorageError::DataInconsistency {
            details: format!("Failed to deserialize state tree node: {}", e),
        })?;

        Ok(node.into_node())
    }

    fn state_tree_versions_get_latest(&self, shard: Shard) -> Result<Option<Version>, StorageError> {
        use crate::schema::state_tree_shard_versions;

        let version = state_tree_shard_versions::table
            .select(state_tree_shard_versions::version)
            .filter(state_tree_shard_versions::shard.eq(shard.as_u32() as i32))
            .order_by(state_tree_shard_versions::version.desc())
            .first::<i64>(self.connection())
            .optional()
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "state_tree_versions_get_latest",
                source: e,
            })?;

        Ok(version.map(|v| v as Version))
    }

    fn epoch_checkpoint_get(&self, epoch: Epoch) -> Result<EpochCheckpoint, StorageError> {
        use crate::schema::epoch_checkpoints;

        let checkpoint = epoch_checkpoints::table
            .filter(epoch_checkpoints::epoch.eq(epoch.as_u64() as i64))
            .first::<sql_models::EpochCheckpoint>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "epoch_checkpoint_get",
                source: e,
            })?;

        checkpoint.try_into()
    }

    fn foreign_substate_pledges_exists_for_transaction_and_address<T: ToSubstateAddress>(
        &self,
        transaction_id: &TransactionId,
        address: T,
    ) -> Result<bool, StorageError> {
        use crate::schema::foreign_substate_pledges;

        let address = address.to_substate_address();
        let count = foreign_substate_pledges::table
            .count()
            .filter(foreign_substate_pledges::transaction_id.eq(serialize_hex(transaction_id)))
            .filter(foreign_substate_pledges::address.eq(serialize_hex(address)))
            .limit(1)
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_substate_pledges_exists",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn foreign_substate_pledges_get_write_pledges_to_transaction<'a, I>(
        &self,
        transaction_id: &TransactionId,
        substate_ids: I,
    ) -> Result<SubstatePledges, StorageError>
    where
        I: IntoIterator<Item = &'a SubstateId>,
    {
        use crate::schema::foreign_substate_pledges;

        let mut substate_ids = substate_ids.into_iter().map(|a| a.to_string()).peekable();
        if substate_ids.peek().is_none() {
            return Ok(SubstatePledges::new());
        }

        let pledges = foreign_substate_pledges::table
            .filter(foreign_substate_pledges::transaction_id.eq(serialize_hex(transaction_id)))
            .filter(foreign_substate_pledges::substate_id.eq_any(substate_ids))
            .filter(foreign_substate_pledges::lock_type.eq(SubstateLockType::Write.as_str()))
            .get_results::<sql_models::ForeignSubstatePledge>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_substate_pledges_any_pledged_to_different_transaction",
                source: e,
            })?;

        pledges.into_iter().map(TryInto::try_into).collect()
    }

    fn foreign_substate_pledges_get_all_by_transaction_id(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<SubstatePledges, StorageError> {
        use crate::schema::foreign_substate_pledges;

        let recs = foreign_substate_pledges::table
            .filter(foreign_substate_pledges::transaction_id.eq(serialize_hex(transaction_id)))
            .get_results::<sql_models::ForeignSubstatePledge>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_substate_pledges_get",
                source: e,
            })?;

        recs.into_iter().map(TryInto::try_into).collect()
    }

    fn burnt_utxos_get(&self, commitment: &UnclaimedConfidentialOutputAddress) -> Result<BurntUtxo, StorageError> {
        use crate::schema::burnt_utxos;

        let burnt_utxo = burnt_utxos::table
            .filter(burnt_utxos::commitment.eq(commitment.to_string()))
            .first::<sql_models::BurntUtxo>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "burnt_utxos_get",
                source: e,
            })?;

        burnt_utxo.try_into()
    }

    fn burnt_utxos_get_all_unproposed(
        &self,
        leaf_block: &BlockId,
        limit: usize,
    ) -> Result<Vec<BurntUtxo>, StorageError> {
        use crate::schema::burnt_utxos;
        if !self.blocks_exists(leaf_block)? {
            return Err(StorageError::NotFound {
                item: "Block",
                key: leaf_block.to_string(),
            });
        }

        if limit == 0 {
            return Ok(Vec::new());
        }

        let locked_block = self.get_current_locked_block()?;
        let exclude_block_ids = self.get_block_ids_with_commands_between(&locked_block.block_id, leaf_block)?;

        let burnt_utxos = burnt_utxos::table
            .filter(
                burnt_utxos::proposed_in_block
                    .is_null()
                    .or(burnt_utxos::proposed_in_block
                        .ne_all(exclude_block_ids)
                        .and(burnt_utxos::proposed_in_block_height.gt(locked_block.height.as_u64() as i64))),
            )
            .limit(limit as i64)
            .get_results::<sql_models::BurntUtxo>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "burnt_utxos_get_all_unproposed",
                source: e,
            })?;

        burnt_utxos.into_iter().map(TryInto::try_into).collect()
    }

    fn burnt_utxos_count(&self) -> Result<u64, StorageError> {
        use crate::schema::burnt_utxos;

        let count = burnt_utxos::table
            .count()
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "burnt_utxos_count",
                source: e,
            })?;

        Ok(count as u64)
    }

    fn foreign_parked_blocks_exists(&self, block_id: &BlockId) -> Result<bool, StorageError> {
        use crate::schema::foreign_parked_blocks;

        let count = foreign_parked_blocks::table
            .count()
            .filter(foreign_parked_blocks::block_id.eq(serialize_hex(block_id)))
            .get_result::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "foreign_parked_blocks_exists",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn validator_epoch_stats_get(
        &self,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<ValidatorConsensusStats, StorageError> {
        use crate::schema::validator_epoch_stats;

        let (participation_shares, missed_proposals) = validator_epoch_stats::table
            .select((
                validator_epoch_stats::participation_shares,
                validator_epoch_stats::missed_proposals,
            ))
            .filter(validator_epoch_stats::public_key.eq(public_key.to_hex()))
            .filter(validator_epoch_stats::epoch.eq(epoch.as_u64() as i64))
            .get_result::<(i64, i64)>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "validator_epoch_stats_get",
                source: e,
            })?;

        Ok(ValidatorConsensusStats {
            missed_proposals: missed_proposals
                .try_into()
                .map_err(|_| StorageError::DataInconsistency {
                    details: "validator_epoch_stats_get: missed_proposals is negative".to_string(),
                })?,
            participation_shares: participation_shares
                .try_into()
                .map_err(|_| StorageError::DataInconsistency {
                    details: "validator_epoch_stats_get: participation_shares is negative".to_string(),
                })?,
        })
    }

    fn validator_epoch_stats_get_nodes_to_evict(
        &self,
        block_id: &BlockId,
        threshold: u64,
        limit: u64,
    ) -> Result<Vec<PublicKey>, StorageError> {
        use crate::schema::{evicted_nodes, validator_epoch_stats};
        if limit == 0 {
            return Ok(vec![]);
        }
        let commit_block = self.get_commit_block()?;

        let block_ids = self.get_block_ids_between(commit_block.block_id(), block_id, 1000)?;

        let pks = validator_epoch_stats::table
            .select(validator_epoch_stats::public_key)
            .left_join(evicted_nodes::table.on(evicted_nodes::public_key.eq(validator_epoch_stats::public_key)))
            .filter(
                // Not evicted
                evicted_nodes::evicted_in_block
                    .is_null()
                    // Not already evicted in uncommitted blocks
                    .or(evicted_nodes::evicted_in_block
                    .ne_all(block_ids)
                    // Not evicted in committed blocks
                    .and(evicted_nodes::evicted_in_block_height.le(commit_block.height().as_u64() as i64))
                ),
            )
            // Only suspended nodes can be evicted
            // .filter(evicted_nodes::suspended_in_block.is_not_null())
            // Not already evicted
            .filter(evicted_nodes::eviction_committed_in_epoch.is_null())
            .filter(validator_epoch_stats::missed_proposals.ge(threshold as i64))
            .filter(validator_epoch_stats::epoch.eq(commit_block.epoch().as_u64() as i64))
            .limit(limit as i64)
            .get_results::<String>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "validator_epoch_stats_get_nodes_to_evict",
                source: e,
            })?;

        pks.iter()
            .map(|s| {
                PublicKey::from_hex(s).map_err(|e| StorageError::DecodingError {
                    operation: "validator_epoch_stats_get_nodes_to_evict",
                    item: "public key",
                    details: format!("Failed to decode public key: {e}"),
                })
            })
            .collect()
    }

    fn suspended_nodes_is_evicted(&self, block_id: &BlockId, public_key: &PublicKey) -> Result<bool, StorageError> {
        use crate::schema::evicted_nodes;

        if !self.blocks_exists(block_id)? {
            return Err(StorageError::QueryError {
                reason: format!("block {} not found", block_id),
            });
        }

        let commit_block = self.get_commit_block()?;
        let block_ids = self.get_block_ids_between(commit_block.block_id(), block_id, 1000)?;

        let count = evicted_nodes::table
            .count()
            .filter(evicted_nodes::public_key.eq(public_key.to_hex()))
            .filter(
                evicted_nodes::evicted_in_block.is_not_null().and(
                    evicted_nodes::evicted_in_block_height
                        .le(commit_block.height().as_u64() as i64)
                        .or(evicted_nodes::evicted_in_block.ne_all(block_ids)),
                ),
            )
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "suspended_nodes_is_evicted",
                source: e,
            })?;

        Ok(count > 0)
    }

    fn evicted_nodes_count(&self, epoch: Epoch) -> Result<u64, StorageError> {
        use crate::schema::evicted_nodes;

        let count = evicted_nodes::table
            .count()
            .filter(evicted_nodes::evicted_in_block.is_not_null())
            .filter(evicted_nodes::eviction_committed_in_epoch.eq(epoch.as_u64() as i64))
            .first::<i64>(self.connection())
            .map_err(|e| SqliteStorageError::DieselError {
                operation: "evicted_nodes_count",
                source: e,
            })?;

        Ok(count as u64)
    }
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub count: i64,
}

#[derive(QueryableByName)]
struct BlockIdSqlValue {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub bid: String,
}
