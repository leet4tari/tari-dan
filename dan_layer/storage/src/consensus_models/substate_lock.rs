//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{fmt, fmt::Display};

use tari_dan_common_types::{LockIntent, SubstateAddress, SubstateLockType, SubstateRequirementRef, ToSubstateAddress};
use tari_engine_types::substate::{SubstateId, SubstateValue};
use tari_transaction::TransactionId;

use crate::{consensus_models::RequireLockIntentRef, StateStoreReadTransaction, StorageError};

#[derive(Debug, Clone, Copy)]
pub struct SubstateLock {
    lock_type: SubstateLockType,
    transaction_id: TransactionId,
    version: u32,
    is_local_only: bool,
}

impl SubstateLock {
    pub fn new(transaction_id: TransactionId, version: u32, lock_type: SubstateLockType, is_local_only: bool) -> Self {
        Self {
            transaction_id,
            version,
            lock_type,
            is_local_only,
        }
    }

    pub fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn lock_type(&self) -> SubstateLockType {
        self.lock_type
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn is_local_only(&self) -> bool {
        self.is_local_only
    }

    pub fn is_write(&self) -> bool {
        self.lock_type.is_write()
    }

    pub fn is_read(&self) -> bool {
        self.lock_type.is_read()
    }

    pub fn is_input(&self) -> bool {
        self.lock_type.is_input()
    }

    pub fn is_output(&self) -> bool {
        self.lock_type.is_output()
    }
}

impl fmt::Display for SubstateLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SubstateLock(version: {}, lock_flag: {}, is_local_only: {}, transaction_id: {})",
            self.version, self.lock_type, self.is_local_only, self.transaction_id,
        )
    }
}

#[derive(Debug, Clone)]
pub struct LockedSubstateValue {
    pub substate_id: SubstateId,
    pub lock: SubstateLock,
    /// The value of the locked substate. This may be None if the substate lock is Output.
    pub value: Option<SubstateValue>,
}

impl LockedSubstateValue {
    pub(crate) fn to_substate_lock_intent(&self) -> RequireLockIntentRef<'_> {
        RequireLockIntentRef::new(&self.substate_id, self.lock.version(), self.lock.lock_type())
    }

    pub fn substate_id(&self) -> &SubstateId {
        &self.substate_id
    }

    pub fn satisfies_requirements<'a, T: Into<SubstateRequirementRef<'a>>>(&self, requirement: T) -> bool {
        let requirement = requirement.into();
        requirement.version().map_or(true, |v| v == self.lock.version) && *requirement.substate_id() == self.substate_id
    }

    pub fn satisfies_lock_intent<T: LockIntent>(&self, lock_intent: T) -> bool {
        lock_intent.version_to_lock() == self.lock.version &&
            self.lock.lock_type.allows(lock_intent.lock_type()) &&
            *lock_intent.substate_id() == self.substate_id
    }

    pub fn take_value(&mut self) -> Option<SubstateValue> {
        self.value.take()
    }
}

impl LockedSubstateValue {
    pub fn get_all_for_transaction<TTx: StateStoreReadTransaction>(
        tx: &TTx,
        transaction_id: &TransactionId,
    ) -> Result<Vec<LockedSubstateValue>, StorageError> {
        tx.substate_locks_get_locked_substates_for_transaction(transaction_id)
    }

    pub fn get_transaction_id_that_has_any_write_locks_for_substates<'a, TTx, I>(
        tx: &TTx,
        substate_ids: I,
        exclude_local_only: bool,
    ) -> Result<Option<TransactionId>, StorageError>
    where
        TTx: StateStoreReadTransaction,
        I: IntoIterator<Item = &'a SubstateId>,
    {
        tx.substate_locks_has_any_write_locks_for_substates(None, substate_ids, exclude_local_only)
    }

    pub fn get_transaction_id_that_conflicts_with_write_locks<'a, TTx, I>(
        tx: &TTx,
        exclude_transaction_id: &TransactionId,
        substate_ids: I,
        exclude_local_only: bool,
    ) -> Result<Option<TransactionId>, StorageError>
    where
        TTx: StateStoreReadTransaction,
        I: IntoIterator<Item = &'a SubstateId>,
    {
        tx.substate_locks_has_any_write_locks_for_substates(
            Some(exclude_transaction_id),
            substate_ids,
            exclude_local_only,
        )
    }
}

impl ToSubstateAddress for LockedSubstateValue {
    fn to_substate_address(&self) -> SubstateAddress {
        SubstateAddress::from_substate_id(&self.substate_id, self.lock.version())
    }
}

impl Display for LockedSubstateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LockedSubstate(substate_id: {}, lock: {})",
            self.substate_id, self.lock,
        )
    }
}
