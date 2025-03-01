//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use tari_common::configuration::Network;
use tari_consensus::{
    hotstuff::substate_store::{LockFailedError, PendingSubstateStore, SubstateStoreError},
    traits::{ReadableSubstateStore, WriteableSubstateStore},
};
use tari_dan_common_types::{
    shard::Shard,
    NodeAddressable,
    NumPreshards,
    PeerAddress,
    SubstateLockType,
    VersionedSubstateId,
};
use tari_dan_storage::{
    consensus_models::{Block, BlockId, QcId, RequireLockIntentRef, SubstateChange, SubstateRecord},
    StateStore,
};
use tari_engine_types::{
    component::{ComponentBody, ComponentHeader},
    substate::{Substate, SubstateId, SubstateValue},
};
use tari_state_store_sqlite::SqliteStateStore;
use tari_template_lib::models::{ComponentAddress, EntityId, ObjectKey};

use crate::support::{logging::setup_logger, TEST_NUM_PRESHARDS};

type TestStore = SqliteStateStore<PeerAddress>;

#[test]
fn it_allows_substate_up_for_v0() {
    let store = create_store();

    let id = new_substate_id(0);
    let value = new_substate_value(0);

    let tx = store.create_read_tx().unwrap();
    let mut store = create_pending_store(&tx);
    // Cannot put version 1
    store
        .put(SubstateChange::Up {
            id: VersionedSubstateId::new(id.clone(), 1),
            shard: Shard::first(),
            transaction_id: tx_id(0),
            substate: Substate::new(1, value.clone()),
        })
        .unwrap_err();

    store
        .put(SubstateChange::Up {
            id: VersionedSubstateId::new(id.clone(), 0),
            transaction_id: tx_id(0),
            shard: Shard::first(),
            substate: Substate::new(0, value),
        })
        .unwrap();

    let s = store.get_latest_change(&id).unwrap().into_up().unwrap();
    assert_substate_eq(s, new_substate(0, 0));
}

#[test]
fn it_allows_down_then_up() {
    setup_logger();
    let store = create_store();

    let id = add_substate(&store, 0, 0);

    let tx = store.create_read_tx().unwrap();
    let mut store = create_pending_store(&tx);

    let s = store.get_latest_change(id.substate_id()).unwrap().into_up().unwrap();
    assert_substate_eq(s, new_substate(0, 0));

    store
        .put(SubstateChange::Down {
            id: id.clone(),
            shard: Shard::first(),
            transaction_id: Default::default(),
        })
        .unwrap();

    store
        .put(SubstateChange::Up {
            id: id.to_next_version(),
            shard: Shard::first(),
            transaction_id: Default::default(),
            substate: new_substate(1, 1),
        })
        .unwrap();

    let s = store.get(id.to_next_version().as_ref()).unwrap();
    assert_substate_eq(s, new_substate(1, 1));
    let s = store.get_latest_change(id.substate_id()).unwrap().into_up().unwrap();
    assert_substate_eq(s, new_substate(1, 1));
}

#[test]
fn it_fails_if_previous_version_is_not_down() {
    let store = create_store();

    let id = add_substate(&store, 0, 0);

    let tx = store.create_read_tx().unwrap();
    let mut store = create_pending_store(&tx);
    let err = store
        .put(SubstateChange::Up {
            id: id.to_next_version(),
            shard: Shard::first(),
            transaction_id: Default::default(),
            substate: new_substate(1, 1),
        })
        .unwrap_err();

    assert!(matches!(err, SubstateStoreError::ExpectedSubstateDown { .. }));
}

#[test]
fn it_disallows_more_than_one_write_lock_non_local_only() {
    let store = create_store();

    let id = add_substate(&store, 0, 0);

    let tx = store.create_read_tx().unwrap();
    let mut store = create_pending_store(&tx);

    store
        .try_lock(
            tx_id(1),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Read),
            true,
        )
        .unwrap();
    store
        .try_lock(
            tx_id(2),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Read),
            true,
        )
        .unwrap();

    let lock = store.new_locks().get(id.substate_id()).unwrap();
    let n = lock.iter().filter(|l| l.is_read()).count();
    assert_eq!(n, 2);

    let err = store
        .try_lock(
            tx_id(3),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Write),
            false,
        )
        .unwrap_err();

    assert!(matches!(
        err.ok_lock_failed().unwrap(),
        LockFailedError::LockConflict { .. }
    ));
}

#[test]
fn it_allows_requesting_the_same_lock_within_one_transaction() {
    let store = create_store();

    let id = add_substate(&store, 0, 0);

    let tx = store.create_read_tx().unwrap();
    let mut store = create_pending_store(&tx);

    store
        .try_lock(
            tx_id(1),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Write),
            false,
        )
        .unwrap();
    // Another transaction cannot lock the same substate
    let err = store
        .try_lock(
            tx_id(2),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Output),
            false,
        )
        .unwrap_err();
    assert!(matches!(
        err.ok_lock_failed().unwrap(),
        LockFailedError::LockConflict { .. }
    ));

    // The same transaction is able to lock as an output
    store
        .try_lock(
            tx_id(1),
            &RequireLockIntentRef::new(id.substate_id(), 0, SubstateLockType::Output),
            false,
        )
        .unwrap();

    let n = store.new_locks().get(id.substate_id()).unwrap().len();
    assert_eq!(n, 2);
}

fn add_substate(store: &TestStore, seed: u8, version: u32) -> VersionedSubstateId {
    let id = new_substate_id(seed);
    let value = new_substate_value(seed);

    store
        .with_write_tx(|tx| {
            SubstateRecord {
                substate_id: id.clone(),
                version,
                substate_value: Some(value),
                state_hash: [seed; 32].into(),
                created_by_transaction: Default::default(),
                created_justify: QcId::zero(),
                created_block: BlockId::zero(),
                created_height: 0.into(),
                created_by_shard: Shard::first(),
                created_at_epoch: 0.into(),
                destroyed: None,
            }
            .create(tx)
        })
        .unwrap();

    VersionedSubstateId::new(id, version)
}

fn create_store() -> TestStore {
    let store = SqliteStateStore::connect(":memory:").unwrap();
    store
        .with_write_tx(|tx| {
            let zero = Block::zero_block(Network::LocalNet, NumPreshards::P256);
            zero.justify().insert(tx)?;
            zero.insert(tx)
        })
        .unwrap();
    store
}

fn create_pending_store<'a, 'tx, TAddr: NodeAddressable>(
    tx: &'a <SqliteStateStore<TAddr> as StateStore>::ReadTransaction<'tx>,
) -> PendingSubstateStore<'a, 'tx, SqliteStateStore<TAddr>> {
    PendingSubstateStore::new(tx, BlockId::zero(), TEST_NUM_PRESHARDS)
}

fn new_substate_id(seed: u8) -> SubstateId {
    ComponentAddress::from_array([seed; ObjectKey::LENGTH]).into()
}

fn new_substate(seed: u8, version: u32) -> Substate {
    Substate::new(version, new_substate_value(seed))
}

fn new_substate_value(seed: u8) -> SubstateValue {
    ComponentHeader {
        template_address: Default::default(),
        module_name: "".to_string(),
        owner_key: None,
        owner_rule: Default::default(),
        access_rules: Default::default(),
        entity_id: [seed; EntityId::LENGTH].into(),
        body: ComponentBody {
            state: tari_bor::Value::Null,
        },
    }
    .into()
}

fn tx_id(seed: u8) -> tari_transaction::TransactionId {
    [seed; tari_transaction::TransactionId::byte_size()].into()
}

fn assert_substate_eq(a: Substate, b: Substate) {
    assert_eq!(a.version(), b.version());
    assert_eq!(
        a.substate_value().as_component().unwrap().entity_id,
        b.substate_value().as_component().unwrap().entity_id
    );
}
