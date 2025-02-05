//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

//! # Consensus tests
//!
//! How to debug the database:
//!
//! Use `Test::builder().debug_sql("/tmp/test{}.db")...` to create a database file for each validator
//! where {} is replaced with the node address.

use std::time::Duration;

use log::info;
use tari_common_types::types::PrivateKey;
use tari_consensus::{hotstuff::HotStuffError, messages::HotstuffMessage};
use tari_crypto::tari_utilities::ByteArray;
use tari_dan_common_types::{
    crypto::create_key_pair,
    optional::Optional,
    Epoch,
    NodeHeight,
    SubstateLockType,
    SubstateRequirement,
    ToSubstateAddress,
    VersionedSubstateId,
};
use tari_dan_storage::{
    consensus_models::{AbortReason, BlockId, Command, Decision, SubstateRecord, TransactionRecord},
    StateStore,
    StateStoreReadTransaction,
};
use tari_engine_types::{
    commit_result::RejectReason,
    published_template::PublishedTemplateAddress,
    substate::SubstateId,
};
use tari_transaction::Transaction;

use crate::support::{
    build_transaction_from,
    helpers,
    load_binary_fixture,
    logging::setup_logger,
    ExecuteSpec,
    Test,
    TestAddress,
    TestVnDestination,
};

// Although these tests will pass with a single thread, we enable multi-threaded mode so that any unhandled race
// conditions can be picked up, plus tests run a little quicker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_transaction() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1"]).start().await;
    // First get transaction in the mempool
    let (tx1, _, _) = test.send_transaction_to_all(Decision::Commit, 1, 1, 1).await;
    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height >= NodeHeight(10) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(tx1.id());

    // Assert all LocalOnly
    test.get_validator(&TestAddress::new("1"))
        .state_store
        .with_read_tx(|tx| {
            let mut block = tx.blocks_get_tip(Epoch(1), test.get_validator(&TestAddress::new("1")).shard_group)?;
            loop {
                block = block.get_parent(tx)?;
                if block.id().is_zero() {
                    break;
                }

                for cmd in block.commands() {
                    assert!(matches!(cmd, Command::LocalOnly(_)));
                }
            }
            Ok::<_, HotStuffError>(())
        })
        .unwrap();
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;

    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_transaction_multi_vn() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;
    let (tx1, _, _) = test.send_transaction_to_all(Decision::Commit, 1, 1, 1).await;
    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height >= NodeHeight(10) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(tx1.id());

    // Assert all LocalOnly
    test.get_validator(&TestAddress::new("1"))
        .state_store
        .with_read_tx(|tx| {
            let mut block = tx.blocks_get_tip(Epoch(1), test.get_validator(&TestAddress::new("1")).shard_group)?;
            loop {
                block = block.get_parent(tx)?;
                if block.id().is_zero() {
                    break;
                }

                for cmd in block.commands() {
                    assert!(matches!(cmd, Command::LocalOnly(_)));
                }
            }
            Ok::<_, HotStuffError>(())
        })
        .unwrap();
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;

    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_transaction_abort() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1"]).start().await;
    // First get transaction in the mempool
    let (tx1, _, _) = test
        .send_transaction_to_all(Decision::Abort(AbortReason::None), 1, 1, 1)
        .await;
    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height >= NodeHeight(10) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx1.id(), Decision::Abort(AbortReason::ExecutionFailure))
        .await;

    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_blocks_with_queued_up_transactions_until_all_committed() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2", "3", "4", "5"])
        .start()
        .await;
    // First get all transactions in the mempool
    for _ in 0..10 {
        test.send_transaction_to_all(Decision::Commit, 1, 5, 1).await;
    }
    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height > NodeHeight(20) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_blocks_with_new_transactions_until_all_committed() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;
    let mut remaining_txs = 10;
    test.start_epoch(Epoch(1)).await;
    loop {
        if remaining_txs > 0 {
            test.send_transaction_to_all(Decision::Commit, 1, 5, 1).await;
        }
        remaining_txs -= 1;
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height > NodeHeight(20) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_requests_missing_transaction_from_local_leader() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;
    // First get all transactions in the mempool of node "2". We send to "2" because it is the leader for the next
    // block. We could send to "1" but the test would have to wait for the block time to be hit and block 1 to be
    // proposed before node "1" can propose block 2 with all the transactions.
    let mut tx_ids = Vec::with_capacity(10);
    for _ in 0..10 {
        let (transaction, inputs) = test.build_transaction(Decision::Commit, 5);
        tx_ids.push(*transaction.id());
        // All VNs will decide the same thing
        test.create_execution_at_destination_for_transaction(
            TestVnDestination::All,
            &transaction,
            inputs
                .into_iter()
                .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
                .collect(),
            vec![],
        );

        test.send_transaction_to_destination(TestVnDestination::Address(TestAddress::new("2")), transaction)
            .await;
    }
    test.start_epoch(Epoch(1)).await;
    loop {
        let (_, _, _, committed_height) = test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        if committed_height >= NodeHeight(10) {
            panic!("Not all transaction committed after {} blocks", committed_height);
        }
    }

    // Check if we clean the missing transactions table in the DB once the transactions are committed
    test.get_validator(&TestAddress::new("2"))
        .state_store
        .with_read_tx(|tx| {
            let mut block_id = BlockId::zero();
            loop {
                let children = tx.blocks_get_all_by_parent(&block_id).unwrap();
                if block_id.is_zero() {
                    break;
                }

                assert_eq!(children.len(), 1);
                for block in children {
                    if block.is_genesis() {
                        continue;
                    }
                    let missing = tx.blocks_get_pending_transactions(block.id()).unwrap();
                    assert!(missing.is_empty());
                    block_id = *block.id();
                }
            }
            Ok::<_, HotStuffError>(())
        })
        .unwrap();

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(&tx_ids[0]);
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_shard_single_transaction() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1"])
        .add_committee(1, vec!["2"])
        .start()
        .await;

    let (tx, _, _) = test.send_transaction_to_all(Decision::Commit, 100, 2, 2).await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("2")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx.id(), Decision::Commit)
        .await;
    test.assert_all_validators_committed(tx.id());

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_validator_propose_blocks_with_new_transactions_until_all_committed() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2", "3", "4", "5"])
        .start()
        .await;
    let mut remaining_txs = 10u32;

    test.start_epoch(Epoch(1)).await;
    loop {
        if remaining_txs > 0 {
            test.send_transaction_to_all(Decision::Commit, 1, 5, 1).await;
        }
        test.on_block_committed().await;
        remaining_txs = remaining_txs.saturating_sub(1);

        if remaining_txs == 0 && test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height > NodeHeight(20) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_shard_propose_blocks_with_new_transactions_until_all_committed() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2", "3"])
        .add_committee(1, vec!["4", "5", "6"])
        .add_committee(2, vec!["7", "8", "9"])
        .start()
        .await;

    let mut tx_ids = Vec::new();
    for _ in 0..20 {
        let (tx, _, _) = test.send_transaction_to_all(Decision::Commit, 100, 2, 1).await;
        tx_ids.push(*tx.id());
    }

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("4")).get_leaf_block();
        let leaf3 = test.get_validator(&TestAddress::new("7")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) || leaf3.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{}/{} blocks",
                leaf1.height, leaf2.height, leaf3.height
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(&tx_ids[0]);

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_shard_group_decides_to_abort() {
    setup_logger();
    let mut test = Test::builder()
        // TODO: investigate, test can take longer than expected
        .with_test_timeout(Duration::from_secs(60))
        .add_committee(0, vec!["1", "2", "3"])
        .add_committee(1, vec!["4", "5", "6"])
        .start()
        .await;

    let (tx1, inputs) = test.build_transaction(Decision::Commit, 5);
    test.send_transaction_to_destination(TestVnDestination::Committee(0), tx1.clone())
        .await;

    // Change the decision on committee 1 to Abort when executing. This test is not technically valid, as all
    // non-byzantine nodes MUST have the same decision given the same pledges. However, this does test that is it not
    // possible for others to COMMIT without all committees agreeing to COMMIT.
    let mut tx2 = tx1.clone();
    tx2.abort(RejectReason::ExecutionFailure("Test aborted".to_string()));

    test.create_execution_at_destination_for_transaction(
        TestVnDestination::Committee(0),
        &tx1,
        inputs
            .iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        vec![],
    )
    .create_execution_at_destination_for_transaction(
        TestVnDestination::Committee(1),
        &tx2,
        inputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        vec![],
    );

    test.send_transaction_to_destination(TestVnDestination::Committee(1), tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("2")).get_leaf_block();
        if leaf1.height > NodeHeight(40) || leaf2.height > NodeHeight(40) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx2.id(), Decision::Abort(AbortReason::ExecutionFailure))
        .await;

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_local_inputs_foreign_outputs() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        // Two output-only committees
        .add_committee(1, vec!["3", "4"])
        .add_committee(2, vec!["5", "6"])
        .start()
        .await;

    let inputs = test.create_substates_on_vns(TestVnDestination::Committee(0), 2);
    let outputs_1 = test.build_outputs_for_committee(1, 1);
    let outputs_2 = test.build_outputs_for_committee(2, 1);

    let tx1 = build_transaction_from(
        Transaction::builder()
            .with_inputs(inputs.iter().cloned().map(|i| i.into()))
            .build_and_seal(&PrivateKey::default()),
        Decision::Commit,
    );
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx1,
        inputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs_1.into_iter().chain(outputs_2).collect(),
    );
    test.send_transaction_to_destination(TestVnDestination::All, tx1.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;
    test.assert_all_validators_committed(tx1.id());

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_local_inputs_foreign_outputs_abort() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .start()
        .await;

    let inputs = test.create_substates_on_vns(TestVnDestination::Committee(0), 2);
    let outputs = test.build_outputs_for_committee(1, 1);
    let transaction = Transaction::builder()
        .with_inputs(inputs.iter().cloned().map(|i| i.into()))
        .build_and_seal(&PrivateKey::default());

    let tx = build_transaction_from(transaction.clone(), Decision::Commit);
    let tx_abort = build_transaction_from(transaction, Decision::Abort(AbortReason::ExecutionFailure));
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::Committee(0),
        &tx,
        inputs
            .clone()
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs.clone(),
    );
    test.send_transaction_to_destination(TestVnDestination::Committee(0), tx.clone())
        .await;

    test.create_execution_at_destination_for_transaction(
        TestVnDestination::Committee(1),
        &tx_abort,
        inputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs,
    );
    test.send_transaction_to_destination(TestVnDestination::Committee(1), tx_abort.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx.id(), Decision::Abort(AbortReason::ExecutionFailure))
        .await;
    test.assert_all_validators_did_not_commit(tx.id());

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_local_inputs_and_outputs_foreign_outputs() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .add_committee(2, vec!["5", "6"])
        .start()
        .await;

    let inputs_0 = test.create_substates_on_vns(TestVnDestination::Committee(0), 2);
    let inputs_1 = test.create_substates_on_vns(TestVnDestination::Committee(1), 2);
    let outputs_0 = test.build_outputs_for_committee(0, 5);
    // Output-only committee
    let outputs_2 = test.build_outputs_for_committee(2, 5);

    let tx1 = build_transaction_from(
        Transaction::builder()
            .with_inputs(inputs_0.iter().chain(&inputs_1).cloned().map(|i| i.into()))
            .build_and_seal(&PrivateKey::default()),
        Decision::Commit,
    );
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx1,
        inputs_0
            .into_iter()
            .chain(inputs_1)
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs_0.into_iter().chain(outputs_2).collect(),
    );
    test.send_transaction_to_destination(TestVnDestination::Committee(0), tx1.clone())
        .await;
    test.send_transaction_to_destination(TestVnDestination::Committee(1), tx1.clone())
        .await;
    // Don't send to committee 2 since they are not involved in inputs

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;
    test.assert_all_validators_committed(tx1.id());

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_output_conflict_abort() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .start()
        .await;

    let (tx1, inputs) = test.build_transaction(Decision::Commit, 5);
    let mut outputs = test.build_outputs_for_committee(0, 1);
    outputs.extend(test.build_outputs_for_committee(1, 1));
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx1,
        inputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs.clone(),
    );
    test.send_transaction_to_destination(TestVnDestination::All, tx1.clone())
        .await;

    let inputs = test.create_substates_on_vns(TestVnDestination::All, 1);
    let tx = Transaction::builder()
        .with_inputs(inputs.iter().cloned().map(|i| i.into()))
        .build_and_seal(&Default::default());
    let tx2 = build_transaction_from(tx, Decision::Commit);
    assert_ne!(tx1.id(), tx2.id());
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx2,
        inputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        outputs,
    );

    let tx_ids = [tx1.id(), tx2.id()];

    test.send_transaction_to_destination(TestVnDestination::All, tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    // Currently not deterministic (test harness) which transaction will arrive first so we check that one transaction
    // is committed and the other is aborted. TODO: It is also possible that both are aborted.
    let tx1_vn1 = test.get_validator(&TestAddress::new("1")).get_transaction(tx_ids[0]);
    let tx2_vn1 = test.get_validator(&TestAddress::new("1")).get_transaction(tx_ids[1]);

    let tx1_vn3 = test.get_validator(&TestAddress::new("3")).get_transaction(tx_ids[0]);
    let tx2_vn3 = test.get_validator(&TestAddress::new("3")).get_transaction(tx_ids[1]);

    assert_eq!(tx1_vn1.final_decision().unwrap(), tx1_vn3.final_decision().unwrap());
    assert_eq!(tx2_vn1.final_decision().unwrap(), tx2_vn3.final_decision().unwrap());
    if tx1_vn1.final_decision().unwrap().is_commit() {
        test.assert_all_validators_committed(tx_ids[0]);
    } else {
        test.assert_all_validators_did_not_commit(tx_ids[0]);
    }

    if tx2_vn1.final_decision().unwrap().is_commit() {
        test.assert_all_validators_committed(tx_ids[1]);
    } else {
        test.assert_all_validators_did_not_commit(tx_ids[1]);
    }

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_inputs_from_previous_outputs() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;

    let (tx1, _, outputs) = test.send_transaction_to_all(Decision::Commit, 1, 5, 5).await;
    let prev_outputs = outputs
        .iter()
        .map(|output| SubstateRequirement::versioned(output.clone(), 0))
        .collect::<Vec<_>>();

    let tx2 = Transaction::builder()
        .with_inputs(prev_outputs.clone())
        .build_and_seal(&Default::default());
    let tx2 = build_transaction_from(tx2.clone(), Decision::Commit);
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx2,
        prev_outputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        vec![],
    );
    test.send_transaction_to_destination(TestVnDestination::All, tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    test.wait_for_n_to_be_finalized(2).await;

    let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
    let leaf2 = test.get_validator(&TestAddress::new("2")).get_leaf_block();
    if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
        panic!(
            "Not all transaction committed after {}/{} blocks",
            leaf1.height, leaf2.height,
        );
    }

    test.assert_all_validators_at_same_height().await;
    // Assert that the decision matches for all validators. If tx2 is sequenced first, then it will be aborted due to
    // the input not existing
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;
    let decision_tx2 = test
        .get_validator(&TestAddress::new("1"))
        .get_transaction(tx2.id())
        .final_decision()
        .expect("tx2 final decision not reached");
    test.assert_all_validators_have_decision(tx2.id(), decision_tx2).await;
    if let Some(reason) = decision_tx2.abort_reason() {
        assert_eq!(reason, AbortReason::OneOrMoreInputsNotFound);
    }

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_inputs_from_previous_outputs() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .start()
        .await;

    let (tx1, _, outputs) = test.send_transaction_to_all(Decision::Commit, 1, 5, 2).await;
    let prev_outputs = outputs
        .iter()
        .map(|output| SubstateRequirement::versioned(output.clone(), 0))
        .collect::<Vec<_>>();

    let tx2 = Transaction::builder()
        .with_inputs(prev_outputs.clone())
        .build_and_seal(&Default::default());
    let tx2 = build_transaction_from(tx2.clone(), Decision::Commit);
    test.create_execution_at_destination_for_transaction(
        TestVnDestination::All,
        &tx2,
        prev_outputs
            .into_iter()
            .map(|input| (input.substate_id().clone(), SubstateLockType::Write))
            .collect(),
        vec![],
    );
    test.send_transaction_to_destination(TestVnDestination::All, tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(30) || leaf2.height > NodeHeight(30) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height,
            );
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;
    test.assert_all_validators_have_decision(tx2.id(), Decision::Abort(AbortReason::OneOrMoreInputsNotFound))
        .await;
    test.assert_all_validators_committed(tx1.id());
    test.assert_all_validators_did_not_commit(tx2.id());

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_input_conflict() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;

    let substate_id = test.create_substates_on_vns(TestVnDestination::All, 1).pop().unwrap();
    let secret = PrivateKey::from_canonical_bytes(&[1u8; 32]).unwrap();

    let tx1 = Transaction::builder()
        .add_input(substate_id.clone())
        .build_and_seal(&secret);
    let tx1 = TransactionRecord::new(tx1);

    let tx2 = Transaction::builder()
        .add_input(substate_id.clone())
        .build_and_seal(&secret);
    let tx2 = TransactionRecord::new(tx2);

    test.add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx1.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![(substate_id.substate_id().clone(), SubstateLockType::Write)],
        new_outputs: vec![],
    })
    .add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx2.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![(substate_id.substate_id().clone(), SubstateLockType::Write)],
        new_outputs: vec![],
    });

    test.network()
        .send_transaction(TestVnDestination::All, tx1.clone())
        .await;
    test.network()
        .send_transaction(TestVnDestination::All, tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf1.height > NodeHeight(30) {
            panic!("Not all transaction committed after {} blocks", leaf1.height,);
        }
    }

    let tx1_decision = test
        .get_validator(&TestAddress::new("1"))
        .get_transaction(tx1.transaction().id())
        .final_decision()
        .expect("tx1 final decision not reached");
    info!("tx1 = {}", tx1.id());
    info!("tx2 = {}", tx2.id());
    if tx1_decision.is_commit() {
        test.assert_all_validators_committed(tx1.id());
        test.assert_all_validators_did_not_commit(tx2.id());
    } else {
        test.assert_all_validators_did_not_commit(tx1.id());
        test.assert_all_validators_committed(tx2.id());
    }

    test.assert_all_validators_at_same_height().await;

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn epoch_change() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;

    test.start_epoch(Epoch(1)).await;
    let mut remaining_txs = 10;

    loop {
        if remaining_txs > 0 {
            test.send_transaction_to_all(Decision::Commit, 1, 5, 1).await;
        }
        remaining_txs -= 1;
        if remaining_txs == 5 {
            test.start_epoch(Epoch(2)).await;
        }

        if remaining_txs <= 0 && test.is_transaction_pool_empty() {
            break;
        }

        let (_, _, epoch, height) = test.on_block_committed().await;
        if height.as_u64() > 1 && epoch == 2u64 {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf1.height > NodeHeight(30) {
            panic!("Not all transaction committed after {} blocks", leaf1.height,);
        }
    }

    // Assert epoch changed
    test.get_validator(&TestAddress::new("1"))
        .state_store
        .with_read_tx(|tx| {
            let mut block = tx.blocks_get_tip(Epoch(1), test.get_validator(&TestAddress::new("1")).shard_group)?;
            loop {
                block = block.get_parent(tx)?;
                if block.id().is_zero() {
                    break;
                }
                if block.is_epoch_end() {
                    return Ok::<_, HotStuffError>(());
                }
            }

            panic!("No epoch end block found");
        })
        .unwrap();

    test.assert_all_validators_at_same_height().await;
    // test.assert_all_validators_committed();

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_node_goes_down() {
    setup_logger();
    let mut test = Test::builder()
        // Allow enough time for leader failures
        .with_test_timeout(Duration::from_secs(60))
        .modify_consensus_constants(|config_mut| {
            // Prevent evictions
            config_mut.missed_proposal_suspend_threshold = 10;
            config_mut.missed_proposal_evict_threshold = 10;
            config_mut.pacemaker_block_time = Duration::from_secs(2);
        })
        .add_committee(0, vec!["1", "2", "3", "4", "5"])
        .start()
        .await;

    let failure_node = TestAddress::new("4");

    let mut tx_ids = Vec::with_capacity(10);
    for _ in 0..10 {
        let (tx, _, _) = test.send_transaction_to_all(Decision::Commit, 1, 2, 1).await;
        tx_ids.push(*tx.id());
    }

    // Take the VN offline - if we do it in the loop below, all transactions may have already been finalized (local
    // only) by committed block 1
    log::info!("😴 {failure_node} is offline");
    test.network()
        .go_offline(TestVnDestination::Address(failure_node.clone()))
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        let (_, _, _, committed_height) = test.on_block_committed().await;

        if committed_height == NodeHeight(1) {
            // This allows a few more leader failures to occur
            test.send_transaction_to_all(Decision::Commit, 1, 2, 1).await;
            test.wait_for_pool_count(TestVnDestination::All, 1).await;
        }

        if test.validators_iter().filter(|vn| vn.address != failure_node).all(|v| {
            let c = v.get_transaction_pool_count();
            log::info!("{} has {} transactions in pool", v.address, c);
            c == 0
        }) {
            break;
        }

        if committed_height > NodeHeight(50) {
            panic!("Not all transaction committed after {} blocks", committed_height);
        }
    }

    test.assert_all_validators_at_same_height_except(&[failure_node.clone()])
        .await;

    test.validators_iter()
        .filter(|vn| vn.address != failure_node)
        .for_each(|v| {
            tx_ids.iter().for_each(|tx_id| {
                assert!(
                    v.has_committed_substates(tx_id),
                    "Validator {} did not commit",
                    v.address
                );
            });
        });

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown_except(&[failure_node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_block_distribution() {
    setup_logger();
    let mut test = Test::builder()
        .with_test_timeout(Duration::from_secs(60))
        .with_message_filter(Box::new(move |from: &TestAddress, to: &TestAddress, msg| {
            if !matches!(msg, HotstuffMessage::ForeignProposalNotification(_)) {
                return true;
            }

            match from.as_str() {
                // We filter out some messages from each node to foreign committees to ensure we sometimes have to
                // rely on other members of the foreign and local committees to receive the foreign proposal.
                "1" => to == "1" || to == "2" || to == "3",
                "4" => to == "4" || to == "5" || to == "6",
                "7" => to == "7" || to == "8" || to == "9",
                _ => true,
            }
        }))
        .add_committee(0, vec!["1", "2", "3"])
        .add_committee(1, vec!["4", "5", "6"])
        .add_committee(2, vec!["7", "8", "9"])
        .start()
        .await;
    for _ in 0..20 {
        test.send_transaction_to_all(Decision::Commit, 1, 5, 1).await;
    }

    test.network().start();
    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("4")).get_leaf_block();
        let leaf3 = test.get_validator(&TestAddress::new("7")).get_leaf_block();
        if leaf1.height > NodeHeight(100) || leaf2.height > NodeHeight(100) || leaf3.height > NodeHeight(100) {
            panic!(
                "Not all transaction committed after {}/{}/{} blocks",
                leaf1.height, leaf2.height, leaf3.height
            );
        }
    }

    test.assert_all_validators_at_same_height().await;

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    log::info!("total messages filtered: {}", test.network().total_messages_filtered());
    // Each leader sends 3 proposals to the both foreign committees, so 6 messages per leader. 18 in total.
    // assert_eq!(test.network().total_messages_filtered(), 18);
    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_unversioned_inputs() {
    setup_logger();
    let mut test = Test::builder().add_committee(0, vec!["1", "2"]).start().await;
    // First get transaction in the mempool
    let inputs = test.create_substates_on_vns(TestVnDestination::All, 1);
    // Remove versions from inputs to test substate version resolution
    let unversioned_inputs = inputs
        .iter()
        .map(|i| SubstateRequirement::new(i.substate_id().clone(), None));
    let tx = Transaction::builder()
        .with_inputs(unversioned_inputs)
        .build_and_seal(&PrivateKey::default());
    let tx = TransactionRecord::new(tx);

    test.send_transaction_to_destination(TestVnDestination::All, tx.clone())
        .await;
    test.add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: inputs
            .into_iter()
            .map(|input| (input.into_substate_id(), SubstateLockType::Write))
            .collect(),
        new_outputs: vec![],
    });

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height >= NodeHeight(10) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(tx.id());

    // Assert all LocalOnly
    test.get_validator(&TestAddress::new("1"))
        .state_store
        .with_read_tx(|tx| {
            let mut block = Some(tx.blocks_get_tip(Epoch(1), test.get_validator(&TestAddress::new("1")).shard_group)?);
            loop {
                block = block.as_ref().unwrap().get_parent(tx).optional()?;
                let Some(b) = block.as_ref() else {
                    break;
                };

                for cmd in b.commands() {
                    assert!(matches!(cmd, Command::LocalOnly(_)));
                }
            }
            Ok::<_, HotStuffError>(())
        })
        .unwrap();

    test.assert_clean_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_unversioned_input_conflict() {
    // CASE: Tx1 and Tx2 use id1 and id2 as inputs. Comm1 sequences Tx1 and simultaneously Comm2 sequences Tx2.
    // When they exchange substates, they will try to sequence either transaction but will pick up the lock conflict and
    // propose to abort.
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .start()
        .await;

    let id0 = test
        .create_substates_on_vns(TestVnDestination::Committee(0), 1)
        .pop()
        .unwrap();
    let id1 = test
        .create_substates_on_vns(TestVnDestination::Committee(1), 1)
        .pop()
        .unwrap();

    let tx1 = Transaction::builder()
        .add_input(SubstateRequirement::unversioned(id0.substate_id().clone()))
        .add_input(SubstateRequirement::unversioned(id1.substate_id().clone()))
        .build_and_seal(&Default::default());
    let tx1 = TransactionRecord::new(tx1);

    let tx2 = Transaction::builder()
        .add_input(SubstateRequirement::unversioned(id0.substate_id().clone()))
        .add_input(SubstateRequirement::unversioned(id1.substate_id().clone()))
        .build_and_seal(&Default::default());
    let tx2 = TransactionRecord::new(tx2);

    test.add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx1.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![
            (id0.substate_id().clone(), SubstateLockType::Write),
            (id1.substate_id().clone(), SubstateLockType::Write),
        ],
        new_outputs: vec![],
    })
    .add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx2.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![
            (id0.substate_id().clone(), SubstateLockType::Write),
            (id1.substate_id().clone(), SubstateLockType::Write),
        ],
        new_outputs: vec![],
    });

    // NOTE: we send tx1 to committee 0 and tx2 to committee 1 to loosely ensure that we create the situation this test
    // is testing. If we sent to all, most of the time one or both of the transactions will commit.
    test.network()
        .send_transaction(TestVnDestination::Committee(0), tx1.clone())
        .await;
    test.network()
        .send_transaction(TestVnDestination::Committee(1), tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(60) && leaf2.height > NodeHeight(60) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height
            );
        }
    }

    test.assert_all_validators_at_same_height().await;

    test.assert_all_validators_have_decision(tx1.id(), Decision::Abort(AbortReason::ForeignPledgeInputConflict))
        .await;
    test.assert_all_validators_have_decision(tx2.id(), Decision::Abort(AbortReason::ForeignPledgeInputConflict))
        .await;

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_unversioned_input_conflict_delay_prepare() {
    // CASE: Tx1 and Tx2 use id1 as an input, Comm1 sequences Tx1 and simultaneously Comm2 sequences Tx2.
    // Since the id2 and id3 substates are uncommon to the transactions and live in Comm2, Comm2 and lock both
    // transactions. Comm1 will not have yet pledged a value for id1 to Tx1. This allows Comm1 to delay sequencing Tx1
    // (due to a soft lock conflict) until Tx2 is finalized. The output of Tx2 will be pledged to Tx1.
    // This is a natural consequence (i.e. no special code) of the local substate locks.
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .start()
        .await;

    let id0 = test
        .create_substates_on_vns(TestVnDestination::Committee(0), 1)
        .pop()
        .unwrap();
    let id1 = test
        .create_substates_on_vns(TestVnDestination::Committee(1), 1)
        .pop()
        .unwrap();
    let id2 = test
        .create_substates_on_vns(TestVnDestination::Committee(1), 1)
        .pop()
        .unwrap();

    let tx1 = Transaction::builder()
        .add_input(SubstateRequirement::unversioned(id0.substate_id().clone()))
        .add_input(SubstateRequirement::unversioned(id1.substate_id().clone()))
        .build_and_seal(&Default::default());
    let tx1 = TransactionRecord::new(tx1);

    let tx2 = Transaction::builder()
        .add_input(SubstateRequirement::unversioned(id0.substate_id().clone()))
        .add_input(SubstateRequirement::unversioned(id2.substate_id().clone()))
        .build_and_seal(&Default::default());
    let tx2 = TransactionRecord::new(tx2);

    test.add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx1.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![
            (id0.substate_id().clone(), SubstateLockType::Write),
            (id1.substate_id().clone(), SubstateLockType::Write),
        ],
        new_outputs: vec![],
    })
    .add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx2.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: vec![
            (id0.substate_id().clone(), SubstateLockType::Write),
            (id2.substate_id().clone(), SubstateLockType::Write),
        ],
        new_outputs: vec![],
    });

    test.network()
        .send_transaction(TestVnDestination::Committee(0), tx1.clone())
        .await;
    test.network()
        .send_transaction(TestVnDestination::Committee(1), tx2.clone())
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }

        let leaf1 = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        let leaf2 = test.get_validator(&TestAddress::new("3")).get_leaf_block();
        if leaf1.height > NodeHeight(60) && leaf2.height > NodeHeight(60) {
            panic!(
                "Not all transaction committed after {}/{} blocks",
                leaf1.height, leaf2.height
            );
        }
    }

    test.assert_all_validators_at_same_height().await;

    test.assert_all_validators_have_decision(tx1.id(), Decision::Commit)
        .await;
    test.assert_all_validators_have_decision(tx2.id(), Decision::Commit)
        .await;

    test.assert_clean_shutdown().await;
    log::info!("total messages sent: {}", test.network().total_messages_sent());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_node_goes_down_and_gets_evicted() {
    setup_logger();
    let failure_node = TestAddress::new("4");

    let mut test = Test::builder()
        // Allow enough time for leader failures
        .with_test_timeout(Duration::from_secs(30))
        .modify_consensus_constants(|config_mut| {
            // The node will be evicted after three missed proposals
            config_mut.missed_proposal_suspend_threshold = 1;
            config_mut.missed_proposal_evict_threshold = 3;
            config_mut.pacemaker_block_time = Duration::from_secs(5);
        })
        .add_committee(0, vec!["1", "2", "3", "4", "5"])
        .add_failure_node(failure_node.clone())
        .start()
        .await;

    for _ in 0..10 {
        test.send_transaction_to_all(Decision::Commit, 1, 2, 1).await;
    }

    // Take the VN offline - if we do it in the loop below, all transactions may have already been finalized (local
    // only) by committed block 1
    log::info!("😴 {failure_node} is offline");
    test.network()
        .go_offline(TestVnDestination::Address(failure_node.clone()))
        .await;

    test.start_epoch(Epoch(1)).await;

    loop {
        let (_, _, _, committed_height) = test.on_block_committed().await;

        // Takes missed_proposal_evict_threshold * 5 (members) + 3 (chain) blocks for nodes to evict. So we need to keep
        // the transactions coming to speed up this test.
        if committed_height >= NodeHeight(1) && committed_height < NodeHeight(20) {
            // This allows a few more leader failures to occur
            test.send_transaction_to_all(Decision::Commit, 1, 2, 1).await;
        }

        let eviction_proofs = test
            .validators()
            .get(&TestAddress::new("1"))
            .unwrap()
            .epoch_manager()
            .eviction_proofs()
            .await;
        if !eviction_proofs.is_empty() {
            break;
        }

        if committed_height >= NodeHeight(40) {
            panic!("Not all transaction committed after {} blocks", committed_height);
        }
    }

    test.assert_all_validators_at_same_height_except(&[failure_node.clone()])
        .await;

    // test.validators_iter()
    //     .filter(|vn| vn.address != failure_node)
    //     .for_each(|v| {
    //         assert!(v.has_committed_substates(), "Validator {} did not commit", v.address);
    //     });

    let (_, failure_node_pk) = helpers::derive_keypair_from_address(&failure_node);
    test.validators()
        .get(&TestAddress::new("1"))
        .unwrap()
        .state_store()
        .with_read_tx(|tx| {
            let leaf = tx.leaf_block_get(Epoch(1))?;
            assert!(
                tx.suspended_nodes_is_evicted(leaf.block_id(), &failure_node_pk)
                    .unwrap(),
                "{failure_node} is not evicted"
            );
            Ok::<_, HotStuffError>(())
        })
        .unwrap();

    let eviction_proofs = test
        .validators()
        .get(&TestAddress::new("1"))
        .unwrap()
        .epoch_manager()
        .eviction_proofs()
        .await;
    for proof in &eviction_proofs {
        assert_eq!(proof.node_to_evict(), &failure_node_pk);
    }

    // Epoch manager state is shared between all validators, so each working validator (4) should create a proof.
    // assert_eq!(eviction_proofs.len(), 4);

    log::info!("total messages sent: {}", test.network().total_messages_sent());
    test.assert_clean_shutdown_except(&[failure_node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multishard_publish_template() {
    setup_logger();
    let mut test = Test::builder()
        .add_committee(0, vec!["1", "2"])
        .add_committee(1, vec!["3", "4"])
        .add_committee(2, vec!["5", "6"])
        .add_committee(3, vec!["7", "8"])
        .start()
        .await;
    // Create and send publish template transaction
    let inputs = test.create_substates_on_vns(TestVnDestination::All, 1);
    let (sk, pk) = create_key_pair();
    let wasm = load_binary_fixture("state.wasm");
    let tx = Transaction::builder()
        .publish_template(wasm.clone())
        .with_inputs(inputs.iter().cloned().map(Into::into))
        .build_and_seal(&sk);
    let tx = TransactionRecord::new(tx);

    test.send_transaction_to_destination(TestVnDestination::All, tx.clone())
        .await;
    let template_id = PublishedTemplateAddress::from_author_and_code(&pk, &wasm);
    test.add_execution_at_destination(TestVnDestination::All, ExecuteSpec {
        transaction: tx.transaction().clone(),
        decision: Decision::Commit,
        fee: 1,
        input_locks: inputs
            .into_iter()
            .map(|input| (input.into_substate_id(), SubstateLockType::Write))
            .collect(),
        new_outputs: vec![SubstateId::Template(template_id)],
    });

    test.start_epoch(Epoch(1)).await;

    loop {
        test.on_block_committed().await;

        if test.is_transaction_pool_empty() {
            break;
        }
        let leaf = test.get_validator(&TestAddress::new("1")).get_leaf_block();
        if leaf.height >= NodeHeight(30) {
            panic!("Not all transaction committed after {} blocks", leaf.height);
        }
    }

    test.assert_all_validators_at_same_height().await;
    test.assert_all_validators_committed(tx.id());

    // Assert all LocalOnly
    let template_substate = test
        .get_validator(&TestAddress::new("1"))
        .state_store
        .with_read_tx(|tx| SubstateRecord::get(tx, &VersionedSubstateId::new(template_id, 0).to_substate_address()))
        .unwrap();
    let binary = template_substate
        .substate_value
        .unwrap()
        .into_template()
        .expect("Expected template substate")
        .binary;
    assert_eq!(binary, wasm, "Template binary does not match");

    test.assert_clean_shutdown().await;
}

// mod dump_data {
//     use super::*;
//     use std::fs::File;
//     use tari_crypto::tari_utilities::hex::from_hex;
//     use tari_consensus::hotstuff::eviction_proof::convert_block_to_sidechain_block_header;
//     use tari_state_store_sqlite::SqliteStateStore;
//
//    fn asd() {
//            let store = SqliteStateStore::<PeerAddress>::connect(
//                "data/swarm/processes/validator-node-01/localnet/data/validator_node/state.db",
//            )
//                .unwrap();
//            let p = store
//                .with_read_tx(|tx| {
//                    let block = tari_dan_storage::consensus_models::Block::get(
//                        tx,
//                        &BlockId::try_from(
//                            from_hex("891d186d2d46b990cc0974dc68734f701eaeb418a1bba487de93905d3986e0e3").unwrap(),
//                        )
//                            .unwrap(),
//                    )?;
//
//                    let commit_block = tari_dan_storage::consensus_models::Block::get(
//                        tx,
//                        &BlockId::try_from(
//                            from_hex("1cdbe5c1a894bcc254b47cf017d4d17608839b7048d1c02162bccd39e7635288").unwrap(),
//                        )
//                            .unwrap(),
//                    )
//                        .unwrap();
//
//                    let mut p = tari_consensus::hotstuff::eviction_proof::generate_eviction_proofs(tx,
// block.justify(), &[                        commit_block.clone(),
//                    ])
//                        .unwrap();
//
//                    eprintln!();
//                    eprintln!("{}", serde_json::to_string_pretty(&commit_block).unwrap());
//                    eprintln!();
//                    eprintln!();
//
//                    let h = convert_block_to_sidechain_block_header(commit_block.header());
//
//                    assert_eq!(h.calculate_hash(), commit_block.header().calculate_hash());
//                    let b = p[0].proof().header().calculate_block_id();
//                    assert_eq!(
//                        p[0].proof().header().calculate_hash(),
//                        commit_block.header().calculate_hash()
//                    );
//                    assert_eq!(b, *commit_block.id().hash());
//                    Ok::<_, HotStuffError>(p.remove(0))
//                })
//                .unwrap();
//            let f = File::options()
//                .create(true)
//                .write(true)
//                .truncate(true)
//                .open("/tmp/eviction_proof.json")
//                .unwrap();
//            serde_json::to_writer_pretty(f, &p).unwrap();
//    }
// }
