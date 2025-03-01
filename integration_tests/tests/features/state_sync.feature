# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@concurrent
@state_sync
Feature: State Sync

  # Ignore: this sometimes fails on CI but passes locally
  @ignore
  Scenario: New validator node registers and syncs
    # Initialize a base node, wallet, miner and VN
    Given a base node BASE
    Given a wallet WALLET connected to base node BASE
    Given a miner MINER connected to base node BASE and wallet WALLET

    # Initialize an indexer
    Given an indexer IDX connected to base node BASE
    # Initialize the wallet daemon
    Given a wallet daemon WALLET_D connected to indexer IDX
    # Initialize a VN
    Given a seed validator node VN connected to base node BASE and wallet daemon WALLET_D
    When miner MINER mines 4 new blocks
    When wallet WALLET has at least 5000 T
    When validator node VN sends a registration transaction to base wallet WALLET
    When miner MINER mines 26 new blocks
    Then VN has scanned to height 27
    And indexer IDX has scanned to height 27
    Then the validator node VN is listed as registered

    When indexer IDX connects to all other validators

    # Submit a few transactions
    When I create an account ACC1 via the wallet daemon WALLET_D with 10000 free coins
    When I create an account UNUSED1 via the wallet daemon WALLET_D
    When I create an account UNUSED2 via the wallet daemon WALLET_D
    When I create an account UNUSED3 via the wallet daemon WALLET_D

    # When I wait for validator VN has leaf block height of at least 15

    # Start a new VN that needs to sync
    Given a validator node VN2 connected to base node BASE and wallet daemon WALLET_D
    Given validator VN2 nodes connect to all other validators
    When indexer IDX connects to all other validators

    When validator node VN2 sends a registration transaction to base wallet WALLET
    When miner MINER mines 23 new blocks
    Then VN has scanned to height 50
    Then VN2 has scanned to height 50
    Then the validator node VN2 is listed as registered

    When I wait for validator VN has leaf block height of at least 1 at epoch 4
    When I wait for validator VN2 has leaf block height of at least 1 at epoch 4

    When I create an account UNUSED4 via the wallet daemon WALLET_D
    When I create an account UNUSED5 via the wallet daemon WALLET_D

    When I wait for validator VN has leaf block height of at least 5 at epoch 4
    When I wait for validator VN2 has leaf block height of at least 5 at epoch 4

