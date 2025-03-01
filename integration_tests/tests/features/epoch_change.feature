# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@concurrent
@epoch_change
Feature: Epoch change
  
  Scenario: EndEpoch command is used on epoch change
    # Initialize a base node, wallet, miner and VN
    Given a base node BASE
    Given a wallet WALLET connected to base node BASE
    Given a miner MINER connected to base node BASE and wallet WALLET

    # Initialize two validator nodes
    Given a validator node VAL connected to base node BASE and wallet daemon WALLET_D
    Given validator VAL nodes connect to all other validators

    # Register VN
    When miner MINER mines 10 new blocks
    When wallet WALLET has at least 2000 T
    When validator node VAL sends a registration transaction to base wallet WALLET
    When miner MINER mines 26 new blocks
    Then the validator node VAL is listed as registered

    # Initialize indexer and connect wallet daemon
    Given an indexer IDX connected to base node BASE
    Given a wallet daemon WALLET_D connected to indexer IDX

    # Create account
    When I create an account ACC via the wallet daemon WALLET_D with 2000000 free coins

    # Publish the "counter" template
    When wallet daemon WALLET_D publishes the template "faucet" using account ACC

    # Push a transaction through to get blocks
#    When I call function "mint" on template "faucet" on VAL with args "amount_10000" named "FAUCET"
    When I call function "mint" on template "faucet" with args "amount_10000" using account ACC to pay fees via wallet daemon WALLET_D named "FAUCET"

    When Block count on VN VAL is at least 6
    When miner MINER mines 7 new blocks
    Then VAL has scanned to height 40
    Then the validator node VAL has ended epoch 3

#  @serial
#  Scenario: Committee is split into two during epoch change
#   # Initialize a base node, wallet, miner and VN
#    Given a base node BASE
#    Given a wallet WALLET connected to base node BASE
#    Given a miner MINER connected to base node BASE and wallet WALLET
#
#   # Initialize validator nodes
#    Given a validator node VAL_1 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_2 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_3 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_4 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_5 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_6 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_7 connected to base node BASE and wallet daemon WALLET_D
#    Given validator VAL_1 nodes connect to all other validators
#    Given validator VAL_2 nodes connect to all other validators
#    Given validator VAL_3 nodes connect to all other validators
#    Given validator VAL_4 nodes connect to all other validators
#    Given validator VAL_5 nodes connect to all other validators
#    Given validator VAL_6 nodes connect to all other validators
#    Given validator VAL_7 nodes connect to all other validators
#
#   # The wallet must have some funds before the VN sends transactions
#    When miner MINER mines 14 new blocks
#    When wallet WALLET has at least 120000 T
#
#   # VN registration
#    When validator node VAL_1 sends a registration transaction to base wallet WALLET
#    When validator node VAL_2 sends a registration transaction to base wallet WALLET
#    When validator node VAL_3 sends a registration transaction to base wallet WALLET
#    When validator node VAL_4 sends a registration transaction to base wallet WALLET
#    When validator node VAL_5 sends a registration transaction to base wallet WALLET
#    When validator node VAL_6 sends a registration transaction to base wallet WALLET
#    When validator node VAL_7 sends a registration transaction to base wallet WALLET
#
#   # Mine them into registered epoch
#    When miner MINER mines 10 new blocks
#    Then VAL_1 has scanned to height 21
#    Then the validator node VAL_1 is listed as registered
#    Then the validator node VAL_2 is listed as registered
#    Then the validator node VAL_3 is listed as registered
#    Then the validator node VAL_4 is listed as registered
#    Then the validator node VAL_5 is listed as registered
#    Then the validator node VAL_6 is listed as registered
#    Then the validator node VAL_7 is listed as registered
#
#
#    When Block count on VN VAL_1 is at least 5
#
#    Given a validator node VAL_8 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_9 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_10 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_11 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_12 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_13 connected to base node BASE and wallet daemon WALLET_D
#    Given a validator node VAL_14 connected to base node BASE and wallet daemon WALLET_D
#    Given validator VAL_8 nodes connect to all other validators
#    Given validator VAL_9 nodes connect to all other validators
#    Given validator VAL_10 nodes connect to all other validators
#    Given validator VAL_11 nodes connect to all other validators
#    Given validator VAL_12 nodes connect to all other validators
#    Given validator VAL_13 nodes connect to all other validators
#    Given validator VAL_14 nodes connect to all other validators
#
#    When validator node VAL_8 sends a registration transaction to base wallet WALLET
#    When validator node VAL_9 sends a registration transaction to base wallet WALLET
#    When validator node VAL_10 sends a registration transaction to base wallet WALLET
#    When validator node VAL_11 sends a registration transaction to base wallet WALLET
#    When validator node VAL_12 sends a registration transaction to base wallet WALLET
#    When validator node VAL_13 sends a registration transaction to base wallet WALLET
#    When validator node VAL_14 sends a registration transaction to base wallet WALLET
#
#    When miner MINER mines 10 new blocks
#
#    Then the validator node VAL_8 is listed as registered
#    Then the validator node VAL_9 is listed as registered
#    Then the validator node VAL_10 is listed as registered
#    Then the validator node VAL_11 is listed as registered
#    Then the validator node VAL_12 is listed as registered
#    Then the validator node VAL_13 is listed as registered
#    Then the validator node VAL_14 is listed as registered
#
#    Then the validator node VAL_1 switches to epoch 3
#    Then the validator node VAL_8 switches to epoch 3
