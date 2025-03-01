# Copyright 2022 The Tari Project
# SPDX-License-Identifier: BSD-3-Clause

@concurrent
@wallet_daemon
Feature: Wallet Daemon

  Scenario: Create account and transfer faucets via wallet daemon
    # Initialize a base node, wallet, miner and VN
    Given a base node BASE
    Given a wallet WALLET connected to base node BASE
    Given a miner MINER connected to base node BASE and wallet WALLET

        # Initialize a VN
    Given a validator node VAL_1 connected to base node BASE and wallet daemon WALLET_D

        # The wallet must have some funds before the VN sends transactions
    When miner MINER mines 4 new blocks
    When wallet WALLET has at least 5000 T

        # VN registration
    When validator node VAL_1 sends a registration transaction to base wallet WALLET
    When miner MINER mines 26 new blocks
    Then VAL_1 has scanned to height 27
    Then the validator node VAL_1 is listed as registered

        # Initialize an indexer
    Given an indexer IDX connected to base node BASE

        # Initialize the wallet daemon
    Given a wallet daemon WALLET_D connected to indexer IDX

    # Publish the "fauset" template
    When I create an account ACC via the wallet daemon WALLET_D with 2000000 free coins
    When wallet daemon WALLET_D publishes the template "faucet" using account ACC

        # Create two accounts to test sending the tokens
    When I create an account ACC_1 via the wallet daemon WALLET_D with 10000 free coins
    When I create an account ACC_2 via the wallet daemon WALLET_D
        # TODO: remove the wait
    When I wait 3 seconds
    When I check the balance of ACC_2 on wallet daemon WALLET_D the amount is exactly 0

        # Create a new Faucet component
    When I call function "mint" on template "faucet" using account ACC_1 to pay fees via wallet daemon WALLET_D with args "10000" named "FAUCET"

        # Submit a transaction manifest
    When I print the cucumber world
        # TODO: remove the wait
    When I wait 5 seconds
    When I submit a transaction manifest via wallet daemon WALLET_D with inputs "FAUCET, ACC_1" named "TX1"
  ```
  let faucet = global!["FAUCET/components/TestFaucet"];
  let mut acc1 = global!["ACC_1/components/Account"];

  // get tokens from the faucet
  let faucet_bucket = faucet.take_free_coins();
  acc1.deposit(faucet_bucket);
  ```
    When I print the cucumber world

        # Submit a transaction manifest
    When I submit a transaction manifest via wallet daemon WALLET_D signed by the key of ACC_1 with inputs "FAUCET, TX1, ACC_2" named "TX2"
  ```
  let mut acc1 = global!["TX1/components/Account"];
  let mut acc2 = global!["ACC_2/components/Account"];
  let faucet_resource = global!["FAUCET/resources/0"];

  // Withdraw 50 of the tokens and send them to acc2
  let tokens = acc1.withdraw(faucet_resource, Amount(50));
  acc2.deposit(tokens);
  acc2.balance(faucet_resource);
  acc1.balance(faucet_resource);
  ```
        # TODO: remove the wait
    When I wait 5 seconds
        # Check balances
        # `take_free_coins` deposits 10000 faucet tokens, allow up to 2000 in fees
    When I check the balance of ACC_1 on wallet daemon WALLET_D the amount is at least 8000
    # TODO: Figure out why this is taking more than 10 seconds to update
    #        When I wait for ACC_2 on wallet daemon WALLET_D to have balance eq 50

  Scenario: Claim and transfer confidential assets via wallet daemon
        # Initialize a base node, wallet, miner and VN
    Given a base node BASE
    Given a wallet WALLET connected to base node BASE
    Given a miner MINER connected to base node BASE and wallet WALLET

        # Initialize a VN
    Given a validator node VN connected to base node BASE and wallet daemon WALLET_D
    When miner MINER mines 4 new blocks
    When wallet WALLET has at least 5000 T
    When validator node VN sends a registration transaction to base wallet WALLET
    When miner MINER mines 26 new blocks
    Then the validator node VN is listed as registered

        # Initialize an indexer
    Given an indexer IDX connected to base node BASE

        # Initialize the wallet daemon
    Given a wallet daemon WALLET_D connected to indexer IDX

        # When I create a component SECOND_LAYER_TARI of template "fees" on VN using "new"
    When I create an account ACCOUNT_1 via the wallet daemon WALLET_D with 10000 free coins
    When I create an account ACCOUNT_2 via the wallet daemon WALLET_D

    When I burn 1000T on wallet WALLET with wallet daemon WALLET_D into commitment COMMITMENT with proof PROOF for ACCOUNT_1, range proof RANGEPROOF and claim public key CLAIM_PUBKEY

        # unfortunately have to wait for this to get into the mempool....
    Then there is 1 transaction in the mempool of BASE within 10 seconds
    When miner MINER mines 13 new blocks
    Then VN has scanned to height 40

    When I convert commitment COMMITMENT into COMM_ADDRESS address
    Then validator node VN has state at COMM_ADDRESS within 20 seconds

    When I claim burn COMMITMENT with PROOF, RANGEPROOF and CLAIM_PUBKEY and spend it into account ACCOUNT_1 via the wallet daemon WALLET_D
    When I print the cucumber world
        # TODO: remove the wait
    When I wait 5 seconds
    When I check the confidential balance of ACCOUNT_1 on wallet daemon WALLET_D the amount is at least 10000
        # When account ACCOUNT_1 reveals 100 burned tokens via wallet daemon WALLET_D
    Then I make a confidential transfer with amount 5 from ACCOUNT_1 to ACCOUNT_2 creating output OUTPUT_TX1 via the wallet_daemon WALLET_D

  Scenario: Create and mint account NFT
    # Initialize a base node, wallet, miner and VN
    Given a network with registered validator VAL_1 and wallet daemon WALLET_D

    # Initialize an indexer
    Given an indexer IDX connected to base node NETWORK_BASE

    # Initialize the wallet daemon
    Given a wallet daemon WALLET_D connected to indexer IDX

    # Create two accounts to test sending the tokens
    When I create an account ACC via the wallet daemon WALLET_D with 10000 free coins

    When I print the cucumber world

    # Mint a new account NFT
    When I mint a new non fungible token NFT on ACC using wallet daemon WALLET_D
