// Copyright 2021. The Tari Project
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
// following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
// disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
// following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
// products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::{fs, panic, process};

use anyhow::Context;
use serde_json::json;
use tari_common::initialize_logging;
use tari_crypto::{keys::PublicKey, ristretto::RistrettoPublicKey};
use tari_dan_app_utilities::configuration::load_configuration;
use tari_dan_wallet_daemon::{
    cli::{Cli, Subcommand},
    config::ApplicationConfig,
    initialize_wallet_sdk,
    run_tari_dan_wallet_daemon,
};
use tari_dan_wallet_sdk::apis::key_manager;
use tari_shutdown::Shutdown;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Setup a panic hook which prints the default rust panic message but also exits the process. This makes a panic in
    // any thread "crash" the system instead of silently continuing.
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        process::exit(1);
    }));

    let cli = Cli::init();
    let config_path = cli.common.config_path();
    let cfg = load_configuration(config_path, true, &cli, cli.common.network)?;
    let mut config = ApplicationConfig::load_from(&cfg)?;

    if let Some(network) = cli.common.network {
        config.dan_wallet_daemon.network = network;
    }

    match cli.command {
        Some(Subcommand::Run) | None => run(cli, config).await?,
        Some(Subcommand::CreateKey {
            key_index,
            set_active,
            output_path,
        }) => {
            let sdk = initialize_wallet_sdk(&config)?;
            let km = sdk.key_manager_api();
            let secret = if let Some(index) = key_index {
                km.derive_key(key_manager::TRANSACTION_BRANCH, index)?
            } else {
                km.next_key(key_manager::TRANSACTION_BRANCH)?
            };

            if set_active {
                km.set_active_key(key_manager::TRANSACTION_BRANCH, secret.key_index)?;
            }

            let json = json!({
                "public_key": RistrettoPublicKey::from_secret_key(&secret.key),
                "key_index": secret.key_index,
            });
            match output_path {
                Some(path) => {
                    let mut file = fs::File::options()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .context("failed to open file for writing")?;
                    serde_json::to_writer_pretty(&mut file, &json).context("failed to encode key json to file")?;
                    println!("Key written to {}", path.display());
                },
                None => {
                    println!("{}", json);
                },
            }

            return Ok(());
        },
    }

    Ok(())
}

async fn run(cli: Cli, config: ApplicationConfig) -> Result<(), anyhow::Error> {
    // Remove the file if it was left behind by a previous run
    let _file = fs::remove_file(config.common.base_path.join("pid"));

    let shutdown = Shutdown::new();
    let shutdown_signal = shutdown.to_signal();

    if let Err(e) = initialize_logging(
        &cli.common.log_config_path("dan_wallet_daemon"),
        &cli.common.get_base_path(),
        include_str!("../log4rs_sample.yml"),
    ) {
        eprintln!("{}", e);
        return Err(e.into());
    }

    run_tari_dan_wallet_daemon(config, shutdown_signal).await
}
