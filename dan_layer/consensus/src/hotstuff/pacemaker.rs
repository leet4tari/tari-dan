//  Copyright 2022 The Tari Project
//  SPDX-License-Identifier: BSD-3-Clause

use std::{
    cmp,
    time::{Duration, Instant},
};

use log::*;
use tari_dan_common_types::NodeHeight;
use tokio::sync::mpsc;

use crate::hotstuff::{
    current_view::CurrentView,
    on_beat::OnBeat,
    on_force_beat::OnForceBeat,
    on_leader_timeout::OnLeaderTimeout,
    pacemaker_handle::{PaceMakerHandle, PacemakerRequest},
    HotStuffError,
};

const LOG_TARGET: &str = "tari::dan::consensus::hotstuff::pacemaker";
const MAX_DELTA: Duration = Duration::from_secs(300);
/// Propose a block OFFSET_BLOCK_TIME sooner than the required block time
const OFFSET_BLOCK_TIME: Duration = Duration::from_secs(1);

pub struct PaceMaker {
    pace_maker_handle: PaceMakerHandle,
    handle_receiver: mpsc::Receiver<PacemakerRequest>,
    current_view: CurrentView,
    current_high_qc_height: NodeHeight,
    block_time: Duration,
}

impl PaceMaker {
    pub fn new(max_base_time: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let on_beat = OnBeat::new();
        let on_force_beat = OnForceBeat::new();
        let on_leader_timeout = OnLeaderTimeout::new();
        let current_view = CurrentView::new();

        Self {
            handle_receiver: receiver,
            pace_maker_handle: PaceMakerHandle::new(
                sender,
                on_beat,
                on_force_beat,
                on_leader_timeout,
                current_view.clone(),
            ),
            current_view,
            current_high_qc_height: NodeHeight(0),
            block_time: max_base_time,
        }
    }

    pub fn clone_handle(&self) -> PaceMakerHandle {
        self.pace_maker_handle.clone()
    }

    pub fn spawn(mut self) {
        let handle = self.clone_handle();
        let on_beat = handle.get_on_beat();
        let on_force_beat = handle.get_on_force_beat();
        let on_leader_timeout = handle.get_on_leader_timeout();

        tokio::spawn(async move {
            if let Err(e) = self.run(on_beat, on_force_beat, on_leader_timeout).await {
                error!(target: LOG_TARGET, "Error (run): {}", e);
            }
        });
    }

    pub async fn run(
        &mut self,
        on_beat: OnBeat,
        on_force_beat: OnForceBeat,
        on_leader_timeout: OnLeaderTimeout,
    ) -> Result<(), HotStuffError> {
        // Don't start the timer until we start the pacemaker
        let leader_timeout = tokio::time::sleep(Duration::MAX);
        let block_timer = tokio::time::sleep(Duration::MAX);
        tokio::pin!(leader_timeout);
        tokio::pin!(block_timer);

        let mut started = false;
        let mut leader_failure_suspended = false;
        let mut leader_failure_triggered_during_suspension = false;

        loop {
            tokio::select! {
                biased;
                maybe_req = self.handle_receiver.recv() => {
                    if let Some(req) = maybe_req {
                        match req {
                           PacemakerRequest::Reset { high_qc_height, reset_block_time } => {
                                if !started {
                                    continue;
                                }
                                leader_failure_suspended = false;
                                leader_failure_triggered_during_suspension = false;

                                if let Some(height) = high_qc_height {
                                    self.current_high_qc_height = height;
                                }
                                leader_timeout.as_mut().reset(self.leader_timeout());
                                if reset_block_time {
                                    block_timer.as_mut().reset(self.block_time());
                                    info!(target: LOG_TARGET, "🧿 Pacemaker Reset! Current height: {}, Delta: {:.2?}", self.current_view, self.delta_time());
                                } else {
                                    info!(target: LOG_TARGET, "🧿 Pacemaker Leader timeout Reset! Current height: {}, Delta: {:.2?}", self.current_view, self.delta_time());
                                }
                           },
                            PacemakerRequest::Start { high_qc_height } => {
                                info!(target: LOG_TARGET, "🚀 Starting pacemaker at leaf height {} and high QC: {}", self.current_view, high_qc_height);
                                leader_failure_suspended = false;
                                leader_failure_triggered_during_suspension = false;
                                if started {
                                    continue;
                                }
                                self.current_high_qc_height = high_qc_height;
                                info!(target: LOG_TARGET, "Reset! Current height: {}, Delta: {:.2?}", self.current_view, self.delta_time());
                                leader_timeout.as_mut().reset(self.leader_timeout());
                                block_timer.as_mut().reset(self.block_time());
                                on_beat.beat();
                                started = true;
                            }
                            PacemakerRequest::Stop => {
                                info!(target: LOG_TARGET, "💤 Stopping pacemaker");
                                started = false;
                                leader_failure_suspended = false;
                                leader_failure_triggered_during_suspension = false;
                                // TODO: we could use futures-rs Either
                                leader_timeout.as_mut().reset(far_future());
                                block_timer.as_mut().reset(far_future());
                            },
                           PacemakerRequest::SuspendLeaderFailure => {
                                if !started {
                                    continue;
                                }
                                leader_failure_suspended = true;
                                debug!(target: LOG_TARGET, "🧿 Pacemaker suspend");
                           },
                            PacemakerRequest::ResumeLeaderFailure => {
                                if !started {
                                    continue;
                                }
                                leader_failure_suspended = false;
                                if leader_failure_triggered_during_suspension {
                                    leader_failure_triggered_during_suspension = false;
                                    leader_timeout.as_mut().reset(self.leader_timeout());
                                    info!(target: LOG_TARGET, "⚠️ Resumed leader timeout! Current view: {}, Delta: {:.2?}", self.current_view, self.delta_time());
                                    on_leader_timeout.leader_timed_out(self.current_view.get_height());
                                }
                                debug!(target: LOG_TARGET, "🧿 Pacemaker resume");
                            }
                        }
                    } else{
                        info!(target: LOG_TARGET, "💤 All pacemaker handles dropped");
                        break;
                    }
                },
                () = &mut block_timer => {
                    block_timer.as_mut().reset(self.block_time());
                    on_force_beat.beat(None);
                }
                () = &mut leader_timeout => {
                    block_timer.as_mut().reset(self.block_time());
                    leader_timeout.as_mut().reset(self.leader_timeout());

                    if leader_failure_suspended {
                        info!(target: LOG_TARGET, "🧿 Leader timeout while suspended. Current view: {}", self.current_view);
                        leader_failure_triggered_during_suspension = true;
                    } else {
                        info!(target: LOG_TARGET, "⚠️ Leader timeout! Current view: {}, Delta: {:.2?}", self.current_view, self.delta_time());
                        on_leader_timeout.leader_timed_out(self.current_view.get_height());
                    }
                },

            }
        }

        Ok(())
    }

    fn block_time(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.block_time.saturating_sub(OFFSET_BLOCK_TIME)
    }

    /// Current leader timeout defined as block_time + delta
    /// This is always greater than the block time.
    /// Ensure that current_height and current_high_qc_height are set before calling this function.
    fn leader_timeout(&self) -> tokio::time::Instant {
        let delta = self.delta_time();
        // TODO: get real avg latency
        let avg_latency = Duration::from_secs(2);
        let offset = self.block_time + delta + avg_latency;
        tokio::time::Instant::now() + offset
    }

    /// Delta time is defined as 2^n where n is the difference in height between the last seen block height and the high
    /// QC height.
    fn delta_time(&self) -> Duration {
        let current_height = self.current_view.get_height();
        if current_height.is_zero() || self.current_high_qc_height.is_zero() {
            // Allow extra time for the first block
            return self.block_time * 2;
        }
        let exp = u32::try_from(cmp::min(
            u64::from(u32::MAX),
            cmp::max(1, current_height.saturating_sub(self.current_high_qc_height).as_u64()),
        ))
        .unwrap_or(u32::MAX);
        cmp::min(
            MAX_DELTA,
            2u64.checked_pow(exp).map(Duration::from_secs).unwrap_or(MAX_DELTA),
        )
    }
}

fn far_future() -> tokio::time::Instant {
    // Taken verbatim from the tokio library:
    // Roughly 30 years from now.
    // API does not provide a way to obtain max `Instant`
    // or convert specific date in the future to instant.
    // 1000 years overflows on macOS, 100 years overflows on FreeBSD.
    tokio::time::Instant::from_std(Instant::now() + Duration::from_secs(86400 * 365 * 30))
}
