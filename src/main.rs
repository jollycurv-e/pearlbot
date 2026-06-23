mod config;
mod hub;
mod pearl;

use std::{collections::HashMap, sync::Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::Result;
use hub::{HubClient, HubEvent, PearlResult};
use tracing::{debug, error, info, warn};

const CONFIG_PATH: &str = "pearlbot.toml";

// Windows default stack is 1MB vs Linux's 8MB; Azalea needs more.
fn main() -> Result<()> {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(run)?
        .join()
        .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    config::write_example(CONFIG_PATH)?;
    let cfg = config::load(CONFIG_PATH)?;

    info!("Loaded {} slot(s)", cfg.slots.len());

    // Per-slot busy flag
    let busy: HashMap<u8, Arc<AtomicBool>> = cfg
        .slots
        .iter()
        .map(|s| (s.number, Arc::new(AtomicBool::new(false))))
        .collect();

    let slot_map: HashMap<u8, _> = cfg
        .slots
        .iter()
        .map(|s| (s.number, s.clone()))
        .collect();

    info!("Connecting to Hub at {}", cfg.hub_url);
    let hub = HubClient::connect(cfg.hub_url.clone(), cfg.hub_api_key.clone()).await?;
    let mut events = hub.subscribe();

    info!("Pearlbot running. Waiting for requests...");

    loop {
        let event = match events.recv().await {
            Ok(e) => e,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("Dropped {n} Hub events (lagged)");
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                error!("Hub event channel closed — reconnecting");
                // Re-subscribe after short wait; the WS task reconnects automatically
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                events = hub.subscribe();
                continue;
            }
        };

        match event {
            HubEvent::Open => info!("Hub WS connected"),
            HubEvent::Close(reason) => warn!("Hub WS: {reason}"),
            HubEvent::Error(e) => error!("Hub WS error: {e}"),
            HubEvent::Unknown(msg) => tracing::trace!("Hub unknown: {msg}"),
            HubEvent::PearlRequest(req) => {
                let req_start = Instant::now();
                info!("PearlRequest received: slot={} requester={} uuid={}",
                    req.slot, req.requester, req.requester_uuid);

                let Some(slot) = slot_map.get(&req.slot) else {
                    warn!("Slot {} not configured — rejecting", req.slot);
                    hub.send_pearl_result(PearlResult {
                        slot: req.slot,
                        success: false,
                        message: format!("Slot {} is not configured", req.slot),
                        requester: req.requester,
                    }).ok();
                    continue;
                };
                debug!("Slot {} found: server={} account={}", req.slot, slot.server, slot.account);

                if !slot.is_whitelisted(&req.requester_uuid) {
                    warn!("{} ({}) not whitelisted for slot {}", req.requester, req.requester_uuid, req.slot);
                    hub.send_pearl_result(PearlResult {
                        slot: req.slot,
                        success: false,
                        message: format!("{} is not whitelisted", req.requester),
                        requester: req.requester,
                    }).ok();
                    continue;
                }
                debug!("{} ({}) is whitelisted", req.requester, req.requester_uuid);

                let Some(trapdoor) = slot.find_trapdoor(&req.requester_uuid) else {
                    warn!("No chamber for {} ({}) on slot {}", req.requester, req.requester_uuid, req.slot);
                    hub.send_pearl_result(PearlResult {
                        slot: req.slot,
                        success: false,
                        message: format!("No chamber configured for {}", req.requester),
                        requester: req.requester,
                    }).ok();
                    continue;
                };
                debug!("Chamber found: trapdoor={:?}", trapdoor);

                let slot_busy = busy.get(&req.slot).expect("slot in busy map");
                if slot_busy.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_err() {
                    warn!("Slot {} busy — rejecting {}", req.slot, req.requester);
                    hub.send_pearl_result(PearlResult {
                        slot: req.slot,
                        success: false,
                        message: format!("Slot {} is busy, try again shortly", req.slot),
                        requester: req.requester,
                    }).ok();
                    continue;
                }
                info!("Slot {} acquired — spawning pearl task for {} elapsed={:.1}ms",
                    req.slot, req.requester, req_start.elapsed().as_secs_f32() * 1000.0);

                let slot_clone = slot.clone();
                let requester = req.requester.clone();
                let hub_clone = hub.clone();
                let busy_flag = slot_busy.clone();
                let slot_num = req.slot;

                tokio::spawn(async move {
                    let task_start = Instant::now();
                    debug!("Pearl task started for {requester} slot={slot_num}");

                    let success = pearl::run_pearl(&slot_clone, &requester, trapdoor).await;

                    let elapsed = task_start.elapsed();
                    info!("Pearl task done — slot={slot_num} success={success} requester={requester} elapsed={:.1}s",
                        elapsed.as_secs_f32());

                    busy_flag.store(false, Ordering::Relaxed);
                    debug!("Slot {slot_num} busy flag released");

                    let message = if success {
                        format!("Pearl pulled for {requester}")
                    } else {
                        format!("Pearl failed for {requester}")
                    };

                    if let Err(e) = hub_clone.send_pearl_result(PearlResult {
                        slot: slot_num,
                        success,
                        message,
                        requester: requester.clone(),
                    }) {
                        error!("Failed to send pearl_result for {requester}: {e}");
                    }
                });
            }
        }
    }
}
