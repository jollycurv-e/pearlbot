use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::Instant;

use anyhow::Result;
use azalea::{
    BlockPos, Vec3,
    core::direction::Direction,
    prelude::*,
    protocol::packets::game::{
        ClientboundGamePacket,
        ServerboundUseItemOn,
        s_use_item_on::BlockHit,
        s_interact::InteractionHand,
    },
    registry::builtin::EntityKind,
};
use tokio::sync::oneshot;
use tracing::{debug, info, warn, error};

use crate::config::{AuthMode, SlotConfig};

const TIMEOUT_TICKS: u32 = 600; // 30s at 20 TPS
const PEARL_PROXIMITY_SQR: f64 = 25.0; // 5 blocks

#[derive(Clone, Component, Default)]
pub struct PearlBotState {
    pub trapdoor: BlockPos,
    pub requester: String,
    pub done_tx: Arc<Mutex<Option<oneshot::Sender<bool>>>>,
    pub nearby_pearls: Arc<Mutex<HashSet<i32>>>,
    pub should_exit: Arc<AtomicBool>,
    pub success: Arc<AtomicBool>,
    pub ticks: Arc<AtomicU32>,
    pub clicked: Arc<AtomicBool>,
    pub click_delay_ms: u64,
}

impl PearlBotState {
    pub fn new(trapdoor: BlockPos, requester: String, done_tx: oneshot::Sender<bool>, click_delay_ms: u64) -> Self {
        Self {
            trapdoor,
            requester,
            done_tx: Arc::new(Mutex::new(Some(done_tx))),
            nearby_pearls: Arc::new(Mutex::new(HashSet::new())),
            should_exit: Arc::new(AtomicBool::new(false)),
            success: Arc::new(AtomicBool::new(false)),
            ticks: Arc::new(AtomicU32::new(0)),
            clicked: Arc::new(AtomicBool::new(false)),
            click_delay_ms,
        }
    }

    fn signal_done(&self) {
        debug!("[pearlbot] signal_done called (success={})", self.success.load(Ordering::Relaxed));
        if let Ok(mut guard) = self.done_tx.lock() {
            if let Some(tx) = guard.take() {
                let result = self.success.load(Ordering::Relaxed);
                debug!("[pearlbot] sending done signal: {result}");
                tx.send(result).ok();
            } else {
                debug!("[pearlbot] signal_done: already sent");
            }
        } else {
            error!("[pearlbot] signal_done: mutex poisoned");
        }
    }
}

async fn handle(bot: Client, event: Event, state: PearlBotState) -> Result<()> {
    match event {
        Event::Spawn => {
            let pos = bot.position();
            let td = &state.trapdoor;
            let reach_sqr = (pos.x - f64::from(td.x)).powi(2)
                + (pos.y - f64::from(td.y)).powi(2)
                + (pos.z - f64::from(td.z)).powi(2);
            info!("[pearlbot] Spawned at ({:.2},{:.2},{:.2}) — trapdoor {:?} reach²={:.2} for {}",
                pos.x, pos.y, pos.z, td, reach_sqr, state.requester);
            if reach_sqr > 25.0 {
                warn!("[pearlbot] Bot is {:.1} blocks from trapdoor — server may reject click (max ~5 blocks)",
                    reach_sqr.sqrt());
            }
        }

        Event::Packet(packet) => {
            match packet.as_ref() {
                ClientboundGamePacket::AddEntity(pkt) => {
                    debug!("[pearlbot] AddEntity: type={:?} id={} at ({:.1},{:.1},{:.1})",
                        pkt.entity_type, pkt.id.0, pkt.position.x, pkt.position.y, pkt.position.z);

                    if pkt.entity_type != EntityKind::EnderPearl {
                        return Ok(());
                    }

                    let td = Vec3 {
                        x: f64::from(state.trapdoor.x),
                        y: f64::from(state.trapdoor.y),
                        z: f64::from(state.trapdoor.z),
                    };
                    let dist_sqr = (pkt.position.x - td.x).powi(2)
                        + (pkt.position.y - td.y).powi(2)
                        + (pkt.position.z - td.z).powi(2);
                    info!("[pearlbot] EnderPearl id={} at ({:.2},{:.2},{:.2}) dist²={:.2} threshold={}",
                        pkt.id.0, pkt.position.x, pkt.position.y, pkt.position.z,
                        dist_sqr, PEARL_PROXIMITY_SQR);

                    if dist_sqr <= PEARL_PROXIMITY_SQR {
                        let mut set = state.nearby_pearls.lock().unwrap();
                        set.insert(pkt.id.0);
                        info!("[pearlbot] -> tracking pearl id={} (nearby_pearls={})", pkt.id.0, set.len());
                    } else {
                        warn!("[pearlbot] -> pearl too far ({:.1} blocks), not tracking", dist_sqr.sqrt());
                    }
                }

                ClientboundGamePacket::RemoveEntities(pkt) => {
                    let removed_ids: Vec<i32> = pkt.entity_ids.iter().map(|e| e.0).collect();
                    let mut set = state.nearby_pearls.lock().unwrap();
                    let removed_nearby: Vec<i32> = removed_ids.iter()
                        .filter(|id| set.contains(id))
                        .copied()
                        .collect();

                    debug!("[pearlbot] RemoveEntities: {:?} — tracking {:?}", removed_ids, set.iter().collect::<Vec<_>>());

                    for id in &removed_nearby {
                        set.remove(id);
                    }
                    drop(set);

                    if !removed_nearby.is_empty() {
                        if state.clicked.load(Ordering::Relaxed) {
                            let ticks = state.ticks.load(Ordering::Relaxed);
                            info!("[pearlbot] Pearl(s) {:?} despawned at tick={} (~{:.1}s) — success for {}",
                                removed_nearby, ticks, ticks as f32 / 20.0, state.requester);
                            state.success.store(true, Ordering::Relaxed);
                            state.should_exit.store(true, Ordering::Relaxed);
                        } else {
                            debug!("[pearlbot] Pearl(s) {:?} removed before click", removed_nearby);
                        }
                    }
                }

                ClientboundGamePacket::Login(pkt) => {
                    info!("[pearlbot] Login packet — entity_id={}", pkt.player_id);
                }

                ClientboundGamePacket::Disconnect(pkt) => {
                    warn!("[pearlbot] Server sent Disconnect packet: {:?}", pkt.reason);
                }

                _ => {
                    debug!("[pearlbot] packet: {:?}", std::mem::discriminant(packet.as_ref()));
                }
            }
        }

        Event::Tick => {
            let ticks = state.ticks.fetch_add(1, Ordering::Relaxed) + 1;

            let nearby_count = state.nearby_pearls.lock().unwrap().len();
            if ticks % 20 == 0 {
                let pos = bot.position();
                let elapsed_s = ticks as f32 / 20.0;
                info!("[pearlbot] t={:.1}s tick={} pos=({:.2},{:.2},{:.2}) nearby_pearls={} clicked={} should_exit={}",
                    elapsed_s, ticks, pos.x, pos.y, pos.z,
                    nearby_count,
                    state.clicked.load(Ordering::Relaxed),
                    state.should_exit.load(Ordering::Relaxed));
            }

            let elapsed_ms = ticks as u64 * 50;
            if nearby_count > 0 && elapsed_ms >= state.click_delay_ms && !state.clicked.swap(true, Ordering::Relaxed) {
                let pos = bot.position();
                let td = &state.trapdoor;
                let reach_sqr = (pos.x - f64::from(td.x)).powi(2)
                    + (pos.y - f64::from(td.y)).powi(2)
                    + (pos.z - f64::from(td.z)).powi(2);
                info!("[pearlbot] tick={} — sending UseItemOn trapdoor {:?} reach²={:.2} nearby_pearls={}",
                    ticks, td, reach_sqr, nearby_count);
                if reach_sqr > 25.0 {
                    warn!("[pearlbot] reach²={:.2} exceeds 25.0 — server will likely reject click", reach_sqr);
                }
                bot.write_packet(ServerboundUseItemOn {
                    hand: InteractionHand::MainHand,
                    block_hit: BlockHit {
                        block_pos: state.trapdoor,
                        direction: Direction::Up,
                        location: Vec3 {
                            x: f64::from(state.trapdoor.x) + 0.5,
                            y: f64::from(state.trapdoor.y) + 1.0,
                            z: f64::from(state.trapdoor.z) + 0.5,
                        },
                        inside: false,
                        world_border: false,
                    },
                    seq: 0,
                });
                debug!("[pearlbot] UseItemOn packet sent");
            }

            if ticks >= TIMEOUT_TICKS && !state.should_exit.load(Ordering::Relaxed) {
                warn!("[pearlbot] Timeout at 30s — nearby_pearls={} clicked={} for {}",
                    nearby_count, state.clicked.load(Ordering::Relaxed), state.requester);
                state.should_exit.store(true, Ordering::Relaxed);
            }

            if state.should_exit.load(Ordering::Relaxed) {
                let ticks = state.ticks.load(Ordering::Relaxed);
                debug!("[pearlbot] Exiting at tick={} (~{:.1}s) success={}",
                    ticks, ticks as f32 / 20.0, state.success.load(Ordering::Relaxed));
                state.signal_done();
                bot.exit();
            }
        }

        Event::Disconnect(reason) => {
            let ticks = state.ticks.load(Ordering::Relaxed);
            let reason_str = reason
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            let success = state.success.load(Ordering::Relaxed);
            if success {
                info!("[pearlbot] Disconnected at tick={} (~{:.1}s): {reason_str}",
                    ticks, ticks as f32 / 20.0);
            } else {
                warn!("[pearlbot] Disconnected before completion at tick={} (~{:.1}s): {reason_str}",
                    ticks, ticks as f32 / 20.0);
            }
            state.signal_done();
        }

        Event::Death(msg) => {
            warn!("[pearlbot] Bot died: {:?}", msg);
        }

        Event::Chat(msg) => {
            debug!("[pearlbot] Chat: {:?}", msg.message());
        }

        _ => {
            debug!("[pearlbot] unhandled event: {:?}", std::mem::discriminant(&event));
        }
    }
    Ok(())
}

pub async fn run_pearl(slot: &SlotConfig, requester: &str, trapdoor: [i32; 3]) -> bool {
    let start = Instant::now();
    info!("[pearlbot] run_pearl start — requester={} trapdoor={:?} server={}:{}",
        requester, trapdoor, slot.server, slot.port);

    let (done_tx, done_rx) = oneshot::channel::<bool>();
    let trapdoor_pos = BlockPos::new(trapdoor[0], trapdoor[1], trapdoor[2]);
    let state = PearlBotState::new(trapdoor_pos, requester.to_owned(), done_tx, slot.click_delay_ms);

    let server = slot.server.clone();
    let port = slot.port;
    let account_name = slot.account.clone();
    let auth = slot.auth.clone();
    let state_clone = state.clone();

    debug!("[pearlbot] spawning Azalea thread for account={}", account_name);

    std::thread::Builder::new()
        .name("pearlbot-azalea".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(async move {
                    let auth_start = Instant::now();
                    let account = match auth {
                        AuthMode::Offline => {
                            debug!("[pearlbot] auth=offline account={}", account_name);
                            Account::offline(&account_name)
                        }
                        AuthMode::Microsoft => {
                            info!("[pearlbot] auth=microsoft — acquiring token for {}", account_name);
                            match Account::microsoft(&account_name).await {
                                Ok(a) => {
                                    info!("[pearlbot] MS auth OK in {:.1}s", auth_start.elapsed().as_secs_f32());
                                    a
                                }
                                Err(e) => {
                                    error!("[pearlbot] MS auth failed after {:.1}s: {e}",
                                        auth_start.elapsed().as_secs_f32());
                                    state_clone.signal_done();
                                    return;
                                }
                            }
                        }
                    };

                    let addr = format!("{server}:{port}");
                    info!("[pearlbot] connecting to {addr}");
                    let connect_start = Instant::now();

                    ClientBuilder::new()
                        .set_handler(handle)
                        .set_state(state_clone)
                        .start(account, addr.as_str())
                        .await;

                    info!("[pearlbot] Azalea client exited after {:.1}s",
                        connect_start.elapsed().as_secs_f32());
                });
        })
        .expect("spawn azalea thread");

    debug!("[pearlbot] waiting for done signal (outer timeout 60s)");
    let result = match tokio::time::timeout(std::time::Duration::from_secs(60), done_rx).await {
        Ok(Ok(success)) => {
            info!("[pearlbot] run_pearl done — success={success} elapsed={:.1}s",
                start.elapsed().as_secs_f32());
            success
        }
        Ok(Err(_)) => {
            warn!("[pearlbot] done channel dropped — elapsed={:.1}s", start.elapsed().as_secs_f32());
            false
        }
        Err(_) => {
            warn!("[pearlbot] outer 60s timeout hit for {requester} — elapsed={:.1}s",
                start.elapsed().as_secs_f32());
            false
        }
    };
    result
}
