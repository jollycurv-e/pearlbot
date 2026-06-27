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

fn dist_sq(pos: &Vec3, td: &BlockPos) -> f64 {
    (pos.x - f64::from(td.x)).powi(2)
        + (pos.y - f64::from(td.y)).powi(2)
        + (pos.z - f64::from(td.z)).powi(2)
}

fn blockpos_to_vec3(bp: &BlockPos) -> Vec3 {
    Vec3 { x: f64::from(bp.x), y: f64::from(bp.y), z: f64::from(bp.z) }
}

fn ticks_to_secs(ticks: u32) -> f32 { ticks as f32 / 20.0 }

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
                if tx.send(result).is_err() {
                    warn!("[pearlbot] signal_done: receiver dropped before send — result lost");
                }
            } else {
                debug!("[pearlbot] signal_done: already sent");
            }
        } else {
            error!("[pearlbot] signal_done: mutex poisoned");
        }
    }
}

fn log_heartbeat(bot: &Client, state: &PearlBotState, ticks: u32, nearby_count: usize) {
    let pos = bot.position();
    info!("[pearlbot] t={:.1}s tick={} pos=({:.2},{:.2},{:.2}) nearby_pearls={} clicked={} should_exit={}",
        ticks_to_secs(ticks), ticks, pos.x, pos.y, pos.z,
        nearby_count,
        state.clicked.load(Ordering::Relaxed),
        state.should_exit.load(Ordering::Relaxed));
}

fn send_trapdoor_click(bot: &Client, state: &PearlBotState, ticks: u32, nearby_count: usize) {
    let pos = bot.position();
    let rsq = dist_sq(&pos, &state.trapdoor);
    info!("[pearlbot] tick={} — sending UseItemOn trapdoor {:?} reach²={:.2} nearby_pearls={}",
        ticks, &state.trapdoor, rsq, nearby_count);
    if rsq > 25.0 {
        warn!("[pearlbot] reach²={:.2} exceeds 25.0 — server will likely reject click", rsq);
    }
    let td_center = blockpos_to_vec3(&state.trapdoor);
    bot.write_packet(ServerboundUseItemOn {
        hand: InteractionHand::MainHand,
        block_hit: BlockHit {
            block_pos: state.trapdoor,
            direction: Direction::Up,
            location: Vec3 { x: td_center.x + 0.5, y: td_center.y + 1.0, z: td_center.z + 0.5 },
            inside: false,
            world_border: false,
        },
        seq: 0,
    });
    debug!("[pearlbot] UseItemOn packet sent");
}

fn check_timeout(state: &PearlBotState, ticks: u32, nearby_count: usize) {
    if ticks >= TIMEOUT_TICKS && !state.should_exit.load(Ordering::Relaxed) {
        warn!("[pearlbot] Timeout at 30s — nearby_pearls={} clicked={} requester={} trapdoor={:?}",
            nearby_count, state.clicked.load(Ordering::Relaxed), state.requester, state.trapdoor);
        state.should_exit.store(true, Ordering::Relaxed);
    }
}

fn check_exit(bot: &Client, state: &PearlBotState, ticks: u32) {
    if state.should_exit.load(Ordering::Relaxed) {
        debug!("[pearlbot] Exiting at tick={} (~{:.1}s) success={}",
            ticks, ticks_to_secs(ticks), state.success.load(Ordering::Relaxed));
        state.signal_done();
        bot.exit();
    }
}

async fn handle(bot: Client, event: Event, state: PearlBotState) -> Result<()> {
    debug!("[pearlbot] handle invoked with event: {:?}", std::mem::discriminant(&event));

    match event {
        Event::Spawn => {
            let pos = bot.position();
            let rsq = dist_sq(&pos, &state.trapdoor);
            info!("[pearlbot] Spawned at ({:.2},{:.2},{:.2}) — trapdoor {:?} reach²={:.2} for {}",
                pos.x, pos.y, pos.z, &state.trapdoor, rsq, state.requester);
            if rsq > 25.0 {
                warn!("[pearlbot] Bot is {:.1} blocks from trapdoor — server may reject click (max ~5 blocks)",
                    rsq.sqrt());
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

                    let dist_sqr = dist_sq(&pkt.position, &state.trapdoor);
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
                                removed_nearby, ticks, ticks_to_secs(ticks), state.requester);
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
                log_heartbeat(&bot, &state, ticks, nearby_count);
            }

            let elapsed_ms = ticks as u64 * 50;
            if nearby_count > 0 && elapsed_ms >= state.click_delay_ms && !state.clicked.swap(true, Ordering::Relaxed) {
                info!("[pearlbot] attempting click at tick={} after {}ms (nearby_pearls={}) requester={}",
                    ticks, elapsed_ms, nearby_count, state.requester);
                send_trapdoor_click(&bot, &state, ticks, nearby_count);
            } else if nearby_count == 0 {
                debug!("[pearlbot] tick={} no nearby pearls yet; waiting requester={}", ticks, state.requester);
            } else {
                debug!("[pearlbot] tick={} click suppressed: nearby_pearls={} elapsed_ms={} click_delay_ms={} clicked={}",
                    ticks, nearby_count, elapsed_ms, state.click_delay_ms, state.clicked.load(Ordering::Relaxed));
            }

            check_timeout(&state, ticks, nearby_count);
            check_exit(&bot, &state, ticks);
        }

        Event::Disconnect(reason) => {
            let ticks = state.ticks.load(Ordering::Relaxed);
            let reason_str = reason
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            let success = state.success.load(Ordering::Relaxed);
            if success {
                info!("[pearlbot] Disconnected at tick={} (~{:.1}s): {reason_str}",
                    ticks, ticks_to_secs(ticks));
            } else {
                warn!("[pearlbot] Disconnected before completion at tick={} (~{:.1}s): {reason_str}",
                    ticks, ticks_to_secs(ticks));
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

async fn resolve_account(auth: &AuthMode, name: &str) -> Result<Account> {
    match auth {
        AuthMode::Offline => {
            debug!("[pearlbot] auth=offline account={}", name);
            Ok(Account::offline(name))
        }
        AuthMode::Microsoft => {
            let auth_start = Instant::now();
            info!("[pearlbot] auth=microsoft — acquiring token for {}", name);
            Ok(Account::microsoft(name).await
                .inspect(|_| info!("[pearlbot] MS auth OK in {:.1}s", auth_start.elapsed().as_secs_f32()))
                .inspect_err(|e| error!("[pearlbot] MS auth failed after {:.1}s: {e}", auth_start.elapsed().as_secs_f32()))?)
        }
    }
}

async fn run_mc_session(state: PearlBotState, auth: AuthMode, account_name: String, server: String, port: u16) {
    info!("[pearlbot] entering run_mc_session");
    info!("[pearlbot] resolving account account={} auth={:?} server={} port={}", account_name, auth, server, port);
    let Ok(account) = resolve_account(&auth, &account_name).await else {
        error!("[pearlbot] account resolution failed for account={} auth={:?}", account_name, auth);
        info!("[pearlbot] leaving run_mc_session (early return — account failure)");
        return;
    };
    let addr = format!("{server}:{port}");
    info!("[pearlbot] connecting to {addr} with account={} auth={:?}", account_name, auth);
    let connect_start = Instant::now();
    
    info!("[pearlbot] right before ClientBuilder::start()");
    let exit_reason = ClientBuilder::new()
        .set_handler(handle)
        .set_state(state)
        .start(account, addr.as_str())
        .await;
    info!("[pearlbot] right after ClientBuilder::start()");

    info!("[pearlbot] Azalea client exited after {:.1}s: {:?}",
        connect_start.elapsed().as_secs_f32(), exit_reason);
    info!("[pearlbot] leaving run_mc_session (normal exit)");
}

pub async fn run_pearl(slot: &SlotConfig, requester: &str, trapdoor: [i32; 3]) -> bool {
    let start = Instant::now();
    info!("[pearlbot] run_pearl start — requester={} trapdoor={:?} server={}:{} account={} auth={:?} click_delay_ms={}",
        requester, trapdoor, slot.server, slot.port, slot.account, slot.auth, slot.click_delay_ms);

    let (done_tx, done_rx) = oneshot::channel::<bool>();
    let trapdoor_pos = BlockPos::new(trapdoor[0], trapdoor[1], trapdoor[2]);
    let state = PearlBotState::new(trapdoor_pos, requester.to_owned(), done_tx, slot.click_delay_ms);

    let server = slot.server.clone();
    let port = slot.port;
    let account_name = slot.account.clone();
    let auth = slot.auth.clone();
    let state_clone = state.clone();

    debug!("[pearlbot] spawning Azalea thread for account={}", account_name);

    if let Err(e) = std::thread::Builder::new()
        .name("pearlbot-azalea".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            info!("[pearlbot] OS thread started");
            
            // Fix: Clone the state here so the closure takes this copy instead of the outer one
            let inner_state = state_clone.clone();
            
            let runtime_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime")
                    .block_on(async move {
                        info!("[pearlbot] tokio runtime entered");
                        
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(50),
                            run_mc_session(inner_state, auth, account_name, server, port),
                        ).await {
                            Ok(_) => info!("[pearlbot] run_mc_session completed naturally within 50s"),
                            Err(_) => warn!("[pearlbot] run_mc_session hit the 50s inner timeout limit!"),
                        }
                        
                        info!("[pearlbot] tokio runtime leaving block_on");
                    });
            }));

            if let Err(panic_err) = runtime_result {
                error!("[pearlbot] OS thread caught a silent panic: {:?}", panic_err);
            }

            // state_clone is now perfectly safe to use here!
            state_clone.signal_done();
            info!("[pearlbot] OS thread exiting");
        })
    {
        error!("[pearlbot] failed to spawn azalea thread: {e}");
        return false;
    }

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