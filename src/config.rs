use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct McUuid(pub String);

impl std::fmt::Display for McUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub hub_url: String,
    pub hub_api_key: String,
    #[serde(default)]
    pub slots: Vec<SlotConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SlotConfig {
    pub number: u8,
    pub account: String,
    pub auth: AuthMode,
    pub server: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub click_delay_ms: u64,
    #[serde(default)]
    pub whitelist: Vec<McUuid>,
    #[serde(default)]
    pub chambers: Vec<ChamberConfig>,
    #[serde(default)]
    pub dispense_block: Option<[i32; 3]>,
    #[serde(default = "default_dispense_max_retries")]
    pub dispense_max_retries: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Offline,
    Microsoft,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChamberConfig {
    pub player: McUuid,
    pub trapdoor: [i32; 3],
}

impl SlotConfig {
    pub fn is_whitelisted(&self, uuid: &McUuid) -> bool {
        self.whitelist.iter().any(|w| w.0.eq_ignore_ascii_case(&uuid.0))
    }

    pub fn find_trapdoor(&self, uuid: &McUuid) -> Option<[i32; 3]> {
        self.chambers
            .iter()
            .find(|c| c.player.0.eq_ignore_ascii_case(&uuid.0))
            .map(|c| c.trapdoor)
    }
}

fn default_port() -> u16 {
    25565
}

fn default_dispense_max_retries() -> u32 {
    3
}

pub fn load(path: &str) -> Result<Config> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading {path}"))?;
    toml::from_str(&text).with_context(|| format!("parsing {path}"))
}

pub fn write_example(path: &str) -> Result<()> {
    if Path::new(path).exists() {
        return Ok(());
    }
    let example = r#"hub_url = "ws://localhost:8001"
hub_api_key = "your_api_key_here"

[[slots]]
number = 1
account = "your_alt_account"
auth = "offline"   # or "microsoft"
server = "play.refinedvanilla.net"
port = 25565
# click_delay_ms = 0   # optional: ms to wait after pearl detected before clicking (default 0)
# UUIDs (not usernames) — copy from namemc.com or /data get entity @s UUID
whitelist = ["550e8400-e29b-41d4-a716-446655440000"]
# dispense_block = [1234, 65, -5678]   # optional: button/lever clicked right after a successful catch, to re-arm the dropper
# dispense_max_retries = 3   # optional: re-click the button this many times if no item entity confirms the dropper fired (default 3)

[[slots.chambers]]
player = "550e8400-e29b-41d4-a716-446655440000"
trapdoor = [1234, 64, -5678]
"#;
    fs::write(path, example).with_context(|| format!("writing {path}"))
}
