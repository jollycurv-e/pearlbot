use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};

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
    pub whitelist: Vec<String>,
    #[serde(default)]
    pub chambers: Vec<ChamberConfig>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Offline,
    Microsoft,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChamberConfig {
    pub player: String,
    pub trapdoor: [i32; 3],
}

impl SlotConfig {
    pub fn is_whitelisted(&self, uuid: &str) -> bool {
        self.whitelist.iter().any(|w| w.eq_ignore_ascii_case(uuid))
    }

    pub fn find_trapdoor(&self, uuid: &str) -> Option<[i32; 3]> {
        self.chambers
            .iter()
            .find(|c| c.player.eq_ignore_ascii_case(uuid))
            .map(|c| c.trapdoor)
    }
}

fn default_port() -> u16 {
    25565
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
# UUIDs (not usernames) — copy from namemc.com or /data get entity @s UUID
whitelist = ["550e8400-e29b-41d4-a716-446655440000"]

[[slots.chambers]]
player = "550e8400-e29b-41d4-a716-446655440000"
trapdoor = [1234, 64, -5678]
"#;
    fs::write(path, example).with_context(|| format!("writing {path}"))
}
