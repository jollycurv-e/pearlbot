
# pearlbot

**pearlbot** is a stasis chamber activation bot for Minecraft servers. A whitelisted player runs `!pearl <slot>` in chat via [ForestBot](https://github.com/jollycurv-e/ForestBot-RS) → the request routes through [Hub](https://github.com/jollycurv-e/Hub) → pearlbot logs in briefly with an alt account, opens the trapdoor, and disconnects once the pearl despawns. The result is whispered back to the player.

Based on Febzey's original pearl bot concept from Simply Vanilla.

Derived from [ShaysBot](https://github.com/ShayBox/ShaysBot) for initial code base.

## How it works

- Per-slot config: one alt account per slot, trapdoor coordinates hardcoded per player
- Whitelist and chamber matching use UUID (not username) to survive name changes
- Pearl detection watches for `AddEntity` (EnderPearl) packets within 5 blocks of the trapdoor; all nearby pearl IDs tracked so multi-pearl chambers work correctly
- Click fires on first tick a pearl is detected; optional `click_delay_ms` per slot for anticheat tuning
- 30s timeout; each request runs in an isolated thread with its own Tokio runtime

## Setup

Copy `example.pearlbot.toml` to `pearlbot.toml` and fill in your values:

```toml
hub_url = "ws://localhost:8001"
hub_api_key = "your_api_key_here"

[[slots]]
number = 1
account = "your_alt_account"
auth = "offline"   # or "microsoft"
server = "play.example.net"
port = 25565
# click_delay_ms = 0   # optional ms delay before clicking (default 0)
whitelist = ["uuid-of-player"]   # UUIDs only — not usernames

[[slots.chambers]]
player = "uuid-of-player"
trapdoor = [1234, 64, -5678]
```

Get UUIDs from [namemc.com](https://namemc.com) or `/data get entity @s UUID` in-game.

For Microsoft auth, the bot will open a browser prompt on first run per slot to authenticate. Subsequent logins use the cached token.

## Running

```sh
cargo run --release
```

Requires [Hub](https://github.com/jollycurv-e/Hub) running with the pearlbot WebSocket client registered (`client-type: pearlbot`).

## Hub routing

- `pearl_request` from any Minecraft client → forwarded to pearlbot
- `pearl_result` from pearlbot → broadcast back to all Minecraft clients → ForestBot whispers the result to the requester
