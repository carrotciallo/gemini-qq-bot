# gemini-qq-bot

[简体中文](README.md) | **English**

A resident QQ group-chat bot: it batches group messages and hands them to
Gemini — through a long-lived [Google Antigravity CLI](https://antigravity.google/)
session — for replies, with pacing control, quota management, long-term
memory and a sticker library. The bot itself only implements a OneBot v11
client; the QQ protocol side is provided by an external OneBot
implementation.

> This project is not affiliated with Tencent or Google. The deployer bears
> all QQ account risk-control, ban and platform-terms risk.

The project is still rough around the edges; issues are welcome.

## Two builds

This repository builds two binaries that share the same library and
database schema:

- **Full build `gemini-qq-bot`**: the complete, production build — long-term
  memory, sticker library, quota pacing, cadence/relevance gates and the
  web admin console. This README mainly describes the full build.
- **Base build `gemini-qq-bot-lite`**: only the proven base of the QQ
  contact and interaction layers — batching, the delivery barrier, the
  submission token, the outbox and crash recovery share the full build's
  mechanisms, while memory, stickers, quota tiers and the admin surface are
  removed; configuration is environment variables only. If you want a
  simple group bot that can just stay up, or a base to grow your own
  features on, start with [docs/LITE.md](docs/LITE.md).

## Features

- **Batching and pacing**: group messages are batched first (silence window
  plus hard caps on message count / characters / images). When an auxiliary
  layer is configured, a model then judges whether "this round of talk has
  finished"; if not, the bot keeps waiting. Gate timeouts, errors and
  unparsable results always fail open — the queue is never blocked by the
  gate. The gate only decides *when* a batch is handed over; it has **no
  say in whether to reply**.
- **Precise triggering, quality over quantity**: only @mentions, replies
  quoting the bot, the owner's messages and the agreed call names count as
  "talking to her"; ambient participation must pass a relevance judgement
  (per-group tier, see below). The product principle is "rather stay silent
  than reply out of place": messages that are too old, replies whose quote
  anchor has expired, and replies the conversation has already moved past
  are dropped instead of sent.
- **Quota pacing**: dynamic tiers (T0–T3) computed from the remaining
  5-hour / weekly dual-bucket quota and the time to reset. When quota gets
  tight, per-group claim caps tighten early instead of burning through and
  going mute: T2 only admits addressed batches, T3 only the owner. A global
  hourly call fuse (default 60, with 15 reserved for @ batches) applies on
  top. Degradation can be disabled (tiers then display only, never
  intervene).
- **Dual-mode auxiliary layer**: chat-side cadence/relevance judgement,
  image tagging and flood summaries can use an OpenAI-compatible gateway
  (aux mode), falling back to a separate Agy auxiliary session when not
  configured. The auxiliary layer has no reply authority; memory folding,
  compaction, summaries and handovers always use a dedicated Agy
  `SummaryModel` and do not hot-switch with the chat-side aux.
- **Long-term memory**: independent memory per group. In the background,
  raw messages are folded every 10 minutes into memory entries with source
  markers (append-only, capped confidence); the read side summarizes fresh
  every turn. When active memory grows too long, a second compaction pass
  handles forgetting — if any compaction step fails, the whole round is
  left untouched; memory is never dropped by guessing.
- **Session rotation and handover**: one resident Agy session per group,
  rotated automatically every 35 turns by default; a new session opens
  with a "handover note" (recent topics and state summary). The source
  defaults are a 90-second hard cap per turn, no new insertions accepted
  after 45 seconds; the whole-turn work budget includes preparation and
  cold start, while post-timeout state settlement / process reaping is
  counted separately. At most 3 new messages may be inserted per turn;
  images wait for the next turn. Deployments may override the defaults;
  180/90 seconds is the operator's deployment setting, not the repository
  default.
- **Sticker library**: received stickers are deduplicated (pHash) and
  tagged into a cache; only after the model adopts one via `sticker_keep`
  does it enter the real library, searchable and sendable; adoption state
  is never rewritten by re-tagging. The pre-selection menu excludes the
  group's 20 most recent sends, then ranks by raw score divided by
  `1 + sends within the last 100`; the menu may be empty when everything
  is cooling down or scores are low.
- **Dual-path image generation**: after `generate_image` registers a job,
  an external web worker executes it, falling back to Agy at most once on
  failure; only the job's own target file is collected — ownership is never
  guessed from a shared session directory. Billing is atomic by Shanghai
  day boundary: 15/day/group autonomous, 5/day/person on request with a
  10-minute cooldown, owner exempt. Pausing or disabling a group cancels
  its undelivered image jobs; billing records are kept.
- **Flood relief**: when one group's backlog floods, old messages are
  summarized and merged before becoming a turn, so a backlog cannot burn
  through the quota; with little call budget left, the newest topics are
  answered first.
- **Web admin console**: a React console (overview / groups / persona card
  / stickers / settings). Newly joined groups are only registered, never
  spoken in, until an admin enables them in the console; aux configuration
  and runtime switches take effect live, without a restart.
- **Event webhooks**: turn completed / deferred / failed and quota tier
  changes can be POSTed to an external system (optional HMAC-SHA256
  signature), best-effort and never blocking the main flow.

## How it works

One message's journey (see the [current architecture entry](docs/ARCHITECTURE.md);
the second half of that page is a clearly marked historical snapshot):

1. OneBot events arrive over a forward WebSocket, pass two dedup layers
   (in-memory + persisted), are normalized and written to the batching
   queue; disabled or paused groups accumulate nothing.
2. Scheduling runs every 500 ms: it checks the global quota tier and the
   hourly budget, then the staleness guard (messages queued over 10
   minutes are consumed outright, preventing out-of-place replies), and
   finally whether the batch is ready to become a turn.
3. The batch goes to that group's resident Agy session (one per group,
   strictly serial; global concurrency is configurable). The model submits
   its reply through the MCP tool `submit_reply` — the only official
   output path: submissions are persisted as a buffer first, and only the
   last qualifying submission is confirmed at the end of the turn; a
   submission ACK does not mean the message was sent. An insertion revokes
   the old token first; only after acceptance is confirmed is a new token
   issued, extending the quote/requester scope. Strict, lenient and
   plain-text salvage paths share the same content screening; salvage is
   not a side channel for arbitrary drafts.
4. A freshness gate runs before sending: if generation took too long, or
   8+ new messages have appeared in the group meanwhile, the whole turn is
   dropped; quoted replies with expired anchors are dropped the same way.
   Messages whose send result is uncertain are marked `unknown` and never
   auto-resent (they can be handled manually in the console).
5. Background loops keep folding / compacting memory and preparing session
   handovers. When something goes wrong, start with the "production
   readiness" blockers and the audit stream on the console overview.

## Architecture at a glance

```
QQ groups ⇄ OneBot implementation (forward WebSocket) ⇄ this bot (Rust)
                                              ├─ Agy CLI resident sessions (one per group, reply generation)
                                              ├─ MCP tool surface (buffered replies / stickers / image-gen registration; not read-only)
                                              ├─ Chat auxiliary layer (cadence/relevance, image understanding, flood relief; aux or Agy)
                                              ├─ Dedicated Agy SummaryModel (memory folding/compaction/summaries/handovers)
                                              ├─ Image-gen executor → external web worker → one Agy fallback
                                              └─ SQLite (messages, memory, stickers, calls, outbox, image jobs, audit)
```

Main module responsibilities:

| Part | Responsibility |
|---|---|
| `src/onebot/` | OneBot v11 client: WS connection and reconnects, event normalization (the security boundary for untrusted input) |
| `src/pipeline/` | Batching, dedup, scheduling (per-group serial + global concurrency), outbound sending and persistence |
| `src/app/` | Inbound handling, turn execution, cadence gate, quota tiers, insertion policy, resident session management |
| `src/agent/` | Agy CLI adapter (PTY resident sessions, step-stream parsing), output contract validation and salvage |
| `src/memory/` | Memory recall / folding / compaction, handover notes, sticker library |
| `src/mcp/` | Model-side tools and permissions: buffered `submit_reply`, image-gen registration `generate_image`, sticker search/adoption |
| `src/admin/` + `web/` | Admin API (`/api/v1`) and the React console |
| `src/store/` | SQLite storage and migrations (all state in one database) |

## Quick start

### Prerequisites

1. A OneBot v11 implementation with a logged-in QQ account. Production is
   verified against [SnowLuma](https://github.com/SnowLuma/SnowLuma); any
   implementation offering a forward WebSocket (such as NapCat or
   Lagrange) should work, with the OneBot v11 standard as the
   compatibility baseline. SnowLuma is a third-party project; obtaining it
   and its license follow its official repository, and this repository
   distributes none of its files.
2. The Agy CLI, signed in to Google (the reply engine; it runs one
   resident session per group).
3. For a bare-metal deployment, a Rust toolchain (stable); or use Docker
   (below).

### Configuration

```bash
cp config.example.toml config.toml
```

The configuration path can be set with the `GEMINI_BOT_CONFIG` environment
variable. Required environment variables:

| Variable | Meaning |
|---|---|
| `ONEBOT_BOT_ID` | The bot's QQ number (digits only) |
| `ONEBOT_WS_URL` | The OneBot implementation's forward WebSocket address, e.g. `ws://127.0.0.1:3001` |
| `OWNER_QQ_ID` | The owner's QQ number (digits only) |
| `ADMIN_TOKEN` | Admin API token (at least 32 characters), or point `ADMIN_TOKEN_FILE` at a file |

The remaining optional variables (aux gateway, event webhooks, database
path, image source allowlist, mihomo egress proxy, …) are documented in the
header comments of `config.example.toml`; parameters inside `config.toml`
(batching window, concurrency, memory budget, fuses, …) carry example
values with per-item comments.

### Bare-metal run

```bash
cargo run --release
```

The admin API listens on `127.0.0.1:18081` by default. The Agy CLI is
looked up in `PATH` as `agy` by default (override with `AGY_BIN`); its
login state lives in `AGY_HOME`.

### Docker deployment

The root Dockerfile builds an image containing the Rust service, mihomo
and the Agy CLI. Note that the Agy CLI has no reproducible public
distribution channel: at build time you must place the agy binary at
`vendor/agy` and pass its sha256 as a build argument; without it the image
contains no CLI and the health check will honestly report "Agy contract
unverified" instead of pretending to be ready.

```bash
docker build --build-arg AGY_SHA256=<sha256 of vendor/agy> -t gemini-qq-bot .

docker run -d --name gemini-qq-bot \
  -v bot-data:/data \
  -v $PWD/config.toml:/etc/gemini-bot/config.toml:ro \
  -v $PWD/aux_api_key: <redacted>
  -e ONEBOT_BOT_ID=<bot QQ number> \
  -e ONEBOT_WS_URL=ws://host.docker.internal:3001 \
  -e OWNER_QQ_ID=<owner QQ number> \
  -e ADMIN_TOKEN=<admin token> \
  -p 127.0.0.1:8080:8080 \
  gemini-qq-bot
```

Inside the image the admin API listens on 8080; all data (the SQLite
database and Agy data) lives in the `/data` volume. For the first
deployment, the Agy CLI must complete its Google sign-in inside the
image's `AGY_HOME` (`/data/agy-home`): you can sign in on the host first
and mount the agy home directory at `/data/agy-home`. When no aux gateway
is used, the `aux_api_key` mount line can be omitted (the bot falls back
to agy auxiliary mode automatically); mihomo is only an optional egress
proxy component (controlled by `MIHOMO_ENABLED`) and is unnecessary when
the network can reach Google directly. The console's static files are not
baked into the image by default; to use the console under Docker, mount
`web/dist` into the container and set `WEB_DIST` (see the next section).

### Web admin console

```bash
cd web
npm install
npm run build        # output in web/dist/
```

Point the bot process's `WEB_DIST` at the build output and the console is
served at `http://<admin listen address>/console/`. Enter the `ADMIN_TOKEN`
you configured at deployment (the token is only kept in the tab's
sessionStorage; the page itself contains no data). The five pages:

- **Overview**: production readiness and blockers, dual-bucket quota and
  reset times, cadence tiers, the aux layer's current mode and health,
  outbox backlog.
- **Groups**: enable newly registered groups; per group — on/off, pause,
  participation tier, session reset, browsing and deleting memory.
- **Persona card**: edit the persona card and save; the next conversation
  turn wears the new card (the output contract is built in and not edited
  here); one click restores the built-in default card.
- **Stickers**: browse the sticker library, filter by adoption state,
  adopt / ignore / enable-disable / delete.
- **Settings**: aux gateway configuration and connectivity test, main
  session auto-rotation and quota degradation switches (each with its cost
  explained) — all live and persisted.

See `web/README.md` for more.

## Group participation tiers

Each group has one participation tier (`ambient_pace`), changed in the
console or via the admin API, effective immediately. Addressed batches
(@ / quote / owner / call name) are unaffected by the tier and always
processed normally:

| Tier | Behavior |
|---|---|
| `addressed_only` | Answers only: triggered solely by @mentions or quotes of the bot; turn context is a fixed range above the triggering message; no relevance judgement |
| `quiet` | Ambient talk passes the relevance judgement, with a 10-minute cooldown |
| `normal` | Default. Ambient talk may join after the relevance judgement |
| `lively` | No relevance judgement; participates actively (the early behavior) |

## Configuration reference (highlights)

Main `config.toml` parameters (the current fields are those in
`config.example.toml` and `src/config.rs`; switch tables from historical
architecture documents are not the current configuration list):

| Setting | Meaning |
|---|---|
| `quiet_window_seconds` / `max_batch_age_seconds` | Batching silence window / maximum wait for one batch |
| `max_batch_messages` / `max_batch_chars` / `max_batch_images` | Per-batch caps on messages / characters / images |
| `agent_concurrency` / `agent_timeout_seconds` | Global concurrent turns / per-turn timeout |
| `hourly_call_budget` | The global hourly call fuse |
| `memory_context_budget` / `compaction_daily_limit` | Character budget for memory injection / daily compaction cap |
| `conversation_turn_limit` / `session_rotation_enabled` | Turn limit and switch for automatic session rotation |
| `quota_degradation_enabled` | Quota degradation switch (off = tiers display only, never intervene in claims) |
| `respawn_backoff_enabled` | Resident session respawn backoff switch (off = immediate respawn after retirement, at the cost of hot-respawning on deterministic faults) |
| `flood_enabled` | Flood-relief summary switch |
| `reply_quote_enabled` | Whether replies carry a quote |
| `bot_call_names` | Calling these names also counts as addressed (default `["Gemini"]`) |
| `heavy_group_ids` | High-traffic groups whose weight doubles in quota forecasting |
| `no_submission_retry_groups` | Groups allowed one exclusive retry on a "no submission all turn" (default empty = off) |
| `resident_*` | Resident session silence period / soft cap / hard cap / per-turn insertion cap, etc. |

## Admin API and extension points

- Admin API: `/api/v1` is the public surface (OpenAPI description in
  `docs/openapi.yaml`), bearer-token authenticated (constant-time
  comparison; the token is never echoed); `/internal/v1` is the legacy
  prefix of the same handlers. Settings endpoints (aux configuration,
  rotation/degradation switches, persona card) all take effect live.
- Historical notes on the aux provider contract and webhooks are in
  `docs/EXTENDING.md`; the current MCP tool/permission boundary is in the
  [current architecture entry](docs/ARCHITECTURE.md), with `src/mcp/` as
  the source of truth for schemas — early "read-only / three tools"
  descriptions must not be treated as the complete permission list.
- Explicitly not doing: plugin loading, scripting engines, hot-plugging,
  inbound webhooks. New capabilities go through the existing contracts
  first.

## Platform support

- Linux: in production use; Docker or a bare-metal binary is recommended.
- Windows: adapted (CI builds for Windows), **not yet tested on real
  hardware** (supported, untested). Test reports are welcome.

  On Windows, run the bare-metal binary (Docker deployments should still
  use Linux). Two platform differences: quota capture uses the Unix PTY
  scraping implementation and is unavailable on Windows, so pacing runs
  the no-snapshot fallback path; session interrupt/kill has no Unix signal
  semantics and goes through the PTY child process's own termination.
  Please set path settings (`GEMINI_DB_PATH`, `AGY_PROJECTS_DIR`, …)
  explicitly via environment variables.

## Development and tests

```bash
cargo fmt --check
cargo test --offline     # dependencies come from the local cargo cache
cd web && npm test       # frontend smoke tests (vitest)
```

Test organization, the coverage map and known gaps are in
`docs/TESTING.md`.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the current architecture
  index, with the `8539f0a` historical snapshot attached (not a current
  configuration registry).
- `docs/EXTENDING.md`: extension points — the auxiliary-layer provider
  contract, the admin API, the MCP tool contract and event webhooks.
- [docs/TESTING.md](docs/TESTING.md): current test evidence / resource
  boundaries, with the historical coverage map attached.
- [docs/MIGRATION-COMPATIBILITY.md](docs/MIGRATION-COMPATIBILITY.md):
  data-bearing migration verification and rollback limits.
- [docs/DEPENDENCY-ADVISORIES.md](docs/DEPENDENCY-ADVISORIES.md): the
  lockfile dependency advisory audit and its known residual items.
- `docs/openapi.yaml`: the OpenAPI description of the admin API.

## License

AGPL-3.0-or-later; see `LICENSE`.
