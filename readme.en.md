# Simple Job Scheduler

**Русский:** [README.md](README.md)

HTTP task scheduler in Rust: interval / cron / one-time schedules, a step constructor (HTTP / JS / local command), SQLite storage, and a Vue + Tailwind web UI (local static assets, no build step, no Node.js).

## Features

- REST API and web UI: dashboard, job list, form with step constructor, execution log.
- Schedule types: interval (`5m`, `2h`, `1d`), cron (5 fields, UTC), one-time run.
- Job pipeline: ordered `http` / `transform` / `command` steps (add, remove, reorder).
- Local commands: program + args (no shell); stdout/stderr go to the journal.
- Retries on failure (configurable count and interval).
- Scheduler tick loop, concurrent run limit, manual “run now”.
- SQLite for jobs and execution history; automatic purge of old log rows.
- Server logs to stdout and rotating log files.
- UI and server message localization (`ru` / `en`).
- Web UI on Vue 3 and Tailwind: all assets live in `web/`; no required Node.js.
- Server-side job field validation (per step kind).

## Requirements

- Rust (stable) and Cargo

## Quick start

```bash
cp .env.example .env
cargo run
```

The server listens on `AJS_HOST:AJS_PORT`. `.env.example` defaults to `127.0.0.1:3000`; without `.env`, the code default port is `6378`.

Open in browser: [http://127.0.0.1:3000](http://127.0.0.1:3000) (or your port from `.env`).

## Build portable version

```bash
cargo build --release
```

### Stopping the server

**Ctrl+C** triggers a graceful shutdown:

1. The HTTP server stops accepting new connections and waits for in-flight requests.
2. The scheduler tick loop stops; no new scheduled or manual runs are started.
3. Active runs are **cancelled** (HTTP and background tasks aborted), locks drain for up to 5 seconds, then the SQLite pool is closed (5 s timeout).

Log lines confirm the signal and clean exit.

## Environment variables

Prefix **`AJS_`** (see `.env.example`):

| Variable                             | Purpose                                                                                                      | Default          |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ---------------- |
| `AJS_HOST`                           | HTTP bind address                                                                                            | `127.0.0.1`      |
| `AJS_PORT`                           | HTTP port                                                                                                    | `6378`           |
| `AJS_DB_PATH`                        | SQLite file path                                                                                             | `./scheduler.db` |
| `AJS_LOG_LEVEL`                      | Log level (`tracing`)                                                                                        | `info`           |
| `AJS_DEFAULT_LANGUAGE`               | Default server/UI language: `ru` or `en`                                                                     | `en`             |
| `AJS_MAX_CONCURRENT_JOBS`            | Max parallel job executions                                                                                  | `10`             |
| `AJS_HTTP_TIMEOUT_SECONDS`        | HTTP and local command timeout (seconds)                                                                     | `60`             |
| `AJS_JOB_TICK_INTERVAL_MS`        | Scheduler tick interval (ms)                                                                                 | `1000`           |
| `AJS_ENABLE_JS_TRANSFORM`         | Enable JS transform (`true`/`false`)                                                                         | `true`           |
| `AJS_RETENTION_DAYS`              | Execution log retention (days)                                                                               | `30`             |
| `AJS_LOG_DIR`                     | File log directory (relative to cwd or absolute)                                                             | `./logs`         |
| `AJS_RUN_OVERDUE_ON_STARTUP`      | Run overdue jobs right after startup (`true`) or reschedule `next_run_at` from now without running (`false`) | `true`           |
| `AJS_DISABLE_ALL_JOBS_ON_STARTUP` | Disable all jobs in the DB on startup (`enabled = 0`); enable manually in the UI                             | `false`          |

### Startup behavior

On launch: migrations → fill missing `next_run_at` → **startup policy** → scheduler tick loop.

- **`AJS_DISABLE_ALL_JOBS_ON_STARTUP=true`** — all jobs stay in the DB but are disabled until you enable them in the UI.
- **`AJS_RUN_OVERDUE_ON_STARTUP=false`** — enabled jobs with `next_run_at` in the past are **not** executed; `next_run_at` is recalculated from the current time (interval/cron get the next slot; past one-time runs clear `next_run_at`).
- **`AJS_RUN_OVERDUE_ON_STARTUP=true`** (default) — overdue enabled jobs run on the first tick (previous behavior).

Typical manual-control setup: `AJS_DISABLE_ALL_JOBS_ON_STARTUP=true`, optionally `AJS_RUN_OVERDUE_ON_STARTUP=false`.

## Logging

- **Server logs** (`tracing`):
  - stdout (with ANSI colors)
  - file `scheduler.log` under `AJS_LOG_DIR`
- **File rotation:**
  - max file size: **2 MB**
  - archived files: up to **10** (plus the active file)
  - naming: `scheduler.log`, `scheduler.1.log`, `scheduler.2.log`, …
- **Job execution history** is stored in SQLite (`execution_logs`), not in server log files.
  - Column **`response_preview`** holds the full final payload (no silent truncation).
  - Column **`steps_log`** is JSON with each step’s result (HTTP status / exit code, full output).
  - Hard cap per step output is **10 MB**; exceeding it fails the step instead of truncating.

## REST API

Base prefix: `/api`. Request bodies are JSON.

| Method   | Path                      | Description                                                       |
| -------- | ------------------------- | ----------------------------------------------------------------- |
| `GET`    | `/api/dashboard`          | Stats and recent runs                                             |
| `GET`    | `/api/settings`           | Public UI settings (`max_step_output_bytes`)                  |
| `GET`    | `/api/jobs`               | List jobs                                                         |
| `POST`   | `/api/jobs`               | Create job                                                        |
| `PUT`    | `/api/jobs/{id}`          | Update job                                                        |
| `DELETE` | `/api/jobs/{id}`          | Delete job                                                        |
| `POST`   | `/api/jobs/{id}/run`      | Run now (background)                                              |
| `POST`   | `/api/jobs/group-enabled` | Enable/disable all jobs in a group (`{ "job_group", "enabled" }`) |
| `GET`    | `/api/jobs/{id}/logs`     | Execution history (up to 100 rows)                                |

Static: `/` — web UI, `/i18n/{lang}.json` — UI strings.

## Web UI

The frontend is a single-page app with **no build step**: Axum serves the `web/` directory as static files.

| File                 | Purpose                           |
| -------------------- | --------------------------------- |
| `index.html`         | Markup and Vue application        |
| `vue.global.prod.js` | Vue 3 (production build)          |
| `tailwindcss.js`     | Tailwind CSS (runtime)            |
| `cron-formatter.js`  | Cron parsing and next-run preview |

Scripts are loaded locally from the site root (`/vue.global.prod.js`, etc.). The UI works with **only the server running** — the browser does not need internet access. Language switch: `ru` / `en` via JSON in `i18n/`.

On the **Jobs** page: optional **group** label (stored in the DB), filters by name, group, and created date, sorting; for a selected group — **Enable group** / **Disable group** shortcuts.

### Job pipeline

A job stores an ordered JSON **`steps`** array. A **`payload`** string is passed between steps (default `{}`).

Step kinds:

1. **`http`** — HTTP GET/POST/PUT/DELETE; body from `body`, from `payload` (`body_from_payload`), or with `{{payload}}` substitution.
2. **`transform`** — JS in boa sandbox: variable `input`, result via `return …` (can be disabled globally with `AJS_ENABLE_JS_TRANSFORM=false`).
3. **`command`** — local process without a shell: `program` + args (space-separated, e.g. `-t processor`); stdout/stderr go to logs and the journal; non-zero exit fails the step.

**`capture_output`** on a step controls whether its output (response body / stdout / JS result) becomes the `payload` for later steps.

On any step failure — retries of the whole pipeline (if enabled), then a log row and `next_run_at` recalculation. Legacy jobs with fetch/transform/send columns migrate to `steps` on startup.

### Validation on save

- **General:** non-empty name.
- **Schedule:** interval / cron / future one-time datetime.
- **http step:** `http(s)://` URL, GET/POST/PUT/DELETE, headers as JSON object.
- **transform step:** non-empty script.
- **command step:** non-empty program; args must not contain NUL.
- **Retry:** `max_retries ≥ 0`, `retry_interval_seconds ≥ 1`.

Errors return **400** with an `error` field (language follows `AJS_DEFAULT_LANGUAGE`).

## Examples

### Create a job (5-minute interval, one HTTP step)

```bash
curl -s -X POST http://127.0.0.1:3000/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Ping API",
    "enabled": true,
    "schedule_type": "interval",
    "schedule_value": "5m",
    "steps": [
      {
        "id": "1",
        "kind": "http",
        "name": "Fetch",
        "method": "GET",
        "url": "https://httpbin.org/get",
        "headers": "{}",
        "capture_output": true
      }
    ],
    "retry_enabled": true,
    "max_retries": 2,
    "retry_interval_seconds": 30
  }'
```

### Local command

```json
"steps": [
  {
    "id": "1",
    "kind": "command",
    "name": "CPU info",
    "program": "dmidecode",
    "args": ["-t", "processor"],
    "capture_output": true
  }
]
```

### Manual run

```bash
curl -s -X POST http://127.0.0.1:3000/api/jobs/{id}/run
```

### Cron (every 6 hours, UTC)

```json
"schedule_type": "cron",
"schedule_value": "0 */6 * * *"
```

## Project layout

```
src/
├── main.rs         — entry point, HTTP server
├── api.rs          — REST handlers
├── scheduler.rs    — tick loop, schedule calculation
├── jobs.rs         — job CRUD and SQLite execution log
├── execution.rs    — http / transform / command step loop
├── validation.rs   — JobInput and step validation
├── logging.rs      — rotating file logs
├── database.rs     — SQLite pool, migrations
├── models.rs       — domain types
├── config.rs       — AJS_* from environment
├── middleware.rs   — HTTP request logging
└── i18n.rs         — server message localization
web/                — Vue UI (local static)
  index.html
  vue.global.prod.js
  tailwindcss.js
  cron-formatter.js
i18n/               — ru.json, en.json
migrations/         — SQL schema
```

## Limitations

- JS transform: no Node.js APIs (network, filesystem, `require`); each run uses a fresh boa context.
- `command` steps run **without a shell** (no pipes/`&&`); args are separate tokens.
- Cron and one-time schedules use **UTC**.
- `next_run_at` comparison in SQLite is lexicographic on RFC3339 strings; keep datetime formats consistent.
- Cron expressions use 5 fields (minute, hour, day of month, month, day of week), classic Unix style.

## License

[MIT](LICENSE)
