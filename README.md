# Herald

A tray app that watches your Azure DevOps activity (pull requests, branches, pipeline runs, releases, approvals) and tells you when something needs you: a small toast, a sound, and an inbox pane of unviewed records. Every event is also available to command hooks, a local WebSocket stream and the `herald` CLI, so you can automate on it.

- Windows is the verified platform. macOS and Linux code paths exist but are untested.
- The core (`herald-core`) is provider-neutral. Azure DevOps is the only provider built (`herald-provider-ado`); a connection block in the config picks the provider.
- Read-only against the provider (GET only). No webhooks or service hooks; Herald polls.

## Install

Build from source. Prerequisites: Rust (stable), MSVC Build Tools with the "Desktop development with C++" workload, Node and npm, and the WebView2 runtime (ships with Windows 11).

```text
cd app
npm install
npm run tauri build -- --config src-tauri/tauri.bundle.conf.json
```

The NSIS installer lands in `target/release/bundle/nsis/` (for example `Herald_0.1.0_x64-setup.exe`). It installs per user and puts:

- the tray app (`herald-app`), and
- the `herald` CLI next to it, with the install folder added to your user `PATH`.

The PATH edit is done with PowerShell, so a long existing PATH is not truncated by the installer. If it fails the installer says so; add the install folder yourself. Uninstall removes the entry. Open a new terminal to pick up the change.

The app identifier is `com.freesaladman.herald`.

Behaviour:

- Start at login is enabled on first launch. Toggle it from the tray menu ("Start at login").
- Single instance: launching Herald again just opens the pane.
- Closing the pane does not quit the app; use the tray menu "Quit".

Tray menu: Open, Snooze (30 min, 1 hour, until tomorrow 08:00), Resume, Mute sound, Start at login, Open config folder, Reload config, Poll now, Quit. The tray icon shows a dot when there are unviewed alerts and a warning variant when a connection needs credentials.

## First run

Herald keeps everything under `HERALD_HOME` (default `~/.config/herald` on every OS):

| Path | Contents |
| --- | --- |
| `config.toml` | configuration; written from a template on first run, reloaded when you save it |
| `herald.db` | SQLite event store and watermarks |
| `logs/` | `herald.<UTC date>.log`, daily, last 7 kept |
| `tmp/` | per-run hook event files; files older than a day are removed at app start |

`herald config path` prints the config file path. A bad edit keeps the last good config running and shows the error in the pane.

### 1. Add a connection

Herald watches nothing until a `[[connections]]` block exists (the pane shows a "no connections" banner).

```toml
[[connections]]
id = "work"                 # lowercase letters, digits, dashes; unique; keys secrets and watermarks
provider = "ado"
enabled = true
auth = { method = "pat" }   # "pat" (default) or "azcli"
bots = ["svc-build@example.com", "Build\\*", "*Build Service*"]
organization = "my-org"
projects = []               # empty = all projects in the organization
```

You can add several connections (for example two organizations).

### 2. Create a PAT

In Azure DevOps create a personal access token with these scopes only:

- Code (Read)
- Build (Read)
- Release (Read)
- Identity (Read)
- Project and Team (Read)
- Environment (Read & manage), only if you want YAML pipeline approvals. OPEN: this scope is unconfirmed; if approvals are missing, check the log for a 403 and the pane for a disabled source.

A 403 on one source disables just that source (shown in the pane) until the config reloads or the app restarts. Herald never writes to Azure DevOps.

### 3. Store the PAT

The PAT is kept in the OS keychain (service `herald`, user = connection id). It is never in the config, the environment, hook payloads, logs or web storage.

- Pane: Connections view, masked entry, then Test.
- CLI: `herald auth set --connection work` (prompts without echo), `herald auth test`, `herald auth clear`. `--connection` may be omitted when exactly one connection exists. `auth set` also tells a running app to pick up the new secret.

Alternative: `auth = { method = "azcli" }` uses `az account get-access-token` from your Azure CLI login; no PAT is stored.

A 401 marks the connection invalid, raises one `auth.failed` alert and pauses that connection until you update the credentials (or 15 minutes pass).

## What is tracked and what alerts

Tracked, with the role you have on a PR:

| Role | Meaning |
| --- | --- |
| author | PRs you created, including drafts |
| reviewer | you are an individual reviewer |
| groupReviewer | a group you belong to is a reviewer (light alerting) |
| commenter | active PRs where you have commented in a thread |
| mentioned | active PRs where you are @mentioned directly (comment or description) |

Precedence is author, reviewer, groupReviewer, commenter, mentioned. A groupReviewer becomes a reviewer once ADO adds you individually (it does when you vote). Group @mentions are not detected.

Also tracked: branches you created (any name), pipeline runs for your PRs, your branches or requested by you, classic releases created from those runs or by you, and approvals waiting on you (classic, including group approvals, and YAML pipeline approvals).

Alert rules:

- Your own actions are silent (they are still stored and sent to hooks and the stream).
- Bots (per-connection `bots` globs on unique or display name) are silent.
- Humans and system outcomes alert, minus `alerts.mute_types`.
- On groupReviewer PRs only `roles.group_reviewer.alert_types` alert.
- Direct @mentions emit `pr.mentioned` (not also a comment event).
- Catch-up: after downtime or sleep Herald queries since the last watermark (UTC server time, at most `max_lookback_days`). Catch-up events go to the pane only, with one `catchup.summary` toast and one sound. The first run seeds silently.
- Snooze suppresses toast and sound only; the pane, tray and hooks still update (hooks stop only with `respect_snooze`). Mute skips the sound only. On Windows, toast and sound are also suppressed while the system reports a non-notifiable state (full-screen app, presentation mode, busy, quiet time).

Event types and default severity (severity drives the card colour and toast duration):

| Type | Severity |
| --- | --- |
| `run.completed` failed, `run.stage.failed`, `deploy.failed`, `pr.vote.changed` -10, `pr.conflicts.detected`, `pr.policy.changed` to rejected/failed/broken/error, `auth.failed` | critical |
| `pr.vote.changed` -5, `pr.rereview.needed`, `approval.pending`, `pr.reviewer.added` for you or your group, `pr.mentioned`, `deploy.canceled`, `run.completed` canceled/partiallySucceeded, `source.disabled` | warn |
| `pr.vote.changed` 5 or 10, `pr.completed`, `deploy.succeeded`, `run.completed` succeeded, `pr.policy.changed` to approved/succeeded, `pr.conflicts.cleared`, `approval.completed` approved | success |
| everything else | info |
| `catchup.summary` | highest severity among the summarised events |

Other types: `pr.created`, `pr.published`, `pr.abandoned`, `pr.reactivated`, `pr.iteration.pushed`, `pr.reviewer.added`, `pr.reviewer.removed`, `pr.votes.reset`, `pr.comment.added`, `pr.comment.replied`, `pr.thread.status.changed`, `pr.autocomplete.set`, `pr.autocomplete.cancelled`, `branch.created`, `branch.pushed`, `branch.forcepushed`, `branch.deleted`, `release.created`, `deploy.started`, `approval.completed`, `record.activated` (fired when you click a pane record; never alerts).

Types starting `run.`, `release.`, `deploy.`, `approval.`, plus `pr.policy.changed`, `pr.conflicts.*`, `pr.rereview.needed`, `catchup.summary`, `auth.failed` and `source.disabled`, have actor kind `system`.

## Config reference

All keys are snake_case TOML. Every section is optional; the defaults are below (and in the template Herald writes).

| Key | Default | Meaning |
| --- | --- | --- |
| `polling.fast_secs` | 60 | pipelines, releases, approvals |
| `polling.pr_secs` | 120 | pull requests and their threads |
| `polling.slow_secs` | 600 | branches, discovery scan, identity and group refresh, local clone index |
| `polling.max_lookback_days` | 14 | how far back the first sync and catch-up look |
| `alerts.mute_types` | `[]` | event type globs that never toast or sound, e.g. `["pr.iteration.*"]` |
| `roles.group_reviewer.alert_types` | `["pr.created", "pr.published", "pr.mentioned"]` | the only types that alert on groupReviewer PRs |
| `sound.file` | `""` | sound file; empty uses the built-in chime; a relative path resolves against the config folder (`HERALD_HOME`) |
| `sound.volume` | 0.8 | 0.0 to 1.0 |
| `toast.duration_secs` | 6 | toast lifetime |
| `toast.critical_duration_secs` | 12 | lifetime for critical toasts |
| `toast.max_visible` | 4 | cards shown before "+N more" |
| `local.scan_roots` | `[]` | folders searched for local clones and worktrees |
| `local.scan_depth` | 2 | how deep below each root to look |
| `stream.port` | 0 | stream port; 0 picks a free one (see Stream) |

Changing `stream.port` applies on reload: the stream restarts on the new port with a new token, so clients must re-read `stream.json`. All keys apply on reload, including `[polling]` intervals, which take effect on running connections without a restart (and, for `slow_secs`, the local clone index rebuild).

### Per-source polling

Each source polls on its tier's interval unless `[polling.sources]` overrides it. Values are seconds, from 15 to 86400. The same bounds now apply to `fast_secs`, `pr_secs` and `slow_secs`, so an older config with, say, `fast_secs = 10` fails to load until it is raised. An unknown source name is a config error.

A source counts a poll as catch-up when its last success is more than three of its own intervals old, so a short override makes catch-up (events go to the summary instead of individual toasts) more likely after a short gap, and a long one less likely.

```toml
[polling.sources]
pipelines = 30
pr_threads = 60
branches = 1800
```

Azure DevOps sources:

| Source | Default tier (interval) | Event types |
| --- | --- | --- |
| `pr_lifecycle` | pr (120 s) | `pr.created`, `pr.completed`, `pr.abandoned`, `pr.reactivated`, `pr.conflicts.detected`, `pr.conflicts.cleared`, `pr.mentioned` |
| `pr_discovery` | slow (600 s) | `pr.mentioned` |
| `pr_threads` | pr (120 s) | `pr.comment.added`, `pr.comment.replied`, `pr.thread.status.changed`, `pr.mentioned` |
| `pr_reviews` | pr (120 s) | `pr.published`, `pr.vote.changed`, `pr.votes.reset`, `pr.reviewer.added`, `pr.reviewer.removed`, `pr.autocomplete.set`, `pr.autocomplete.cancelled`, `pr.iteration.pushed`, `pr.rereview.needed` |
| `pr_policies` | pr (120 s) | `pr.policy.changed` |
| `branches` | slow (600 s) | `branch.created`, `branch.pushed`, `branch.forcepushed`, `branch.deleted` |
| `pipelines` | fast (60 s) | `run.completed`, `run.stage.failed`, `approval.pending`, `approval.completed` |
| `releases` | fast (60 s) | `release.created`, `deploy.started`, `deploy.succeeded`, `deploy.failed`, `deploy.canceled`, `approval.pending`, `approval.completed` |

A `poll now` still polls every source once. The PR sources share a short-lived thread cache (20 s), so setting `pr_threads` and `pr_reviews` at or near the 15 s minimum may reuse one fetch for both.

`local.scan_roots` lets Herald map an event to your checkout: it reads each repo's git remotes (and worktrees) and fills `local.repoPath` (main clone) and `local.worktreePath` (a worktree on the PR source branch or the branch). The index rebuilds every `slow_secs`.

`[[connections]]` keys: `id`, `provider`, `enabled` (default true), `auth.method` (`pat` or `azcli` for ado), `bots` (globs), and provider keys: for ado `organization` (required) and `projects` (names; empty = all).

Globs (`mute_types`, `bots`, hook `on` and `match`) are case-insensitive, with `\` literal (so `Build\*` works the same on every OS).

## Hooks

A hook runs a command, or a built-in action, when an event matches. Hooks are hot-reloaded with the config.

```toml
# Built-in: open the event url (default, shipped in the template).
[[hooks]]
name = "open-link"
on = ["record.activated"]
action = "open-url"

# Command: argv list, no shell.
[[hooks]]
name = "notify-failed-build"
on = ["run.completed"]
match = { "run.result" = ["failed", "partiallySucceeded"] }
run = { default = ["my-notifier", "--title", "{title}", "--url", "{url}"] }
env = { MY_TOKEN_NAME = "build-alerts" }
timeout_secs = 30
debounce_secs = 10

# Per-OS commands: the OS key wins over default.
[[hooks]]
name = "log-pr-created"
on = ["pr.created"]
run = { windows = ["cmd", "/C", "echo %HERALD_PR_ID% >> C:\\temp\\prs.txt"], default = ["logger", "{pr.id}"] }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `name` | required | unique hook name |
| `on` | required | event type globs, e.g. `["pr.created", "run.*"]` |
| `action` | none | `"open-url"`; use either `action` or `run` |
| `run` | none | `{ default, windows, macos, linux }` argv lists |
| `match` | `{}` | extra conditions: dotted event path = glob, or a list meaning any of. All keys must match; a missing path never matches; an empty list never matches |
| `cwd` | none | working directory (placeholders allowed; see Hook safety) |
| `env` | `{}` | extra environment variables (placeholders allowed) |
| `catch_up` | false | also run for catch-up events |
| `include_test` | true | also run for test events (`herald emit-test`) |
| `respect_snooze` | false | skip while snoozed |
| `debounce_secs` | 0 | ignore repeats for the same hook and subject within this window |
| `detached` | false | start the process and do not wait, time out or kill it |
| `timeout_secs` | 60 | kill after this long (0 also means 60); not for detached |

Gates, in order: type and `match`; catch-up; test; snooze; debounce and re-entrancy (keyed by hook name and subject, so a hook cannot pile up on one PR; detached hooks skip the re-entrancy check). At most 4 non-detached hooks run at once. Hooks fire on catch-up events only with `catch_up = true`.

Each run records start, exit code, error and the last 4 KB of stderr in `herald.db` (`hook_runs`). On timeout, or when Herald shuts down, the whole process tree is killed (taskkill /T on Windows, the process group elsewhere). A normal exit does not kill grandchildren, so a launcher that spawns a long-lived process leaves it running until Herald exits on Windows; use `detached = true` for anything that should outlive Herald. Detached hooks are never killed by Herald. On Windows, non-detached hook children also run in a kill-on-close Job Object, so they die with Herald even if it crashes.

### Payload

- stdin: the event as JSON (camelCase; see the Event shape below).
- `HERALD_EVENT_FILE`: path to a file with the same JSON (under `tmp/`), handy for tools that cannot read stdin.
- Environment: `HERALD_EVENT_TYPE`, `HERALD_EVENT_ID`, `HERALD_EVENT_FILE`, `HERALD_SUBJECT`, `HERALD_CONNECTION`, `HERALD_PROVIDER`, and when present `HERALD_URL`, `HERALD_PR_ID`, `HERALD_REPO`, `HERALD_BRANCH` (short name), `HERALD_LOCAL_PATH` (worktree path, else repo path), plus the hook's own `env`.
- Placeholders: `{dotted.camelCase.path}` in `run` argv, `cwd` and `env` values expand from the event JSON. Numbers and bools become text. Use `{{` and `}}` for literal braces. A missing placeholder stops the run and records an error. Examples: `{title}`, `{url}`, `{pr.id}`, `{pr.sourceBranch}` (full ref, e.g. `refs/heads/feature/x`), `{branch.shortName}`, `{repo.name}`, `{run.result}`, `{actor.displayName}`, `{local.worktreePath}`, `{data.vote}`.

Event shape (abridged): `id`, `type`, `connection {id, provider}`, `occurredUtc`, `detectedUtc`, `inferred`, `catchUp`, `test`, `severity`, `actor {kind, id, displayName, uniqueName}`, `alert`, `title`, `subtitle`, `url`, `subjectKey`, and when relevant `repo`, `pr {id, title, sourceBranch, targetBranch, isDraft, role}`, `branch {name, shortName}`, `run`, `deployment`, `comment`, `approval`, `local {repoPath, worktreePath}`, `data`. Run `herald emit-test` and `herald tail --json` to see real payloads.

### Hook safety

Event text comes from other people (PR titles, branch names, comments). Treat every placeholder value as hostile input.

- Argv-only (no shell) is the safe default: Herald passes each argv element to the program as is, with no re-parsing.
- A run is in a shell context when any argv element is a known shell (this covers wrappers too: `env sh -c ...`, `sudo bash ...`). The shell list: `cmd`, `sh`, `bash`, `zsh`, `dash`, `ksh`, `mksh`, `ash`, `fish`, `busybox`, `pwsh`, `powershell`, `wsl`, `csh`, `tcsh`, `nu`, `ssh`, `su`, `runuser`. From the first shell on, a placeholder is allowed only as a positional parameter of an sh-family shell (`sh`, `bash`, `zsh`, `dash`, `ksh`, `mksh`, `ash`) run with `-c`: the `-c` flag (optionally clustered with value-less flags, like `-ce`) must be the element directly after the shell, and the next element, the command string, must not start with `-` or `+` (the shell would read it as an option). The elements after the command string become `$0`, `$1`, ... and are data, e.g. `["sh", "-c", "echo \"$1\"", "_", "{pr.id}"]`. The command string and everything before it may not contain a placeholder. Any other shape has no positionals, so every placeholder after the shell is refused and an error recorded: options before `-c` (`bash -e -c ...`), `-c` after a script name, all of `cmd` (any `/c`, `/k`, `/r`, glued `/c...`, and everything after it), all of `pwsh` and `powershell` (any flag, including `-File`, `-Command`, `-EncodedCommand`), `wsl`, `ssh`, `su`, `runuser`, `csh`, `tcsh`, `fish`, `nu`, `busybox`, and sh-family shells without `-c`.
- Command wrappers run their arguments as a command, and Herald does not parse their options, so no placeholder is allowed in any argument from the first wrapper up to the first shell (or to the end of the argv when there is no shell): `env`, `sudo`, `doas`, `timeout`, `nice`, `nohup`, `setsid`, `stdbuf`, `xargs`, `watch`, `flock`, `script`, `chroot`, `runas`, `start`, `time`, `exec`, `command`. The check is by name, so it also refuses harmless uses such as `sudo git checkout -- {branch.shortName}`. `["env", "FOO=1", "sh", "-c", "true", "_", "{pr.id}"]` is fine: the placeholder is after the shell, in a positional slot.
- A positional placeholder must still match a strict allowlist: letters, digits and `. _ / @ : + -`. Spaces and backslashes are not allowed, so a title with spaces or quotes is refused by design (use `{pr.id}` or `{branch.shortName}`).
- Pass everything else (titles, comments, anything with spaces) through environment variables, the event file or stdin: `"$HERALD_BRANCH"` (quoted) in sh, `$env:HERALD_BRANCH` in PowerShell, or read `HERALD_EVENT_FILE` / stdin. Hook `env` entries work the same way.
- The program (the first argv element of every `run` list) must be a literal: a placeholder there is a config error.
- Hook `env` entries named `BASH_ENV`, `ENV`, `PS4`, `PROMPT_COMMAND`, `SHELLOPTS`, `BASHOPTS` or starting with `BASH_FUNC_` (case-insensitive) are executed by shells, so a placeholder in their value is a config error.
- Values that fail the allowlist (anything with spaces, such as titles or comments) can only reach a shell through `HERALD_*` environment variables or the JSON at `HERALD_EVENT_FILE`. There is no `$env:HERALD_PR_TITLE`-style variable: the variables that exist are `HERALD_EVENT_TYPE`, `HERALD_EVENT_ID`, `HERALD_EVENT_FILE`, `HERALD_SUBJECT`, `HERALD_CONNECTION`, `HERALD_PROVIDER`, and when present `HERALD_URL`, `HERALD_PR_ID`, `HERALD_REPO`, `HERALD_BRANCH`, `HERALD_LOCAL_PATH`.
- cmd also expands `%NAME%` (and `!NAME!`) after parsing. If a cmd command line references a hook `env` entry or a `HERALD_*` variable by name, Herald checks that variable's value against the allowlist too, and refuses the run if it is not allowed. `.bat` and `.cmd` scripts must use `setlocal EnableDelayedExpansion` and `!VAR!`, and avoid `for /f` over untrusted values (it re-parses them).
- Never put placeholders inside interpreter code strings: `python -c`, `node -e`, `perl -e`, `ruby -e`, `osascript -e`, `mshta`, `rundll32`. These are code, not data; pass values as separate args or env vars and read them in the script.
- In plain argv (no shell), a value can still start with `-` and be read as an option (option injection), e.g. `git checkout {pr.sourceBranch}`. Put `--` before values: `["git", "checkout", "--", "{pr.sourceBranch}"]`, or use a program that takes explicit flags.
- `cwd` should come only from `local.*` placeholders (`{local.worktreePath}`, `{local.repoPath}`), which are paths from your own disk scan, not event text.
- Nested shells: the first shell decides where placeholders may go, and the allowlist of the strictest shell present applies (the sh family beats cmd and PowerShell).
- Not enforced, so a documented risk: interpreter code strings (`python -c`, `node -e`, ...), option injection in plain argv (use `--`), and other programs that take a command as an argument and are not in the wrapper list (e.g. `find -exec`, `wt`, `schtasks`).
- Console programs started with `detached = true` open their own console window. Launch GUI programs or a terminal emulator, or run non-detached with a timeout.
- Use the `open-url` action to open links; it parses the event url and opens only `http` and `https` (never `file:`, UNC or custom schemes). Events without a url are skipped quietly.
- Loop safety: debounce plus the per-subject re-entrancy guard stop a hook that triggers events from re-firing forever; keep `on` and `match` narrow anyway.

## Stream

Herald serves a WebSocket on `127.0.0.1` at `/events` while the app runs. Connection details are in `stream.json`:

- Windows: `%LOCALAPPDATA%\herald\stream.json`
- macOS and Linux: `$HERALD_HOME/stream.json` (default `~/.config/herald/stream.json`), mode 600

It contains `{ "port", "token", "pid" }`. The token is 32 random bytes in hex and changes on every app start; the file is removed on exit. Set `stream.port` for a fixed port.

Auth: send `Authorization: Bearer <token>`. Requests from a non-loopback peer, with a wrong or missing token, or carrying any `Origin` header (so browsers cannot connect) are refused.

Client to server (JSON text frames):

| Message | Reply |
| --- | --- |
| `{"type":"subscribe","types":["pr.*"],"match":{"pr.role":"author"},"includeCatchUp":true}` (all fields optional; no `types` = everything; `includeCatchUp` defaults to true) | `{"type":"ok"}`, then events |
| `{"type":"emit","event":{"type":"pr.created","title":"..."}}` publishes a test event (id `test:<uuid>`, `test = true`; connection `test`, actor kind `human` unless given); `type` is required | `{"type":"ok","id":"test:..."}` |
| `{"type":"credentials-changed","connectionId":"work"}` | `{"type":"ok"}` |

Server to client: `{"type":"event","event":{...}}`, `{"type":"ok"}`, `{"type":"error","message":"..."}` (for example `lagged N events` when a slow client misses events).

Limits: 16 authenticated clients, 64 KiB per message, 3 s to complete the handshake, keep-alive pings every 30 s.

## CLI

`herald` talks to the running app over the stream, and reads the store and config directly. Add `-v` for debug logging on stderr.

| Command | What it does |
| --- | --- |
| `herald tail [--type GLOB]... [--match path=glob]... [--no-catch-up] [--json]` | stream live events, one line each (`HH:MM:SS  type  title  url`, local time); `--json` prints raw event JSON, one per line. Repeating `--match` for one key means any-of. Exits 1 when the app is not running or stops; rerun to reconnect. Ctrl+C exits 0 |
| `herald emit-test <type> [--title T] [--pr-id N] [--repo R] [--branch B] [--url U] [--connection ID] [--set key=value]...` | publish a test event through the running app; prints its id. `--set` overrides any dotted path (JSON value or plain string) |
| `herald events --since <when> [--limit N] [--json]` | list stored events, oldest first. `<when>` is RFC3339, a date `YYYY-MM-DD` (UTC midnight) or a duration `30m`, `2h`, `7d`, `1w`. Default limit 200; no app needed |
| `herald auth set\|test\|clear [--connection ID]` | manage a connection's secret (see First run) |
| `herald config path` | print the config file path |

## Adding a provider

All provider-specific code lives outside `herald-core`.

1. Create a crate that depends on `herald-core` and implements `ProviderFactory` (kind, secret label, config validation, `connect`, `parse_remote` for git remotes), `Connection` (identity, server time, `sources()`, `credentials_changed`) and one or more `Source`s (a tier: Fast, Pr or Slow, and `poll` returning events). Follow the rules in `herald_core::provider`: use the connection's server time for windows and watermarks, set `catch_up` and `connection` on every event, leave severity, alert and `local` to the core, and use stable event ids that start with the connection id.
2. Register the factory where the registry is built, in the app (`app/src-tauri`) and the CLI (`crates/cli`): `ProviderRegistry::new(vec![Arc::new(MyFactory), ...])`.
3. Use `herald_core::testing::FakeProvider` as the model for tests.

A GitHub provider is a non-goal for now.

## Development

Layout:

- `crates/core` (`herald-core`): events, config, store, engine, scheduler, bus, hooks, stream. No provider dependency.
- `crates/provider-ado` (`herald-provider-ado`): Azure DevOps provider.
- `crates/cli` (`herald-cli`, binary `herald`).
- `app/`: React + TypeScript (Vite) frontend; `app/src-tauri` (`herald-app`): tray, toast and pane windows, alerts, sound.

Commands:

```text
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

cd app
npm run lint
npx vitest run
npm run build
```

Dev runs: point at a scratch home and skip autostart so you do not touch your real setup.

```text
HERALD_HOME=/path/to/scratch HERALD_NO_AUTOSTART=1 npm run tauri dev
```

Logs are in `$HERALD_HOME/logs/herald.<UTC date>.log`; set `RUST_LOG` for more app detail (the CLI uses `-v`). Secrets and tokens are never logged.

Known limitations:

- macOS and Linux are untested (code-complete behind `cfg` gates; no do-not-disturb detection there).
- The PAT scope for YAML pipeline approvals is unconfirmed (see First run).
- The exact property names ADO uses for a removed reviewer (`CodeReviewReviewersUpdatedRemovedIdentity`) and for reset-all-votes system threads are unconfirmed; those events may be missing or partial.
- Windows Job Object assignment happens just after a child starts, so a grandchild spawned in that instant can escape it.
- No updater; no history viewer.

## License

MIT. See [LICENSE](LICENSE).
