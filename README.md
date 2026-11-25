# Herald

A tray app that watches your Azure DevOps activity (pull requests, branches, pipeline runs, releases, approvals) and tells you when something needs you: a small toast, a sound, and an inbox pane of unviewed records. Every event is also available to command hooks, a local WebSocket stream and the `herald` CLI, so you can automate on it.

- Windows is the verified platform. macOS builds are published but not yet verified on a Mac; Linux is untested and not packaged.
- The core (`herald-core`) is provider-neutral. Azure DevOps is the only provider built (`herald-provider-ado`); a connection block in the config picks the provider.
- Read-only against the provider (GET only). No webhooks or service hooks; Herald polls.

## Install

Installers are published on the [releases page](https://github.com/freesaladman/Herald/releases). They are not code-signed yet.

### Windows

In PowerShell:

```powershell
irm https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.ps1 | iex
```

The script downloads the latest installer, checks its SHA-256, installs silently and starts Herald. Run it again to update. Set `$env:HERALD_VERSION = "v0.1.0"` first to pick a release.

Or download `Herald_<version>_x64-setup.exe` from the releases page and run it. Because it is unsigned, SmartScreen may say "Windows protected your PC": choose **More info**, then **Run anyway**.

The installer is per user (no admin) and puts:

- the tray app (`herald-app`), and
- the `herald` CLI next to it, with the install folder added to your user `PATH`.

The PATH edit is done with PowerShell, so a long existing PATH is not truncated by the installer. If it fails the installer says so; add the install folder yourself. Uninstall (Settings > Apps) removes the entry. Open a new terminal to pick up the change.

### macOS (not verified)

The macOS builds are produced by CI but have not been tested on a Mac yet. Reports welcome.

In Terminal:

```sh
curl -fsSL https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.sh | sh
```

The script picks the Apple Silicon or Intel build, checks its SHA-256, installs `Herald.app` into `/Applications` (or `~/Applications`), links the `herald` CLI into `~/.local/bin` and starts Herald. Run it again to update. `HERALD_VERSION`, `HERALD_APP_DIR` and `HERALD_BIN_DIR` override the release and locations.

Or download the `.dmg` for your Mac (`aarch64` for Apple Silicon, `x64` for Intel). The app is ad-hoc signed but not notarized, so macOS blocks a downloaded copy on first open: go to **System Settings > Privacy & Security** and choose **Open Anyway**, or run `xattr -dr com.apple.quarantine /Applications/Herald.app`. The CLI is at `Herald.app/Contents/MacOS/herald`.

To uninstall, quit Herald from its menu and delete `Herald.app` and the `~/.local/bin/herald` link.

### Build from source

Prerequisites: Rust (stable), Node and npm. On Windows also MSVC Build Tools with the "Desktop development with C++" workload and the WebView2 runtime (ships with Windows 11); on macOS the Xcode command line tools.

```text
cd app
npm install
npm run tauri build -- --config src-tauri/tauri.bundle.conf.json          # Windows: NSIS installer
npm run tauri build -- --config src-tauri/tauri.bundle.macos.conf.json    # macOS: .app and .dmg
```

Output lands under `target/release/bundle/` (`nsis/`, or `macos/` and `dmg/`).

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

A 401 marks the connection invalid, raises one `auth.failed` alert and pauses that connection until you update the credentials (or 15 minutes pass). Missing credentials (no PAT stored, or an Azure CLI login that has lapsed so `az` asks you to run `az login`) raise one `auth.failed` alert with `data.reason = "missing"` per lapse and idle the connection; after signing in again choose **Poll now** from the tray.

## What is tracked and what alerts

Tracked, with the role you have on a PR:

| Role | Meaning |
| --- | --- |
| author | PRs you created, including drafts |
| reviewer | you are an individual reviewer |
| groupReviewer | a group you belong to is a reviewer |
| commenter | active PRs where you have commented in a thread |
| mentioned | active PRs where you are @mentioned directly (comment or description) |

Precedence is author, reviewer, groupReviewer, commenter, mentioned. A groupReviewer becomes a reviewer once ADO adds you individually (it does when you vote). Group @mentions are not detected.

Also tracked: branches you created (any name), pipeline runs for your PRs, your branches or requested by you, classic releases created from those runs or by you, and approvals waiting on you (classic, including group approvals, and YAML pipeline approvals).

Notification levels decide what an event does. Each event gets one level when Herald detects it:

| Level | Effect |
| --- | --- |
| `alert` | toast, sound, pane, tray count, hooks and stream (the default) |
| `pane` | pane, hooks and stream; no toast or sound, and it does not bump the tray or unviewed count |
| `ignore` | stored in `herald.db` only (see `herald events`); no pane row, hook or stream delivery |

Levels are set per category, the event type prefix: `pr`, `branch`, `run`, `release`, `deploy` and `approval`. The system types `auth.failed`, `source.disabled`, `catchup.summary` and `record.activated` are not configurable.

```toml
[notify]
pr = "pane"           # PR events show in the pane without a toast
run = "ignore"        # pipeline runs are only stored

[notify.group]
approval = "pane"     # approvals that reached me through a group
pr = "ignore"

[[connections]]
id = "oss"
# ...
notify = { pr = "alert", group = { approval = "ignore" } }
```

- `[notify.group]` applies to events with `viaGroup = true`, set by the provider when an event reached you only through a group. For Azure DevOps that is group-reviewer PR events not directly about you (`pr.created`, `pr.published`, `pr.rereview.needed`, and `pr.reviewer.added`/`removed` for your group) and approvals assigned to a group you are in. Mentions of you and replies on your threads are direct, and so are pushes on group-reviewer PRs you commented on (`viaGroup` is false), so `[notify.group]` does not apply to them.
- Approvals already pending when you upgrade are treated as direct (no `viaGroup`) when they complete.
- A connection can override with `notify = { ... }` (same keys, including a `group` table).
- Lookup order, first match wins: connection group, connection, global group, global, then `alert`. The group steps apply only to `viaGroup` events. With the example above, on `oss` a group approval is `ignore` (connection group), a group `pr` event is `alert` (the connection's `pr` beats the global group's `pr`) and a direct `run` event is `ignore` (global). On other connections a group `pr` event is `ignore` (global group). A category unset at every step is `alert`.

Level rules (`[[notify.rules]]`) set the level of events by type and content, before the lookup above:

```toml
[notify]
pr = "ignore"
run = "ignore"

[[notify.rules]]
on = ["pr.reviewer.added"]
match = { "data.isMe" = "true", viaGroup = "false" }   # added to a PR as me, not via a group
level = "alert"

[[notify.rules]]
on = ["pr.comment.replied"]
match = { "actor.kind" = ["human", "bot"] }
level = "alert"

[[notify.rules]]
on = ["run.completed"]
match = { "run.result" = "failed", "pr.id" = "*" }     # failed runs that belong to a PR
level = "alert"

[[notify.rules]]
on = ["pr.policy.changed"]
match = { "data.state" = ["rejected", "failed", "broken", "error"], "data.previousState" = ["approved", "succeeded", "queued", "running", "notSet"] }
level = "alert"

# pushes on PRs I reviewed and commented on; pane still runs hooks
[[notify.rules]]
on = ["pr.iteration.pushed"]
match = { "data.iCommented" = "true", "pr.role" = ["reviewer", "groupReviewer"] }
level = "pane"
```

- Rules are checked in file order. The first match sets the level and overrides the connection, group and global lookup. An event no rule matches uses that lookup unchanged.
- `on` is a list of event type globs (required). `level` is `alert`, `pane` or `ignore` (required). `match` is optional and works as in [Hooks](#hooks): dotted camelCase event JSON paths, glob values, case-insensitive, a list means any of, bools and numbers are compared as text, and a missing or null path never matches.
- `actor.kind` is the classified actor: `me`, `human`, `bot` or `system`. The [Event reference](#event-reference) lists every path, `data` key and value set you can match on; [Testing a rule](#testing-a-rule) shows how to try a rule with `herald emit-test`.
- The system types `auth.failed`, `source.disabled`, `catchup.summary` and `record.activated` ignore rules. With every category set to `ignore`, the first three still alert.
- The own and bot cap applies after a rule: a bot reply matched as `alert` lands in the pane, and your own actions never toast or show in the pane. Hooks run for `alert` and `pane`, not `ignore`.
- Rules are global. For one connection use `match = { "connection.id" = "..." }`; `notify.rules` under a connection is a config error. An unknown key in a rule is a parse error.
- A policy whose first observed state is already a failure has no `previousState`, so a rule on `data.previousState` does not match it.

Other rules:

- Your own actions never toast or show in the pane (they are still stored and sent to hooks and the stream).
- Bots (per-connection `bots` globs on unique or display name) never toast, but their events show in the pane. The category level can lower them further.
- Levels are stamped when an event is detected and stored with it. Changing the config is not retroactive: the pane, tray count and stream replay follow the stored level.
- Catch-up summary: the `catchup.summary` toast counts only alert events. Pane-level catch-up events land in the pane; ignore-level ones stay in the database.
- On PRs you did not author, only review asks (`pr.created`, `pr.published`, `pr.rereview.needed`, `pr.reviewer.added`/`removed` for you or your group), `pr.mentioned`, and comment and status events on threads you commented in are emitted. Builds, policies, votes, conflicts and closes are emitted only for your own PRs.
- Pushes (`pr.iteration.pushed`) are also emitted on PRs where you are a reviewer or group reviewer and have commented in a thread. Every push event carries `data.iCommented` (`true` or `false`). Mention-only threads and commenter-only PRs (no reviewer role) do not count. Thread discovery runs on the slow tier (`polling.slow_secs`, default 600 s), so a push seen before discovery has picked up your first comment on that PR is not emitted later. On a reviewer PR where you voted, one push can emit both `pr.iteration.pushed` and `pr.rereview.needed`; a re-review hook should pick one type.
- Direct @mentions emit `pr.mentioned` (not also a comment event).
- Catch-up: after downtime or sleep Herald queries since the last watermark (UTC server time, at most `max_lookback_days`). Catch-up events do not toast; one `catchup.summary` toast and one sound cover the alert ones. The first run seeds silently.
- Snooze suppresses toast and sound only; the pane, tray and hooks still update (hooks stop only with `respect_snooze`). Mute skips the sound only. On Windows, toast and sound are also suppressed while the system reports a non-notifiable state (full-screen app, presentation mode, busy, quiet time).

Event types and default severity (severity drives the card colour and toast duration):

| Type | Severity |
| --- | --- |
| `run.completed` failed, `run.stage.failed`, `deploy.failed`, `pr.vote.changed` -10, `pr.conflicts.detected`, `pr.policy.changed` to rejected/failed/broken/error, `auth.failed` | critical |
| `pr.vote.changed` -5, `pr.rereview.needed`, `approval.pending`, `pr.reviewer.added` for you or your group, `pr.mentioned`, `deploy.canceled`, `run.completed` canceled/partiallySucceeded, `source.disabled` | warn |
| `pr.vote.changed` 5 or 10, `pr.completed`, `deploy.succeeded`, `run.completed` succeeded, `pr.policy.changed` to approved/succeeded, `pr.conflicts.cleared`, `approval.completed` approved | success |
| everything else | info |
| `catchup.summary` | highest severity among the summarised events |

Other types: `pr.created`, `pr.published`, `pr.abandoned`, `pr.reactivated`, `pr.iteration.pushed`, `pr.reviewer.added`, `pr.reviewer.removed`, `pr.votes.reset`, `pr.comment.added`, `pr.comment.replied`, `pr.thread.status.changed`, `pr.autocomplete.set`, `pr.autocomplete.cancelled`, `branch.created`, `branch.pushed`, `branch.forcepushed`, `branch.deleted`, `release.created`, `deploy.started`, `approval.completed`, `record.activated` (fired when you click a pane record; never alerts or shows in the pane).

Types starting `run.`, `release.`, `deploy.`, `approval.`, plus `pr.policy.changed`, `pr.conflicts.*`, `pr.rereview.needed`, `catchup.summary`, `auth.failed` and `source.disabled`, have actor kind `system`.

## Config reference

All keys are snake_case TOML. Every section is optional; the defaults are below (and in the template Herald writes).

| Key | Default | Meaning |
| --- | --- | --- |
| `polling.fast_secs` | 60 | pipelines, releases, approvals |
| `polling.pr_secs` | 120 | pull requests and their threads |
| `polling.slow_secs` | 600 | branches, discovery scan, identity and group refresh, local clone index |
| `polling.max_lookback_days` | 14 | how far back the first sync and catch-up look |
| `notify.<category>` | `"alert"` | level (`alert`, `pane` or `ignore`) for `pr`, `branch`, `run`, `release`, `deploy` or `approval` events |
| `notify.group.<category>` | the category's normal level | level for those events when they reached you only through a group |
| `notify.rules[].on` | required | event type globs the rule applies to |
| `notify.rules[].match` | `{}` | dotted event JSON paths to glob values, as in hooks; all must match |
| `notify.rules[].level` | required | `alert`, `pane` or `ignore`, set when the rule matches |
| `sound.file` | `""` | sound file; empty uses the built-in chime; a relative path resolves against the config folder (`HERALD_HOME`) |
| `sound.volume` | 0.8 | 0.0 to 1.0 |
| `toast.duration_secs` | 6 | toast lifetime |
| `toast.critical_duration_secs` | 12 | lifetime for critical toasts |
| `toast.max_visible` | 4 | cards shown before "+N more" |
| `local.scan_roots` | `[]` | folders searched for local clones and worktrees |
| `local.scan_depth` | 2 | how deep below each root to look |
| `stream.port` | 0 | stream port; 0 picks a free one (see Stream) |

The old `alerts.mute_types` and `roles.group_reviewer.alert_types` keys are ignored with a warning in the log; use `[notify]` instead. An unknown category or a level other than `alert`, `pane` or `ignore` is a config error, as is a rule with a missing or empty `on`, a bad glob, a missing or unknown `level`, or rules under a connection.

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

`[[connections]]` keys: `id`, `provider`, `enabled` (default true), `auth.method` (`pat` or `azcli` for ado), `bots` (globs), `notify` (per-connection levels, see above), and provider keys: for ado `organization` (required) and `projects` (names; empty = all).

Globs (`bots`, hook `on` and `match`) are case-insensitive, with `\` literal (so `Build\*` works the same on every OS).

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

# Re-review helper: actor.kind human/bot leaves out your own pushes (hooks/review.sh is relative to HERALD_HOME).
[[hooks]]
name = "review-push"
on = ["pr.iteration.pushed"]
match = { "pr.role" = ["reviewer", "groupReviewer"], "data.iCommented" = "true", "actor.kind" = ["human", "bot"] }
run = { default = ["hooks/review.sh", "--", "{pr.id}"] }
debounce_secs = 60

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
| `cwd` | `HERALD_HOME` | working directory; a relative path is under `HERALD_HOME` (placeholders allowed; see Hook safety). A relative program path in `run`, such as `hooks/notify`, resolves against it too, so scripts can live in `$HERALD_HOME/hooks/` |
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

- stdin: the event as JSON (camelCase; see [Event reference](#event-reference)).
- `HERALD_EVENT_FILE`: path to a file with the same JSON (under `tmp/`), handy for tools that cannot read stdin.
- Environment: `HERALD_EVENT_TYPE`, `HERALD_EVENT_ID`, `HERALD_EVENT_FILE`, `HERALD_SUBJECT`, `HERALD_CONNECTION`, `HERALD_PROVIDER`, and when present `HERALD_URL`, `HERALD_PR_ID`, `HERALD_REPO`, `HERALD_BRANCH` (short name), `HERALD_LOCAL_PATH` (worktree path, else repo path), plus the hook's own `env`.
- Placeholders: `{dotted.camelCase.path}` in `run` argv, `cwd` and `env` values expand from the event JSON. Numbers and bools become text. Use `{{` and `}}` for literal braces. A missing placeholder stops the run and records an error. Examples: `{title}`, `{url}`, `{pr.id}`, `{pr.sourceBranch}` (full ref, e.g. `refs/heads/feature/x`), `{branch.shortName}`, `{repo.name}`, `{run.result}`, `{actor.displayName}`, `{local.worktreePath}`, `{data.vote}`.

Event shape: every field, ref and per-type `data` key is listed in the [Event reference](#event-reference). `match = { viaGroup = "true" }` selects group events. Test events (`herald emit-test`, stream `emit`) go through the same levels, so an ignored category neither shows nor runs hooks. Run `herald tail --json` to see real payloads.

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

## Event reference

Every event is one JSON object with camelCase keys (hook stdin, the stream, `herald tail --json`, `herald events --json`). Hook `match` tables and `[[notify.rules]]` `match` address these fields by dotted path, e.g. `data.isMe`, `run.result`, `pr.role`. An absent ref or key is `null` or missing, and a missing path never matches. Values marked (ADO) are Azure DevOps values; the core does not interpret them.

### Common fields

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | unique; test events start `test:` |
| `seq` | number | store sequence; only on stored or streamed events |
| `type` | string | see the tables below |
| `connection` | `{id, provider}` | test events use `test` unless `--connection` is given |
| `occurredUtc`, `detectedUtc` | RFC 3339 UTC | provider time and detection time |
| `inferred` | bool | Herald derived the event (by diffing or a guess) instead of reading it from the provider; `occurredUtc` is then the detection time, except on `pr.thread.status.changed`, where it is the thread's last update time |
| `catchUp`, `test` | bool | seen after downtime or sleep; published by `herald emit-test` |
| `severity` | string | `critical`, `warn`, `success`, `info` |
| `actor` | `{kind, id, displayName, uniqueName}` | `kind` is `me`, `human`, `bot` or `system`; `uniqueName` can be `null` |
| `alert`, `level` | bool, string | `level` is `alert`, `pane` or `ignore`; `alert` is true only for `alert` |
| `viaGroup` | bool | the event reached you only through a group |
| `title`, `subtitle`, `url`, `subjectKey` | strings | `subtitle` and `url` can be `null`; `subjectKey` groups events about one subject, e.g. `<connection>:pr:<repoId>:<prId>` |
| `repo`, `pr`, `branch`, `run`, `deployment`, `comment`, `approval`, `local` | object or `null` | the refs below |
| `data` | object or `null` | per-type keys below |

### Refs

| Ref | Fields |
| --- | --- |
| `repo` | `id`, `name`, `namespace` (ADO: the project; can be `null`) |
| `pr` | `id` (number), `title`, `sourceBranch` and `targetBranch` (full refs, e.g. `refs/heads/main`), `isDraft` (bool), `role` (`author`, `reviewer`, `groupReviewer`, `commenter`, `mentioned`) |
| `branch` | `name` (full ref), `shortName` |
| `run` | `id`, `definition` (pipeline name), `sourceBranch` (full ref), `reason`, `status`, `result` (value sets below), `stage` (string or `null`; set only on `run.stage.failed`) |
| `deployment` | `releaseId`, `releaseName`, `definition` (release definition name), `environment`, `deploymentId`, `status` (the last three are `null` when not known) |
| `comment` | `threadId`, `commentId`, `excerpt` (at most 200 characters), `threadStatus` |
| `approval` | `id`, `kind`, `stage`, `status` |
| `local` | `repoPath`, `worktreePath` (either can be `null`); added by Herald when a local clone matches `repo` (see `local.scan_roots`), never by the provider |

All ids are strings except `pr.id`.

### Value sets

| Field | Values |
| --- | --- |
| `run.result` | (ADO) `succeeded`, `partiallySucceeded`, `failed`, `canceled`; empty while the build has no result (always so on `run.stage.failed`). Passed through, so other values are possible |
| `run.status` | (ADO) passed through from the build: `completed` on `run.completed`, `inProgress` on `run.stage.failed`; empty on the `run` ref of an approval |
| `run.reason` | (ADO) the build reason as the provider reports it (for example `manual`, `individualCI`, `schedule`, `pullRequest`); open-ended; empty on the `run` ref of an approval |
| `approval.kind` | (ADO) `ado-classic` (release approval; `approval.stage` is the environment name, `id` is numeric) and `ado-yaml` (YAML pipeline approval; `approval.stage` is the pipeline name, `id` is a GUID) |
| `approval.status` | (ADO) `pending` (only on `approval.pending`), `approved`, `rejected`, `canceled`, `skipped`, `timedOut`; any other final status the provider reports is passed through unchanged on `approval.completed` |
| `deployment.status` | (ADO) `inProgress`, `succeeded`, `failed`, `partiallySucceeded`, `canceled`; `null` on `release.created` and on approvals |
| `comment.threadStatus` | (ADO) the thread status as the provider reports it, for example `active`, `fixed`, `wontFix`, `closed`, `byDesign`, `pending`; open-ended; empty when the thread has none |
| `data.state`, `data.previousState` (`pr.policy.changed`) | (ADO) lowercase provider states, except `notSet` and `notApplicable` in camelCase; `partiallySucceeded` is mapped to `failed`; an empty state becomes `broken` for a policy evaluation and `notSet` for a status post. Terminal states, which always emit: `approved`, `succeeded`, `rejected`, `failed`, `broken`, `error`. Bad: `rejected`, `failed`, `broken`, `error`. Pending: `running`, `queued`, `pending`. Any other value is passed through; it can be a `previousState` but is never a `state` |

### Pull requests

Herald fetches votes, pushes and reviewer changes only for PRs where you are author, reviewer or groupReviewer; comment threads are also watched on commenter-only and mention-only PRs (roles `commenter` and `mentioned`). On a PR you did not author, only review asks (`pr.created`, `pr.published`, `pr.rereview.needed`, `pr.reviewer.added`/`removed` for you or your group), `pr.mentioned`, events on threads you are in, and pushes with `data.iCommented = true` are emitted; the rest is own-PRs only. All `pr.*` events carry `repo` and `pr`.

| Type | Also carries | `data` | Emitted when |
| --- | --- | --- | --- |
| `pr.created` | none | none | A PR of yours, or one you review, is first tracked: created since the previous poll (after downtime: since the last watermark, at most `max_lookback_days`) it is "PR created"; otherwise, for a reviewer or groupReviewer PR, it is "Review requested" with `inferred = true` and `occurredUtc` set to the detection time. An older PR where you are only author, commenter or mentioned emits nothing. Actor is the creator |
| `pr.published` | none | none | A draft was published |
| `pr.completed`, `pr.abandoned` | none | none | Own PRs. Actor is who closed it |
| `pr.reactivated` | none | none | An own PR that was closed is active again. `inferred`, actor `system` |
| `pr.conflicts.detected`, `pr.conflicts.cleared` | none | none | Own PRs, when the merge status changes to or from `conflicts`. `inferred`, actor `system` |
| `pr.iteration.pushed` | none | `reason` (string: (ADO) the iteration reason, `push` when empty; for example `forcePush`), `iCommented` (bool) | A new iteration after the first. Own PRs, and reviewer or groupReviewer PRs where you commented in a thread (`iCommented` true); mention-only threads and commenter-only PRs do not count. Actor is who pushed |
| `pr.reviewer.added`, `pr.reviewer.removed` | none | `reviewerId`, `reviewerName` (strings), `isMe`, `isMyGroup` (bools) | The reviewer list changed; one event per reviewer. Own PRs, and others only when `isMe` or `isMyGroup`. `viaGroup` is true when `isMyGroup` and not `isMe`. When you are added individually, `pr.role` on the event is `reviewer` |
| `pr.vote.changed` | none | `vote` (number, for example `10` approved, `5` approved with suggestions, `0` reset, `-5` waiting for author, `-10` rejected), `voterId` (string, empty if unknown), `voterIsMe` (bool) | Own PRs. Actor is the voter |
| `pr.votes.reset` | none | none | Own PRs, when all or several votes were reset. Actor `system` |
| `pr.rereview.needed` | none | none | You are a reviewer (not groupReviewer) and either a push arrived after you voted, or your vote was reset. One push can emit this and `pr.iteration.pushed`. Actor `system` |
| `pr.autocomplete.set`, `pr.autocomplete.cancelled` | none | none | Own PRs. Actor is who changed it |
| `pr.policy.changed` | none | `policyName` (string), `state`, `previousState` (strings, see Value sets; `previousState` is `null` when the policy was first seen in this state) | Own PRs only. A policy or status check reaches a terminal state, or goes back to running, queued or pending after a bad result (a rebuild). A PR's first poll and a policy first seen in a non-terminal state stay silent. Actor `system` |
| `pr.comment.added`, `pr.comment.replied` | `comment` | none | A text comment: `added` is the first comment of a thread, `replied` any later one. All threads on own PRs; on other PRs only threads you are in (found on the slow tier). Your own comments are emitted too (actor kind `me`) |
| `pr.mentioned` | `comment` for a comment mention; none for a description mention | none | You are @mentioned directly by someone else, in a comment or a description. Group mentions are not detected. `pr.role` is `mentioned` unless you have another role |
| `pr.thread.status.changed` | `comment` (`commentId` and `excerpt` empty) | `status`, `previousStatus` (strings, the thread status values above) | The thread status changed. `inferred`; the actor is guessed from the comments and can be a human named `Unknown` |

### Branches

`branch.*` events carry `repo` and `branch` and cover branches you created (any name; only `refs/heads/` refs). Actor is who pushed. `data` on all four: `pushId` (number), `oldObjectId`, `newObjectId` (commit ids; all zeros for "no commit": `oldObjectId` on create, `newObjectId` on delete).

| Type | Emitted when |
| --- | --- |
| `branch.created` | You created the branch |
| `branch.pushed` | A push to one of your branches that is not a force push; your own pushes are always `branch.pushed` |
| `branch.forcepushed` | Someone else's push left one of your branches without its old commits (the new tip is missing commits the old tip had). If that check fails the push is reported as `branch.pushed` |
| `branch.deleted` | One of your branches was deleted by anyone |

### Pipelines, releases and approvals

All of these have actor kind `system` and no `data`.

| Type | Carries | Emitted when |
| --- | --- | --- |
| `run.completed` | `run`; `pr` and `repo` when the build ran on the merge ref of a PR you authored | A build finished that you requested, or that ran on one of your branches or on your own PR's merge ref. `run.status` is `completed` |
| `run.stage.failed` | same, with `run.stage` set to the stage name | A stage of such a build that is still running has failed; once per stage. `run.status` is `inProgress` |
| `release.created` | `deployment` with only `releaseId`, `releaseName`, `definition` | A classic release created by you, or from one of your runs |
| `deploy.started`, `deploy.succeeded`, `deploy.failed`, `deploy.canceled` | `deployment` (all fields) | A deployment of such a release changed state. `deploy.failed` covers `failed` and `partiallySucceeded` (`deployment.status` tells which); `deploy.canceled` only after the deployment had started. Queued and other states emit nothing |
| `approval.pending`, `approval.completed` | `approval`; `deployment` (release, definition, environment) for `ado-classic`; `run` (only `id` and `definition`, when known) for `ado-yaml` | An approval is waiting on you or a group you are in (`viaGroup` is true when it is assigned to a group, not to you), and later it left your pending set with its final status (`approval.completed`; `approval.status` is never `pending` there) |

### System events

Actor kind is `system` (display name `Herald`), except `record.activated`. These four types are not configurable and ignore level rules.

| Type | `data` | Emitted when |
| --- | --- | --- |
| `auth.failed` | `reason`: `unauthorized` (the provider rejected the credentials) or `missing` (no secret stored, or the Azure CLI login lapsed) | Once per lapse, on the first failure |
| `source.disabled` | `source` (the polling source: `pr_lifecycle`, `pr_discovery`, `pr_reviews`, `pr_threads`, `pr_policies`, `branches`, `pipelines`, `releases`), `reason` (provider message) | A source was refused access (forbidden) and is switched off |
| `catchup.summary` | `counts` (object, event type to count), `total` (number), `sinceUtc` (RFC 3339), `top` (ids of up to 5 events with severity warn or higher, most severe first) | End of a catch-up; counts only alert-level events. `severity` is the highest among them. Subject key is `app` |
| `record.activated` | `originalId`, `originalType` | You opened a pane record. Copies `url`, `title`, `repo`, `pr`, `branch`, `run`, `deployment`, `local` and `test` from the original; actor kind `me`; level `pane` and never shown in the pane. Use it to run an `open-url` hook |

### Testing a rule

- `herald events --since 1h` lists stored events, including `ignore` ones, with a `[pane]` or `[ignore]` marker after the type (no marker means `alert`). `--json` shows the full event, including `level` and `data`, so you can see exactly what a rule matches against. It needs no running app.
- `herald emit-test <type>` publishes a test event through the running app. It goes through the same classifier as real events (level rules, connection, group and global levels, the own and bot cap), is stored, and runs hooks unless the hook has `include_test = false`. A test event has `connection.id = "test"` (pass `--connection ID` to exercise a connection override), `test = true`, a human actor with no identity and `viaGroup = false`; set anything else with `--set`.
- `--set path=value` takes a dotted path. The value is parsed as JSON when it is valid JSON (`true`, `-10`, `["a"]`) and is a plain string otherwise; to force a string that looks like JSON, quote it: `--set 'data.reviewerName="123"'`. Rules compare bools and numbers as text, so `"data.isMe" = "true"` matches `--set data.isMe=true`.

With the first and fourth example rules from the level rules (and `pr = "ignore"`):

```sh
herald emit-test pr.reviewer.added --pr-id 42 --title "Added as reviewer" --set data.isMe=true
herald emit-test pr.reviewer.added --pr-id 42 --title "Someone else" --set data.isMe=false
herald emit-test pr.policy.changed --pr-id 42 --set data.state=failed --set data.previousState=queued
herald events --since 5m
```

The first matches the `data.isMe` rule and is `alert` (a toast); the second matches no rule and falls back to `pr = "ignore"`, so `herald events` marks it `[ignore]`; the third matches the policy rule and is `alert`. Adding `--set actor.kind=bot` to the first shows the bot cap: the level drops to `pane`. Ignored test events are stored but not broadcast, so `herald tail` does not show them; use `herald events`.

If a rule never fires, check the event first (`herald events --json`), then the path (camelCase, `data.` prefix), the value (text, case-insensitive, globs) and the rule order (first match wins). A config edit that fails validation keeps the last good config running; the errors show in the pane and are logged as "config reload rejected" (or "config invalid; running with defaults" when the app starts with a bad file) in `$HERALD_HOME/logs/herald.<UTC date>.log`. Deprecated keys (`alerts.mute_types`, `roles.group_reviewer.alert_types`) log a warning there and are ignored.

## Stream

Herald serves a WebSocket on `127.0.0.1` at `/events` while the app runs. Connection details are in `stream.json`:

- Windows: `%LOCALAPPDATA%\herald\stream.json`
- macOS and Linux: `$HERALD_HOME/stream.json` (default `~/.config/herald/stream.json`), mode 600

It contains `{ "port", "token", "pid" }`. The token is 32 random bytes in hex and changes on every app start; the file is removed on exit. Set `stream.port` for a fixed port.

Auth: send `Authorization: Bearer <token>`. Requests from a non-loopback peer, with a wrong or missing token, or carrying any `Origin` header (so browsers cannot connect) are refused.

Client to server (JSON text frames):

| Message | Reply |
| --- | --- |
| `{"type":"subscribe","types":["pr.*"],"match":{"pr.role":"author"},"includeCatchUp":true,"afterSeq":120}` (all fields optional; no `types` = everything; `includeCatchUp` defaults to true; `afterSeq` replays stored events, see Replay) | `{"type":"ok","latestSeq":130,"oldestSeq":1,"storeId":"..."}`, then events |
| `{"type":"emit","event":{"type":"pr.created","title":"..."}}` publishes a test event (id `test:<uuid>`, `test = true`; connection `test`, actor kind `human` unless given); `type` is required | `{"type":"ok","id":"test:..."}` |
| `{"type":"credentials-changed","connectionId":"work"}` | `{"type":"ok"}` |

Server to client: `{"type":"event","event":{...}}`, `{"type":"ok"}`, `{"type":"error","message":"..."}` (for example `lagged N events` when a slow client misses events and did not subscribe with `afterSeq`), and the replay messages below.

### Replay

Every stored event has a `seq`: strictly increasing, never reused (also after pruning), but with possible gaps, so do not assume it is dense. Events are sent live in seq order. Replay and live delivery skip `ignore`-level events; a client that fetches by seq (for example `herald events --after X`) may see stored ignore rows and can filter on `level`. Transient events (for example `record.activated`) are not stored, have no `seq` and are always forwarded live.

The `ok` reply to every `subscribe` carries `latestSeq` (0 when the store is empty), `oldestSeq` (`null` when empty) and `storeId` (identifies the database; it changes if the database is recreated). Remember the highest `seq` you processed and the `storeId`; after a reconnect, send `afterSeq` with that value:

- Herald sends the `ok` first, then every stored event with `seq > afterSeq` that matches your filters, oldest first, then continues live. Events published while the replay runs are not missed, and after the `ok` nothing is sent twice: live events with a `seq` at or below the last one sent or scanned are dropped. Unfiltered events a client receives before its first `ok` are not part of the replay contract; it may see them again in the replay.
- At most 5000 matching events are replayed, and at most 50000 stored events are scanned, per subscribe. If more remain, Herald sends `{"type":"overflow","replayedThrough":X,"latestSeq":L}` (`X` is the last event sent, or the last one scanned when the scan cap was hit) and continues live from `L`. Fetch `X+1` through `L` yourself (for example with `herald events --after X`).
- If `afterSeq` is greater than `latestSeq` (the database was recreated, or it is a different store), Herald sends `{"type":"reset","latestSeq":L,"storeId":S}` after the `ok` and continues live from `L`. Discard your saved position.
- The replay runs alongside ping, pong and your other messages. A new `subscribe` cancels a replay in progress and restarts after the last `seq` that replay sent. In seq mode, after the `ok`, a `seq` already sent on the connection is not sent again (open a new connection to replay with a different filter from an earlier point).
- A client that falls behind the live feed (`lagged`) is caught up automatically by replaying from the last `seq` sent, instead of receiving a `lagged` error.
- A subscribe without `afterSeq` behaves as before: live events only, no dedupe.
- If the database predates sequence numbers (opened read-only), a subscribe with `afterSeq` gets an `error` reply; open the Herald app once to upgrade it.

### Event sequence numbers and the database upgrade

Sequence numbers need a newer database schema. The first start of a build with this feature upgrades the database in place. Before it does, Herald copies the old file to `<db>.v1.bak`, or to `<db>.v1.<UTC yyyymmddThhmmss>.bak` if that name is taken; a failed attempt may leave another timestamped backup.

Schema version 3 adds notification levels. Its upgrade first copies the database to `herald.db.v2.bak` (or `herald.db.v2.<UTC yyyymmddThhmmss>.bak`). Old events get level `alert` when their stored alert flag was set and `pane` otherwise, so the pane and unviewed count look as before. A database upgraded from version 1 is backed up at each step.

- Older builds refuse an upgraded database (version 3 included) because its schema version is newer than they know. To roll back, stop Herald, restore the backup over the database file and start the older build.
- A read-only CLI on a database that has not been upgraded cannot serve `--after` or a subscribe with `afterSeq`; it reports "open the Herald app once to upgrade". The CLI never upgrades a database; on one that predates version 3 it still reads events, but rows show level `alert` until the app upgrades it.

Limits: 16 authenticated clients, 64 KiB per message, 3 s to complete the handshake, keep-alive pings every 30 s.

## CLI

`herald` talks to the running app over the stream, and reads the store and config directly. Add `-v` for debug logging on stderr.

| Command | What it does |
| --- | --- |
| `herald tail [--type GLOB]... [--match path=glob]... [--no-catch-up] [--after SEQ] [--json]` | stream live events, one line each (`[seq  ]HH:MM:SS  type  [level  ]title  url`, local time; the level appears only as `[pane]`, since tail never receives `ignore` events); `--json` prints raw event JSON, one per line, and nothing else on stdout. `--after SEQ` first replays stored events with a higher seq (see Replay), then continues live; the replay `ok`, `overflow` and `reset` notices are printed to stderr as short lines. Repeating `--match` for one key means any-of. Exits 1 when the app is not running or stops; rerun to reconnect. Ctrl+C exits 0 |
| `herald emit-test <type> [--title T] [--pr-id N] [--repo R] [--branch B] [--url U] [--connection ID] [--set key=value]...` | publish a test event through the running app; prints its id. `--set` overrides any dotted path (JSON value or plain string) |
| `herald events (--since <when> \| --after <seq>) [--limit N] [--json]` | list stored events, oldest first; exactly one of `--since` and `--after` is required. `<when>` is RFC3339, a date `YYYY-MM-DD` (UTC midnight) or a duration `30m`, `2h`, `7d`, `1w`; `--after` lists events with a higher `seq`. Lists every stored event, including `ignore` level. Each event shows its `seq` (leading column in text, `seq` field in JSON) and its `level` (`[pane]` or `[ignore]` marker in text, `level` field in JSON). Default limit 200; no app needed. A database that predates sequence numbers makes `--after` fail with a message; open the app once to upgrade it |
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

Releasing:

1. Set the same version in `app/src-tauri/tauri.conf.json`, `app/package.json` and the crate manifests, and commit.
2. Tag and push: `git tag v0.2.0 && git push origin v0.2.0`.
3. The `release` workflow builds the Windows installer and both macOS builds, then creates a draft release with them, their `.sha256` files and generated notes. It fails if the tag does not match `tauri.conf.json`.
4. Review the draft and publish it. The install scripts only see published, non-prerelease releases.

Running the workflow by hand (Actions > release > Run workflow) builds the same files as workflow artifacts without creating a release.

Known limitations:

- macOS is unverified and Linux is untested (code-complete behind `cfg` gates; no do-not-disturb detection there). Installers are unsigned.
- The PAT scope for YAML pipeline approvals is unconfirmed (see First run).
- The exact property names ADO uses for a removed reviewer (`CodeReviewReviewersUpdatedRemovedIdentity`) and for reset-all-votes system threads are unconfirmed; those events may be missing or partial.
- Windows Job Object assignment happens just after a child starts, so a grandchild spawned in that instant can escape it.
- No updater; no history viewer.

## License

MIT. See [LICENSE](LICENSE).
