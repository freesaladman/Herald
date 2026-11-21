# Herald

Desktop watcher (Tauri 2 + React) and CLI that polls a provider (Azure DevOps first), classifies events, and raises toasts, sounds and hooks.

## Layout
- `crates/core` (`herald-core`): events, config, store, engine, scheduler, bus, hooks, stream. Must NOT depend on any provider crate and has no provider-specific names or URLs in code (exception: the commented example connection in `assets/config.default.toml`, doc comments).
- `crates/provider-ado` (`herald-provider-ado`): Azure DevOps provider; depends on core.
- `crates/cli` (`herald-cli`, bin `herald`): depends on core + provider-ado.
- `app/` (Vite + React + TS, npm); `app/src-tauri` (`herald-app`, lib `herald_app_lib`): tray, toast and pane windows, alerts, sound; depends on core + provider-ado.
- Registry is built only in app and CLI: `ProviderRegistry::new(vec![Arc::new(herald_provider_ado::AdoProviderFactory)])`.

## Rules
- Rust edition 2024; `cargo fmt`; clippy clean with `-D warnings`.
- No `unwrap`/`expect` outside tests and startup-fatal paths. `thiserror` in libraries, `anyhow` only in binaries.
- tokio; spawned tasks are owned by Engine or hold a handle. All times `DateTime<Utc>`; local time only in the UI.
- `tracing` for logs. Never log secrets, tokens or Authorization headers.
- OS-specific code behind `#[cfg]` with fallbacks.
- Tests: unit, wiremock, JSON fixtures; engine tests via FakeProvider. Tests never touch the real home/runtime dirs or set env vars.
- TS: strict, ESLint clean, function components, no `any`, Vitest.
- Comments: terse, non-obvious only, no change history, no em dashes.
- Stream access checks: token, loopback peer, reject any `Origin`.
- Provider access is GET only.

## Commands
- `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`
- In `app/`: `npm run build`, `npm run lint`, `npx vitest run`

## Env
- Rust toolchain on PATH (RUSTUP_HOME/CARGO_HOME may point anywhere); keep scratch files outside the repo.
- `tauri build` caches NSIS under `%LOCALAPPDATA%\tauri`; expected.
