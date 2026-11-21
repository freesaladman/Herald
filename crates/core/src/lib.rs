//! Provider-neutral core of herald: events, config, store, engine, bus, hooks and stream.

pub mod bus;
pub mod classify;
pub mod config;
pub mod engine;
pub mod event;
pub mod hooks;
pub mod local_repos;
pub mod process;
pub mod provider;
pub mod scheduler;
pub mod secrets;
pub mod store;
pub mod stream;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
