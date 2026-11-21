//! Azure DevOps provider for herald.

pub mod auth;
pub mod client;
pub mod connection;
pub mod dto_git;
pub mod git;
pub mod pipelines;
pub mod releases;
pub mod sources;

pub use connection::AdoProviderFactory;
