//! Hook matching and running.

pub mod matcher;
pub mod runner;

pub use matcher::{
    CompiledHookFilter, MissingPlaceholder, expand, lookup, match_table, type_matches,
};
pub use runner::HookRunner;
pub(crate) use runner::placeholder_paths;
