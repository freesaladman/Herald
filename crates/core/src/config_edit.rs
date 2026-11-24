//! Format-preserving edits of the `[[connections]]` entries in config.toml.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use toml_edit::{ArrayOfTables, DocumentMut, InlineTable, Item, Table, Value, value};

use crate::config::{AuthConfig, Config, ConfigError, ConnectionConfig, NotifyConfig};
use crate::provider::ProviderRegistry;

const RESERVED_KEYS: [&str; 5] = ["id", "provider", "enabled", "auth", "bots"];

/// Editable form of a connection. Never carries secrets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionDraft {
    pub id: String,
    pub provider: String,
    pub enabled: bool,
    pub auth_method: String,
    pub bots: Vec<String>,
    /// Provider fields: a string or an array of strings.
    pub settings: BTreeMap<String, serde_json::Value>,
}

impl ConnectionDraft {
    pub fn from_config(cfg: &ConnectionConfig) -> Self {
        let settings = cfg
            .settings
            .iter()
            .map(|(k, v)| {
                let json = serde_json::to_value(v).unwrap_or(serde_json::Value::Null);
                (k.clone(), json)
            })
            .collect();
        Self {
            id: cfg.id.clone(),
            provider: cfg.provider.clone(),
            enabled: cfg.enabled,
            auth_method: cfg.auth.method.clone(),
            bots: cfg.bots.clone(),
            settings,
        }
    }

    pub fn to_connection_config(&self) -> Result<ConnectionConfig, ConfigError> {
        let mut settings = toml::Table::new();
        for (key, v) in &self.settings {
            let path = format!("settings.{key}");
            if RESERVED_KEYS.contains(&key.as_str()) {
                return Err(ConfigError::new(path, "is a reserved key"));
            }
            settings.insert(key.clone(), json_to_toml(v, &path)?);
        }
        Ok(ConnectionConfig {
            id: self.id.clone(),
            provider: self.provider.clone(),
            enabled: self.enabled,
            auth: AuthConfig {
                method: self.auth_method.clone(),
            },
            bots: self.bots.clone(),
            notify: NotifyConfig::default(),
            settings,
        })
    }
}

fn json_to_toml(v: &serde_json::Value, path: &str) -> Result<toml::Value, ConfigError> {
    match v {
        serde_json::Value::String(s) => Ok(toml::Value::String(s.clone())),
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| match item {
                serde_json::Value::String(s) => Ok(toml::Value::String(s.clone())),
                _ => Err(ConfigError::new(path, "list items must be strings")),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(toml::Value::Array),
        _ => Err(ConfigError::new(
            path,
            "must be a string or a list of strings",
        )),
    }
}

/// Keys are written in a fixed order: id, provider, enabled, auth, bots, then settings sorted.
fn connection_table(cfg: &ConnectionConfig) -> Result<Table, ConfigError> {
    let mut t = Table::new();
    t.insert("id", value(cfg.id.as_str()));
    t.insert("provider", value(cfg.provider.as_str()));
    t.insert("enabled", value(cfg.enabled));
    let mut auth = InlineTable::new();
    auth.insert("method", cfg.auth.method.as_str().into());
    t.insert("auth", Item::Value(Value::InlineTable(auth)));
    if !cfg.bots.is_empty() {
        t.insert("bots", Item::Value(string_array(&cfg.bots)));
    }
    let mut keys: Vec<&String> = cfg.settings.keys().collect();
    keys.sort();
    for key in keys {
        let text = cfg.settings[key].to_string();
        let parsed = text
            .parse::<Value>()
            .map_err(|e| ConfigError::new(format!("settings.{key}"), e.to_string()))?;
        t.insert(key, Item::Value(parsed));
    }
    Ok(t)
}

fn string_array(items: &[String]) -> Value {
    Value::Array(items.iter().map(|s| Value::from(s.as_str())).collect())
}

fn read_document(path: &Path) -> Result<String, Vec<ConfigError>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(Config::default_toml().to_string())
        }
        Err(e) => Err(io_errors(path, &e)),
    }
}

fn io_errors(path: &Path, e: &std::io::Error) -> Vec<ConfigError> {
    vec![ConfigError::new(path.display().to_string(), e.to_string())]
}

fn parse_document(path: &Path, text: &str) -> Result<DocumentMut, Vec<ConfigError>> {
    text.parse::<DocumentMut>()
        .map_err(|e| vec![ConfigError::new(path.display().to_string(), e.to_string())])
}

fn connections_mut(doc: &mut DocumentMut) -> Result<&mut ArrayOfTables, Vec<ConfigError>> {
    if !doc.contains_key("connections") {
        doc.insert("connections", Item::ArrayOfTables(ArrayOfTables::new()));
    }
    doc["connections"].as_array_of_tables_mut().ok_or_else(|| {
        vec![ConfigError::new(
            "connections",
            "must be an array of tables ([[connections]])",
        )]
    })
}

fn entry_id(t: &Table) -> Option<&str> {
    t.get("id").and_then(Item::as_str)
}

fn position_of(tables: &ArrayOfTables, id: &str) -> Option<usize> {
    tables.iter().position(|t| entry_id(t) == Some(id))
}

/// A validated new config text, ready to write. Nothing is on disk until `write`.
#[derive(Debug)]
pub struct PreparedEdit {
    path: PathBuf,
    text: String,
}

impl PreparedEdit {
    pub fn write(self) -> std::io::Result<()> {
        write_atomic(&self.path, &self.text)
    }
}

/// Builds and validates the document that adds a connection (`original_id` None) or
/// replaces the entry with that id in place. Keys of the old entry that the provider
/// does not describe and the draft does not carry are kept.
pub fn prepare_save(
    path: &Path,
    draft: &ConnectionDraft,
    original_id: Option<&str>,
    registry: &ProviderRegistry,
) -> Result<PreparedEdit, Vec<ConfigError>> {
    let cfg = draft.to_connection_config().map_err(|e| vec![e])?;
    let new_table = connection_table(&cfg).map_err(|e| vec![e])?;

    let mut doc = parse_document(path, &read_document(path)?)?;
    let tables = connections_mut(&mut doc)?;

    let target = match original_id {
        Some(orig) => Some(position_of(tables, orig).ok_or_else(|| {
            vec![ConfigError::new(
                "id",
                format!("no connection with id '{orig}'"),
            )]
        })?),
        None => None,
    };
    let index = target.unwrap_or(tables.len());
    let clash = tables
        .iter()
        .enumerate()
        .any(|(i, t)| Some(i) != target && entry_id(t) == Some(cfg.id.as_str()));
    if clash {
        return Err(vec![ConfigError::new(
            format!("connections[{index}].id"),
            format!("duplicate connection id '{}'", cfg.id),
        )]);
    }

    match target {
        Some(i) => {
            if let Some(slot) = tables.get_mut(i) {
                let mut table = new_table;
                carry_over_unknown_keys(slot, &mut table, draft, registry);
                *table.decor_mut() = slot.decor().clone();
                *slot = table;
            }
        }
        None => {
            let mut table = new_table;
            if !doc_is_empty_text(&doc) {
                table.decor_mut().set_prefix("\n");
            }
            connections_mut(&mut doc)?.push(table);
        }
    }

    prepare(path, doc.to_string(), registry)
}

/// Keys the user added by hand: present in the old entry, unknown to the provider's
/// descriptor, absent from the draft. Descriptor keys missing from the draft stay removed.
fn carry_over_unknown_keys(
    old: &Table,
    new: &mut Table,
    draft: &ConnectionDraft,
    registry: &ProviderRegistry,
) {
    let Some(factory) = registry.get(&draft.provider) else {
        return;
    };
    let described: Vec<String> = factory
        .connection_fields()
        .into_iter()
        .map(|f| f.key)
        .collect();
    for (key, item) in old.iter() {
        let known = RESERVED_KEYS.contains(&key) || described.iter().any(|d| d == key);
        if !known && !draft.settings.contains_key(key) {
            new.insert(key, item.clone());
        }
    }
}

fn doc_is_empty_text(doc: &DocumentMut) -> bool {
    doc.to_string().trim().is_empty()
}

/// Removes the connection with `id`. Other entries are left as they are.
pub fn remove_connection(
    path: &Path,
    id: &str,
    registry: &ProviderRegistry,
) -> Result<(), Vec<ConfigError>> {
    prepare_remove(path, id, registry)?
        .write()
        .map_err(|e| io_errors(path, &e))
}

fn prepare_remove(
    path: &Path,
    id: &str,
    registry: &ProviderRegistry,
) -> Result<PreparedEdit, Vec<ConfigError>> {
    let mut doc = parse_document(path, &read_document(path)?)?;
    let tables = connections_mut(&mut doc)?;
    let index = position_of(tables, id).ok_or_else(|| {
        vec![ConfigError::new(
            "id",
            format!("no connection with id '{id}'"),
        )]
    })?;
    let removed_position = tables.get(index).and_then(Table::position);
    let removed = tables.remove(index);
    let kept = unrelated_comments(removed.decor().prefix().and_then(|p| p.as_str()));
    if tables.is_empty() {
        doc.remove("connections");
    }
    if let Some(kept) = kept {
        hand_over_comments(&mut doc, removed_position, &kept);
    }
    prepare(path, doc.to_string(), registry)
}

/// The part of a removed entry's prefix up to its last blank line: a header or comments
/// about other things. A block directly above the entry belongs to it and goes with it.
fn unrelated_comments(prefix: Option<&str>) -> Option<String> {
    let prefix = prefix?;
    let lines: Vec<&str> = prefix.split_inclusive('\n').collect();
    let last_blank = lines.iter().rposition(|l| l.trim().is_empty())?;
    let kept = lines[..=last_blank].concat();
    (!kept.trim().is_empty()).then_some(kept)
}

/// Puts `comments` before the first table that follows `position`, else at the end of the file.
fn hand_over_comments(doc: &mut DocumentMut, position: Option<isize>, comments: &str) {
    let next = position.and_then(|pos| {
        let mut best: Option<(isize, String, Option<usize>)> = None;
        for (key, item) in doc.iter() {
            let found: Vec<(isize, Option<usize>)> = match item {
                Item::Table(t) => t.position().map(|p| (p, None)).into_iter().collect(),
                Item::ArrayOfTables(a) => a
                    .iter()
                    .enumerate()
                    .filter_map(|(i, t)| t.position().map(|p| (p, Some(i))))
                    .collect(),
                _ => Vec::new(),
            };
            for (p, i) in found {
                if p > pos && best.as_ref().is_none_or(|(b, ..)| p < *b) {
                    best = Some((p, key.to_string(), i));
                }
            }
        }
        best
    });
    let table = next.and_then(|(_, key, idx)| match (doc.get_mut(&key)?, idx) {
        (Item::Table(t), None) => Some(t),
        (Item::ArrayOfTables(a), Some(i)) => a.get_mut(i),
        _ => None,
    });
    match table {
        Some(t) => {
            let existing = t
                .decor()
                .prefix()
                .and_then(|p| p.as_str())
                .unwrap_or_default()
                .trim_start_matches(['\n', '\r']);
            let merged = format!("{comments}{existing}");
            t.decor_mut().set_prefix(merged);
        }
        None => {
            let trailing = doc.trailing().as_str().unwrap_or_default().to_string();
            doc.set_trailing(format!("{trailing}\n{comments}"));
        }
    }
}

fn prepare(
    path: &Path,
    text: String,
    registry: &ProviderRegistry,
) -> Result<PreparedEdit, Vec<ConfigError>> {
    let cfg = Config::from_toml_str(&text)?;
    let errors = cfg.validate_providers(registry);
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(PreparedEdit {
        path: path.to_path_buf(),
        text,
    })
}

/// `prepare_save` then `write`.
pub fn save_connection(
    path: &Path,
    draft: &ConnectionDraft,
    original_id: Option<&str>,
    registry: &ProviderRegistry,
) -> Result<(), Vec<ConfigError>> {
    prepare_save(path, draft, original_id, registry)?
        .write()
        .map_err(|e| io_errors(path, &e))
}

/// Temp file next to the target, then rename over it (replaces an existing file on every OS).
fn write_atomic(path: &Path, text: &str) -> std::io::Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(dir) = dir {
        std::fs::create_dir_all(dir)?;
    }
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    let tmp = path.with_file_name(name);
    let result = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::sync::Arc;

    fn registry(provider: &FakeProvider) -> ProviderRegistry {
        ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(provider.clone()))])
    }

    fn draft(id: &str) -> ConnectionDraft {
        ConnectionDraft {
            id: id.into(),
            provider: "fake".into(),
            enabled: true,
            auth_method: "token".into(),
            bots: vec![],
            settings: BTreeMap::from([("host".to_string(), json!("example.test"))]),
        }
    }

    fn load(path: &Path) -> Config {
        Config::load(path).expect("config parses")
    }

    #[test]
    fn add_to_default_template_keeps_comments_and_parses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());

        save_connection(&path, &draft("one"), None, &reg).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        for line in Config::default_toml()
            .lines()
            .filter(|l| l.starts_with('#'))
        {
            assert!(text.contains(line), "lost comment: {line}");
        }
        assert!(text.contains(
            "[[connections]]\nid = \"one\"\nprovider = \"fake\"\nenabled = true\nauth = { method = \"token\" }\nhost = \"example.test\"\n"
        ));
        let cfg = load(&path);
        assert_eq!(cfg.connections.len(), 1);
        assert_eq!(cfg.hooks.len(), 1);
    }

    #[test]
    fn update_replaces_in_place_and_keeps_surrounding_comments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        let text = "# top\n\n# first conn\n[[connections]]\nid = \"a\"\nprovider = \"fake\"\nauth = { method = \"token\" }\nhost = \"old\"\n\n# second conn\n[[connections]]\nid = \"b\"\nprovider = \"fake\"\nauth = { method = \"none\" }\n\n# the hook\n[[hooks]]\nname = \"h\"\non = [\"x\"]\naction = \"open-url\"\n";
        std::fs::write(&path, text).unwrap();

        let mut d = draft("a");
        d.settings.insert("host".into(), json!("new"));
        d.bots = vec!["bot*".into()];
        save_connection(&path, &d, Some("a"), &reg).unwrap();

        let out = std::fs::read_to_string(&path).unwrap();
        for c in ["# top", "# first conn", "# second conn", "# the hook"] {
            assert!(out.contains(c), "lost {c}");
        }
        let cfg = load(&path);
        let ids: Vec<_> = cfg.connections.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(cfg.connections[0].settings["host"].as_str(), Some("new"));
        assert_eq!(cfg.connections[0].bots, ["bot*"]);
        assert!(out.find("# first conn") < out.find("host = \"new\""));
        assert!(out.find("host = \"new\"") < out.find("# second conn"));
    }

    #[test]
    fn rename_changes_the_id_of_the_same_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        save_connection(&path, &draft("a"), None, &reg).unwrap();
        save_connection(&path, &draft("b"), None, &reg).unwrap();

        save_connection(&path, &draft("renamed"), Some("a"), &reg).unwrap();

        let ids: Vec<_> = load(&path).connections.into_iter().map(|c| c.id).collect();
        assert_eq!(ids, ["renamed", "b"]);
    }

    #[test]
    fn duplicate_and_unknown_original_ids_are_rejected_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        save_connection(&path, &draft("a"), None, &reg).unwrap();
        save_connection(&path, &draft("b"), None, &reg).unwrap();
        let before = std::fs::read(&path).unwrap();

        let dup_add = save_connection(&path, &draft("a"), None, &reg).unwrap_err();
        assert_eq!(dup_add[0].path, "connections[2].id");
        let dup_rename = save_connection(&path, &draft("b"), Some("a"), &reg).unwrap_err();
        assert_eq!(dup_rename[0].path, "connections[0].id");
        let unknown = save_connection(&path, &draft("c"), Some("zzz"), &reg).unwrap_err();
        assert!(unknown[0].message.contains("zzz"));

        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn invalid_draft_leaves_file_byte_identical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let provider = FakeProvider::new();
        let reg = registry(&provider);
        save_connection(&path, &draft("a"), None, &reg).unwrap();
        let before = std::fs::read(&path).unwrap();
        provider.set_validate_errors(vec![ConfigError::new("host", "host is required")]);

        let errs = save_connection(&path, &draft("b"), None, &reg).unwrap_err();

        assert!(errs.iter().any(|e| e.path == "connections[0].host"));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!dir.path().join("config.toml.tmp").exists());
    }

    #[test]
    fn invalid_id_charset_is_reported_at_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());

        let errs = save_connection(&path, &draft("Bad Id"), None, &reg).unwrap_err();

        assert_eq!(errs[0].path, "connections[0].id");
        assert!(!path.exists());
    }

    #[test]
    fn remove_drops_only_that_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        save_connection(&path, &draft("a"), None, &reg).unwrap();
        save_connection(&path, &draft("b"), None, &reg).unwrap();

        remove_connection(&path, "a", &reg).unwrap();
        let ids: Vec<_> = load(&path).connections.into_iter().map(|c| c.id).collect();
        assert_eq!(ids, ["b"]);

        assert!(remove_connection(&path, "a", &reg).is_err());
        remove_connection(&path, "b", &reg).unwrap();
        let cfg = load(&path);
        assert!(cfg.connections.is_empty());
        assert_eq!(cfg.hooks.len(), 1);
    }

    #[test]
    fn list_setting_round_trips_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        let mut d = draft("a");
        d.settings.insert("projects".into(), json!(["p1", "p2"]));

        save_connection(&path, &d, None, &reg).unwrap();

        let cfg = load(&path);
        assert_eq!(ConnectionDraft::from_config(&cfg.connections[0]), d);
    }

    #[test]
    fn draft_config_round_trip_and_unsupported_types() {
        let mut d = draft("a");
        d.bots = vec!["x*".into()];
        d.settings.insert("projects".into(), json!(["p1"]));
        let cfg = d.to_connection_config().unwrap();
        assert_eq!(ConnectionDraft::from_config(&cfg), d);

        d.settings.insert("port".into(), json!(8080));
        let err = d.to_connection_config().unwrap_err();
        assert_eq!(err.path, "settings.port");
    }

    #[test]
    fn prepare_save_writes_nothing_until_write_and_errors_never_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());

        let prepared = prepare_save(&path, &draft("a"), None, &reg).unwrap();
        assert!(!path.exists());
        prepared.write().unwrap();
        assert_eq!(load(&path).connections.len(), 1);

        let before = std::fs::read(&path).unwrap();
        assert!(prepare_save(&path, &draft("a"), None, &reg).is_err());
        assert!(prepare_save(&path, &draft("b"), Some("zzz"), &reg).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn edit_keeps_hand_added_keys_but_drops_cleared_descriptor_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        let text = "[[connections]]\nid = \"a\"\nprovider = \"fake\"\nauth = { method = \"token\" }\nhost = \"old\"\nextra = \"keep\"\nmine = [\"x\"]\n";
        std::fs::write(&path, text).unwrap();

        let mut d = draft("a");
        d.settings.clear();
        d.settings.insert("mine".into(), json!(["y"]));
        save_connection(&path, &d, Some("a"), &reg).unwrap();

        let cfg = load(&path);
        let s = &cfg.connections[0].settings;
        assert!(!s.contains_key("host"));
        assert_eq!(s["extra"].as_str(), Some("keep"));
        assert_eq!(s["mine"].as_array().unwrap().len(), 1);
        assert_eq!(s["mine"][0].as_str(), Some("y"));
    }

    #[test]
    fn edit_keeps_hand_written_notify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        let text = "[[connections]]\nid = \"a\"\nprovider = \"fake\"\nauth = { method = \"token\" }\nhost = \"old\"\nnotify = { pr = \"ignore\" }\n";
        std::fs::write(&path, text).unwrap();

        save_connection(&path, &draft("a"), Some("a"), &reg).unwrap();

        let cfg = load(&path);
        assert_eq!(cfg.connections[0].notify.levels["pr"], "ignore");
        assert!(!cfg.connections[0].settings.contains_key("notify"));
    }

    #[test]
    fn remove_keeps_header_comment_above_the_first_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let reg = registry(&FakeProvider::new());
        let text = "[polling]\nfast_secs = 60\n\n# Header\n# more header\n\n# about a\n[[connections]]\nid = \"a\"\nprovider = \"fake\"\nauth = { method = \"token\" }\n\n# about b\n[[connections]]\nid = \"b\"\nprovider = \"fake\"\nauth = { method = \"token\" }\n\n[[hooks]]\nname = \"h\"\non = [\"x\"]\naction = \"open-url\"\n";
        std::fs::write(&path, text).unwrap();

        remove_connection(&path, "a", &reg).unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        assert!(out.contains("# Header\n# more header\n"), "{out}");
        assert!(!out.contains("# about a"), "{out}");
        assert!(out.find("# more header") < out.find("# about b"), "{out}");
        assert_eq!(load(&path).connections.len(), 1);

        remove_connection(&path, "b", &reg).unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        assert!(out.contains("# Header"), "{out}");
        assert!(out.find("# Header") < out.find("[[hooks]]"), "{out}");
        assert_eq!(load(&path).hooks.len(), 1);
    }
}
