//! Config file types, loading and provider-neutral validation, plus the home/runtime dirs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use globset::{Glob, GlobBuilder};
use serde::{Deserialize, Serialize};

use crate::hooks::placeholder_paths;

const DEFAULT_TOML: &str = include_str!("../assets/config.default.toml");

/// SQLite store file name under `Paths::home`.
pub const DB_FILE: &str = "herald.db";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    pub home: PathBuf,
    pub runtime: PathBuf,
}

impl Paths {
    pub fn from_env() -> Paths {
        Paths {
            home: home_dir(),
            runtime: runtime_dir(),
        }
    }

    /// Creates both directories.
    pub fn ensure(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.home)?;
        std::fs::create_dir_all(&self.runtime)
    }
}

pub fn home_dir() -> PathBuf {
    home_dir_from(|k| std::env::var(k).ok(), dirs::home_dir())
}

pub fn runtime_dir() -> PathBuf {
    runtime_dir_from(
        |k| std::env::var(k).ok(),
        dirs::home_dir(),
        dirs::data_local_dir(),
    )
}

pub fn home_dir_from(env: impl Fn(&str) -> Option<String>, user_home: Option<PathBuf>) -> PathBuf {
    match env("HERALD_HOME") {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => user_home
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".config")
            .join("herald"),
    }
}

pub fn runtime_dir_from(
    env: impl Fn(&str) -> Option<String>,
    user_home: Option<PathBuf>,
    data_local: Option<PathBuf>,
) -> PathBuf {
    if cfg!(windows)
        && let Some(d) = data_local
    {
        return d.join("herald");
    }
    home_dir_from(env, user_home)
}

/// Glob flags shared by every matcher: identical on all OSes, case-insensitive.
pub fn build_glob(pattern: &str) -> Result<Glob, globset::Error> {
    GlobBuilder::new(pattern)
        .backslash_escape(false)
        .case_insensitive(true)
        .build()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigError {
    pub path: String,
    pub message: String,
}

impl ConfigError {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub polling: PollingConfig,
    pub alerts: AlertsConfig,
    pub roles: RolesConfig,
    pub sound: SoundConfig,
    pub toast: ToastConfig,
    pub local: LocalConfig,
    pub stream: StreamConfig,
    pub connections: Vec<ConnectionConfig>,
    pub hooks: Vec<HookConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PollingConfig {
    pub fast_secs: u64,
    pub pr_secs: u64,
    pub slow_secs: u64,
    pub max_lookback_days: i64,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            fast_secs: 60,
            pr_secs: 120,
            slow_secs: 600,
            max_lookback_days: 14,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertsConfig {
    pub mute_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RolesConfig {
    pub group_reviewer: GroupReviewerRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupReviewerRole {
    pub alert_types: Vec<String>,
}

impl Default for GroupReviewerRole {
    fn default() -> Self {
        Self {
            alert_types: ["pr.created", "pr.published", "pr.mentioned"]
                .map(String::from)
                .to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SoundConfig {
    pub file: String,
    pub volume: f32,
}

impl Default for SoundConfig {
    fn default() -> Self {
        Self {
            file: String::new(),
            volume: 0.8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ToastConfig {
    pub duration_secs: u64,
    pub critical_duration_secs: u64,
    pub max_visible: usize,
}

impl Default for ToastConfig {
    fn default() -> Self {
        Self {
            duration_secs: 6,
            critical_duration_secs: 12,
            max_visible: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LocalConfig {
    pub scan_roots: Vec<String>,
    pub scan_depth: usize,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            scan_roots: Vec::new(),
            scan_depth: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamConfig {
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub method: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    pub provider: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub bots: Vec<String>,
    /// Provider-specific keys.
    #[serde(flatten)]
    pub settings: toml::Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookAction {
    #[serde(rename = "open-url")]
    OpenUrl,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RunSpec {
    pub default: Option<Vec<String>>,
    pub windows: Option<Vec<String>>,
    pub macos: Option<Vec<String>>,
    pub linux: Option<Vec<String>>,
}

impl RunSpec {
    /// The per-OS argv if set, else the default.
    pub fn for_current_os(&self) -> Option<&Vec<String>> {
        let os = if cfg!(windows) {
            &self.windows
        } else if cfg!(target_os = "macos") {
            &self.macos
        } else {
            &self.linux
        };
        os.as_ref().or(self.default.as_ref())
    }

    fn all(&self) -> impl Iterator<Item = (&'static str, &Vec<String>)> {
        [
            ("default", &self.default),
            ("windows", &self.windows),
            ("macos", &self.macos),
            ("linux", &self.linux),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    One(String),
    Any(Vec<String>),
}

impl MatchValue {
    pub fn patterns(&self) -> &[String] {
        match self {
            MatchValue::One(s) => std::slice::from_ref(s),
            MatchValue::Any(v) => v,
        }
    }
}

pub type MatchTable = BTreeMap<String, MatchValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HookConfig {
    pub name: String,
    pub on: Vec<String>,
    pub action: Option<HookAction>,
    pub run: Option<RunSpec>,
    pub r#match: MatchTable,
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub catch_up: bool,
    pub respect_snooze: bool,
    pub include_test: bool,
    pub detached: bool,
    pub timeout_secs: u64,
    pub debounce_secs: u64,
}

impl Default for HookConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            on: Vec::new(),
            action: None,
            run: None,
            r#match: MatchTable::new(),
            cwd: None,
            env: BTreeMap::new(),
            catch_up: false,
            respect_snooze: false,
            include_test: true,
            detached: false,
            timeout_secs: 60,
            debounce_secs: 0,
        }
    }
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn default_toml() -> &'static str {
        DEFAULT_TOML
    }

    /// Writes the template if the file does not exist. Returns whether it wrote.
    pub fn ensure_file(path: &Path) -> std::io::Result<bool> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) => return Err(e),
        };
        std::io::Write::write_all(&mut file, DEFAULT_TOML.as_bytes())?;
        Ok(true)
    }

    pub fn load(path: &Path) -> Result<Config, Vec<ConfigError>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| vec![ConfigError::new(path.display().to_string(), e.to_string())])?;
        Self::from_toml_str(&text)
    }

    pub fn from_toml_str(text: &str) -> Result<Config, Vec<ConfigError>> {
        let cfg: Config = toml::from_str(text).map_err(|e| vec![parse_error(text, &e)])?;
        let errors = cfg.validate();
        if errors.is_empty() {
            Ok(cfg)
        } else {
            Err(errors)
        }
    }

    fn validate(&self) -> Vec<ConfigError> {
        let mut errs = Vec::new();

        if !(0.0..=1.0).contains(&self.sound.volume) {
            errs.push(ConfigError::new("sound.volume", "must be between 0 and 1"));
        }
        check_globs(&mut errs, "alerts.mute_types", &self.alerts.mute_types);
        check_globs(
            &mut errs,
            "roles.group_reviewer.alert_types",
            &self.roles.group_reviewer.alert_types,
        );

        let mut seen_ids: Vec<&str> = Vec::new();
        for (i, c) in self.connections.iter().enumerate() {
            let at = format!("connections[{i}]");
            let id_ok = !c.id.is_empty()
                && c.id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            if !id_ok {
                errs.push(ConfigError::new(
                    format!("{at}.id"),
                    format!("'{}' must match [a-z0-9-]+", c.id),
                ));
            }
            if seen_ids.contains(&c.id.as_str()) {
                errs.push(ConfigError::new(
                    format!("{at}.id"),
                    format!("duplicate connection id '{}'", c.id),
                ));
            }
            seen_ids.push(&c.id);
            if c.provider.trim().is_empty() {
                errs.push(ConfigError::new(
                    format!("{at}.provider"),
                    "provider is required",
                ));
            }
            check_globs(&mut errs, &format!("{at}.bots"), &c.bots);
        }

        let mut seen_names: Vec<&str> = Vec::new();
        for (i, h) in self.hooks.iter().enumerate() {
            let at = format!("hooks[{i}]");
            if h.name.trim().is_empty() {
                errs.push(ConfigError::new(format!("{at}.name"), "name is required"));
            } else if seen_names.contains(&h.name.as_str()) {
                errs.push(ConfigError::new(
                    format!("{at}.name"),
                    format!("duplicate hook name '{}'", h.name),
                ));
            }
            seen_names.push(&h.name);
            check_globs(&mut errs, &format!("{at}.on"), &h.on);
            match (&h.action, &h.run) {
                (Some(_), Some(_)) => {
                    errs.push(ConfigError::new(&at, "set either action or run, not both"))
                }
                (None, None) => {
                    errs.push(ConfigError::new(&at, "one of action or run is required"))
                }
                (None, Some(run)) => {
                    if run.all().next().is_none() {
                        errs.push(ConfigError::new(
                            format!("{at}.run"),
                            "needs at least one argv list (default, windows, macos or linux)",
                        ));
                    }
                    for (k, argv) in run.all() {
                        if argv.first().is_none_or(|a| a.is_empty()) {
                            errs.push(ConfigError::new(
                                format!("{at}.run.{k}"),
                                "argv must start with a program",
                            ));
                        } else if argv
                            .first()
                            .is_some_and(|a| !placeholder_paths(a).is_empty())
                        {
                            errs.push(ConfigError::new(
                                format!("{at}.run.{k}"),
                                "the program (first argv element) must be a literal, not a placeholder",
                            ));
                        }
                    }
                }
                (Some(_), None) => {}
            }
            for (name, value) in &h.env {
                if shell_executed_env(name) && !placeholder_paths(value).is_empty() {
                    errs.push(ConfigError::new(
                        format!("{at}.env.{name}"),
                        "a placeholder is not allowed in a variable that shells execute",
                    ));
                }
            }
            for (key, value) in &h.r#match {
                check_globs(&mut errs, &format!("{at}.match.{key}"), value.patterns());
            }
        }
        errs
    }
}

/// Env names whose value a shell runs as code or startup commands.
fn shell_executed_env(name: &str) -> bool {
    let upper = name.to_uppercase();
    upper.starts_with("BASH_FUNC_")
        || [
            "BASH_ENV",
            "ENV",
            "PS4",
            "PROMPT_COMMAND",
            "SHELLOPTS",
            "BASHOPTS",
        ]
        .contains(&upper.as_str())
}

fn check_globs(errs: &mut Vec<ConfigError>, path: &str, patterns: &[String]) {
    for p in patterns {
        if let Err(e) = build_glob(p) {
            errs.push(ConfigError::new(path, format!("invalid glob '{p}': {e}")));
        }
    }
}

fn parse_error(text: &str, e: &toml::de::Error) -> ConfigError {
    let path = match e.span() {
        Some(span) => {
            let upto = &text[..span.start.min(text.len())];
            let line = upto.matches('\n').count() + 1;
            let col = upto.len() - upto.rfind('\n').map_or(0, |i| i + 1) + 1;
            format!("line {line}, column {col}")
        }
        None => "file".to_string(),
    };
    ConfigError::new(path, e.message())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn err_paths(toml: &str) -> Vec<String> {
        Config::from_toml_str(toml)
            .expect_err("should be rejected")
            .into_iter()
            .map(|e| e.path)
            .collect()
    }

    #[test]
    fn default_template_parses_with_no_connections() {
        let cfg = Config::from_toml_str(Config::default_toml()).expect("template valid");
        assert!(cfg.connections.is_empty());
        assert_eq!(cfg.hooks.len(), 1);
        assert_eq!(cfg.hooks[0].action, Some(HookAction::OpenUrl));
        assert_eq!(cfg.polling, PollingConfig::default());
        assert_eq!(cfg.roles, RolesConfig::default());
        assert_eq!(cfg.toast, ToastConfig::default());
    }

    #[test]
    fn template_examples_are_valid_when_uncommented() {
        let mut out = Vec::new();
        let mut in_block = false;
        for line in Config::default_toml().lines() {
            if line.starts_with("# [[") {
                in_block = true;
            } else if line.is_empty() {
                in_block = false;
            }
            match line.strip_prefix("# ") {
                Some(rest) if in_block => out.push(rest),
                _ if in_block => {}
                _ => out.push(line),
            }
        }
        let cfg = Config::from_toml_str(&out.join("\n")).expect("examples valid");
        assert_eq!(cfg.connections.len(), 1);
        assert_eq!(cfg.hooks.len(), 2);
    }

    #[test]
    fn minimal_file_gets_defaults_and_hook_defaults() {
        let cfg = Config::from_toml_str(
            "[[hooks]]\nname = \"x\"\non = [\"pr.*\"]\nrun = { default = [\"echo\"] }\n",
        )
        .unwrap();
        let h = &cfg.hooks[0];
        assert!(h.include_test && !h.catch_up && !h.detached);
        assert_eq!((h.timeout_secs, h.debounce_secs), (60, 0));
        assert_eq!(cfg.sound.volume, 0.8);
    }

    #[test]
    fn provider_settings_are_flattened() {
        let cfg = Config::from_toml_str(
            r#"
[[connections]]
id = "conn-a"
provider = "fake"
auth = { method = "pat" }
bots = ["Build\\*"]
organization = "contoso"
projects = ["a", "b"]
"#,
        )
        .unwrap();
        let c = &cfg.connections[0];
        assert!(c.enabled);
        assert_eq!(c.auth.method, "pat");
        assert_eq!(c.bots, vec!["Build\\*"]);
        assert_eq!(c.settings["organization"].as_str(), Some("contoso"));
        assert_eq!(c.settings["projects"].as_array().map(|a| a.len()), Some(2));
        assert!(!c.settings.contains_key("id"));
    }

    #[test]
    fn build_bot_glob_matches_backslash_name() {
        let g = build_glob("Build\\*").unwrap().compile_matcher();
        assert!(g.is_match("Build\\Service"));
        assert!(g.is_match("build\\service"));
    }

    #[test]
    fn rejects_bad_connections() {
        let p = err_paths(
            r#"
[[connections]]
id = "Bad_Id"
provider = ""
bots = ["[oops"]
[[connections]]
id = "conn-dup"
provider = "fake"
[[connections]]
id = "conn-dup"
provider = "fake"
"#,
        );
        assert_eq!(
            p,
            vec![
                "connections[0].id",
                "connections[0].provider",
                "connections[0].bots",
                "connections[2].id"
            ]
        );
    }

    #[test]
    fn rejects_bad_hooks() {
        let p = err_paths(
            r#"
[[hooks]]
name = "a"
on = ["[x"]
action = "open-url"
run = { default = ["echo"] }
[[hooks]]
name = "a"
on = []
[[hooks]]
name = "b"
on = []
run = {}
[[hooks]]
name = "c"
on = []
action = "open-url"
match = { "pr.title" = ["ok", "[bad"] }
"#,
        );
        assert_eq!(
            p,
            vec![
                "hooks[0].on",
                "hooks[0]",
                "hooks[1].name",
                "hooks[1]",
                "hooks[2].run",
                "hooks[3].match.pr.title"
            ]
        );
    }

    #[test]
    fn rejects_placeholders_in_the_program_and_in_shell_executed_env() {
        let p = err_paths(
            r#"
[[hooks]]
name = "a"
on = []
run = { default = ["{pr.id}", "x"], windows = ["{{lit}}", "x"], linux = ["{ a }"] }
[[hooks]]
name = "b"
on = []
run = { default = ["echo"] }
env = { bash_env = "{pr.title}", PS4 = "{x}", BASH_FUNC_f = "{x}", ENV = "ok", SAFE = "{x}", PROMPT_COMMAND = "{{x}}" }
"#,
        );
        assert_eq!(
            p,
            vec![
                "hooks[0].run.default",
                "hooks[0].run.linux",
                "hooks[1].env.BASH_FUNC_f",
                "hooks[1].env.PS4",
                "hooks[1].env.bash_env",
            ]
        );
    }

    #[test]
    fn rejects_bad_globs_and_volume() {
        let p = err_paths(
            "[alerts]\nmute_types = [\"[\"]\n[roles.group_reviewer]\nalert_types = [\"[\"]\n[sound]\nvolume = 1.5\n",
        );
        assert_eq!(
            p,
            vec![
                "sound.volume",
                "alerts.mute_types",
                "roles.group_reviewer.alert_types"
            ]
        );
    }

    #[test]
    fn parse_error_reports_location() {
        let errs = Config::from_toml_str("[polling]\nfast_secs = \"x\"\n").unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].path.starts_with("line 2"), "{}", errs[0].path);
    }

    #[test]
    fn ensure_file_writes_template_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("config.toml");
        assert!(Config::ensure_file(&path).unwrap());
        assert!(!Config::ensure_file(&path).unwrap());
        assert!(Config::load(&path).is_ok());
    }

    #[test]
    fn ensure_file_keeps_existing_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path, "# mine
",
        )
        .unwrap();
        assert!(!Config::ensure_file(&path).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "# mine
"
        );
    }

    #[test]
    fn home_dir_honours_herald_home_without_touching_env() {
        let user = Some(PathBuf::from("/u"));
        let set = |k: &str| (k == "HERALD_HOME").then(|| "/custom".to_string());
        assert_eq!(home_dir_from(set, user.clone()), PathBuf::from("/custom"));
        let empty = |_: &str| Some(String::new());
        assert_eq!(
            home_dir_from(empty, user.clone()),
            PathBuf::from("/u").join(".config").join("herald")
        );
        assert_eq!(
            home_dir_from(|_| None, user),
            PathBuf::from("/u").join(".config").join("herald")
        );
    }

    #[test]
    fn runtime_dir_follows_os_rule() {
        let got = runtime_dir_from(
            |_| None,
            Some(PathBuf::from("/u")),
            Some(PathBuf::from("/local")),
        );
        if cfg!(windows) {
            assert_eq!(got, PathBuf::from("/local").join("herald"));
        } else {
            assert_eq!(got, PathBuf::from("/u").join(".config").join("herald"));
        }
    }
}
