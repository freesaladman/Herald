//! Provider factory and per-connection shared state for Azure DevOps.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use herald_core::config::{ConfigError, ConnectionConfig};
use herald_core::event::event_types as ev;
use herald_core::provider::{
    AuthMethodSpec, Connection, FieldKind, FieldSpec, Identity, ProviderError, ProviderFactory,
    RemoteRepo, ScopeOption, SecretSpec, Source, SourceInfo, Tier,
};
use herald_core::secrets::SecretStore;
use percent_encoding::percent_decode_str;
use tokio::sync::Mutex;
use url::Url;

use crate::auth::AdoAuth;
use crate::client::{AdoClient, Project};
use crate::sources::branches::BranchesSource;
use crate::sources::pipelines::PipelinesSource;
use crate::sources::pr_discovery::PrDiscoverySource;
use crate::sources::pr_policies::PrPoliciesSource;
use crate::sources::pr_reviews::PrReviewsSource;
use crate::sources::pr_threads::PrThreadsSource;
use crate::sources::prs::PrLifecycleSource;
use crate::sources::releases::ReleasesSource;

const KIND: &str = "ado";
const PROJECTS_TTL: Duration = Duration::from_secs(600);

pub struct AdoProviderFactory;

/// `^[A-Za-z0-9][A-Za-z0-9._-]*$`
fn is_valid_organization(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

impl ProviderFactory for AdoProviderFactory {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn secret_label(&self) -> &'static str {
        "Personal access token"
    }

    fn display_name(&self) -> &'static str {
        "Azure DevOps"
    }

    fn connection_fields(&self) -> Vec<FieldSpec> {
        vec![
            FieldSpec {
                key: "organization".into(),
                label: "Organization".into(),
                kind: FieldKind::Text,
                required: true,
                placeholder: Some("my-org".into()),
                help: Some("The name after dev.azure.com/".into()),
                advanced: false,
            },
            FieldSpec {
                key: "projects".into(),
                label: "Projects".into(),
                kind: FieldKind::Scope,
                required: false,
                placeholder: None,
                help: Some(
                    "Projects to watch. Leave all unticked to watch every project you can see."
                        .into(),
                ),
                advanced: false,
            },
        ]
    }

    fn default_bots(&self) -> Vec<String> {
        vec!["*Build Service*".into()]
    }

    fn auth_methods(&self) -> Vec<AuthMethodSpec> {
        vec![
            AuthMethodSpec {
                id: "pat".into(),
                label: "Personal access token".into(),
                help: None,
                secret: Some(SecretSpec {
                    label: "Personal access token".into(),
                    help: Some(
                        "Scopes: Code (Read), Build (Read), Release (Read), Identity (Read)".into(),
                    ),
                }),
            },
            AuthMethodSpec {
                id: "azcli".into(),
                label: "Azure CLI login".into(),
                help: Some("Uses az account get-access-token; nothing is stored".into()),
                secret: None,
            },
        ]
    }

    fn validate(&self, cfg: &ConnectionConfig) -> Vec<ConfigError> {
        let mut errors = Vec::new();
        match cfg.settings.get("organization") {
            None => errors.push(ConfigError::new("organization", "organization is required")),
            Some(v) => match v.as_str() {
                Some(s) if is_valid_organization(s.trim()) => {}
                Some(s) if !s.trim().is_empty() => errors.push(ConfigError::new(
                    "organization",
                    "organization may only contain letters, digits, '.', '_' and '-', and must start with a letter or digit",
                )),
                _ => errors.push(ConfigError::new(
                    "organization",
                    "organization must be a non-empty string",
                )),
            },
        }
        if let Some(v) = cfg.settings.get("projects") {
            let ok = v
                .as_array()
                .is_some_and(|a| a.iter().all(|p| p.as_str().is_some()));
            if !ok {
                errors.push(ConfigError::new(
                    "projects",
                    "projects must be an array of strings",
                ));
            }
        }
        if !matches!(cfg.auth.method.trim(), "" | "pat" | "azcli") {
            errors.push(ConfigError::new(
                "auth.method",
                format!(
                    "unknown auth method '{}', expected pat or azcli",
                    cfg.auth.method.trim()
                ),
            ));
        }
        errors
    }

    fn connect(
        &self,
        cfg: &ConnectionConfig,
        secrets: Arc<dyn SecretStore>,
    ) -> Result<Arc<dyn Connection>, ProviderError> {
        let organization = cfg
            .settings
            .get("organization")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ProviderError::Config("organization is required".into()))?
            .to_string();
        let projects = cfg
            .settings
            .get("projects")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|p| p.as_str())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let auth = AdoAuth::from_config(cfg, secrets)?;
        let auth_method = auth.method().to_string();
        let client = Arc::new(AdoClient::new(&organization, auth));
        Ok(Arc::new(AdoConnection::new(
            cfg.id.clone(),
            auth_method,
            AdoShared::new(client, organization, projects),
        )))
    }

    fn sources(&self) -> Vec<SourceInfo> {
        let info = |name, tier, event_types: &[&'static str]| SourceInfo {
            name,
            tier,
            event_types: event_types.to_vec(),
        };
        vec![
            info(
                "pr_lifecycle",
                Tier::Pr,
                &[
                    ev::PR_CREATED,
                    ev::PR_COMPLETED,
                    ev::PR_ABANDONED,
                    ev::PR_REACTIVATED,
                    ev::PR_CONFLICTS_DETECTED,
                    ev::PR_CONFLICTS_CLEARED,
                    ev::PR_MENTIONED,
                ],
            ),
            info("pr_discovery", Tier::Slow, &[ev::PR_MENTIONED]),
            info(
                "pr_threads",
                Tier::Pr,
                &[
                    ev::PR_COMMENT_ADDED,
                    ev::PR_COMMENT_REPLIED,
                    ev::PR_THREAD_STATUS_CHANGED,
                    ev::PR_MENTIONED,
                ],
            ),
            info(
                "pr_reviews",
                Tier::Pr,
                &[
                    ev::PR_PUBLISHED,
                    ev::PR_VOTE_CHANGED,
                    ev::PR_VOTES_RESET,
                    ev::PR_REVIEWER_ADDED,
                    ev::PR_REVIEWER_REMOVED,
                    ev::PR_AUTOCOMPLETE_SET,
                    ev::PR_AUTOCOMPLETE_CANCELLED,
                    ev::PR_ITERATION_PUSHED,
                    ev::PR_REREVIEW_NEEDED,
                ],
            ),
            info("pr_policies", Tier::Pr, &[ev::PR_POLICY_CHANGED]),
            info(
                "branches",
                Tier::Slow,
                &[
                    ev::BRANCH_CREATED,
                    ev::BRANCH_PUSHED,
                    ev::BRANCH_FORCEPUSHED,
                    ev::BRANCH_DELETED,
                ],
            ),
            info(
                "pipelines",
                Tier::Fast,
                &[
                    ev::RUN_COMPLETED,
                    ev::RUN_STAGE_FAILED,
                    ev::APPROVAL_PENDING,
                    ev::APPROVAL_COMPLETED,
                ],
            ),
            info(
                "releases",
                Tier::Fast,
                &[
                    ev::RELEASE_CREATED,
                    ev::DEPLOY_STARTED,
                    ev::DEPLOY_SUCCEEDED,
                    ev::DEPLOY_FAILED,
                    ev::DEPLOY_CANCELED,
                    ev::APPROVAL_PENDING,
                    ev::APPROVAL_COMPLETED,
                ],
            ),
        ]
    }

    fn parse_remote(&self, git_remote_url: &str) -> Option<RemoteRepo> {
        parse_remote(git_remote_url)
    }

    fn host_key(&self, cfg: &ConnectionConfig) -> Option<String> {
        let org = cfg.settings.get("organization")?.as_str()?.trim();
        (!org.is_empty()).then(|| org.to_lowercase())
    }
}

fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn remote(org: &str, project: &str, repo: &str) -> Option<RemoteRepo> {
    if org.is_empty() || project.is_empty() || repo.is_empty() {
        return None;
    }
    Some(RemoteRepo {
        provider: KIND.to_string(),
        host_key: decode(org).to_lowercase(),
        namespace: Some(decode(project)),
        repo_name: decode(repo),
    })
}

fn parse_remote(raw: &str) -> Option<RemoteRepo> {
    let trimmed = raw.trim().trim_end_matches('/');
    let trimmed = trimmed.strip_suffix(".git").unwrap_or(trimmed);

    for prefix in [
        "git@ssh.dev.azure.com:v3/",
        "ssh://git@ssh.dev.azure.com/v3/",
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let parts: Vec<&str> = rest.split('/').collect();
            return match parts[..] {
                [org, project, repo] => remote(org, project, repo),
                _ => None,
            };
        }
    }

    let url = Url::parse(trimmed).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.to_lowercase();
    let segments: Vec<&str> = url.path_segments()?.filter(|s| !s.is_empty()).collect();
    if host == "dev.azure.com" {
        return match segments[..] {
            [org, project, "_git", repo] => remote(org, project, repo),
            _ => None,
        };
    }
    let org = host.strip_suffix(".visualstudio.com")?;
    let segments = match segments.first() {
        Some(s) if s.eq_ignore_ascii_case("DefaultCollection") => &segments[1..],
        _ => &segments[..],
    };
    match segments {
        [project, "_git", repo] => remote(org, project, repo),
        _ => None,
    }
}

/// State every source of one connection shares.
pub struct AdoShared {
    pub client: Arc<AdoClient>,
    pub organization: String,
    /// Configured project names; empty means all projects.
    pub configured_projects: Vec<String>,
    projects_cache: Mutex<Option<(Instant, Vec<Project>)>>,
}

impl AdoShared {
    pub fn new(
        client: Arc<AdoClient>,
        organization: String,
        configured_projects: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            organization,
            configured_projects,
            projects_cache: Mutex::new(None),
        })
    }

    /// Configured projects (names resolved to ids) or every project, cached for the Slow tier.
    pub async fn projects(&self) -> Result<Vec<Project>, ProviderError> {
        let mut cache = self.projects_cache.lock().await;
        if let Some((at, list)) = cache.as_ref()
            && at.elapsed() < PROJECTS_TTL
        {
            return Ok(list.clone());
        }
        let all = self.client.projects().await?;
        let list = if self.configured_projects.is_empty() {
            all
        } else {
            let mut picked = Vec::new();
            for name in &self.configured_projects {
                match all.iter().find(|p| p.name.eq_ignore_ascii_case(name)) {
                    Some(p) => picked.push(p.clone()),
                    None => tracing::warn!(project = %name, "configured project not found"),
                }
            }
            picked
        };
        *cache = Some((Instant::now(), list.clone()));
        Ok(list)
    }
}

/// Project names are the stored value: `AdoShared::projects` matches config entries by name.
fn project_scopes(projects: Vec<Project>) -> Vec<ScopeOption> {
    let mut options: Vec<ScopeOption> = projects
        .into_iter()
        .filter(|p| {
            p.state
                .as_deref()
                .is_none_or(|s| s.eq_ignore_ascii_case("wellFormed"))
        })
        .map(|p| ScopeOption {
            id: p.name.clone(),
            name: p.name,
        })
        .collect();
    options.sort_by_key(|o| o.name.to_lowercase());
    options
}

pub struct AdoConnection {
    id: String,
    auth_method: String,
    shared: Arc<AdoShared>,
}

impl AdoConnection {
    pub fn new(id: String, auth_method: String, shared: Arc<AdoShared>) -> Self {
        Self {
            id,
            auth_method,
            shared,
        }
    }

    pub fn shared(&self) -> &Arc<AdoShared> {
        &self.shared
    }
}

impl Connection for AdoConnection {
    fn id(&self) -> &str {
        &self.id
    }

    fn provider(&self) -> &'static str {
        KIND
    }

    fn auth_method(&self) -> &str {
        &self.auth_method
    }

    fn identify(&self) -> BoxFuture<'_, Result<Identity, ProviderError>> {
        Box::pin(async move { Ok(self.shared.client.identify().await?) })
    }

    fn server_time(&self) -> BoxFuture<'_, Result<DateTime<Utc>, ProviderError>> {
        Box::pin(async move { Ok(self.shared.client.server_time().await?) })
    }

    fn sources(&self) -> Vec<Box<dyn Source>> {
        // Fixed order; each source task appends one constructor here.
        vec![
            Box::new(PrLifecycleSource::new(self.shared.clone())),
            Box::new(PrDiscoverySource::new(self.shared.clone())),
            Box::new(PrThreadsSource::new(self.shared.clone())),
            Box::new(PrReviewsSource::new(self.shared.clone())),
            Box::new(PrPoliciesSource::new(self.shared.clone())),
            Box::new(BranchesSource::new(self.shared.clone())),
            Box::new(PipelinesSource::new(self.shared.clone())),
            Box::new(ReleasesSource::new(self.shared.clone())),
        ]
    }

    fn credentials_changed(&self) {
        self.shared.client.invalidate_auth();
    }

    fn list_scopes<'a>(
        &'a self,
        field_key: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ScopeOption>, ProviderError>> {
        Box::pin(async move {
            if field_key != "projects" {
                return Err(ProviderError::Config(format!(
                    "unknown scope field {field_key}"
                )));
            }
            Ok(project_scopes(self.shared.client.projects().await?))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use herald_core::config::Config;
    use herald_core::provider::ProviderRegistry;

    fn conn(extra: &str, auth: &str) -> ConnectionConfig {
        let text = format!(
            "[[connections]]\nid = \"c1\"\nprovider = \"ado\"\nauth = {{ method = \"{auth}\" }}\n{extra}\n"
        );
        Config::from_toml_str(&text).unwrap().connections.remove(0)
    }

    #[test]
    fn descriptor_serialises_camel_case() {
        let reg = ProviderRegistry::new(vec![Arc::new(AdoProviderFactory)]);
        let json = serde_json::to_value(reg.descriptors()).unwrap();
        let expected = serde_json::json!([{
            "kind": "ado",
            "displayName": "Azure DevOps",
            "fields": [
                {
                    "key": "organization",
                    "label": "Organization",
                    "kind": "text",
                    "required": true,
                    "placeholder": "my-org",
                    "help": "The name after dev.azure.com/",
                    "advanced": false
                },
                {
                    "key": "projects",
                    "label": "Projects",
                    "kind": "scope",
                    "required": false,
                    "placeholder": null,
                    "help": "Projects to watch. Leave all unticked to watch every project you can see.",
                    "advanced": false
                }
            ],
            "authMethods": [
                {
                    "id": "pat",
                    "label": "Personal access token",
                    "help": null,
                    "secret": {
                        "label": "Personal access token",
                        "help": "Scopes: Code (Read), Build (Read), Release (Read), Identity (Read)"
                    }
                },
                {
                    "id": "azcli",
                    "label": "Azure CLI login",
                    "help": "Uses az account get-access-token; nothing is stored",
                    "secret": null
                }
            ],
            "defaultBots": ["*Build Service*"],
            "botsHelp": "Activity from these accounts won't alert you. Globs on the account's unique or display name."
        }]);
        assert_eq!(json, expected);
    }

    fn paths(errors: Vec<ConfigError>) -> Vec<String> {
        errors.into_iter().map(|e| e.path).collect()
    }

    #[test]
    fn validate_accepts_good_and_flags_bad_fields() {
        let f = AdoProviderFactory;
        assert!(
            f.validate(&conn(
                "organization = \"contoso\"\nprojects = [\"a\"]",
                "pat"
            ))
            .is_empty()
        );
        assert!(
            f.validate(&conn("organization = \"contoso\"", ""))
                .is_empty()
        );
        assert!(
            f.validate(&conn("organization = \"contoso\"", "pat "))
                .is_empty()
        );
        assert_eq!(paths(f.validate(&conn("", "pat"))), ["organization"]);
        assert_eq!(
            paths(f.validate(&conn("organization = \"\"", "pat"))),
            ["organization"]
        );
        for bad in ["a/b", "-lead", ".lead", "a b", "a?x=1", "a#b", "a%2Fb"] {
            assert_eq!(
                paths(f.validate(&conn(&format!("organization = \"{bad}\""), "pat"))),
                ["organization"],
                "{bad}"
            );
        }
        assert!(
            f.validate(&conn("organization = \"my.org_1-x\"", "pat"))
                .is_empty()
        );
        assert_eq!(
            paths(f.validate(&conn("organization = \"contoso\"", "oauth"))),
            ["auth.method"]
        );
        assert_eq!(
            paths(f.validate(&conn("organization = \"contoso\"\nprojects = \"a\"", "pat"))),
            ["projects"]
        );
        assert_eq!(
            paths(f.validate(&conn("organization = \"contoso\"\nprojects = [1]", "pat"))),
            ["projects"]
        );
    }

    #[test]
    fn validate_paths_are_relative_to_the_connection() {
        let registry = ProviderRegistry::new(vec![Arc::new(AdoProviderFactory)]);
        let cfg = Config::from_toml_str(
            "[[connections]]\nid = \"c1\"\nprovider = \"ado\"\nauth = { method = \"pat\" }\n",
        )
        .unwrap();
        let errors = cfg.validate_providers(&registry);
        assert_eq!(paths(errors), ["connections[0].organization"]);
    }

    #[test]
    fn parse_remote_handles_every_url_form() {
        let want = |ns: &str, repo: &str| {
            Some(RemoteRepo {
                provider: "ado".into(),
                host_key: "contoso".into(),
                namespace: Some(ns.into()),
                repo_name: repo.into(),
            })
        };
        let f = AdoProviderFactory;
        for url in [
            "https://user@dev.azure.com/Contoso/Proj/_git/repo",
            "https://dev.azure.com/contoso/Proj/_git/repo.git",
            "https://dev.azure.com/contoso/Proj/_git/repo/",
            "https://contoso.visualstudio.com/Proj/_git/repo",
            "https://contoso.visualstudio.com/DefaultCollection/Proj/_git/repo",
            "git@ssh.dev.azure.com:v3/contoso/Proj/repo",
            "git@ssh.dev.azure.com:v3/contoso/Proj/repo.git",
        ] {
            assert_eq!(f.parse_remote(url), want("Proj", "repo"), "{url}");
        }
        assert_eq!(
            f.parse_remote("https://dev.azure.com/contoso/My%20Proj/_git/my%20repo.git"),
            want("My Proj", "my repo")
        );
    }

    #[test]
    fn host_key_is_lowercased_organization_matching_parse_remote() {
        let f = AdoProviderFactory;
        let cfg = |org: &str| {
            Config::from_toml_str(&format!(
                "[[connections]]
id = \"c1\"
provider = \"ado\"
auth = {{ method = \"pat\" }}
organization = \"{org}\"
"
            ))
            .unwrap()
            .connections
            .remove(0)
        };
        let key = f.host_key(&cfg("  Contoso "));
        assert_eq!(key.as_deref(), Some("contoso"));
        for url in [
            "https://dev.azure.com/Contoso/Proj/_git/repo",
            "https://contoso.visualstudio.com/Proj/_git/repo",
        ] {
            assert_eq!(f.parse_remote(url).map(|r| r.host_key), key, "{url}");
        }
        let mut no_org = cfg("x");
        no_org.settings.remove("organization");
        assert_eq!(f.host_key(&no_org), None);
    }

    #[test]
    fn parse_remote_rejects_other_hosts() {
        let f = AdoProviderFactory;
        for url in [
            "https://github.com/contoso/repo.git",
            "git@github.com:contoso/repo.git",
            "https://dev.azure.com/contoso/Proj",
            "file:///tmp/repo",
            "not a url",
        ] {
            assert_eq!(f.parse_remote(url), None, "{url}");
        }
    }

    fn mock_connection(server: &wiremock::MockServer) -> AdoConnection {
        let secrets = herald_core::secrets::MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&conn("organization = \"org\"", "pat"), Arc::new(secrets))
            .unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &format!("{u}/vssps"));
        AdoConnection::new(
            "c1".into(),
            "pat".into(),
            AdoShared::new(Arc::new(client), "org".into(), Vec::new()),
        )
    }

    #[tokio::test]
    async fn declared_sources_match_the_connection_sources() {
        let server = wiremock::MockServer::start().await;
        let actual: Vec<_> = mock_connection(&server)
            .sources()
            .iter()
            .map(|s| (s.name(), s.tier()))
            .collect();
        let declared: Vec<_> = AdoProviderFactory
            .sources()
            .iter()
            .map(|s| (s.name, s.tier))
            .collect();
        assert_eq!(declared, actual);
        assert!(
            AdoProviderFactory
                .sources()
                .iter()
                .all(|s| !s.event_types.is_empty())
        );
    }

    #[tokio::test]
    async fn list_scopes_projects_pages_filters_and_sorts_by_name() {
        use wiremock::matchers::{method, path, query_param, query_param_is_missing};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_apis/projects"))
            .and(query_param_is_missing("continuationToken"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "count": 2, "value": [
                        { "id": "1", "name": "zeta", "state": "wellFormed" },
                        { "id": "2", "name": "Gone", "state": "deleting" }
                    ]}))
                    .insert_header("x-ms-continuationtoken", "next"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/_apis/projects"))
            .and(query_param("continuationToken", "next"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 2, "value": [
                    { "id": "3", "name": "Alpha", "state": "wellFormed" },
                    { "id": "4", "name": "mid", "state": "wellFormed" }
                ]
            })))
            .mount(&server)
            .await;
        let got = mock_connection(&server)
            .list_scopes("projects")
            .await
            .unwrap();
        let pairs: Vec<_> = got
            .iter()
            .map(|o| (o.id.as_str(), o.name.as_str()))
            .collect();
        assert_eq!(
            pairs,
            [("Alpha", "Alpha"), ("mid", "mid"), ("zeta", "zeta")]
        );
    }

    #[tokio::test]
    async fn list_scopes_rejects_unknown_field() {
        let server = wiremock::MockServer::start().await;
        assert!(matches!(
            mock_connection(&server).list_scopes("nope").await,
            Err(ProviderError::Config(_))
        ));
    }
}
