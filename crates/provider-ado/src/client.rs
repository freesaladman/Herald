//! HTTP client for Azure DevOps: GET helpers with retry, paging and error mapping.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use herald_core::provider::{Identity, ProviderError};
use percent_encoding::{AsciiSet, CONTROLS, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, DATE, HeaderMap, HeaderValue, LOCATION, RETRY_AFTER};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::auth::AdoAuth;

const DEFAULT_API_VERSION: &str = "7.1";
const CONNECTION_DATA: &str = "_apis/connectionData?api-version=7.1-preview";
const CONTINUATION_HEADER: &str = "x-ms-continuationtoken";
const MAX_RETRIES: u32 = 3;
const MAX_RETRY_AFTER_SECS: u64 = 300;
const EXCERPT_CHARS: usize = 300;
const MAX_TOTAL_THROTTLE_SLEEP: Duration = Duration::from_secs(60);
const MAX_DESCRIPTOR_QUERY_CHARS: usize = 3_500;
const SUPPRESS_REDIRECT_HEADER: &str = "x-tfs-fedauthredirect";

/// Characters that must be escaped in a query value, on top of non-ASCII (always escaped).
const QUERY_VALUE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'%')
    .add(b'&')
    .add(b'#')
    .add(b'+')
    .add(b';')
    .add(b',');

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Host {
    Dev,
    Vsrm,
    Vssps,
}

/// Path-segment encoding: RFC 3986 unreserved characters stay as they are, so GUIDs and
/// hyphenated names read normally in URLs and logs.
const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'.')
    .remove(b'-')
    .remove(b'_')
    .remove(b'~');

/// Percent-encodes one URL path segment (project, repository, organization names and ids).
pub(crate) fn encode_segment(s: &str) -> String {
    utf8_percent_encode(s, PATH_SEGMENT).to_string()
}

/// Safety net against a server that ignores the paging parameters.
const MAX_PAGES: usize = 1000;

fn too_many_pages() -> AdoError {
    AdoError::TooManyPages { pages: MAX_PAGES }
}

#[derive(Debug, thiserror::Error)]
pub enum AdoError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("throttled, retry after {retry_after:?}")]
    Throttled { retry_after: Duration },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("http {status}: {body_excerpt}")]
    Http { status: u16, body_excerpt: String },
    #[error("paging stopped after {pages} pages")]
    TooManyPages { pages: usize },
    #[error("server repeated a continuation token")]
    PagingCycle,
    #[error("could not decode response from {path}: {detail}")]
    Decode { path: String, detail: String },
    #[error(transparent)]
    Auth(ProviderError),
}

impl From<AdoError> for ProviderError {
    fn from(e: AdoError) -> Self {
        match e {
            AdoError::Unauthorized => ProviderError::Unauthorized,
            AdoError::Forbidden(m) => ProviderError::Forbidden(m),
            AdoError::NotFound(m) => ProviderError::NotFound(m),
            AdoError::Throttled { retry_after } => ProviderError::Throttled { retry_after },
            AdoError::Transport(m) => ProviderError::Transient(m),
            AdoError::Http { status, .. } if status >= 500 => {
                ProviderError::Transient(format!("http {status}"))
            }
            AdoError::Http { .. }
            | AdoError::TooManyPages { .. }
            | AdoError::PagingCycle
            | AdoError::Decode { .. } => ProviderError::Other(e.to_string()),
            AdoError::Auth(p) => p,
        }
    }
}

/// The `{ "count", "value": [...] }` envelope used by list endpoints.
#[derive(Debug, Deserialize)]
pub struct ListResponse<T> {
    #[serde(default)]
    pub count: Option<u64>,
    #[serde(default = "Vec::new")]
    pub value: Vec<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
}

pub struct AdoClient {
    organization: String,
    auth: AdoAuth,
    http: reqwest::Client,
    dev: String,
    vsrm: String,
    vssps: String,
    membership_warned: AtomicBool,
    descriptor_warned: AtomicBool,
    pub(crate) threads_cache: crate::git::ThreadsCache,
}

impl AdoClient {
    /// Production constructor: PR threads are cached briefly so the Pr-tier sources share one fetch.
    pub fn new(organization: &str, auth: AdoAuth) -> AdoClient {
        let org = encode_segment(organization);
        let mut client = Self::with_base_urls(
            organization,
            auth,
            &format!("https://dev.azure.com/{org}"),
            &format!("https://vsrm.dev.azure.com/{org}"),
            &format!("https://vssps.dev.azure.com/{org}"),
        );
        client.threads_cache = crate::git::ThreadsCache::new(crate::git::THREADS_CACHE_TTL);
        client
    }

    pub fn with_base_urls(
        organization: &str,
        auth: AdoAuth,
        dev: &str,
        vsrm: &str,
        vssps: &str,
    ) -> AdoClient {
        let http = reqwest::Client::builder()
            .gzip(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .user_agent("herald")
            .build()
            .unwrap_or_else(|e| {
                tracing::error!("falling back to default http client: {e}");
                reqwest::Client::new()
            });
        let trim = |s: &str| s.trim_end_matches('/').to_string();
        AdoClient {
            organization: organization.to_string(),
            auth,
            http,
            dev: trim(dev),
            vsrm: trim(vsrm),
            vssps: trim(vssps),
            membership_warned: AtomicBool::new(false),
            descriptor_warned: AtomicBool::new(false),
            // Off here so tests that rewrite mocks between polls see fresh data.
            threads_cache: crate::git::ThreadsCache::new(Duration::ZERO),
        }
    }

    pub fn organization(&self) -> &str {
        &self.organization
    }

    pub fn invalidate_auth(&self) {
        self.auth.invalidate();
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        host: Host,
        path_and_query: &str,
    ) -> Result<T, AdoError> {
        let (_, body) = self.get_raw(host, path_and_query).await?;
        decode(&body, path_and_query)
    }

    /// Follows `x-ms-continuationtoken` until the header is absent.
    pub async fn get_paged<T: DeserializeOwned>(
        &self,
        host: Host,
        path_and_query: &str,
    ) -> Result<Vec<T>, AdoError> {
        let mut all = Vec::new();
        let mut seen = HashSet::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let pq = match &token {
                Some(t) => append_query(
                    path_and_query,
                    &format!(
                        "continuationToken={}",
                        utf8_percent_encode(t, NON_ALPHANUMERIC)
                    ),
                ),
                None => path_and_query.to_string(),
            };
            let (headers, body) = self.get_raw(host, &pq).await?;
            let page: ListResponse<T> = decode(&body, path_and_query)?;
            all.extend(page.value);
            token = headers
                .get(CONTINUATION_HEADER)
                .and_then(|v| v.to_str().ok())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            match &token {
                None => return Ok(all),
                Some(t) if !seen.insert(t.clone()) => {
                    return Err(AdoError::PagingCycle);
                }
                Some(_) => {}
            }
        }
        Err(too_many_pages())
    }

    /// For endpoints that page with `$top`/`$skip`; stops on a short page.
    pub async fn get_top_skip<T: DeserializeOwned>(
        &self,
        host: Host,
        path_and_query: &str,
        page_size: usize,
    ) -> Result<Vec<T>, AdoError> {
        let page_size = page_size.max(1);
        let mut all = Vec::new();
        let mut skip = 0usize;
        for _ in 0..MAX_PAGES {
            let pq = append_query(path_and_query, &format!("$top={page_size}&$skip={skip}"));
            let page: ListResponse<T> = self.get_json(host, &pq).await?;
            let n = page.value.len();
            all.extend(page.value);
            if n < page_size {
                return Ok(all);
            }
            skip += n;
        }
        Err(too_many_pages())
    }

    pub async fn server_time(&self) -> Result<DateTime<Utc>, AdoError> {
        let (headers, _) = self.get_raw(Host::Dev, CONNECTION_DATA).await?;
        headers
            .get(DATE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .ok_or_else(|| AdoError::Decode {
                path: "_apis/connectionData".to_string(),
                detail: "missing Date header".to_string(),
            })
    }

    pub async fn identify(&self) -> Result<Identity, AdoError> {
        let data: ConnectionData = self.get_json(Host::Dev, CONNECTION_DATA).await?;
        let user = data.authenticated_user;
        let unique_name = user
            .properties
            .as_ref()
            .and_then(|p| p.account.as_ref())
            .and_then(|a| a.value.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| user.provider_display_name.clone());
        let group_ids = match self.group_ids(&user.id).await {
            Ok(ids) => ids,
            Err(AdoError::Unauthorized | AdoError::Forbidden(_) | AdoError::NotFound(_)) => {
                if !self.membership_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!("group membership lookup not permitted; group roles disabled");
                }
                Vec::new()
            }
            Err(e) => return Err(e),
        };
        Ok(Identity {
            id: user.id,
            display_name: user.provider_display_name,
            unique_name,
            group_ids,
        })
    }

    pub async fn projects(&self) -> Result<Vec<Project>, AdoError> {
        self.get_paged(Host::Dev, "_apis/projects").await
    }

    async fn group_ids(&self, me: &str) -> Result<Vec<String>, AdoError> {
        let list: ListResponse<MembershipIdentity> = self
            .get_json(
                Host::Vssps,
                &format!(
                    "_apis/identities?identityIds={me}&queryMembership=Expanded&api-version=7.1"
                ),
            )
            .await?;
        let descriptors: Vec<String> = list.value.into_iter().flat_map(|i| i.member_of).collect();
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for joined in descriptor_queries(&descriptors) {
            let resolved: ListResponse<Option<IdOnly>> = match self
                .get_json(
                    Host::Vssps,
                    &format!("_apis/identities?descriptors={joined}&api-version=7.1"),
                )
                .await
            {
                Ok(r) => r,
                Err(AdoError::NotFound(_) | AdoError::Unauthorized | AdoError::Forbidden(_)) => {
                    if !self.descriptor_warned.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            "descriptor resolve request was refused or not found; skipping that chunk"
                        );
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            ids.extend(
                resolved
                    .value
                    .into_iter()
                    .flatten()
                    .map(|i| i.id)
                    .filter(|id| seen.insert(id.clone())),
            );
        }
        Ok(ids)
    }

    fn base(&self, host: Host) -> &str {
        match host {
            Host::Dev => &self.dev,
            Host::Vsrm => &self.vsrm,
            Host::Vssps => &self.vssps,
        }
    }

    async fn get_raw(
        &self,
        host: Host,
        path_and_query: &str,
    ) -> Result<(HeaderMap, Vec<u8>), AdoError> {
        let mut url = format!(
            "{}/{}",
            self.base(host),
            path_and_query.trim_start_matches('/')
        );
        if !path_and_query.contains("api-version=") {
            url = append_query(&url, &format!("api-version={DEFAULT_API_VERSION}"));
        }
        let mut attempt = 0u32;
        let mut slept = Duration::ZERO;
        loop {
            let mut auth =
                HeaderValue::from_str(&self.auth.authorization().await.map_err(AdoError::Auth)?)
                    .map_err(|_| {
                        AdoError::Auth(ProviderError::Other("invalid authorization".into()))
                    })?;
            auth.set_sensitive(true);
            let resp = self
                .http
                .get(&url)
                .header(AUTHORIZATION, auth)
                .header(ACCEPT, "application/json")
                .header(SUPPRESS_REDIRECT_HEADER, "Suppress")
                .send()
                .await
                .map_err(transport_error)?;
            let status = resp.status();
            let headers = resp.headers().clone();
            // Bad credentials redirect to a sign-in page (302, or 203 html when followed).
            if status.is_redirection() {
                return Err(if is_sign_in_redirect(&headers) {
                    AdoError::Unauthorized
                } else {
                    AdoError::Http {
                        status: status.as_u16(),
                        body_excerpt: "redirect".to_string(),
                    }
                });
            }
            if status == StatusCode::NON_AUTHORITATIVE_INFORMATION {
                return Err(AdoError::Unauthorized);
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE
            {
                let wait = retry_after(&headers, attempt);
                if attempt >= MAX_RETRIES || slept + wait > MAX_TOTAL_THROTTLE_SLEEP {
                    return Err(AdoError::Throttled { retry_after: wait });
                }
                tracing::debug!(
                    status = status.as_u16(),
                    ?wait,
                    "retrying throttled request"
                );
                tokio::time::sleep(wait).await;
                slept += wait;
                attempt += 1;
                continue;
            }
            let body = resp.bytes().await.map_err(transport_error)?.to_vec();
            if status.is_success() {
                return Ok((headers, body));
            }
            let excerpt = excerpt(&body);
            return Err(match status {
                StatusCode::UNAUTHORIZED => AdoError::Unauthorized,
                StatusCode::FORBIDDEN => AdoError::Forbidden(excerpt),
                StatusCode::NOT_FOUND => AdoError::NotFound(path_only(path_and_query).to_string()),
                _ => AdoError::Http {
                    status: status.as_u16(),
                    body_excerpt: excerpt,
                },
            });
        }
    }
}

#[derive(Deserialize)]
struct ConnectionData {
    #[serde(rename = "authenticatedUser")]
    authenticated_user: AuthenticatedUser,
}

#[derive(Deserialize)]
struct AuthenticatedUser {
    id: String,
    #[serde(rename = "providerDisplayName", default)]
    provider_display_name: String,
    properties: Option<UserProperties>,
}

#[derive(Deserialize)]
struct UserProperties {
    #[serde(rename = "Account")]
    account: Option<PropertyValue>,
}

#[derive(Deserialize)]
struct PropertyValue {
    #[serde(rename = "$value")]
    value: Option<String>,
}

#[derive(Deserialize)]
struct MembershipIdentity {
    #[serde(rename = "memberOf", default)]
    member_of: Vec<String>,
}

#[derive(Deserialize)]
struct IdOnly {
    id: String,
}

/// A missing `Location` counts as a sign-in redirect; other targets must look like one.
fn is_sign_in_redirect(headers: &HeaderMap) -> bool {
    let Some(location) = headers.get(LOCATION) else {
        return true;
    };
    let location = String::from_utf8_lossy(location.as_bytes()).to_lowercase();
    [
        "_signin",
        "login.microsoftonline",
        "spsprodsignin",
        "/_apis/authentication",
    ]
    .iter()
    .any(|m| location.contains(m))
}

fn transport_error(e: reqwest::Error) -> AdoError {
    let e = e.without_url();
    let mut msg = e.to_string();
    let mut source = std::error::Error::source(&e);
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    AdoError::Transport(msg)
}

/// Joins encoded descriptors with commas into query values under the length cap.
fn descriptor_queries(descriptors: &[String]) -> Vec<String> {
    let mut queries = Vec::new();
    let mut current = String::new();
    for d in descriptors {
        let encoded = utf8_percent_encode(d, QUERY_VALUE).to_string();
        if !current.is_empty() && current.len() + 1 + encoded.len() > MAX_DESCRIPTOR_QUERY_CHARS {
            queries.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push(',');
        }
        current.push_str(&encoded);
    }
    if !current.is_empty() {
        queries.push(current);
    }
    queries
}

fn decode<T: DeserializeOwned>(body: &[u8], path_and_query: &str) -> Result<T, AdoError> {
    let mut de = serde_json::Deserializer::from_slice(body);
    serde_path_to_error::deserialize(&mut de).map_err(|e| decode_error(path_and_query, &e))
}

/// Carries serde's message with the field path and line/column; never the response body.
pub(crate) fn decode_error(
    path_and_query: &str,
    e: &serde_path_to_error::Error<serde_json::Error>,
) -> AdoError {
    AdoError::Decode {
        path: path_only(path_and_query).to_string(),
        detail: format!("{} (at {})", e.inner(), e.path()),
    }
}

fn path_only(pq: &str) -> &str {
    pq.split('?').next().unwrap_or(pq)
}

fn append_query(pq: &str, extra: &str) -> String {
    let sep = if pq.contains('?') { '&' } else { '?' };
    format!("{pq}{sep}{extra}")
}

fn excerpt(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .chars()
        .take(EXCERPT_CHARS)
        .collect()
}

// Missing header backs off 1s, 2s, 4s.
fn retry_after(headers: &HeaderMap, attempt: u32) -> Duration {
    let secs = headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or_else(|| 1u64 << attempt.min(8));
    Duration::from_secs(secs.min(MAX_RETRY_AFTER_SECS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use serde_json::json;
    use std::sync::Arc;
    use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const PAT: &str = "super-secret-pat";

    fn basic_pat() -> String {
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!(":{PAT}"),
        )
    }

    fn test_auth() -> AdoAuth {
        let secrets = MemorySecretStore::new();
        secrets.set("conn", PAT).unwrap();
        let cfg = ConnectionConfig {
            id: "conn".into(),
            provider: "ado".into(),
            enabled: true,
            auth: AuthConfig {
                method: "pat".into(),
            },
            bots: Vec::new(),
            settings: Default::default(),
        };
        AdoAuth::from_config(&cfg, Arc::new(secrets)).unwrap()
    }

    fn client(server: &MockServer) -> AdoClient {
        let u = server.uri();
        AdoClient::with_base_urls("org", test_auth(), &u, &u, &format!("{u}/vssps"))
    }

    fn list(values: serde_json::Value) -> ResponseTemplate {
        let count = values.as_array().map_or(0, Vec::len);
        ResponseTemplate::new(200).set_body_json(json!({ "count": count, "value": values }))
    }

    #[tokio::test]
    async fn decode_error_names_the_field_path_and_position_but_not_the_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/P/_apis/things"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"value":[{"id":1,"inner":{"name":{"secret":"hunter2"}}}]}"#,
                ),
            )
            .mount(&server)
            .await;
        #[derive(Debug, Deserialize)]
        struct Thing {
            #[allow(dead_code)]
            inner: Inner,
        }
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[allow(dead_code)]
            name: String,
        }
        let err = client(&server)
            .get_paged::<Thing>(Host::Dev, "P/_apis/things?api-version=7.1")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("could not decode response from P/_apis/things: "),
            "{err}"
        );
        assert!(
            err.contains("invalid type: map, expected a string"),
            "{err}"
        );
        assert!(err.contains("value[0].inner.name"), "{err}");
        assert!(err.contains("line 1 column"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[test]
    fn segments_keep_hyphens_and_encode_spaces() {
        assert_eq!(
            encode_segment("6f1c2a9e-0b7d-4e3a-9c58-2d4f7a1e8b30"),
            "6f1c2a9e-0b7d-4e3a-9c58-2d4f7a1e8b30"
        );
        assert_eq!(encode_segment("Project One"), "Project%20One");
        assert_eq!(encode_segment("a/b"), "a%2Fb");
    }

    #[tokio::test]
    async fn sends_basic_auth_and_default_api_version() {
        let server = MockServer::start().await;
        let expected = format!("Basic {}", basic_pat());
        Mock::given(method("GET"))
            .and(path("/_apis/thing"))
            .and(header("authorization", expected.as_str()))
            .and(query_param("api-version", "7.1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"a": 1})))
            .expect(1)
            .mount(&server)
            .await;
        let v: serde_json::Value = client(&server)
            .get_json(Host::Dev, "_apis/thing")
            .await
            .unwrap();
        assert_eq!(v["a"], 1);
    }

    #[tokio::test]
    async fn status_codes_map_to_errors() {
        let server = MockServer::start().await;
        for (p, status, body) in [
            ("/a", 401, ""),
            ("/b", 403, "no access"),
            ("/c", 404, ""),
            ("/d", 500, "boom"),
        ] {
            Mock::given(path(p))
                .respond_with(ResponseTemplate::new(status).set_body_string(body))
                .mount(&server)
                .await;
        }
        let c = client(&server);
        let get = |p: &'static str| c.get_json::<serde_json::Value>(Host::Dev, p);
        assert!(matches!(get("a").await, Err(AdoError::Unauthorized)));
        assert!(matches!(get("b").await, Err(AdoError::Forbidden(m)) if m == "no access"));
        assert!(matches!(get("c").await, Err(AdoError::NotFound(_))));
        let e = get("d").await.unwrap_err();
        assert!(matches!(&e, AdoError::Http { status: 500, .. }));
        assert!(matches!(
            ProviderError::from(e),
            ProviderError::Transient(_)
        ));
    }

    #[tokio::test]
    async fn redirect_and_203_map_to_unauthorized() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/redir"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", "/_signin?realm=x"))
            .mount(&server)
            .await;
        Mock::given(path("/_apis/html"))
            .respond_with(
                ResponseTemplate::new(203)
                    .insert_header("Content-Type", "text/html")
                    .set_body_string("<html>sign in</html>"),
            )
            .mount(&server)
            .await;
        Mock::given(path("/_apis/connectionData"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Date", "Fri, 20 Nov 2026 10:30:00 GMT")
                    .insert_header("Location", "/_signin?realm=x"),
            )
            .mount(&server)
            .await;
        let c = client(&server);
        for p in ["_apis/redir", "_apis/html"] {
            let r = c.get_json::<serde_json::Value>(Host::Dev, p).await;
            assert!(matches!(r, Err(AdoError::Unauthorized)), "{p}");
        }
        assert!(matches!(c.server_time().await, Err(AdoError::Unauthorized)));
        server.reset().await;
        Mock::given(path("/_apis/connectionData"))
            .respond_with(
                ResponseTemplate::new(203).insert_header("Date", "Fri, 20 Nov 2026 10:30:00 GMT"),
            )
            .mount(&server)
            .await;
        assert!(matches!(c.server_time().await, Err(AdoError::Unauthorized)));
    }

    #[tokio::test]
    async fn redirect_without_location_is_unauthorized_other_redirects_are_http() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/nolocation"))
            .respond_with(ResponseTemplate::new(302))
            .mount(&server)
            .await;
        Mock::given(path("/_apis/moved"))
            .respond_with(
                ResponseTemplate::new(301).insert_header("Location", "https://example.com/new"),
            )
            .mount(&server)
            .await;
        Mock::given(path("/_apis/aad"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "Location",
                "https://LOGIN.MICROSOFTONLINE.com/common/oauth2",
            ))
            .mount(&server)
            .await;
        let c = client(&server);
        let get = |p: &'static str| c.get_json::<serde_json::Value>(Host::Dev, p);
        assert!(matches!(
            get("_apis/nolocation").await,
            Err(AdoError::Unauthorized)
        ));
        assert!(matches!(
            get("_apis/aad").await,
            Err(AdoError::Unauthorized)
        ));
        assert!(matches!(
            get("_apis/moved").await,
            Err(AdoError::Http { status: 301, body_excerpt }) if body_excerpt == "redirect"
        ));
    }

    #[tokio::test]
    async fn requests_suppress_auth_redirect() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/thing"))
            .and(header("X-TFS-FedAuthRedirect", "Suppress"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        client(&server)
            .get_json::<serde_json::Value>(Host::Dev, "_apis/thing")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn long_throttle_wait_returns_throttled_without_sleeping() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/x"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "120"))
            .expect(1)
            .mount(&server)
            .await;
        let e = client(&server)
            .get_json::<serde_json::Value>(Host::Dev, "_apis/x")
            .await
            .unwrap_err();
        assert!(
            matches!(e, AdoError::Throttled { retry_after } if retry_after == Duration::from_secs(120))
        );
    }

    #[tokio::test]
    async fn repeated_continuation_token_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/projects"))
            .respond_with(list(json!([])).insert_header(CONTINUATION_HEADER, "same"))
            .mount(&server)
            .await;
        let e = client(&server)
            .projects()
            .await
            .expect_err("cycle must error");
        assert!(matches!(e, AdoError::PagingCycle));
        assert!(matches!(ProviderError::from(e), ProviderError::Other(_)));
    }

    #[tokio::test]
    async fn top_skip_gives_up_after_max_pages() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/z"))
            .respond_with(list(json!([1, 2])))
            .expect(MAX_PAGES as u64)
            .mount(&server)
            .await;
        let r: Result<Vec<i32>, _> = client(&server).get_top_skip(Host::Dev, "_apis/z", 2).await;
        assert_too_many_pages(r);
    }

    #[tokio::test]
    async fn paged_gives_up_after_max_pages() {
        let server = MockServer::start().await;
        let n = std::sync::atomic::AtomicUsize::new(0);
        Mock::given(path("/_apis/projects"))
            .respond_with(move |_: &wiremock::Request| {
                let i = n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                list(json!([1])).insert_header(CONTINUATION_HEADER, format!("t{i}").as_str())
            })
            .mount(&server)
            .await;
        let r: Result<Vec<i32>, _> = client(&server).get_paged(Host::Dev, "_apis/projects").await;
        assert_too_many_pages(r);
    }

    fn assert_too_many_pages(r: Result<Vec<i32>, AdoError>) {
        let e = r.expect_err("paging cap must error");
        assert!(matches!(e, AdoError::TooManyPages { pages } if pages == MAX_PAGES));
        assert!(
            matches!(ProviderError::from(e), ProviderError::Other(m) if m.contains("paging stopped"))
        );
    }

    #[test]
    fn organization_is_percent_encoded_in_base_urls() {
        let c = AdoClient::new("a/b c", test_auth());
        assert_eq!(c.dev, "https://dev.azure.com/a%2Fb%20c");
        let c = AdoClient::new("my.org-1_x", test_auth());
        assert_eq!(c.dev, "https://dev.azure.com/my.org-1_x");
    }

    #[tokio::test]
    async fn top_skip_zero_page_size_terminates() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/z"))
            .respond_with(list(json!([])))
            .expect(1)
            .mount(&server)
            .await;
        let v: Vec<i32> = client(&server)
            .get_top_skip(Host::Dev, "_apis/z", 0)
            .await
            .unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn descriptor_queries_split_by_encoded_length_and_encode_minimally() {
        let ds: Vec<String> = (0..100)
            .map(|i| format!("aad.{i};{}", "x".repeat(150)))
            .collect();
        let qs = descriptor_queries(&ds);
        assert!(qs.len() > 1);
        assert!(qs.iter().all(|q| q.len() <= MAX_DESCRIPTOR_QUERY_CHARS));
        assert!(qs[0].starts_with("aad.0%3Bx"));
        assert_eq!(qs.join(",").matches(',').count(), 99);
        assert_eq!(descriptor_queries(&["a.b-c_d".to_string()]), ["a.b-c_d"]);
    }

    #[tokio::test]
    async fn throttled_request_is_retried_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/x"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(path("/_apis/x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        let v: serde_json::Value = client(&server)
            .get_json(Host::Dev, "_apis/x")
            .await
            .unwrap();
        assert_eq!(v["ok"], true);
    }

    #[tokio::test]
    async fn persistent_throttle_returns_throttled_after_retries() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/x"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "0"))
            .expect(4)
            .mount(&server)
            .await;
        let e = client(&server)
            .get_json::<serde_json::Value>(Host::Dev, "_apis/x")
            .await
            .unwrap_err();
        assert!(matches!(e, AdoError::Throttled { .. }));
    }

    #[test]
    fn retry_after_is_capped() {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_static("9999"));
        assert_eq!(retry_after(&h, 0), Duration::from_secs(300));
    }

    #[tokio::test]
    async fn paged_follows_continuation_token() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/projects"))
            .and(query_param_is_missing("continuationToken"))
            .respond_with(
                list(json!([{"id": "1", "name": "a"}])).insert_header(CONTINUATION_HEADER, "tok 1"),
            )
            .mount(&server)
            .await;
        Mock::given(path("/_apis/projects"))
            .and(query_param("continuationToken", "tok 1"))
            .respond_with(list(json!([{"id": "2", "name": "b"}])))
            .mount(&server)
            .await;
        let p = client(&server).projects().await.unwrap();
        assert_eq!(
            p.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[tokio::test]
    async fn top_skip_stops_on_short_page() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/git/pullrequests"))
            .and(query_param("$skip", "0"))
            .respond_with(list(json!([1, 2])))
            .mount(&server)
            .await;
        Mock::given(path("/_apis/git/pullrequests"))
            .and(query_param("$skip", "2"))
            .and(query_param("$top", "2"))
            .respond_with(list(json!([3])))
            .expect(1)
            .mount(&server)
            .await;
        let v: Vec<i32> = client(&server)
            .get_top_skip(
                Host::Dev,
                "_apis/git/pullrequests?searchCriteria.status=active",
                2,
            )
            .await
            .unwrap();
        assert_eq!(v, [1, 2, 3]);
    }

    #[tokio::test]
    async fn server_time_parses_date_header() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/connectionData"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Date", "Fri, 20 Nov 2026 10:30:00 GMT")
                    .set_body_json(json!({})),
            )
            .mount(&server)
            .await;
        let t = client(&server).server_time().await.unwrap();
        assert_eq!(t.to_rfc3339(), "2026-11-20T10:30:00+00:00");
    }

    async fn mount_connection_data(server: &MockServer) {
        Mock::given(path("/_apis/connectionData"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "authenticatedUser": {
                    "id": "me-id",
                    "providerDisplayName": "Me Person",
                    "properties": { "Account": { "$type": "System.String", "$value": "me@example.com" } }
                }
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn identify_resolves_group_ids() {
        let server = MockServer::start().await;
        mount_connection_data(&server).await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param("identityIds", "me-id"))
            .respond_with(list(
                json!([{ "id": "me-id", "memberOf": ["vssgp.A", "vssgp.B"] }]),
            ))
            .mount(&server)
            .await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param("descriptors", "vssgp.A,vssgp.B"))
            .respond_with(list(json!([{ "id": "g1" }, { "id": "g2" }])))
            .mount(&server)
            .await;
        let me = client(&server).identify().await.unwrap();
        assert_eq!(me.id, "me-id");
        assert_eq!(me.display_name, "Me Person");
        assert_eq!(me.unique_name, "me@example.com");
        assert_eq!(me.group_ids, ["g1", "g2"]);
    }

    #[tokio::test]
    async fn identify_skips_null_entries_and_keeps_ids_when_a_chunk_404s() {
        let server = MockServer::start().await;
        mount_connection_data(&server).await;
        let descriptors: Vec<String> = (0..40)
            .map(|i| format!("vssgp.{i}{}", "y".repeat(150)))
            .collect();
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param("identityIds", "me-id"))
            .respond_with(list(json!([{ "id": "me-id", "memberOf": descriptors }])))
            .mount(&server)
            .await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param_is_missing("identityIds"))
            .and(wiremock::matchers::query_param_contains(
                "descriptors",
                "vssgp.0y",
            ))
            .respond_with(list(json!([{ "id": "g1" }, null])))
            .mount(&server)
            .await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param_is_missing("identityIds"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let me = client(&server).identify().await.unwrap();
        assert_eq!(me.group_ids, ["g1"]);
    }

    #[tokio::test]
    async fn identify_membership_unauthorized_gives_empty_groups() {
        let server = MockServer::start().await;
        mount_connection_data(&server).await;
        Mock::given(path("/vssps/_apis/identities"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        assert!(
            client(&server)
                .identify()
                .await
                .unwrap()
                .group_ids
                .is_empty()
        );
    }

    #[tokio::test]
    async fn identify_connection_data_unauthorized_is_unauthorized() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/connectionData"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        assert!(matches!(
            client(&server).identify().await,
            Err(AdoError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn descriptor_chunk_forbidden_keeps_ids_from_other_chunks() {
        let server = MockServer::start().await;
        mount_connection_data(&server).await;
        let descriptors: Vec<String> = (0..40)
            .map(|i| format!("vssgp.{i}{}", "y".repeat(150)))
            .collect();
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param("identityIds", "me-id"))
            .respond_with(list(json!([{ "id": "me-id", "memberOf": descriptors }])))
            .mount(&server)
            .await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param_is_missing("identityIds"))
            .and(wiremock::matchers::query_param_contains(
                "descriptors",
                "vssgp.0y",
            ))
            .respond_with(list(json!([{ "id": "g1" }])))
            .mount(&server)
            .await;
        Mock::given(path("/vssps/_apis/identities"))
            .and(query_param_is_missing("identityIds"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let me = client(&server).identify().await.unwrap();
        assert_eq!(me.group_ids, ["g1"]);
    }

    #[tokio::test]
    async fn transport_error_includes_source_chain_but_not_url() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let secrets = MemorySecretStore::new();
        secrets.set("conn", PAT).unwrap();
        let cfg = ConnectionConfig {
            id: "conn".into(),
            provider: "ado".into(),
            enabled: true,
            auth: AuthConfig {
                method: "pat".into(),
            },
            bots: Vec::new(),
            settings: Default::default(),
        };
        let auth = AdoAuth::from_config(&cfg, Arc::new(secrets)).unwrap();
        let u = format!("http://127.0.0.1:{port}");
        let c = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        let e = c
            .get_json::<serde_json::Value>(Host::Dev, "_apis/secret-path")
            .await
            .unwrap_err();
        let AdoError::Transport(msg) = e else {
            panic!("expected transport error, got {e:?}");
        };
        assert!(msg.contains(": "), "no source chain: {msg}");
        assert!(
            !msg.contains("127.0.0.1") && !msg.contains("secret-path"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn identify_membership_forbidden_gives_empty_groups() {
        let server = MockServer::start().await;
        mount_connection_data(&server).await;
        Mock::given(path("/vssps/_apis/identities"))
            .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
            .mount(&server)
            .await;
        let me = client(&server).identify().await.unwrap();
        assert_eq!(me.id, "me-id");
        assert!(me.group_ids.is_empty());
    }

    #[tokio::test]
    async fn error_display_never_contains_pat() {
        let server = MockServer::start().await;
        Mock::given(path("/_apis/bad"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let c = client(&server);
        let errors = [
            c.get_json::<serde_json::Value>(Host::Dev, "_apis/bad")
                .await
                .unwrap_err(),
            c.get_json::<serde_json::Value>(Host::Dev, "_apis/missing")
                .await
                .unwrap_err(),
        ];
        for e in errors {
            let s = format!("{e} {e:?}");
            assert!(!s.contains(PAT) && !s.contains(&basic_pat()), "{s}");
        }
    }
}
