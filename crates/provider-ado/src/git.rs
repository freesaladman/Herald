//! Typed wrappers over the ADO git, policy and PR endpoints.
//!
//! The PR lists are org-level; everything else is project-scoped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use tokio::time::Instant;

use crate::client::{AdoClient, AdoError, Host};
use crate::dto_git::{
    GitCommitDiffs, GitPullRequest, GitPullRequestCommentThread, GitPullRequestIteration,
    GitPullRequestStatus, GitPush, GitRef, GitRepository, PolicyEvaluationRecord,
};

const PAGE_SIZE: usize = 100;
const POLICY_API_VERSION: &str = "7.1-preview.1";

/// Assumes polling.pr_secs is well above this (default 120 s) so each tick fetches fresh data.
/// With pr_secs at or below 20 s a tick may reuse the previous tick's threads.
pub(crate) const THREADS_CACHE_TTL: Duration = Duration::from_secs(20);

type ThreadsKey = (String, String, i64);
type ThreadsSlot =
    Arc<tokio::sync::Mutex<Option<(Instant, Arc<Vec<GitPullRequestCommentThread>>)>>>;

/// Short-lived per-PR cache of thread lists. A slot's async mutex is held across the fetch, so
/// concurrent callers for one PR share a single request. Failed fetches leave the slot empty.
pub(crate) struct ThreadsCache {
    ttl: Duration,
    now: Box<dyn Fn() -> Instant + Send + Sync>,
    slots: Mutex<HashMap<ThreadsKey, ThreadsSlot>>,
}

impl ThreadsCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            now: Box::new(Instant::now),
            slots: Mutex::new(HashMap::new()),
        }
    }

    fn fresh(&self, at: Instant) -> bool {
        (self.now)().saturating_duration_since(at) < self.ttl
    }

    fn slot(&self, key: ThreadsKey) -> ThreadsSlot {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        // Drop idle slots whose entry has expired (or never filled) to bound memory.
        slots.retain(|_, s| {
            Arc::strong_count(s) > 1
                || s.try_lock()
                    .map(|g| g.as_ref().is_some_and(|(at, _)| self.fresh(*at)))
                    .unwrap_or(true)
        });
        slots.entry(key).or_default().clone()
    }
}

fn enc(s: &str) -> String {
    crate::client::encode_segment(s)
}

fn ts(t: DateTime<Utc>) -> String {
    enc(&t.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn pr_path(project: &str, repo_id: &str, pr_id: i64) -> String {
    format!(
        "{}/_apis/git/repositories/{}/pullRequests/{pr_id}",
        enc(project),
        enc(repo_id)
    )
}

impl AdoClient {
    /// Org-level. `project` filters client-side by project name (case-insensitive).
    pub async fn active_pull_requests(
        &self,
        project: Option<&str>,
    ) -> Result<Vec<GitPullRequest>, AdoError> {
        let mut prs: Vec<GitPullRequest> = self
            .get_top_skip(
                Host::Dev,
                "_apis/git/pullrequests?searchCriteria.status=active&searchCriteria.includeLinks=false",
                PAGE_SIZE,
            )
            .await?;
        if let Some(p) = project {
            prs.retain(|pr| pr.repository.project.name.eq_ignore_ascii_case(p));
        }
        Ok(prs)
    }

    /// Org-level. PRs closed at or after `since`, optionally only those created by `creator_id`.
    pub async fn closed_pull_requests_since(
        &self,
        since: DateTime<Utc>,
        creator_id: Option<&str>,
    ) -> Result<Vec<GitPullRequest>, AdoError> {
        let mut pq = format!(
            "_apis/git/pullrequests?searchCriteria.status=all&searchCriteria.minTime={}&searchCriteria.queryTimeRangeType=closed&searchCriteria.includeLinks=false",
            ts(since)
        );
        if let Some(id) = creator_id {
            pq.push_str(&format!("&searchCriteria.creatorId={}", enc(id)));
        }
        self.get_top_skip(Host::Dev, &pq, PAGE_SIZE).await
    }

    pub async fn repositories(&self, project: &str) -> Result<Vec<GitRepository>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!("{}/_apis/git/repositories", enc(project)),
        )
        .await
    }

    pub async fn pull_request(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
    ) -> Result<GitPullRequest, AdoError> {
        self.get_json(Host::Dev, &pr_path(project, repo_id, pr_id))
            .await
    }

    pub async fn pr_threads(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
    ) -> Result<Vec<GitPullRequestCommentThread>, AdoError> {
        let cache = &self.threads_cache;
        if cache.ttl.is_zero() {
            return self.fetch_pr_threads(project, repo_id, pr_id).await;
        }
        let slot = cache.slot((project.to_string(), repo_id.to_string(), pr_id));
        let mut entry = slot.lock().await;
        if let Some((at, threads)) = entry.as_ref()
            && cache.fresh(*at)
        {
            return Ok(threads.as_ref().clone());
        }
        let threads = Arc::new(self.fetch_pr_threads(project, repo_id, pr_id).await?);
        *entry = Some(((cache.now)(), threads.clone()));
        Ok(threads.as_ref().clone())
    }

    async fn fetch_pr_threads(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
    ) -> Result<Vec<GitPullRequestCommentThread>, AdoError> {
        let raw: Vec<serde_json::Value> = self
            .get_paged(
                Host::Dev,
                &format!("{}/threads", pr_path(project, repo_id, pr_id)),
            )
            .await?;
        // One odd thread must not hide the rest of the PR.
        Ok(raw
            .into_iter()
            .filter_map(|v| {
                let id = v.get("id").and_then(|i| i.as_i64());
                match serde_path_to_error::deserialize(v) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        tracing::debug!(thread_id = ?id, "skipping undecodable thread: {e}");
                        None
                    }
                }
            })
            .collect())
    }

    pub async fn pr_iterations(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
    ) -> Result<Vec<GitPullRequestIteration>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!("{}/iterations", pr_path(project, repo_id, pr_id)),
        )
        .await
    }

    pub async fn pr_statuses(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
    ) -> Result<Vec<GitPullRequestStatus>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!("{}/statuses", pr_path(project, repo_id, pr_id)),
        )
        .await
    }

    pub async fn policy_evaluations(
        &self,
        project: &str,
        project_id: &str,
        pr_id: i64,
    ) -> Result<Vec<PolicyEvaluationRecord>, AdoError> {
        let artifact = format!("vstfs:///CodeReview/CodeReviewId/{project_id}/{pr_id}");
        self.get_paged(
            Host::Dev,
            &format!(
                "{}/_apis/policy/evaluations?artifactId={}&api-version={POLICY_API_VERSION}",
                enc(project),
                enc(&artifact)
            ),
        )
        .await
    }

    /// Branch refs with their creators. ADO has no creator filter, so callers filter client-side.
    pub async fn refs_heads(&self, project: &str, repo_id: &str) -> Result<Vec<GitRef>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!(
                "{}/_apis/git/repositories/{}/refs?filter=heads/&includeStatuses=false",
                enc(project),
                enc(repo_id)
            ),
        )
        .await
    }

    pub async fn pushes_since(
        &self,
        project: &str,
        repo_id: &str,
        from: DateTime<Utc>,
    ) -> Result<Vec<GitPush>, AdoError> {
        self.get_top_skip(
            Host::Dev,
            &format!(
                "{}/_apis/git/repositories/{}/pushes?searchCriteria.fromDate={}&searchCriteria.includeRefUpdates=true",
                enc(project),
                enc(repo_id),
                ts(from)
            ),
            PAGE_SIZE,
        )
        .await
    }

    /// Ahead/behind counts of `target` relative to `base` (commit ids).
    pub async fn commit_diff_counts(
        &self,
        project: &str,
        repo_id: &str,
        base: &str,
        target: &str,
    ) -> Result<GitCommitDiffs, AdoError> {
        self.get_json(
            Host::Dev,
            &format!(
                "{}/_apis/git/repositories/{}/diffs/commits?baseVersion={}&baseVersionType=commit&targetVersion={}&targetVersionType=commit&$top=0",
                enc(project),
                enc(repo_id),
                enc(base),
                enc(target)
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::auth::AdoAuth;

    fn client(server: &MockServer) -> AdoClient {
        let secrets = MemorySecretStore::new();
        secrets.set("conn", "pat").unwrap();
        let cfg = ConnectionConfig {
            notify: Default::default(),
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
        let u = server.uri();
        AdoClient::with_base_urls("org", auth, &u, &u, &u)
    }

    fn pr(id: i64, project: &str) -> serde_json::Value {
        json!({ "pullRequestId": id, "repository": { "project": { "name": project } } })
    }

    #[tokio::test]
    async fn active_pull_requests_is_org_level_and_pages_with_top_skip() {
        let server = MockServer::start().await;
        let full: Vec<_> = (0..100).map(|i| pr(i, "Alpha")).collect();
        Mock::given(method("GET"))
            .and(path("/_apis/git/pullrequests"))
            .and(query_param("searchCriteria.status", "active"))
            .and(query_param("searchCriteria.includeLinks", "false"))
            .and(query_param("$skip", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": full })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/_apis/git/pullrequests"))
            .and(query_param("$skip", "100"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "value": [pr(500, "beta"), pr(501, "Alpha")] })),
            )
            .expect(2)
            .mount(&server)
            .await;
        let c = client(&server);
        assert_eq!(c.active_pull_requests(None).await.unwrap().len(), 102);
        let beta = c.active_pull_requests(Some("BETA")).await.unwrap();
        assert_eq!(beta.len(), 1);
        assert_eq!(beta[0].pull_request_id, 500);
    }

    #[tokio::test]
    async fn closed_pull_requests_since_formats_min_time_and_creator() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_apis/git/pullrequests"))
            .and(query_param("searchCriteria.status", "all"))
            .and(query_param("searchCriteria.queryTimeRangeType", "closed"))
            .and(query_param(
                "searchCriteria.minTime",
                "2026-11-18T01:02:03Z",
            ))
            .and(query_param("searchCriteria.creatorId", "me-id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": [] })))
            .expect(1)
            .mount(&server)
            .await;
        let since = Utc.with_ymd_and_hms(2026, 11, 18, 1, 2, 3).unwrap();
        let prs = client(&server)
            .closed_pull_requests_since(since, Some("me-id"))
            .await
            .unwrap();
        assert!(prs.is_empty());
    }

    #[tokio::test]
    async fn policy_evaluations_is_project_level_with_encoded_artifact_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/Project%20One/_apis/policy/evaluations"))
            .and(query_param(
                "artifactId",
                "vstfs:///CodeReview/CodeReviewId/proj-id/42",
            ))
            .and(query_param("api-version", "7.1-preview.1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "value": [{ "evaluationId": "e1", "status": "approved" }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let evals = client(&server)
            .policy_evaluations("Project One", "proj-id", 42)
            .await
            .unwrap();
        assert_eq!(evals[0].status, "approved");
        let raw = &server.received_requests().await.unwrap()[0];
        assert!(
            raw.url
                .query()
                .unwrap()
                .contains("vstfs%3A%2F%2F%2FCodeReview")
        );
    }

    #[tokio::test]
    async fn pushes_since_is_project_scoped_with_encoded_from_date() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/Project%20One/_apis/git/repositories/r1/pushes"))
            .and(query_param(
                "searchCriteria.fromDate",
                "2026-11-18T01:02:03Z",
            ))
            .and(query_param("searchCriteria.includeRefUpdates", "true"))
            .and(query_param("$top", "100"))
            .and(query_param("$skip", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": [] })))
            .expect(1)
            .mount(&server)
            .await;
        let since = Utc.with_ymd_and_hms(2026, 11, 18, 1, 2, 3).unwrap();
        let pushes = client(&server)
            .pushes_since("Project One", "r1", since)
            .await
            .unwrap();
        assert!(pushes.is_empty());
        let raw = &server.received_requests().await.unwrap()[0];
        assert!(
            raw.url
                .query()
                .unwrap()
                .contains("2026-11-18T01%3A02%3A03Z")
        );
    }

    #[tokio::test]
    async fn refs_heads_and_diff_counts_build_expected_queries() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/P/_apis/git/repositories/r1/refs"))
            .and(query_param("filter", "heads/"))
            .and(query_param("includeStatuses", "false"))
            .and(query_param_is_missing("filter.creator"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": [] })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/P/_apis/git/repositories/r1/diffs/commits"))
            .and(query_param("baseVersion", "aaa"))
            .and(query_param("targetVersion", "bbb"))
            .and(query_param("baseVersionType", "commit"))
            .and(query_param("targetVersionType", "commit"))
            .and(query_param("$top", "0"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "aheadCount": 2, "behindCount": 5 })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let c = client(&server);
        assert!(c.refs_heads("P", "r1").await.unwrap().is_empty());
        let d = c.commit_diff_counts("P", "r1", "aaa", "bbb").await.unwrap();
        assert_eq!((d.ahead_count, d.behind_count), (2, 5));
    }

    fn cached_client(server: &MockServer) -> AdoClient {
        let mut c = client(server);
        c.threads_cache = ThreadsCache::new(THREADS_CACHE_TTL);
        c
    }

    async fn mount_threads(server: &MockServer, resp: ResponseTemplate, times: u64) {
        Mock::given(method("GET"))
            .and(path("/P/_apis/git/repositories/r1/pullRequests/7/threads"))
            .respond_with(resp)
            .expect(times)
            .mount(server)
            .await;
    }

    fn threads_body() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": [{ "id": 3 }] }))
    }

    #[tokio::test]
    async fn concurrent_pr_threads_calls_share_one_request() {
        let server = MockServer::start().await;
        mount_threads(&server, threads_body(), 1).await;
        let c = cached_client(&server);
        let (a, b) = tokio::join!(c.pr_threads("P", "r1", 7), c.pr_threads("P", "r1", 7));
        assert_eq!(a.unwrap()[0].id, 3);
        assert_eq!(b.unwrap()[0].id, 3);
    }

    #[tokio::test]
    async fn pr_threads_refetches_after_ttl() {
        let server = MockServer::start().await;
        mount_threads(&server, threads_body(), 2).await;
        let base = Instant::now();
        let offset = Arc::new(AtomicU64::new(0));
        let mut c = client(&server);
        c.threads_cache = ThreadsCache::new(THREADS_CACHE_TTL);
        let o = offset.clone();
        c.threads_cache.now =
            Box::new(move || base + Duration::from_secs(o.load(Ordering::SeqCst)));
        c.pr_threads("P", "r1", 7).await.unwrap();
        offset.store(THREADS_CACHE_TTL.as_secs() - 1, Ordering::SeqCst);
        c.pr_threads("P", "r1", 7).await.unwrap();
        offset.store(THREADS_CACHE_TTL.as_secs() + 1, Ordering::SeqCst);
        c.pr_threads("P", "r1", 7).await.unwrap();
    }

    #[tokio::test]
    async fn pr_threads_skips_a_malformed_thread_and_keeps_the_rest() {
        let server = MockServer::start().await;
        let body = json!({ "value": [
            { "id": 1, "comments": [] },
            { "id": 2, "comments": "not a list" },
            { "id": 3, "identities": null, "comments": [{ "id": 1, "author": { "displayName": "Person 1" } }] },
        ] });
        mount_threads(&server, ResponseTemplate::new(200).set_body_json(body), 1).await;
        let c = client(&server);
        let ids: Vec<i64> = c
            .pr_threads("P", "r1", 7)
            .await
            .unwrap()
            .iter()
            .map(|t| t.id)
            .collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn segments_keep_unreserved_characters_and_encode_spaces() {
        assert_eq!(
            pr_path("11111111-2222-3333-4444-555555555555", "my.repo_x~1", 7),
            "11111111-2222-3333-4444-555555555555/_apis/git/repositories/my.repo_x~1/pullRequests/7"
        );
        assert_eq!(enc("Project One/x"), "Project%20One%2Fx");
    }

    #[tokio::test]
    async fn pr_threads_errors_are_not_cached() {
        let server = MockServer::start().await;
        mount_threads(&server, ResponseTemplate::new(400), 2).await;
        let c = cached_client(&server);
        assert!(c.pr_threads("P", "r1", 7).await.is_err());
        assert!(c.pr_threads("P", "r1", 7).await.is_err());
    }
}
