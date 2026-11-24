//! Slow-tier source: finds threads I commented in or was mentioned in on any active PR.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream;
use herald_core::event::{CommentRef, Event, PrRef, PrRole, event_types, ids, subject};
use herald_core::provider::{PollOutcome, ProviderError, Source, SyncContext, Tier};

use super::{
    SNAPSHOT_TRACKED_PRS, SNAPSHOT_TRACKED_THREADS, ThreadReason, TrackedPrs, TrackedThread,
    TrackedThreads, active_prs, actor_from, is_fatal, mentions, pr_key, pr_web_url, project_ref,
    repo_ref, window_start,
};
use crate::connection::AdoShared;
use crate::dto_git::{Comment, GitPullRequest, GitPullRequestCommentThread, IdentityRef};

const CONCURRENCY: usize = 4;
const EXCERPT_CHARS: usize = 200;

pub struct PrDiscoverySource {
    shared: Arc<AdoShared>,
}

/// A mention of me by someone else, awaiting emission.
struct Mention {
    thread_id: i64,
    comment_id: i64,
    published: DateTime<Utc>,
    /// Latest of published and last-updated, so an edit re-enters the window.
    changed: DateTime<Utc>,
    author: Option<IdentityRef>,
    excerpt: String,
    status: String,
}

struct Found {
    threads: Vec<TrackedThread>,
    mentions: Vec<Mention>,
}

type Fetched = (usize, Option<Vec<GitPullRequestCommentThread>>);

impl PrDiscoverySource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    /// `Ok(None)` means skip this PR (logged); errors that mean the poll can't proceed are returned.
    async fn threads(
        &self,
        pr: &GitPullRequest,
    ) -> Result<Option<Vec<GitPullRequestCommentThread>>, ProviderError> {
        let project = project_ref(&pr.repository.project.id, &pr.repository.project.name);
        match self
            .shared
            .client
            .pr_threads(project, &pr.repository.id, pr.pull_request_id)
            .await
            .map_err(ProviderError::from)
        {
            Ok(t) => Ok(Some(t)),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!(
                    "skipping threads of PR {} in {}: {e}",
                    pr.pull_request_id,
                    pr.repository.id
                );
                Ok(None)
            }
        }
    }

    async fn fetch(&self, i: usize, pr: &GitPullRequest) -> Result<Fetched, ProviderError> {
        Ok((i, self.threads(pr).await?))
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let old: TrackedThreads = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_THREADS)
            .await?
            .unwrap_or_default();
        let tracked: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await?
            .unwrap_or_default();
        let roles: HashMap<String, PrRole> = tracked
            .prs
            .iter()
            .map(|p| (pr_key(&p.repo_id, p.pr_id), p.role))
            .collect();
        let active = active_prs(&self.shared).await?;
        let org = &self.shared.organization;

        let fetches: Vec<BoxFuture<'_, Result<Fetched, ProviderError>>> = active
            .iter()
            .enumerate()
            .map(|(i, pr)| -> BoxFuture<'_, _> { Box::pin(self.fetch(i, pr)) })
            .collect();
        // try_collect drops the stream on the first fatal error, cancelling in-flight requests.
        let mut results: Vec<Fetched> = stream::iter(fetches)
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        results.sort_by_key(|(i, _)| *i);

        let window = window_start(ctx);
        let mut threads = Vec::new();
        let mut mention_events: Vec<(usize, Mention)> = Vec::new();
        let mut failed: HashSet<String> = HashSet::new();
        for (i, fetched) in results {
            let pr = &active[i];
            let Some(fetched) = fetched else {
                failed.insert(pr_key(&pr.repository.id, pr.pull_request_id));
                continue;
            };
            let found = discover(org, pr, &fetched, ctx);
            threads.extend(found.threads);
            mention_events.extend(
                found
                    .mentions
                    .into_iter()
                    .filter(|m| m.changed >= window)
                    .map(|m| (i, m)),
            );
        }
        // A skipped PR still lets the watermark advance; its mentions in that window rely on 023 if tracked.
        threads.extend(
            old.threads
                .into_iter()
                .filter(|t| failed.contains(&pr_key(&t.repo_id, t.pr_id))),
        );
        ctx.snapshots
            .put(SNAPSHOT_TRACKED_THREADS, &TrackedThreads { threads })
            .await?;

        if ctx.watermark.is_none() {
            return Ok(PollOutcome::default());
        }
        let c = &ctx.connection.id;
        let events = mention_events
            .into_iter()
            .map(|(i, m)| {
                let pr = &active[i];
                let repo = &pr.repository;
                let role = roles
                    .get(&pr_key(&repo.id, pr.pull_request_id))
                    .copied()
                    .unwrap_or(PrRole::Mentioned);
                let url = format!(
                    "{}?discussionId={}",
                    pr_web_url(org, &repo.project.name, &repo.name, pr.pull_request_id),
                    m.thread_id
                );
                let mut e = Event::new(
                    ctx.connection.clone(),
                    event_types::PR_MENTIONED,
                    m.published,
                    ctx.server_now,
                    format!("Mentioned in PR: {}", pr.title),
                    subject::pr(c, &repo.id, pr.pull_request_id),
                )
                .with_id(ids::pr_mention(
                    c,
                    &repo.id,
                    pr.pull_request_id,
                    &m.thread_id.to_string(),
                    &m.comment_id.to_string(),
                ));
                e.catch_up = ctx.catch_up;
                e.url = Some(url);
                e.subtitle = Some(repo.name.clone());
                if let Some(a) = &m.author {
                    e.actor = actor_from(a, &ctx.me);
                }
                e.repo = Some(repo_ref(&repo.id, &repo.name, &repo.project.name));
                e.pr = Some(PrRef {
                    id: pr.pull_request_id,
                    title: pr.title.clone(),
                    source_branch: pr.source_ref_name.clone(),
                    target_branch: pr.target_ref_name.clone(),
                    is_draft: pr.is_draft,
                    role,
                });
                e.comment = Some(CommentRef {
                    thread_id: m.thread_id.to_string(),
                    comment_id: m.comment_id.to_string(),
                    excerpt: m.excerpt,
                    thread_status: m.status,
                });
                e
            })
            .collect();
        Ok(PollOutcome { events })
    }
}

impl Source for PrDiscoverySource {
    fn name(&self) -> &'static str {
        "pr_discovery"
    }

    fn tier(&self) -> Tier {
        Tier::Slow
    }

    fn poll<'a>(
        &'a mut self,
        ctx: &'a SyncContext,
    ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>> {
        Box::pin(self.poll_inner(ctx))
    }
}

fn is_text(c: &Comment) -> bool {
    !c.is_deleted && (c.comment_type.is_empty() || c.comment_type.eq_ignore_ascii_case("text"))
}

fn discover(
    org: &str,
    pr: &GitPullRequest,
    fetched: &[GitPullRequestCommentThread],
    ctx: &SyncContext,
) -> Found {
    let repo = &pr.repository;
    let is_me = |c: &Comment| {
        c.author
            .as_ref()
            .is_some_and(|a| !a.id.is_empty() && a.id.eq_ignore_ascii_case(&ctx.me.id))
    };
    let mut found = Found {
        threads: Vec::new(),
        mentions: Vec::new(),
    };
    for thread in fetched.iter().filter(|t| !t.is_deleted) {
        if thread.thread_type().is_some() {
            continue;
        }
        let comments: Vec<&Comment> = thread
            .comments
            .iter()
            .filter(|c| is_text(c) && c.published_date.is_some())
            .collect();
        let mine = comments
            .iter()
            .filter(|c| is_me(c))
            .filter_map(|c| c.published_date)
            .min();
        let mut first_mention: Option<DateTime<Utc>> = None;
        for c in comments.iter().filter(|c| !is_me(c)) {
            let content = c.content.as_deref().unwrap_or_default();
            let Some(published) = c.published_date else {
                continue;
            };
            if !mentions(content, &ctx.me.id) {
                continue;
            }
            first_mention = Some(first_mention.map_or(published, |f| f.min(published)));
            found.mentions.push(Mention {
                thread_id: thread.id,
                comment_id: c.id,
                published,
                changed: c.last_updated_date.map_or(published, |u| u.max(published)),
                author: c.author.clone(),
                excerpt: excerpt(content, &ctx.me.id),
                status: thread.status.clone().unwrap_or_default(),
            });
        }
        let (since, reason) = match (mine, first_mention) {
            (Some(m), Some(f)) => (m.min(f), ThreadReason::Commenter),
            (Some(m), None) => (m, ThreadReason::Commenter),
            (None, Some(f)) => (f, ThreadReason::Mentioned),
            (None, None) => continue,
        };
        found.threads.push(TrackedThread {
            repo_id: repo.id.clone(),
            repo_name: repo.name.clone(),
            project: repo.project.name.clone(),
            project_id: repo.project.id.clone(),
            pr_id: pr.pull_request_id,
            thread_id: thread.id,
            since_utc: since,
            reason,
            title: pr.title.clone(),
            source_ref: pr.source_ref_name.clone(),
            target_ref: pr.target_ref_name.clone(),
            is_draft: pr.is_draft,
            url: pr_web_url(org, &repo.project.name, &repo.name, pr.pull_request_id),
        });
    }
    found
}

/// Replaces raw `@<guid>` tokens with `@you` (me) or `@user`, then truncates.
pub(super) fn excerpt(content: &str, me_id: &str) -> String {
    let mut out = String::new();
    let mut rest = content;
    while let Some(start) = rest.find("@<") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('>') else {
            rest = &rest[start..];
            break;
        };
        out.push_str(if after[..end].eq_ignore_ascii_case(me_id) {
            "@you"
        } else {
            "@user"
        });
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out.chars().take(EXCERPT_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::TrackedPr;
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::event::ActorKind;
    use herald_core::provider::Identity;
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::auth::AdoAuth;
    use crate::client::AdoClient;

    const ME: &str = "aaaa-me";

    fn at(day: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, day, h, 0, 0).unwrap()
    }

    fn cfg() -> ConnectionConfig {
        ConnectionConfig {
            notify: Default::default(),
            id: "c1".into(),
            provider: "ado".into(),
            enabled: true,
            auth: AuthConfig {
                method: "pat".into(),
            },
            bots: Vec::new(),
            settings: Default::default(),
        }
    }

    fn me() -> Identity {
        Identity {
            id: ME.into(),
            display_name: "Me".into(),
            unique_name: "me@example.test".into(),
            group_ids: Vec::new(),
        }
    }

    fn source(server: &MockServer) -> PrDiscoverySource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PrDiscoverySource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn pr(id: i64) -> Value {
        json!({
            "pullRequestId": id,
            "repository": { "id": "Repo-1", "name": "widgets", "project": { "id": "p1", "name": "Alpha" } },
            "status": "active",
            "createdBy": { "id": "other", "displayName": "Pat", "uniqueName": "pat@example.test" },
            "title": format!("change {id}"),
            "sourceRefName": "refs/heads/topic",
            "targetRefName": "refs/heads/main",
            "isDraft": false,
            "reviewers": [],
        })
    }

    fn comment(id: i64, author: &str, content: &str, when: DateTime<Utc>) -> Value {
        json!({
            "id": id, "parentCommentId": 0,
            "author": { "id": author, "displayName": "Person", "uniqueName": "p@example.test" },
            "content": content, "publishedDate": when, "commentType": "text",
        })
    }

    fn thread(id: i64, comments: Vec<Value>) -> Value {
        json!({
            "id": id, "status": "active", "isDeleted": false,
            "publishedDate": at(10, 0), "lastUpdatedDate": at(10, 0),
            "properties": {}, "comments": comments,
        })
    }

    fn system_thread() -> Value {
        let mut t = thread(99, vec![comment(1, ME, &format!("@<{ME}>"), at(10, 0))]);
        t["properties"] =
            json!({ "CodeReviewThreadType": { "$type": "System.String", "$value": "VoteUpdate" } });
        t
    }

    async fn serve_active(server: &MockServer, prs: Vec<Value>) {
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/_apis/git/pullrequests"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": prs })))
            .mount(server)
            .await;
    }

    async fn serve_threads(server: &MockServer, pr_id: i64, body: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(format!(
                "/p1/_apis/git/repositories/Repo-1/pullRequests/{pr_id}/threads"
            )))
            .respond_with(body)
            .mount(server)
            .await;
    }

    fn ok(threads: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": threads }))
    }

    async fn run(
        src: &mut PrDiscoverySource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = now;
        src.poll(&c).await.map(|o| o.events)
    }

    async fn snapshot(ctx: &SyncContext) -> Vec<TrackedThread> {
        let s: TrackedThreads = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_THREADS)
            .await
            .unwrap()
            .unwrap();
        s.threads
    }

    #[tokio::test]
    async fn seed_writes_threads_and_emits_nothing() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1)]).await;
        serve_threads(
            &server,
            1,
            ok(vec![
                thread(5, vec![comment(1, ME, "mine", at(10, 2))]),
                system_thread(),
            ]),
        )
        .await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        assert!(
            run(&mut src, &ctx, None, at(10, 5))
                .await
                .unwrap()
                .is_empty()
        );
        let t = snapshot(&ctx).await;
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].thread_id, 5);
        assert_eq!(t[0].repo_name, "widgets");
        assert_eq!(t[0].project_id, "p1");
        assert_eq!(t[0].title, "change 1");
        assert_eq!(t[0].target_ref, "refs/heads/main");
    }

    #[tokio::test]
    async fn my_comment_tracks_thread_from_my_first_comment() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1)]).await;
        serve_threads(
            &server,
            1,
            ok(vec![thread(
                5,
                vec![
                    comment(1, "other", "hello", at(10, 1)),
                    comment(3, &ME.to_uppercase(), "later", at(10, 4)),
                    comment(2, ME, "first", at(10, 2)),
                ],
            )]),
        )
        .await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 5))
            .await
            .unwrap();
        assert!(events.is_empty());
        let t = snapshot(&ctx).await;
        assert_eq!(t[0].reason, ThreadReason::Commenter);
        assert_eq!(t[0].since_utc, at(10, 2));
    }

    #[tokio::test]
    async fn mention_by_other_emits_event_and_tracks_thread() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1)]).await;
        let long = format!("hey @<{}> {}", ME.to_uppercase(), "x".repeat(300));
        serve_threads(
            &server,
            1,
            ok(vec![
                thread(5, vec![comment(7, "other", &long, at(10, 3))]),
                thread(
                    6,
                    vec![comment(8, ME, &format!("note to self @<{ME}>"), at(10, 3))],
                ),
                thread(
                    9,
                    vec![comment(10, "other", &format!("old @<{ME}>"), at(1, 3))],
                ),
            ]),
        )
        .await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        // Role comes from the tracked snapshot.
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_PRS,
                &TrackedPrs {
                    prs: vec![TrackedPr {
                        repo_id: "repo-1".into(),
                        pr_id: 1,
                        role: PrRole::Reviewer,
                        ..Default::default()
                    }],
                },
            )
            .await
            .unwrap();
        let mut src = source(&server);
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 5))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event_type, "pr.mentioned");
        assert_eq!(e.id, "c1:pr:Repo-1:1:thread:5:mention:7");
        assert_eq!(e.occurred_utc, at(10, 3));
        assert_eq!(e.actor.id, "other");
        assert_eq!(e.actor.kind, ActorKind::Human);
        assert_eq!(e.pr.as_ref().unwrap().role, PrRole::Reviewer);
        let c = e.comment.as_ref().unwrap();
        assert_eq!((c.thread_id.as_str(), c.comment_id.as_str()), ("5", "7"));
        assert_eq!(c.thread_status, "active");
        assert_eq!(c.excerpt.chars().count(), 200);
        assert!(c.excerpt.starts_with("hey @you x"));
        assert!(
            e.url
                .as_deref()
                .unwrap()
                .ends_with("/pullrequest/1?discussionId=5")
        );

        let t = snapshot(&ctx).await;
        let by_id: HashMap<i64, &TrackedThread> = t.iter().map(|t| (t.thread_id, t)).collect();
        assert_eq!(by_id[&5].reason, ThreadReason::Mentioned);
        assert_eq!(by_id[&5].since_utc, at(10, 3));
        assert_eq!(by_id[&6].reason, ThreadReason::Commenter);
        assert_eq!(by_id[&9].reason, ThreadReason::Mentioned);
    }

    #[tokio::test]
    async fn failed_thread_fetch_carries_previous_threads_forward() {
        let server = MockServer::start().await;
        let mut second = pr(2);
        second["title"] = json!("other pr");
        serve_active(&server, vec![pr(1), second.clone()]).await;
        let mine = |id| ok(vec![thread(id, vec![comment(1, ME, "a", at(10, 1))])]);
        serve_threads(&server, 1, mine(5)).await;
        serve_threads(&server, 2, mine(6)).await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 5)).await.unwrap();
        assert_eq!(snapshot(&ctx).await.len(), 2);

        serve_active(&server, vec![pr(1), second]).await;
        serve_threads(&server, 1, ok(vec![])).await;
        serve_threads(&server, 2, ResponseTemplate::new(400)).await;
        run(&mut src, &ctx, Some(at(10, 5)), at(10, 6))
            .await
            .unwrap();
        let ids: Vec<i64> = snapshot(&ctx).await.iter().map(|t| t.thread_id).collect();
        assert_eq!(ids, [6]);

        // PR 2 no longer active: its threads are dropped.
        serve_active(&server, vec![pr(1)]).await;
        serve_threads(&server, 1, ok(vec![])).await;
        run(&mut src, &ctx, Some(at(10, 6)), at(10, 7))
            .await
            .unwrap();
        assert!(snapshot(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn transient_error_aborts_without_writing() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1), pr(2)]).await;
        let mine = ok(vec![thread(5, vec![comment(1, ME, "a", at(10, 1))])]);
        serve_threads(&server, 1, mine).await;
        serve_threads(&server, 2, ResponseTemplate::new(500)).await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        let err = run(&mut src, &ctx, None, at(10, 5)).await.unwrap_err();
        assert!(matches!(err, ProviderError::Transient(_)), "{err:?}");
        assert!(
            ctx.snapshots
                .get::<TrackedThreads>(SNAPSHOT_TRACKED_THREADS)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn since_is_earliest_of_my_comment_and_mention() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1)]).await;
        serve_threads(
            &server,
            1,
            ok(vec![thread(
                5,
                vec![
                    comment(1, "other", &format!("@<{ME}> look"), at(10, 1)),
                    comment(2, ME, "on it", at(10, 3)),
                ],
            )]),
        )
        .await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 5)).await.unwrap();
        let t = snapshot(&ctx).await;
        assert_eq!(t[0].reason, ThreadReason::Commenter);
        assert_eq!(t[0].since_utc, at(10, 1));
    }

    #[tokio::test]
    async fn edited_old_mention_emits_with_published_time() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1)]).await;
        let mut edited = comment(7, "other", &format!("@<{ME}> added later"), at(1, 3));
        edited["lastUpdatedDate"] = json!(at(10, 3));
        serve_threads(&server, 1, ok(vec![thread(5, vec![edited])])).await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
        let mut src = source(&server);
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 5))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].occurred_utc, at(1, 3));
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:thread:5:mention:7");
    }

    #[tokio::test]
    async fn fatal_thread_error_stops_the_fan_out() {
        for status in [401u16, 500] {
            let server = MockServer::start().await;
            let prs: Vec<Value> = (1..=8).map(pr).collect();
            serve_active(&server, prs).await;
            for id in 1..=8 {
                serve_threads(&server, id, ResponseTemplate::new(status)).await;
            }
            let (ctx, _s) = sync_context(cfg(), me(), at(10, 5), None, false);
            let mut src = source(&server);
            let err = run(&mut src, &ctx, None, at(10, 5)).await.unwrap_err();
            assert!(
                matches!(
                    err,
                    ProviderError::Unauthorized | ProviderError::Transient(_)
                ),
                "{err:?}"
            );
            let thread_requests = server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.url.path().ends_with("/threads"))
                .count();
            assert!(
                thread_requests <= CONCURRENCY,
                "{status}: {thread_requests} thread requests"
            );
        }
    }
}
