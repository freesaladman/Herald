//! Pr-tier source: new comments and status changes in threads of tracked PRs and tracked threads.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream;
use herald_core::config::build_glob;
use herald_core::event::{
    Actor, ActorKind, CommentRef, Event, PrRef, PrRole, event_types, ids, subject,
};
use herald_core::provider::{PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::pr_discovery::excerpt;
use super::{
    SNAPSHOT_TRACKED_PRS, SNAPSHOT_TRACKED_THREADS, ThreadReason, TrackedPrs, TrackedThreads,
    actor_from, is_fatal, mentions, pr_key, pr_web_url, project_ref, repo_ref, window_start,
};
use crate::connection::AdoShared;
use crate::dto_git::{Comment, GitPullRequestCommentThread, IdentityRef};

const CONCURRENCY: usize = 4;
const SNAPSHOT_SEEN: &str = "pr_threads:seen";
/// Extra reach before the window for a thread known only from discovery, which can lag a comment.
const DISCOVERY_SLACK: Duration = Duration::seconds(120);
/// A comment this close to the thread's last update is taken as the change that updated it.
const ACTOR_TOLERANCE: Duration = Duration::seconds(60);

pub struct PrThreadsSource {
    shared: Arc<AdoShared>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SeenState {
    /// Keyed `{pr_key}:{threadId}`.
    threads: BTreeMap<String, ThreadSeen>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ThreadSeen {
    comment_ids: Vec<i64>,
    status: String,
    last_updated: Option<DateTime<Utc>>,
}

/// One PR to fetch, with everything an event needs so no PR lookup is required.
struct Involved {
    key: String,
    repo_id: String,
    repo_name: String,
    project: String,
    project_id: String,
    pr_id: i64,
    title: String,
    source_ref: String,
    target_ref: String,
    is_draft: bool,
    role: PrRole,
    /// Tracked PR: every thread counts.
    all_threads: bool,
    /// Tracked threads on this PR: thread id -> its `since_utc`.
    since: HashMap<i64, DateTime<Utc>>,
}

type Fetched = (usize, Option<Vec<GitPullRequestCommentThread>>);

impl PrThreadsSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    /// `Ok(None)` means skip this PR (logged); errors that mean the poll can't proceed are returned.
    async fn fetch(&self, i: usize, pr: &Involved) -> Result<Fetched, ProviderError> {
        let project = project_ref(&pr.project_id, &pr.project);
        match self
            .shared
            .client
            .pr_threads(project, &pr.repo_id, pr.pr_id)
            .await
            .map_err(ProviderError::from)
        {
            Ok(t) => Ok((i, Some(t))),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!("skipping threads of PR {} in {}: {e}", pr.pr_id, pr.repo_id);
                Ok((i, None))
            }
        }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let involved = involved_prs(ctx).await?;
        let old: SeenState = ctx.snapshots.get(SNAPSHOT_SEEN).await?.unwrap_or_default();

        let fetches: Vec<BoxFuture<'_, Result<Fetched, ProviderError>>> = involved
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

        let seeding = ctx.watermark.is_none();
        let window = window_start(ctx);
        let lookback_floor = ctx.server_now - Duration::days(ctx.config.polling.max_lookback_days);
        let discovery_cut = window
            - Duration::seconds(
                i64::try_from(ctx.config.polling.slow_secs).unwrap_or(i64::MAX / 2),
            )
            - DISCOVERY_SLACK;
        let org = &self.shared.organization;
        let mut state = SeenState::default();
        let mut events = Vec::new();
        for (i, fetched) in results {
            let pr = &involved[i];
            let prefix = format!("{}:", pr.key);
            let Some(threads) = fetched else {
                // Keep the previous state so the next successful poll diffs against it.
                state.threads.extend(
                    old.threads
                        .iter()
                        .filter(|(k, _)| k.starts_with(&prefix))
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
                continue;
            };
            for thread in threads.iter().filter(|t| !t.is_deleted) {
                if thread.thread_type().is_some() {
                    continue;
                }
                let since = pr.since.get(&thread.id);
                if !pr.all_threads && since.is_none() {
                    continue;
                }
                let key = format!("{prefix}{}", thread.id);
                let previous = old.threads.get(&key);
                // Seen threads diff exactly by comment id. A thread first seen through discovery
                // alone can predate the window by the discovery lag, but never its tracking.
                let cut = match (previous, pr.all_threads, since) {
                    (Some(_), _, _) => lookback_floor,
                    (None, false, Some(since)) => discovery_cut.max(*since),
                    (None, _, _) => window,
                };
                let mut seen = previous.cloned().unwrap_or_default();
                let status = thread.status.clone().unwrap_or_default();
                if !seeding {
                    events.extend(new_comment_events(ctx, org, pr, thread, &seen, cut));
                    if let Some(p) = previous.filter(|p| p.status != status) {
                        events.push(status_event(ctx, org, pr, thread, &p.status));
                    }
                }
                seen.comment_ids = text_comments(thread).map(|c| c.id).collect();
                seen.status = status;
                seen.last_updated = thread.last_updated_date;
                state.threads.insert(key, seen);
            }
        }
        ctx.snapshots.put(SNAPSHOT_SEEN, &state).await?;
        Ok(PollOutcome { events })
    }
}

impl Source for PrThreadsSource {
    fn name(&self) -> &'static str {
        "pr_threads"
    }

    fn tier(&self) -> Tier {
        Tier::Pr
    }

    fn poll<'a>(
        &'a mut self,
        ctx: &'a SyncContext,
    ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>> {
        Box::pin(self.poll_inner(ctx))
    }
}

/// Tracked PRs (all threads of my own, my threads of the rest), plus tracked threads.
async fn involved_prs(ctx: &SyncContext) -> Result<Vec<Involved>, ProviderError> {
    let tracked: TrackedPrs = ctx
        .snapshots
        .get(SNAPSHOT_TRACKED_PRS)
        .await?
        .unwrap_or_default();
    let threads: TrackedThreads = ctx
        .snapshots
        .get(SNAPSHOT_TRACKED_THREADS)
        .await?
        .unwrap_or_default();
    let mut map: BTreeMap<String, Involved> = BTreeMap::new();
    for p in tracked.prs {
        let key = pr_key(&p.repo_id, p.pr_id);
        map.insert(
            key.clone(),
            Involved {
                key,
                repo_id: p.repo_id,
                repo_name: p.repo_name,
                project: p.project,
                project_id: p.project_id,
                pr_id: p.pr_id,
                title: p.title,
                source_ref: p.source_ref,
                target_ref: p.target_ref,
                is_draft: p.is_draft,
                // On PRs I did not author, only threads I am in (from discovery) are watched.
                all_threads: p.role == PrRole::Author,
                role: p.role,
                since: HashMap::new(),
            },
        );
    }
    for t in threads.threads {
        let key = pr_key(&t.repo_id, t.pr_id);
        let entry = map.entry(key.clone()).or_insert_with(|| Involved {
            key,
            repo_id: t.repo_id.clone(),
            repo_name: t.repo_name.clone(),
            project: t.project.clone(),
            project_id: t.project_id.clone(),
            pr_id: t.pr_id,
            title: t.title.clone(),
            source_ref: t.source_ref.clone(),
            target_ref: t.target_ref.clone(),
            is_draft: t.is_draft,
            role: match t.reason {
                ThreadReason::Commenter => PrRole::Commenter,
                ThreadReason::Mentioned => PrRole::Mentioned,
            },
            all_threads: false,
            since: HashMap::new(),
        });
        entry.since.insert(t.thread_id, t.since_utc);
    }
    Ok(map.into_values().collect())
}

fn text_comments(thread: &GitPullRequestCommentThread) -> impl Iterator<Item = &Comment> {
    thread.comments.iter().filter(|c| {
        !c.is_deleted && (c.comment_type.is_empty() || c.comment_type.eq_ignore_ascii_case("text"))
    })
}

fn pr_ref(pr: &Involved) -> PrRef {
    PrRef {
        id: pr.pr_id,
        title: pr.title.clone(),
        source_branch: pr.source_ref.clone(),
        target_branch: pr.target_ref.clone(),
        is_draft: pr.is_draft,
        role: pr.role,
    }
}

fn thread_url(org: &str, pr: &Involved, thread_id: i64) -> String {
    format!(
        "{}?discussionId={thread_id}",
        pr_web_url(org, &pr.project, &pr.repo_name, pr.pr_id)
    )
}

fn unknown_actor() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: String::new(),
        display_name: "Unknown".into(),
        unique_name: None,
    }
}

fn new_comment_events(
    ctx: &SyncContext,
    org: &str,
    pr: &Involved,
    thread: &GitPullRequestCommentThread,
    seen: &ThreadSeen,
    cut: DateTime<Utc>,
) -> Vec<Event> {
    let c = &ctx.connection.id;
    let first_id = text_comments(thread).map(|c| c.id).min();
    let mut events = Vec::new();
    for comment in text_comments(thread) {
        let Some(published) = comment.published_date else {
            continue;
        };
        if seen.comment_ids.contains(&comment.id) || published < cut {
            continue;
        }
        let content = comment.content.as_deref().unwrap_or_default();
        let by_me = comment
            .author
            .as_ref()
            .is_some_and(|a| !a.id.is_empty() && a.id.eq_ignore_ascii_case(&ctx.me.id));
        let thread_id = thread.id.to_string();
        let comment_id = comment.id.to_string();
        let (event_type, id, title) = if !by_me && mentions(content, &ctx.me.id) {
            (
                event_types::PR_MENTIONED,
                ids::pr_mention(c, &pr.repo_id, pr.pr_id, &thread_id, &comment_id),
                format!("Mentioned in PR: {}", pr.title),
            )
        } else {
            let (t, title) = if Some(comment.id) == first_id {
                (
                    event_types::PR_COMMENT_ADDED,
                    format!("New comment on PR: {}", pr.title),
                )
            } else {
                (
                    event_types::PR_COMMENT_REPLIED,
                    format!("New reply on PR: {}", pr.title),
                )
            };
            (
                t,
                ids::pr_comment(c, &pr.repo_id, pr.pr_id, &thread_id, &comment_id),
                title,
            )
        };
        let mut e = Event::new(
            ctx.connection.clone(),
            event_type,
            published,
            ctx.server_now,
            title,
            subject::pr(c, &pr.repo_id, pr.pr_id),
        )
        .with_id(id);
        e.catch_up = ctx.catch_up;
        e.url = Some(thread_url(org, pr, thread.id));
        e.subtitle = Some(pr.repo_name.clone());
        e.actor = comment
            .author
            .as_ref()
            .map_or_else(unknown_actor, |a| actor_from(a, &ctx.me));
        e.repo = Some(repo_ref(&pr.repo_id, &pr.repo_name, &pr.project));
        e.pr = Some(pr_ref(pr));
        e.comment = Some(CommentRef {
            thread_id,
            comment_id,
            excerpt: excerpt(content, &ctx.me.id),
            thread_status: thread.status.clone().unwrap_or_default(),
        });
        events.push(e);
    }
    events
}

/// ADO does not say who changed a text thread's status, so this is inferred from the comments.
fn status_actor(ctx: &SyncContext, pr: &Involved, thread: &GitPullRequestCommentThread) -> Actor {
    let last = text_comments(thread)
        .max_by_key(|c| c.id)
        .and_then(|c| c.author.as_ref().map(|a| (c, a)));
    if let (Some((comment, author)), Some(updated)) = (last, thread.last_updated_date) {
        let near = comment
            .last_updated_date
            .is_some_and(|t| (updated - t).abs() <= ACTOR_TOLERANCE);
        if near {
            return actor_from(author, &ctx.me);
        }
    }
    let starter = text_comments(thread)
        .min_by_key(|c| c.id)
        .and_then(|c| c.author.as_ref());
    if let Some(a) = starter.filter(|a| is_bot(ctx, a)) {
        return actor_from(a, &ctx.me);
    }
    match last {
        Some((_, author)) if pr.role == PrRole::Author => {
            let actor = actor_from(author, &ctx.me);
            if actor.kind == ActorKind::Me {
                actor
            } else {
                unknown_actor()
            }
        }
        _ => unknown_actor(),
    }
}

fn is_bot(ctx: &SyncContext, identity: &IdentityRef) -> bool {
    ctx.connection_config
        .bots
        .iter()
        .filter_map(|p| build_glob(p).ok())
        .map(|g| g.compile_matcher())
        .any(|m| m.is_match(&identity.unique_name) || m.is_match(&identity.display_name))
}

fn status_event(
    ctx: &SyncContext,
    org: &str,
    pr: &Involved,
    thread: &GitPullRequestCommentThread,
    previous: &str,
) -> Event {
    let c = &ctx.connection.id;
    let status = thread.status.clone().unwrap_or_default();
    let occurred = thread.last_updated_date.unwrap_or(ctx.server_now);
    let actor = status_actor(ctx, pr, thread);
    let mut e = Event::new(
        ctx.connection.clone(),
        event_types::PR_THREAD_STATUS_CHANGED,
        occurred,
        ctx.server_now,
        format!("Thread {status} on PR: {}", pr.title),
        subject::pr(c, &pr.repo_id, pr.pr_id),
    )
    .with_id(ids::pr_thread_status(
        c,
        &pr.repo_id,
        pr.pr_id,
        &thread.id.to_string(),
        &status,
        occurred,
    ));
    e.inferred = true;
    e.catch_up = ctx.catch_up;
    e.url = Some(thread_url(org, pr, thread.id));
    e.subtitle = Some(pr.repo_name.clone());
    e.actor = actor;
    e.repo = Some(repo_ref(&pr.repo_id, &pr.repo_name, &pr.project));
    e.pr = Some(pr_ref(pr));
    e.comment = Some(CommentRef {
        thread_id: thread.id.to_string(),
        comment_id: String::new(),
        excerpt: String::new(),
        thread_status: status.clone(),
    });
    e.data = json!({ "status": status, "previousStatus": previous });
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AdoAuth;
    use crate::client::AdoClient;
    use crate::sources::{TrackedPr, TrackedThread};
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::provider::Identity;
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ME: &str = "aaaa-me";

    fn at(day: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, day, h, 0, 0).unwrap()
    }

    fn cfg() -> ConnectionConfig {
        ConnectionConfig {
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

    fn source(server: &MockServer) -> PrThreadsSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PrThreadsSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn comment(id: i64, author: &str, content: &str, when: DateTime<Utc>) -> Value {
        json!({
            "id": id, "parentCommentId": if id == 1 { 0 } else { 1 },
            "author": { "id": author, "displayName": "Person", "uniqueName": "p@example.test" },
            "content": content, "publishedDate": when, "lastUpdatedDate": when,
            "commentType": "text",
        })
    }

    fn thread(id: i64, status: &str, updated: DateTime<Utc>, comments: Vec<Value>) -> Value {
        json!({
            "id": id, "status": status, "isDeleted": false,
            "publishedDate": at(1, 0), "lastUpdatedDate": updated,
            "properties": {}, "comments": comments,
        })
    }

    async fn serve(server: &MockServer, pr_id: i64, body: ResponseTemplate) {
        server.reset().await;
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

    /// Tracks one thread of PR 1 as discovery would after I commented on it.
    async fn track_thread(ctx: &SyncContext, thread_id: i64, since: DateTime<Utc>) {
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_THREADS,
                &TrackedThreads {
                    threads: vec![TrackedThread {
                        repo_id: "Repo-1".into(),
                        repo_name: "widgets".into(),
                        project: "Alpha".into(),
                        project_id: "p1".into(),
                        pr_id: 1,
                        thread_id,
                        since_utc: since,
                        reason: ThreadReason::Commenter,
                        title: "change".into(),
                        source_ref: String::new(),
                        target_ref: String::new(),
                        is_draft: false,
                        url: String::new(),
                    }],
                },
            )
            .await
            .unwrap();
    }

    async fn track_pr(ctx: &SyncContext, pr_id: i64) {
        track_pr_as(ctx, pr_id, PrRole::Author).await;
    }

    async fn track_pr_as(ctx: &SyncContext, pr_id: i64, role: PrRole) {
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_PRS,
                &TrackedPrs {
                    prs: vec![TrackedPr {
                        repo_id: "Repo-1".into(),
                        repo_name: "widgets".into(),
                        project: "Alpha".into(),
                        project_id: "p1".into(),
                        pr_id,
                        role,
                        title: "change".into(),
                        ..Default::default()
                    }],
                },
            )
            .await
            .unwrap();
    }

    async fn run(
        src: &mut PrThreadsSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = now;
        src.poll(&c).await.map(|o| o.events)
    }

    fn base_threads() -> Vec<Value> {
        vec![thread(
            5,
            "active",
            at(10, 1),
            vec![comment(1, "other", "first", at(10, 1))],
        )]
    }

    /// Seeds with `base_threads`, then polls again with `next`.
    async fn seed_then(
        src: &mut PrThreadsSource,
        server: &MockServer,
        ctx: &SyncContext,
        next: Vec<Value>,
    ) -> Vec<Event> {
        serve(server, 1, ok(base_threads())).await;
        assert!(run(src, ctx, None, at(10, 2)).await.unwrap().is_empty());
        serve(server, 1, ok(next)).await;
        run(src, ctx, Some(at(10, 2)), at(10, 5)).await.unwrap()
    }

    #[tokio::test]
    async fn seed_emits_nothing() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        serve(&server, 1, ok(base_threads())).await;
        let mut src = source(&server);
        assert!(
            run(&mut src, &ctx, None, at(10, 2))
                .await
                .unwrap()
                .is_empty()
        );
        let s: SeenState = ctx.snapshots.get(SNAPSHOT_SEEN).await.unwrap().unwrap();
        assert_eq!(s.threads["repo-1:1:5"].comment_ids, vec![1]);
    }

    #[tokio::test]
    async fn reply_new_thread_and_my_comment() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        let mut src = source(&server);
        let next = vec![
            thread(
                5,
                "active",
                at(10, 4),
                vec![
                    comment(1, "other", "first", at(10, 1)),
                    comment(2, "other", "reply", at(10, 3)),
                    comment(3, &ME.to_uppercase(), "mine", at(10, 4)),
                ],
            ),
            thread(
                6,
                "active",
                at(10, 4),
                vec![comment(1, "other", "brand new", at(10, 4))],
            ),
        ];
        let events = seed_then(&mut src, &server, &ctx, next).await;
        let by: Vec<(&str, &str)> = events
            .iter()
            .map(|e| (e.event_type.as_str(), e.id.as_str()))
            .collect();
        assert_eq!(
            by,
            vec![
                ("pr.comment.replied", "c1:pr:Repo-1:1:thread:5:comment:2"),
                ("pr.comment.replied", "c1:pr:Repo-1:1:thread:5:comment:3"),
                ("pr.comment.added", "c1:pr:Repo-1:1:thread:6:comment:1"),
            ]
        );
        assert_eq!(events[0].occurred_utc, at(10, 3));
        assert_eq!(events[0].pr.as_ref().unwrap().role, PrRole::Author);
        assert_eq!(events[0].comment.as_ref().unwrap().excerpt, "reply");
        assert!(
            events[0]
                .url
                .as_deref()
                .unwrap()
                .ends_with("/pullrequest/1?discussionId=5")
        );
        assert_eq!(events[1].actor.kind, herald_core::event::ActorKind::Me);
    }

    #[tokio::test]
    async fn on_a_pr_i_did_not_author_only_my_threads_are_watched() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr_as(&ctx, 1, PrRole::Reviewer).await;
        track_thread(&ctx, 5, at(10, 1)).await;
        let mut src = source(&server);
        let next = vec![
            thread(
                5,
                "active",
                at(10, 3),
                vec![
                    comment(1, "other", "first", at(10, 1)),
                    comment(2, "other", "reply", at(10, 3)),
                ],
            ),
            thread(
                6,
                "active",
                at(10, 4),
                vec![comment(1, "other", "not mine", at(10, 4))],
            ),
        ];
        let events = seed_then(&mut src, &server, &ctx, next).await;
        let ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["c1:pr:Repo-1:1:thread:5:comment:2"]);
        assert_eq!(events[0].pr.as_ref().unwrap().role, PrRole::Reviewer);
    }

    #[tokio::test]
    async fn mention_emits_only_shared_mention_event() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        let mut src = source(&server);
        let next = vec![thread(
            5,
            "active",
            at(10, 3),
            vec![
                comment(1, "other", "first", at(10, 1)),
                comment(2, "other", &format!("hey @<{ME}> look"), at(10, 3)),
            ],
        )];
        let events = seed_then(&mut src, &server, &ctx, next).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "pr.mentioned");
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:thread:5:mention:2");
        assert_eq!(events[0].comment.as_ref().unwrap().excerpt, "hey @you look");
    }

    #[tokio::test]
    async fn status_change_emits_inferred_event_with_data() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        let mut src = source(&server);
        let next = vec![thread(
            5,
            "fixed",
            at(10, 4),
            vec![comment(1, "other", "first", at(10, 1))],
        )];
        let events = seed_then(&mut src, &server, &ctx, next).await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event_type, "pr.thread.status.changed");
        assert!(e.inferred);
        assert_eq!(e.occurred_utc, at(10, 4));
        assert_eq!(
            e.data,
            json!({ "status": "fixed", "previousStatus": "active" })
        );
        // The last comment predates the thread update, so the actor is unknown.
        assert_eq!(e.actor.id, "");
        assert!(e.id.contains(":thread:5:status:fixed:"));
    }

    #[tokio::test]
    async fn first_sight_of_old_thread_does_not_replay_history() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(20, 0), None, false);
        // Tracked via prs:threads only: nothing is known of the PR beyond the snapshot fields.
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_THREADS,
                &TrackedThreads {
                    threads: vec![TrackedThread {
                        repo_id: "Repo-1".into(),
                        repo_name: "widgets".into(),
                        project: "Alpha".into(),
                        project_id: "p1".into(),
                        pr_id: 1,
                        thread_id: 5,
                        since_utc: at(2, 0),
                        reason: ThreadReason::Commenter,
                        title: "elsewhere".into(),
                        source_ref: "refs/heads/topic".into(),
                        target_ref: "refs/heads/main".into(),
                        is_draft: false,
                        url: String::new(),
                    }],
                },
            )
            .await
            .unwrap();
        serve(
            &server,
            1,
            ok(vec![
                thread(
                    5,
                    "active",
                    at(20, 1),
                    vec![
                        comment(1, "other", "ancient", at(3, 0)),
                        comment(2, "other", "recent", at(20, 1)),
                    ],
                ),
                thread(
                    6,
                    "active",
                    at(20, 1),
                    vec![comment(1, "other", "untracked", at(20, 1))],
                ),
            ]),
        )
        .await;
        let mut src = source(&server);
        // Watermark just before `now`: window is ~19:58 on day 19, far past the old comment.
        let events = run(&mut src, &ctx, Some(at(19, 23)), at(20, 2))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:thread:5:comment:2");
        let pr = events[0].pr.as_ref().unwrap();
        assert_eq!(
            (pr.role, pr.title.as_str()),
            (PrRole::Commenter, "elsewhere")
        );
        let s: SeenState = ctx.snapshots.get(SNAPSHOT_SEEN).await.unwrap().unwrap();
        assert_eq!(s.threads.len(), 1);
        assert_eq!(s.threads["repo-1:1:5"].comment_ids, vec![1, 2]);
    }

    async fn track_thread_only(ctx: &SyncContext, since_utc: DateTime<Utc>) {
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_THREADS,
                &TrackedThreads {
                    threads: vec![TrackedThread {
                        repo_id: "Repo-1".into(),
                        repo_name: "widgets".into(),
                        project: "Alpha".into(),
                        project_id: "p1".into(),
                        pr_id: 1,
                        thread_id: 5,
                        since_utc,
                        reason: ThreadReason::Commenter,
                        title: "elsewhere".into(),
                        source_ref: "refs/heads/topic".into(),
                        target_ref: "refs/heads/main".into(),
                        is_draft: false,
                        url: String::new(),
                    }],
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn discovery_only_thread_keeps_reply_made_before_the_window() {
        let server = MockServer::start().await;
        let t0 = at(10, 1);
        let (ctx, _s) = sync_context(cfg(), me(), t0, None, false);
        track_thread_only(&ctx, t0).await;
        let reply = t0 + Duration::minutes(5);
        serve(
            &server,
            1,
            ok(vec![thread(
                5,
                "active",
                reply,
                vec![
                    comment(1, ME, "mine", t0),
                    comment(2, "other", "reply", reply),
                ],
            )]),
        )
        .await;
        let mut src = source(&server);
        // Discovery recorded the thread at T0+8m; this is the first poll that sees it, at T0+9m.
        let events = run(
            &mut src,
            &ctx,
            Some(t0 + Duration::minutes(8)),
            t0 + Duration::minutes(9),
        )
        .await
        .unwrap();
        let ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        assert!(
            ids.contains(&"c1:pr:Repo-1:1:thread:5:comment:2"),
            "{ids:?}"
        );
    }

    #[tokio::test]
    async fn seen_thread_diffs_by_comment_id_beyond_the_window() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        let mut src = source(&server);
        // The reply predates the poll window (watermark - 2m) but was not seen before.
        let late = thread(
            5,
            "active",
            at(10, 3),
            vec![
                comment(1, "other", "first", at(10, 1)),
                comment(2, "other", "late", at(10, 3)),
            ],
        );
        serve(&server, 1, ok(base_threads())).await;
        run(&mut src, &ctx, None, at(10, 2)).await.unwrap();
        serve(&server, 1, ok(vec![late])).await;
        let events = run(&mut src, &ctx, Some(at(10, 6)), at(10, 7))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:thread:5:comment:2");
    }

    #[tokio::test]
    async fn non_fatal_fetch_failure_keeps_seen_state_for_the_next_status_change() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        let mut src = source(&server);
        serve(&server, 1, ok(base_threads())).await;
        run(&mut src, &ctx, None, at(10, 2)).await.unwrap();

        serve(&server, 1, ResponseTemplate::new(404)).await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 3))
            .await
            .unwrap();
        assert!(events.is_empty());
        let s: SeenState = ctx.snapshots.get(SNAPSHOT_SEEN).await.unwrap().unwrap();
        assert_eq!(s.threads["repo-1:1:5"].status, "active");

        let fixed = thread(
            5,
            "fixed",
            at(10, 4),
            vec![comment(1, "other", "first", at(10, 1))],
        );
        serve(&server, 1, ok(vec![fixed])).await;
        let events = run(&mut src, &ctx, Some(at(10, 3)), at(10, 5))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "pr.thread.status.changed");
        assert_eq!(events[0].data["previousStatus"], "active");
    }

    fn secs(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 10, h, m, s).unwrap()
    }

    /// Seeds `base_threads`, then resolves the thread (updated at hour 4) with `comments`.
    async fn resolver(role: PrRole, bots: &[&str], comments: Vec<Value>) -> Actor {
        let server = MockServer::start().await;
        let mut conn = cfg();
        conn.bots = bots.iter().map(|b| (*b).to_string()).collect();
        let (ctx, _s) = sync_context(conn, me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_PRS,
                &TrackedPrs {
                    prs: vec![TrackedPr {
                        repo_id: "Repo-1".into(),
                        project_id: "p1".into(),
                        pr_id: 1,
                        role,
                        ..Default::default()
                    }],
                },
            )
            .await
            .unwrap();
        if role != PrRole::Author {
            track_thread(&ctx, 5, at(10, 1)).await;
        }
        let mut src = source(&server);
        let next = vec![thread(5, "fixed", at(10, 4), comments)];
        let events = seed_then(&mut src, &server, &ctx, next).await;
        let status = events
            .into_iter()
            .find(|e| e.event_type == "pr.thread.status.changed")
            .unwrap();
        status.actor
    }

    #[tokio::test]
    async fn status_actor_is_me_for_my_reply_just_before_the_resolve() {
        let actor = resolver(
            PrRole::Reviewer,
            &[],
            vec![
                comment(1, "other", "first", at(10, 1)),
                comment(2, ME, "done", secs(3, 59, 56)),
            ],
        )
        .await;
        assert_eq!(actor.kind, ActorKind::Me);
    }

    #[tokio::test]
    async fn status_actor_is_the_recent_commenter() {
        let actor = resolver(
            PrRole::Reviewer,
            &[],
            vec![
                comment(1, "other", "first", at(10, 1)),
                comment(2, "bob", "fixed it", secs(3, 59, 30)),
            ],
        )
        .await;
        assert_eq!(actor.id, "bob");
    }

    #[tokio::test]
    async fn status_actor_is_the_bot_that_started_the_thread() {
        let mut first = comment(1, "bot-id", "finding", at(10, 1));
        first["author"]["uniqueName"] = json!("Build\\svc");
        let actor = resolver(PrRole::Reviewer, &["Build\\*"], vec![first]).await;
        assert_eq!(actor.id, "bot-id");
    }

    #[tokio::test]
    async fn status_actor_is_me_on_my_own_pr_when_my_comment_is_last() {
        let comments = vec![
            comment(1, "other", "first", at(10, 1)),
            comment(2, ME, "will do", at(10, 2)),
        ];
        let actor = resolver(PrRole::Author, &[], comments.clone()).await;
        assert_eq!(actor.kind, ActorKind::Me);
        let actor = resolver(PrRole::Reviewer, &[], comments).await;
        assert_eq!((actor.kind, actor.id.as_str()), (ActorKind::Human, ""));
    }

    #[tokio::test]
    async fn transient_aborts_without_writing() {
        let server = MockServer::start().await;
        let (ctx, _s) = sync_context(cfg(), me(), at(10, 2), None, false);
        track_pr(&ctx, 1).await;
        serve(&server, 1, ResponseTemplate::new(500)).await;
        let mut src = source(&server);
        let err = run(&mut src, &ctx, Some(at(10, 1)), at(10, 2))
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::Transient(_)));
        assert!(
            ctx.snapshots
                .get::<SeenState>(SNAPSHOT_SEEN)
                .await
                .unwrap()
                .is_none()
        );
    }
}
