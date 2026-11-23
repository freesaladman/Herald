//! Pr-tier source: review activity on tracked PRs, from system threads and iterations.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream;
use herald_core::event::{Actor, ActorKind, Event, PrRef, PrRole, event_types, ids, subject};
use herald_core::provider::{PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    SNAPSHOT_TRACKED_PRS, TrackedPr, TrackedPrs, actor_from, is_fatal, pr_key, pr_web_url,
    project_ref, repo_ref, wanted_on_pr, window_start,
};
use crate::connection::AdoShared;
use crate::dto_git::{GitPullRequestCommentThread, GitPullRequestIteration, IdentityRef};

const CONCURRENCY: usize = 4;
const SNAPSHOT_SEEN: &str = "pr_reviews:seen";
/// A reset thread this soon after a push is the same push's fallout.
const PUSH_RESET_WINDOW: Duration = Duration::minutes(2);

pub struct PrReviewsSource {
    shared: Arc<AdoShared>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SeenState {
    /// Keyed by `pr_key`.
    prs: BTreeMap<String, PrSeen>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PrSeen {
    threads: Vec<i64>,
    iterations: Vec<i64>,
    my_vote: Option<MyVote>,
    /// Creation time of the latest iteration that raised a re-review.
    last_iteration_rereview: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MyVote {
    value: i32,
    time: DateTime<Utc>,
}

struct PrData {
    threads: Vec<GitPullRequestCommentThread>,
    iterations: Vec<GitPullRequestIteration>,
}

type Fetched = (usize, Option<PrData>);

impl PrReviewsSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    /// `Ok(None)` means skip this PR (logged); errors that mean the poll cannot proceed are returned.
    async fn fetch(&self, i: usize, pr: &TrackedPr) -> Result<Fetched, ProviderError> {
        let project = project_ref(&pr.project_id, &pr.project);
        let client = &self.shared.client;
        let both = tokio::try_join!(
            client.pr_threads(project, &pr.repo_id, pr.pr_id),
            client.pr_iterations(project, &pr.repo_id, pr.pr_id),
        );
        match both.map_err(ProviderError::from) {
            Ok((threads, iterations)) => Ok((
                i,
                Some(PrData {
                    threads,
                    iterations,
                }),
            )),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!("skipping reviews of PR {} in {}: {e}", pr.pr_id, pr.repo_id);
                Ok((i, None))
            }
        }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let tracked: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await?
            .unwrap_or_default();
        let old: SeenState = ctx.snapshots.get(SNAPSHOT_SEEN).await?.unwrap_or_default();

        let fetches: Vec<BoxFuture<'_, Result<Fetched, ProviderError>>> = tracked
            .prs
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
        let org = &self.shared.organization;
        let mut state = SeenState::default();
        let mut events = Vec::new();
        for (i, fetched) in results {
            let pr = &tracked.prs[i];
            let key = pr_key(&pr.repo_id, pr.pr_id);
            let previous = old.prs.get(&key);
            let Some(data) = fetched else {
                // Keep the previous state so the next successful poll diffs against it.
                if let Some(p) = previous {
                    state.prs.insert(key, p.clone());
                }
                continue;
            };
            // A PR seen for the first time must not replay history older than the window.
            let cut = if previous.is_some() {
                lookback_floor
            } else {
                window
            };
            let mut seen = previous.cloned().unwrap_or_default();
            let found = review_events(ctx, org, pr, data, &mut seen, cut, previous.is_none());
            if !seeding {
                events.extend(found.into_iter().filter(wanted_on_pr));
            }
            state.prs.insert(key, seen);
        }
        ctx.snapshots.put(SNAPSHOT_SEEN, &state).await?;
        Ok(PollOutcome { events })
    }
}

impl Source for PrReviewsSource {
    fn name(&self) -> &'static str {
        "pr_reviews"
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

/// Diffs one PR's system threads and iterations against `seen`, updating it in place.
fn review_events(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    data: PrData,
    seen: &mut PrSeen,
    cut: DateTime<Utc>,
    first_sight: bool,
) -> Vec<Event> {
    let mut system: Vec<&GitPullRequestCommentThread> = data
        .threads
        .iter()
        .filter(|t| !t.is_deleted && t.thread_type().is_some())
        .collect();
    system.sort_by_key(|t| (t.published_date, t.id));

    let mut events = Vec::new();
    for thread in system {
        let occurred = thread.published_date.unwrap_or(ctx.server_now);
        let reset_mine = track_vote(ctx, thread, occurred, &mut seen.my_vote);
        if seen.threads.contains(&thread.id) {
            continue;
        }
        seen.threads.push(thread.id);
        if occurred < cut {
            continue;
        }
        let mut found = thread_events(ctx, org, pr, thread, occurred);
        if first_sight {
            // The PR's first sight already announces it as a review request or creation.
            found.retain(|e| {
                e.event_type != event_types::PR_REVIEWER_ADDED
                    || !(e.data["isMe"] == true || e.data["isMyGroup"] == true)
            });
        }
        events.extend(found);
        // A push and the reset it triggers can land in different polls; one re-review covers both.
        let after_push_rereview = seen
            .last_iteration_rereview
            .is_some_and(|c| c <= occurred && occurred - c <= PUSH_RESET_WINDOW);
        if reset_mine && pr.role == PrRole::Reviewer && !after_push_rereview {
            events.push(rereview_event(
                ctx,
                org,
                pr,
                occurred,
                ids::pr_rereview_reset(
                    &ctx.connection.id,
                    &pr.repo_id,
                    pr.pr_id,
                    &thread.id.to_string(),
                ),
            ));
        }
    }

    let mut iterations = data.iterations;
    iterations.sort_by_key(|it| it.id);
    for it in iterations {
        if seen.iterations.contains(&it.id) {
            continue;
        }
        seen.iterations.push(it.id);
        let created = it.created_date.unwrap_or(ctx.server_now);
        // Iteration 1 is the PR's creation, not a push.
        if it.id <= 1 || created < cut {
            continue;
        }
        events.push(iteration_event(ctx, org, pr, &it, created));
        // The PR list's vote is authoritative; the thread-derived one only fills in when absent.
        let current = pr.my_vote.or(seen.my_vote.map(|v| v.value)).unwrap_or(0);
        let voted_before = current != 0 && seen.my_vote.is_none_or(|v| created > v.time);
        // A reset in the same poll already raised the reset re-review for this push.
        let reset_covers = seen.my_vote.is_some_and(|v| {
            v.value == 0 && v.time >= created && v.time - created <= PUSH_RESET_WINDOW
        });
        if pr.role == PrRole::Reviewer && voted_before && !reset_covers {
            seen.last_iteration_rereview = Some(created);
            events.push(rereview_event(
                ctx,
                org,
                pr,
                created,
                ids::pr_rereview(
                    &ctx.connection.id,
                    &pr.repo_id,
                    pr.pr_id,
                    &it.id.to_string(),
                ),
            ));
        }
    }
    events
}

/// Updates my latest vote from a system thread; true when the thread reset a vote of mine.
fn track_vote(
    ctx: &SyncContext,
    thread: &GitPullRequestCommentThread,
    time: DateTime<Utc>,
    vote: &mut Option<MyVote>,
) -> bool {
    let mine = |i: &IdentityRef| is_me(ctx, &i.id);
    match thread.thread_type() {
        Some("VoteUpdate") => {
            let by_me = thread
                .property_identity("CodeReviewVotedByIdentity")
                .is_some_and(mine);
            if let (true, Some(value)) = (by_me, vote_value(thread)) {
                *vote = Some(MyVote { value, time });
            }
            false
        }
        Some("ResetMultipleVotes") => {
            let reset = thread
                .property_identity_list("CodeReviewResetMultipleVotesExampleVoterIdentities")
                .into_iter()
                .any(mine);
            if reset {
                *vote = Some(MyVote { value: 0, time });
            }
            reset
        }
        Some("ResetAllVotes") => {
            let had_vote = vote.is_some_and(|v| v.value != 0);
            if had_vote {
                *vote = Some(MyVote { value: 0, time });
            }
            had_vote
        }
        _ => false,
    }
}

fn is_me(ctx: &SyncContext, id: &str) -> bool {
    !id.is_empty() && id.eq_ignore_ascii_case(&ctx.me.id)
}

fn vote_value(thread: &GitPullRequestCommentThread) -> Option<i32> {
    thread
        .property_str("CodeReviewVoteResult")?
        .trim()
        .parse()
        .ok()
}

fn vote_label(vote: i32) -> String {
    match vote {
        10 => "Approved".into(),
        5 => "Approved with suggestions".into(),
        0 => "Reset".into(),
        -5 => "Waiting for author".into(),
        -10 => "Rejected".into(),
        other => other.to_string(),
    }
}

fn system_actor() -> Actor {
    Actor {
        kind: ActorKind::System,
        id: String::new(),
        display_name: "System".into(),
        unique_name: None,
    }
}

fn actor_of(ctx: &SyncContext, identity: Option<&IdentityRef>) -> Actor {
    identity.map_or_else(system_actor, |i| actor_from(i, &ctx.me))
}

fn base_event(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    event_type: &str,
    occurred: DateTime<Utc>,
    title: String,
    id: String,
) -> Event {
    let mut e = Event::new(
        ctx.connection.clone(),
        event_type,
        occurred,
        ctx.server_now,
        title,
        subject::pr(&ctx.connection.id, &pr.repo_id, pr.pr_id),
    )
    .with_id(id);
    e.catch_up = ctx.catch_up;
    e.url = Some(pr_web_url(org, &pr.project, &pr.repo_name, pr.pr_id));
    e.subtitle = Some(pr.repo_name.clone());
    e.repo = Some(repo_ref(&pr.repo_id, &pr.repo_name, &pr.project));
    e.pr = Some(PrRef {
        id: pr.pr_id,
        title: pr.title.clone(),
        source_branch: pr.source_ref.clone(),
        target_branch: pr.target_ref.clone(),
        is_draft: pr.is_draft,
        role: pr.role,
    });
    e
}

/// Identity-valued property holding either one key or a JSON array of keys.
fn identities<'a>(thread: &'a GitPullRequestCommentThread, name: &str) -> Vec<&'a IdentityRef> {
    match thread.property_str(name) {
        Some(raw) if raw.trim_start().starts_with('[') => thread.property_identity_list(name),
        Some(_) => thread.property_identity(name).into_iter().collect(),
        None => Vec::new(),
    }
}

fn thread_events(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    thread: &GitPullRequestCommentThread,
    occurred: DateTime<Utc>,
) -> Vec<Event> {
    let id = ids::pr_system(
        &ctx.connection.id,
        &pr.repo_id,
        pr.pr_id,
        &thread.id.to_string(),
    );
    let make = |event_type: &str, title: String, actor: Actor| {
        let mut e = base_event(ctx, org, pr, event_type, occurred, title, id.clone());
        e.actor = actor;
        e
    };
    match thread.thread_type() {
        Some("VoteUpdate") => {
            let Some(vote) = vote_value(thread) else {
                return Vec::new();
            };
            let voter = thread.property_identity("CodeReviewVotedByIdentity");
            let name = voter.map_or("Someone", |v| v.display_name.as_str());
            let mut e = make(
                event_types::PR_VOTE_CHANGED,
                format!("{name} voted {}", vote_label(vote)),
                actor_of(ctx, voter),
            );
            e.data = json!({
                "vote": vote,
                "voterId": voter.map(|v| v.id.as_str()).unwrap_or_default(),
                "voterIsMe": voter.is_some_and(|v| is_me(ctx, &v.id)),
            });
            vec![e]
        }
        Some("ReviewersUpdate") => {
            let by = thread.property_identity("CodeReviewReviewersUpdatedByIdentity");
            let groups = [
                (
                    "CodeReviewReviewersUpdatedAddedIdentity",
                    event_types::PR_REVIEWER_ADDED,
                    "added as reviewer",
                ),
                (
                    "CodeReviewReviewersUpdatedRemovedIdentity",
                    event_types::PR_REVIEWER_REMOVED,
                    "removed as reviewer",
                ),
            ];
            let mut events: Vec<Event> = Vec::new();
            for (prop, event_type, text) in groups {
                for r in identities(thread, prop) {
                    let mut e = make(
                        event_type,
                        format!("{} {text}", r.display_name),
                        actor_of(ctx, by),
                    );
                    // The first event keeps the shared thread id; later ones add the reviewer.
                    if !events.is_empty() {
                        e.id = format!("{}:{}", e.id, r.id);
                    }
                    let my_group = ctx
                        .me
                        .group_ids
                        .iter()
                        .any(|g| g.eq_ignore_ascii_case(&r.id));
                    e.data = json!({
                        "reviewerId": r.id,
                        "reviewerName": r.display_name,
                        "isMe": is_me(ctx, &r.id),
                        "isMyGroup": my_group,
                    });
                    // The snapshot role may predate this add (e.g. group reviewer); I'm now an individual one.
                    if event_type == event_types::PR_REVIEWER_ADDED
                        && is_me(ctx, &r.id)
                        && let Some(p) = e.pr.as_mut()
                    {
                        p.role = PrRole::Reviewer;
                    }
                    events.push(e);
                }
            }
            events
        }
        Some("ResetMultipleVotes" | "ResetAllVotes") => vec![make(
            event_types::PR_VOTES_RESET,
            "Votes were reset".into(),
            system_actor(),
        )],
        Some("IsDraftUpdate") if thread.property_str("CodeReviewIsDraftNowSet") == Some("0") => {
            let by = thread.property_identity("CodeReviewIsDraftUpdatedByIdentity");
            vec![make(
                event_types::PR_PUBLISHED,
                format!("PR published: {}", pr.title),
                actor_of(ctx, by),
            )]
        }
        Some("AutoCompleteUpdate") => {
            let by = thread.property_identity("CodeReviewAutoCompleteUpdatedByIdentity");
            let (event_type, title) = match thread.property_str("CodeReviewAutoCompleteNowSet") {
                Some("1") => (
                    event_types::PR_AUTOCOMPLETE_SET,
                    format!("Auto-complete set: {}", pr.title),
                ),
                Some("0") => (
                    event_types::PR_AUTOCOMPLETE_CANCELLED,
                    format!("Auto-complete cancelled: {}", pr.title),
                ),
                _ => return Vec::new(),
            };
            vec![make(event_type, title, actor_of(ctx, by))]
        }
        _ => Vec::new(),
    }
}

fn iteration_event(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    it: &GitPullRequestIteration,
    created: DateTime<Utc>,
) -> Event {
    let mut e = base_event(
        ctx,
        org,
        pr,
        event_types::PR_ITERATION_PUSHED,
        created,
        format!("New push on PR: {}", pr.title),
        ids::pr_iteration(
            &ctx.connection.id,
            &pr.repo_id,
            pr.pr_id,
            &it.id.to_string(),
        ),
    );
    e.actor = actor_of(ctx, it.pushed_by.as_ref().or(it.author.as_ref()));
    let reason = it
        .reason
        .as_deref()
        .filter(|r| !r.is_empty())
        .unwrap_or("push");
    e.data = json!({ "reason": reason });
    e
}

fn rereview_event(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    occurred: DateTime<Utc>,
    id: String,
) -> Event {
    let mut e = base_event(
        ctx,
        org,
        pr,
        event_types::PR_REREVIEW_NEEDED,
        occurred,
        format!("Re-review needed: {}", pr.title),
        id,
    );
    e.actor = system_actor();
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AdoAuth;
    use crate::client::AdoClient;
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::provider::Identity;
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ME: &str = "aaaa-me";
    const GROUP: &str = "bbbb-group";

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
            group_ids: vec![GROUP.into()],
        }
    }

    fn source(server: &MockServer) -> PrReviewsSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PrReviewsSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn ident(key: &str, id: &str, name: &str) -> Value {
        json!({ "displayName": name, "id": id, "uniqueName": "person@example.com", "key": key })
    }

    fn prop(v: Value) -> Value {
        json!({ "$type": "System.String", "$value": v })
    }

    /// `props` are `(name, value)` pairs; `who` maps identity keys to `(id, name)`.
    fn sys(
        id: i64,
        kind: &str,
        when: DateTime<Utc>,
        props: &[(&str, &str)],
        who: &[(&str, &str, &str)],
    ) -> Value {
        let mut p = serde_json::Map::new();
        p.insert("CodeReviewThreadType".into(), prop(json!(kind)));
        for (k, v) in props {
            p.insert((*k).into(), prop(json!(v)));
        }
        let identities: serde_json::Map<String, Value> = who
            .iter()
            .map(|(k, i, n)| ((*k).into(), ident(k, i, n)))
            .collect();
        json!({
            "id": id, "isDeleted": false, "publishedDate": when, "lastUpdatedDate": when,
            "properties": p, "identities": identities, "comments": [],
        })
    }

    fn vote_thread(id: i64, when: DateTime<Utc>, voter: (&str, &str), vote: &str) -> Value {
        sys(
            id,
            "VoteUpdate",
            when,
            &[
                ("CodeReviewVoteResult", vote),
                ("CodeReviewVotedByIdentity", "1"),
            ],
            &[("1", voter.0, voter.1)],
        )
    }

    fn iteration(id: i64, when: DateTime<Utc>) -> Value {
        json!({
            "id": id, "createdDate": when, "reason": "push",
            "author": { "id": "pusher", "displayName": "Person 9", "uniqueName": "p9@example.com" },
        })
    }

    async fn serve(server: &MockServer, threads: ResponseTemplate, iterations: ResponseTemplate) {
        server.reset().await;
        let base = "/p1/_apis/git/repositories/Repo-1/pullRequests/1";
        Mock::given(method("GET"))
            .and(path(format!("{base}/threads")))
            .respond_with(threads)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{base}/iterations")))
            .respond_with(iterations)
            .mount(server)
            .await;
    }

    fn ok(items: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": items }))
    }

    async fn track(ctx: &SyncContext, role: PrRole) {
        track_with_vote(ctx, role, None).await;
    }

    async fn track_with_vote(ctx: &SyncContext, role: PrRole, my_vote: Option<i32>) {
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_PRS,
                &TrackedPrs {
                    prs: vec![TrackedPr {
                        repo_id: "Repo-1".into(),
                        repo_name: "widgets".into(),
                        project: "Alpha".into(),
                        project_id: "p1".into(),
                        pr_id: 1,
                        role,
                        title: "change".into(),
                        my_vote,
                        ..Default::default()
                    }],
                },
            )
            .await
            .unwrap();
    }

    async fn run(
        src: &mut PrReviewsSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = now;
        src.poll(&c).await.map(|o| o.events)
    }

    fn types(events: &[Event]) -> Vec<&str> {
        events.iter().map(|e| e.event_type.as_str()).collect()
    }

    /// Seeds on an empty PR, then polls with the given threads/iterations at day 10 12:00.
    async fn seeded_poll(role: PrRole, threads: Vec<Value>, iterations: Vec<Value>) -> Vec<Event> {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, role).await;
        let mut src = source(&server);
        serve(&server, ok(vec![]), ok(vec![iteration(1, at(1, 0))])).await;
        assert!(
            run(&mut src, &ctx, None, at(10, 0))
                .await
                .unwrap()
                .is_empty()
        );
        serve(&server, ok(threads), ok(iterations)).await;
        run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn seed_emits_nothing_and_first_sight_skips_old_history() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(20, 0), None, false);
        track(&ctx, PrRole::Author).await;
        let mut src = source(&server);
        let old = vec![
            vote_thread(5, at(2, 0), ("other", "Person 2"), "10"),
            vote_thread(6, at(20, 1), ("other", "Person 2"), "-5"),
        ];
        serve(
            &server,
            ok(old.clone()),
            ok(vec![iteration(1, at(1, 0)), iteration(2, at(2, 0))]),
        )
        .await;
        assert!(
            run(&mut src, &ctx, None, at(20, 0))
                .await
                .unwrap()
                .is_empty()
        );
        // Everything seen during the seed stays silent afterwards.
        assert!(
            run(&mut src, &ctx, Some(at(20, 0)), at(20, 2))
                .await
                .unwrap()
                .is_empty()
        );

        // A PR first seen after the seed emits only what falls inside the window.
        let (ctx2, _s2) = sync_context(cfg(), me(), at(20, 2), Some(at(20, 0)), false);
        track(&ctx2, PrRole::Author).await;
        let mut src2 = source(&server);
        let events = run(&mut src2, &ctx2, Some(at(20, 0)), at(20, 2))
            .await
            .unwrap();
        assert_eq!(types(&events), ["pr.vote.changed"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:system:6");
    }

    #[tokio::test]
    async fn vote_event_resolves_voter_and_data() {
        let events = seeded_poll(
            PrRole::Author,
            vec![
                vote_thread(7, at(10, 6), ("other", "Person 2"), "-10"),
                vote_thread(8, at(10, 7), (ME, "Me"), "5"),
            ],
            vec![iteration(1, at(1, 0))],
        )
        .await;
        assert_eq!(types(&events), ["pr.vote.changed", "pr.vote.changed"]);
        let first = &events[0];
        assert_eq!(first.title, "Person 2 voted Rejected");
        assert_eq!(first.occurred_utc, at(10, 6));
        assert_eq!(first.actor.id, "other");
        assert_eq!(
            first.data,
            json!({ "vote": -10, "voterId": "other", "voterIsMe": false })
        );
        assert_eq!(events[1].data["voterIsMe"], true);
        assert_eq!(events[1].actor.kind, ActorKind::Me);
        assert_eq!(first.pr.as_ref().unwrap().role, PrRole::Author);
        assert_eq!(first.subject_key, "c1:pr:Repo-1:1");
    }

    #[tokio::test]
    async fn reviewers_added_flag_my_group_and_keep_ids_unique() {
        let t = sys(
            9,
            "ReviewersUpdate",
            at(10, 3),
            &[
                ("CodeReviewReviewersUpdatedByIdentity", "2"),
                ("CodeReviewReviewersUpdatedAddedIdentity", r#"["1","3"]"#),
                ("CodeReviewReviewersUpdatedRemovedIdentity", "4"),
            ],
            &[
                ("1", GROUP, "Team A"),
                ("2", "adder", "Person 3"),
                ("3", ME, "Me"),
                ("4", "gone", "Person 4"),
            ],
        );
        let events = seeded_poll(PrRole::Author, vec![t], vec![iteration(1, at(1, 0))]).await;
        assert_eq!(
            types(&events),
            [
                "pr.reviewer.added",
                "pr.reviewer.added",
                "pr.reviewer.removed"
            ]
        );
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:system:9");
        assert_eq!(events[1].id, format!("c1:pr:Repo-1:1:system:9:{ME}"));
        assert_eq!(events[2].id, "c1:pr:Repo-1:1:system:9:gone");
        assert_eq!(events[0].data["isMyGroup"], true);
        assert_eq!(events[0].data["isMe"], false);
        assert_eq!(events[1].data["isMe"], true);
        assert_eq!(events[1].data["isMyGroup"], false);
        assert_eq!(events[0].data["reviewerName"], "Team A");
        assert_eq!(events[0].actor.id, "adder");
    }

    #[tokio::test]
    async fn published_and_autocomplete_threads() {
        let by = [("1", "actor", "Person 5")];
        let threads = vec![
            sys(
                10,
                "IsDraftUpdate",
                at(10, 1),
                &[
                    ("CodeReviewIsDraftNowSet", "0"),
                    ("CodeReviewIsDraftUpdatedByIdentity", "1"),
                ],
                &by,
            ),
            sys(
                11,
                "IsDraftUpdate",
                at(10, 2),
                &[("CodeReviewIsDraftNowSet", "1")],
                &by,
            ),
            sys(
                12,
                "AutoCompleteUpdate",
                at(10, 3),
                &[
                    ("CodeReviewAutoCompleteNowSet", "1"),
                    ("CodeReviewAutoCompleteUpdatedByIdentity", "1"),
                ],
                &by,
            ),
            sys(
                13,
                "AutoCompleteUpdate",
                at(10, 4),
                &[("CodeReviewAutoCompleteNowSet", "0")],
                &by,
            ),
        ];
        let events = seeded_poll(PrRole::Author, threads, vec![iteration(1, at(1, 0))]).await;
        assert_eq!(
            types(&events),
            [
                "pr.published",
                "pr.autocomplete.set",
                "pr.autocomplete.cancelled"
            ]
        );
        assert_eq!(events[0].actor.id, "actor");
        assert_eq!(events[2].actor.kind, ActorKind::System);
    }

    #[tokio::test]
    async fn reset_of_my_vote_emits_reset_and_rereview() {
        let threads = vec![
            vote_thread(20, at(9, 1), (ME, "Me"), "10"),
            sys(
                21,
                "ResetMultipleVotes",
                at(10, 5),
                &[(
                    "CodeReviewResetMultipleVotesExampleVoterIdentities",
                    r#"["1"]"#,
                )],
                &[("1", ME, "Me")],
            ),
        ];
        // The seed poll already knows the vote; only the reset is new.
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Reviewer).await;
        let mut src = source(&server);
        serve(
            &server,
            ok(threads[..1].to_vec()),
            ok(vec![iteration(1, at(1, 0))]),
        )
        .await;
        run(&mut src, &ctx, None, at(10, 0)).await.unwrap();
        serve(&server, ok(threads), ok(vec![iteration(1, at(1, 0))])).await;
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap();
        // The reset itself is not wanted on a PR I did not author; the re-review ask is.
        assert_eq!(types(&events), ["pr.rereview.needed"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:rereview:reset:21");
        assert_eq!(events[0].actor.kind, ActorKind::System);
    }

    #[tokio::test]
    async fn new_iteration_after_my_vote_needs_rereview_once() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Reviewer).await;
        let mut src = source(&server);
        let vote = vote_thread(30, at(9, 1), (ME, "Me"), "10");
        serve(
            &server,
            ok(vec![vote.clone()]),
            ok(vec![iteration(1, at(1, 0))]),
        )
        .await;
        run(&mut src, &ctx, None, at(10, 0)).await.unwrap();

        let its = vec![iteration(1, at(1, 0)), iteration(2, at(10, 4))];
        serve(&server, ok(vec![vote]), ok(its)).await;
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap();
        assert_eq!(types(&events), ["pr.rereview.needed"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:rereview:2");
        assert!(
            run(&mut src, &ctx, Some(at(10, 12)), at(10, 13))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn iteration_without_my_vote_has_no_rereview() {
        let events = seeded_poll(
            PrRole::Reviewer,
            vec![],
            vec![iteration(1, at(1, 0)), iteration(2, at(10, 4))],
        )
        .await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn push_to_my_pr_emits_iteration() {
        let events = seeded_poll(
            PrRole::Author,
            vec![],
            vec![iteration(1, at(1, 0)), iteration(2, at(10, 4))],
        )
        .await;
        assert_eq!(types(&events), ["pr.iteration.pushed"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:iteration:2");
        assert_eq!(events[0].data, json!({ "reason": "push" }));
        assert_eq!(events[0].actor.id, "pusher");
    }

    #[tokio::test]
    async fn on_a_pr_i_did_not_author_only_review_asks_are_kept() {
        let t = reviewers_added(9, at(10, 3), &[(ME, "Me"), ("other", "Person 2")]);
        let threads = vec![
            t,
            vote_thread(10, at(10, 4), ("other", "Person 2"), "10"),
            sys(
                11,
                "AutoCompleteUpdate",
                at(10, 5),
                &[("CodeReviewAutoCompleteNowSet", "1")],
                &[],
            ),
        ];
        let its = vec![iteration(1, at(1, 0)), iteration(2, at(10, 6))];
        let events = seeded_poll(PrRole::Reviewer, threads, its).await;
        assert_eq!(types(&events), ["pr.reviewer.added"]);
        assert_eq!(events[0].data["isMe"], true);
    }

    #[tokio::test]
    async fn transient_aborts_without_writing() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), Some(at(9, 0)), false);
        track(&ctx, PrRole::Reviewer).await;
        let mut src = source(&server);
        serve(&server, ResponseTemplate::new(500), ok(vec![])).await;
        let err = run(&mut src, &ctx, Some(at(9, 0)), at(10, 0))
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

    fn reviewers_added(id: i64, when: DateTime<Utc>, added: &[(&str, &str)]) -> Value {
        let keys: Vec<String> = (1..=added.len()).map(|n| n.to_string()).collect();
        let who: Vec<(&str, &str, &str)> = added
            .iter()
            .zip(&keys)
            .map(|((i, n), k)| (k.as_str(), *i, *n))
            .collect();
        sys(
            id,
            "ReviewersUpdate",
            when,
            &[(
                "CodeReviewReviewersUpdatedAddedIdentity",
                &serde_json::to_string(&keys).unwrap(),
            )],
            &who,
        )
    }

    #[tokio::test]
    async fn first_sight_skips_my_reviewer_added_but_keeps_others() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 12), Some(at(10, 0)), false);
        track(&ctx, PrRole::Author).await;
        let mut src = source(&server);
        let t = reviewers_added(
            9,
            at(10, 3),
            &[(ME, "Me"), (GROUP, "Team A"), ("other", "Person 2")],
        );
        serve(&server, ok(vec![t]), ok(vec![iteration(1, at(1, 0))])).await;
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap();
        assert_eq!(types(&events), ["pr.reviewer.added"]);
        assert_eq!(events[0].data["reviewerId"], "other");
    }

    #[tokio::test]
    async fn my_add_is_a_reviewer_role_event_even_with_stale_group_role() {
        let t = reviewers_added(9, at(10, 3), &[(ME, "Me"), (GROUP, "Team A")]);
        let events =
            seeded_poll(PrRole::GroupReviewer, vec![t], vec![iteration(1, at(1, 0))]).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data["isMe"], true);
        assert_eq!(events[0].pr.as_ref().unwrap().role, PrRole::Reviewer);
        assert_eq!(events[1].pr.as_ref().unwrap().role, PrRole::GroupReviewer);
    }

    #[tokio::test]
    async fn list_vote_of_zero_suppresses_iteration_rereview() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track_with_vote(&ctx, PrRole::Reviewer, Some(10)).await;
        let mut src = source(&server);
        let vote = vote_thread(30, at(9, 1), (ME, "Me"), "10");
        serve(
            &server,
            ok(vec![vote.clone()]),
            ok(vec![iteration(1, at(1, 0))]),
        )
        .await;
        run(&mut src, &ctx, None, at(10, 0)).await.unwrap();

        // The reset thread doesn't list me, but the list now says my vote is 0.
        track_with_vote(&ctx, PrRole::Reviewer, Some(0)).await;
        let reset = sys(
            31,
            "ResetMultipleVotes",
            at(10, 3),
            &[(
                "CodeReviewResetMultipleVotesExampleVoterIdentities",
                r#"["1"]"#,
            )],
            &[("1", "other", "Person 2")],
        );
        let its = vec![iteration(1, at(1, 0)), iteration(2, at(10, 4))];
        serve(&server, ok(vec![vote, reset]), ok(its)).await;
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap();
        // No re-review; the reset and push themselves are not wanted on a PR I did not author.
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn reset_right_after_push_rereview_is_not_a_second_rereview() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Reviewer).await;
        let mut src = source(&server);
        let vote = vote_thread(30, at(9, 1), (ME, "Me"), "10");
        serve(
            &server,
            ok(vec![vote.clone()]),
            ok(vec![iteration(1, at(1, 0))]),
        )
        .await;
        run(&mut src, &ctx, None, at(10, 0)).await.unwrap();

        let pushed = at(10, 4);
        let its = vec![iteration(1, at(1, 0)), iteration(2, pushed)];
        serve(&server, ok(vec![vote.clone()]), ok(its.clone())).await;
        let first = run(&mut src, &ctx, Some(at(10, 0)), at(10, 5))
            .await
            .unwrap();
        assert_eq!(types(&first), ["pr.rereview.needed"]);

        let reset = sys(
            31,
            "ResetMultipleVotes",
            pushed + Duration::minutes(1),
            &[(
                "CodeReviewResetMultipleVotesExampleVoterIdentities",
                r#"["1"]"#,
            )],
            &[("1", ME, "Me")],
        );
        serve(&server, ok(vec![vote, reset]), ok(its)).await;
        let second = run(&mut src, &ctx, Some(at(10, 5)), at(10, 6))
            .await
            .unwrap();
        assert!(second.is_empty());
    }
}
