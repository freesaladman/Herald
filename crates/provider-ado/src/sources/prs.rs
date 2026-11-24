//! Pr-tier source: PR lifecycle (created, closed, reactivated, merge conflicts, description mentions).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use herald_core::event::{Actor, ActorKind, Event, PrRef, PrRole, event_types, ids, subject};
use herald_core::provider::{Identity, PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};

use super::{
    SNAPSHOT_TRACKED_PRS, TrackedPr, TrackedPrs, active_prs, actor_from, is_fatal, mentions,
    pr_key, pr_web_url, project_ref, repo_ref, wanted_on_pr, window_start,
};
use crate::connection::AdoShared;
use crate::dto_git::GitPullRequest;

const SNAPSHOT_LIFECYCLE: &str = "prs:lifecycle";
const CLOSED_KEEP: Duration = Duration::days(14);
/// A tracked PR whose single GET has been forbidden this long is dropped. Fixed rather than
/// tied to the lookback (default 14 days) so a revoked permission stops costing a request
/// every tick within a day, while a short permission blip still recovers.
const FORBIDDEN_GIVE_UP: Duration = Duration::hours(24);
/// The org-level list cuts `description` at about this many characters.
const LIST_DESCRIPTION_LIMIT: usize = 400;

/// Outcome of a single PR GET.
enum Fetched {
    Pr(Box<GitPullRequest>),
    /// The PR no longer exists.
    NotFound,
    /// Access denied; the caller keeps its previous state until it has lasted too long.
    Forbidden,
    /// Failed for a non-fatal reason; the caller keeps its previous state.
    Skipped,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Lifecycle {
    /// Recently closed tracked PRs, kept so a reactivation can be recognised.
    closed: Vec<ClosedPr>,
    /// Last-seen description state per PR; entries outlive a PR's absence from the list.
    descriptions: BTreeMap<String, DescriptionState>,
    /// When a tracked PR's single GET first came back forbidden, until a GET succeeds.
    forbidden_since: BTreeMap<String, DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DescriptionState {
    /// Hash of the (possibly truncated) list value.
    list_hash: u64,
    /// Hash of the full description.
    full_hash: u64,
    mentioned: bool,
    seen_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClosedPr {
    pr: TrackedPr,
    closed_utc: DateTime<Utc>,
}

pub struct PrLifecycleSource {
    shared: Arc<AdoShared>,
}

impl PrLifecycleSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    async fn in_scope(&self, project: &str) -> Result<bool, ProviderError> {
        if self.shared.configured_projects.is_empty() {
            return Ok(true);
        }
        Ok(self
            .shared
            .projects()
            .await?
            .iter()
            .any(|p| p.name.eq_ignore_ascii_case(project)))
    }

    /// Errors that mean the poll can't proceed are returned; with `tolerate_transient` a
    /// transient failure is skipped instead.
    async fn fetch_pr(
        &self,
        project: &str,
        repo_id: &str,
        pr_id: i64,
        tolerate_transient: bool,
    ) -> Result<Fetched, ProviderError> {
        match self
            .shared
            .client
            .pull_request(project, repo_id, pr_id)
            .await
            .map_err(ProviderError::from)
        {
            Ok(p) => Ok(Fetched::Pr(Box::new(p))),
            Err(ProviderError::NotFound(_)) => Ok(Fetched::NotFound),
            Err(e @ ProviderError::Forbidden(_)) => {
                tracing::warn!("skipping PR {pr_id} in {repo_id}: {e}");
                Ok(Fetched::Forbidden)
            }
            Err(e @ ProviderError::Transient(_)) if tolerate_transient => {
                tracing::warn!("skipping PR {pr_id} in {repo_id}: {e}");
                Ok(Fetched::Skipped)
            }
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!("skipping PR {pr_id} in {repo_id}: {e}");
                Ok(Fetched::Skipped)
            }
        }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let old: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await?
            .unwrap_or_default();
        let mut state: Lifecycle = ctx
            .snapshots
            .get(SNAPSHOT_LIFECYCLE)
            .await?
            .unwrap_or_default();
        let active = active_prs(&self.shared).await?;
        let org = &self.shared.organization;

        let old_by_key: HashMap<String, &TrackedPr> = old
            .prs
            .iter()
            .map(|p| (pr_key(&p.repo_id, p.pr_id), p))
            .collect();

        // Each entry: tracked form, the source PR, and whether I have a role on it.
        let mut seen: Vec<(TrackedPr, &GitPullRequest, bool)> = Vec::new();
        let mut descriptions = state.descriptions.clone();
        let mut mention_changed: HashSet<String> = HashSet::new();

        let to_fetch: Vec<(String, &GitPullRequest)> = active
            .iter()
            .filter(|pr| {
                let list_desc = pr.description.as_deref().unwrap_or_default();
                let k = pr_key(&pr.repository.id, pr.pull_request_id);
                is_truncated(list_desc)
                    && state
                        .descriptions
                        .get(&k)
                        .is_none_or(|p| p.list_hash != fnv(list_desc))
            })
            .map(|pr| (pr_key(&pr.repository.id, pr.pull_request_id), pr))
            .collect();
        let fetches: Vec<BoxFuture<'_, Result<(String, Fetched), ProviderError>>> = to_fetch
            .into_iter()
            .map(|(k, pr)| -> BoxFuture<'_, _> {
                Box::pin(async move {
                    let project =
                        project_ref(&pr.repository.project.id, &pr.repository.project.name);
                    let f = self
                        .fetch_pr(project, &pr.repository.id, pr.pull_request_id, true)
                        .await?;
                    Ok((k, f))
                })
            })
            .collect();
        let mut fetched: HashMap<String, Fetched> = stream::iter(fetches)
            .buffer_unordered(4)
            .try_collect()
            .await?;

        for pr in &active {
            let role = derive_role(pr, &ctx.me);
            let k = pr_key(&pr.repository.id, pr.pull_request_id);
            let list_desc = pr.description.as_deref().unwrap_or_default();
            let list_hash = fnv(list_desc);
            let prior = state.descriptions.get(&k);
            let (full_hash, mentioned, store) = if !is_truncated(list_desc) {
                (list_hash, mentions(list_desc, &ctx.me.id), true)
            } else if let Some(p) = prior.filter(|p| p.list_hash == list_hash) {
                (p.full_hash, p.mentioned, true)
            } else {
                match fetched.remove(&k) {
                    Some(Fetched::Pr(full)) => {
                        let text = full.description.as_deref().unwrap_or_default();
                        (fnv(text), mentions(text, &ctx.me.id), true)
                    }
                    _ => {
                        let (h, m) = prior.map_or_else(
                            || (list_hash, mentions(list_desc, &ctx.me.id)),
                            |p| (p.full_hash, p.mentioned),
                        );
                        (h, m, false)
                    }
                }
            };
            if store {
                descriptions.insert(
                    k.clone(),
                    DescriptionState {
                        list_hash,
                        full_hash,
                        mentioned,
                        seen_utc: ctx.server_now,
                    },
                );
            }
            if mentioned && prior.is_none_or(|p| !p.mentioned || p.full_hash != full_hash) {
                mention_changed.insert(k.clone());
            }
            let mut t = to_tracked(org, pr, role.unwrap_or(PrRole::Mentioned), &ctx.me);
            keep_settled_merge_status(&mut t, old_by_key.get(&k).copied());
            seen.push((t, pr, role.is_some()));
        }
        let mut tracked: Vec<TrackedPr> = seen
            .iter()
            .filter(|(_, _, mine)| *mine)
            .map(|(t, _, _)| t.clone())
            .collect();

        if ctx.watermark.is_none() {
            ctx.snapshots
                .put(SNAPSHOT_TRACKED_PRS, &TrackedPrs { prs: tracked })
                .await?;
            descriptions.retain(|_, d| d.seen_utc >= ctx.server_now - CLOSED_KEEP);
            state.descriptions = descriptions;
            ctx.snapshots.put(SNAPSHOT_LIFECYCLE, &state).await?;
            return Ok(PollOutcome::default());
        }

        let window = window_start(ctx);
        let now = ctx.server_now;
        let c = &ctx.connection.id;
        let mut events = Vec::new();

        for (t, pr, mine) in &seen {
            let k = pr_key(&t.repo_id, t.pr_id);
            let creator = || {
                pr.created_by
                    .as_ref()
                    .map(|i| actor_from(i, &ctx.me))
                    .unwrap_or_else(unknown_actor)
            };
            let was_tracked = old_by_key.get(&k);

            if *mine {
                let reopened = state
                    .closed
                    .iter()
                    .position(|x| pr_key(&x.pr.repo_id, x.pr.pr_id) == k);
                if let Some(i) = reopened {
                    state.closed.remove(i);
                    let mut e = build(
                        ctx,
                        event_types::PR_REACTIVATED,
                        ids::pr_reactivated(c, &t.repo_id, t.pr_id, now),
                        now,
                        t,
                        format!("PR reactivated: {}", t.title),
                    );
                    e.inferred = true;
                    e.actor = system_actor();
                    events.push(e);
                } else if was_tracked.is_none()
                    && let Some(created) = pr.creation_date.filter(|d| *d >= window)
                {
                    let mut e = build(
                        ctx,
                        event_types::PR_CREATED,
                        ids::pr_created(c, &t.repo_id, t.pr_id),
                        created,
                        t,
                        format!("PR created: {}", t.title),
                    );
                    e.actor = creator();
                    events.push(e);
                } else if was_tracked.is_none()
                    && matches!(t.role, PrRole::Reviewer | PrRole::GroupReviewer)
                    && pr.creation_date.is_some_and(|d| d < window)
                {
                    let mut e = build(
                        ctx,
                        event_types::PR_CREATED,
                        ids::pr_created(c, &t.repo_id, t.pr_id),
                        now,
                        t,
                        format!("Review requested: {}", t.title),
                    );
                    e.inferred = true;
                    e.actor = creator();
                    events.push(e);
                }
            }

            if let Some(o) = was_tracked {
                let before = o.merge_status.as_deref() == Some("conflicts");
                let after = t.merge_status.as_deref() == Some("conflicts");
                if before != after {
                    let (ty, label) = if after {
                        (event_types::PR_CONFLICTS_DETECTED, "Merge conflicts")
                    } else {
                        (event_types::PR_CONFLICTS_CLEARED, "Merge conflicts cleared")
                    };
                    let status = t.merge_status.as_deref().unwrap_or("succeeded");
                    let mut e = build(
                        ctx,
                        ty,
                        ids::pr_merge(c, &t.repo_id, t.pr_id, status, now),
                        now,
                        t,
                        format!("{label}: {}", t.title),
                    );
                    e.inferred = true;
                    e.actor = system_actor();
                    events.push(e);
                }
            }

            if mention_changed.contains(&k) {
                let fresh = pr
                    .creation_date
                    .filter(|d| *d >= window && was_tracked.is_none());
                let mut e = build(
                    ctx,
                    event_types::PR_MENTIONED,
                    ids::pr_description_mention(c, &t.repo_id, t.pr_id),
                    fresh.unwrap_or(now),
                    t,
                    format!("Mentioned in PR description: {}", t.title),
                );
                e.inferred = fresh.is_none();
                e.actor = creator();
                events.push(e);
            }
        }

        let active_keys: HashSet<String> = seen
            .iter()
            .map(|(t, _, _)| pr_key(&t.repo_id, t.pr_id))
            .collect();
        let mut forbidden_since: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
        for o in &old.prs {
            if active_keys.contains(&pr_key(&o.repo_id, o.pr_id)) {
                continue;
            }
            let fetched = match self
                .fetch_pr(
                    project_ref(&o.project_id, &o.project),
                    &o.repo_id,
                    o.pr_id,
                    false,
                )
                .await?
            {
                Fetched::Pr(p) => p,
                Fetched::NotFound => continue,
                // Unknown state: keep tracking so it is not later seen as newly tracked.
                Fetched::Skipped => {
                    let k = pr_key(&o.repo_id, o.pr_id);
                    if let Some(since) = state.forbidden_since.get(&k) {
                        forbidden_since.insert(k, *since);
                    }
                    tracked.push(o.clone());
                    continue;
                }
                Fetched::Forbidden => {
                    let k = pr_key(&o.repo_id, o.pr_id);
                    let since = state.forbidden_since.get(&k).copied().unwrap_or(now);
                    if now - since >= FORBIDDEN_GIVE_UP {
                        tracing::info!(
                            "no longer tracking PR {} in {}: forbidden since {since}",
                            o.pr_id,
                            o.repo_id
                        );
                    } else {
                        forbidden_since.insert(k, since);
                        tracked.push(o.clone());
                    }
                    continue;
                }
            };
            let (ty, id, label) = match fetched.status.as_str() {
                "completed" => (
                    event_types::PR_COMPLETED,
                    ids::pr_completed(c, &o.repo_id, o.pr_id),
                    "PR completed",
                ),
                "abandoned" => (
                    event_types::PR_ABANDONED,
                    ids::pr_abandoned(c, &o.repo_id, o.pr_id),
                    "PR abandoned",
                ),
                "active" => {
                    // Missing from the list but still active: a list hiccup, not a close.
                    if self.in_scope(&fetched.repository.project.name).await? {
                        let mut t = to_tracked(org, &fetched, o.role, &ctx.me);
                        keep_settled_merge_status(&mut t, Some(o));
                        tracked.push(t);
                    }
                    continue;
                }
                _ => continue,
            };
            let closed = fetched.closed_date.unwrap_or(now);
            let mut e = build(ctx, ty, id, closed, o, format!("{label}: {}", o.title));
            e.actor = fetched
                .closed_by
                .as_ref()
                .map(|i| actor_from(i, &ctx.me))
                .unwrap_or_else(unknown_actor);
            events.push(e);
            let mut gone = o.clone();
            gone.status = fetched.status;
            state.closed.push(ClosedPr {
                pr: gone,
                closed_utc: closed,
            });
        }
        state.closed.retain(|x| x.closed_utc >= now - CLOSED_KEEP);
        descriptions.retain(|_, d| d.seen_utc >= now - CLOSED_KEEP);
        state.descriptions = descriptions;
        state.forbidden_since = forbidden_since;

        ctx.snapshots
            .put(SNAPSHOT_TRACKED_PRS, &TrackedPrs { prs: tracked })
            .await?;
        ctx.snapshots.put(SNAPSHOT_LIFECYCLE, &state).await?;
        events.retain(wanted_on_pr);
        Ok(PollOutcome { events })
    }
}

impl Source for PrLifecycleSource {
    fn name(&self) -> &'static str {
        "pr_lifecycle"
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

/// The list cuts descriptions at a UTF-16 length.
fn is_truncated(list_desc: &str) -> bool {
    list_desc.encode_utf16().count() >= LIST_DESCRIPTION_LIMIT
}

/// A transient merge status must not hide a conflict transition: fall back to the old one.
fn keep_settled_merge_status(t: &mut TrackedPr, old: Option<&TrackedPr>) {
    if matches!(
        t.merge_status.as_deref(),
        None | Some("" | "queued" | "notSet")
    ) {
        t.merge_status = old.and_then(|o| o.merge_status.clone());
    }
}

/// Stable across toolchains, unlike `DefaultHasher`.
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn derive_role(pr: &GitPullRequest, me: &Identity) -> Option<PrRole> {
    let is_me = |id: &str| !id.is_empty() && id.eq_ignore_ascii_case(&me.id);
    if pr.created_by.as_ref().is_some_and(|c| is_me(&c.id)) {
        return Some(PrRole::Author);
    }
    if pr.reviewers.iter().any(|r| !r.is_container && is_me(&r.id)) {
        return Some(PrRole::Reviewer);
    }
    let in_group = |id: &str| me.group_ids.iter().any(|g| g.eq_ignore_ascii_case(id));
    if pr
        .reviewers
        .iter()
        .any(|r| r.is_container && in_group(&r.id))
    {
        return Some(PrRole::GroupReviewer);
    }
    None
}

/// My own vote from the list's `reviewers[]`; group entries never count.
fn my_vote_of(pr: &GitPullRequest, me: &Identity) -> Option<i32> {
    pr.reviewers
        .iter()
        .find(|r| !r.is_container && !r.id.is_empty() && r.id.eq_ignore_ascii_case(&me.id))
        .map(|r| r.vote)
}

fn to_tracked(org: &str, pr: &GitPullRequest, role: PrRole, me: &Identity) -> TrackedPr {
    let project = &pr.repository.project;
    TrackedPr {
        repo_id: pr.repository.id.clone(),
        repo_name: pr.repository.name.clone(),
        project: project.name.clone(),
        project_id: project.id.clone(),
        pr_id: pr.pull_request_id,
        role,
        source_ref: pr.source_ref_name.clone(),
        target_ref: pr.target_ref_name.clone(),
        is_draft: pr.is_draft,
        title: pr.title.clone(),
        url: pr_web_url(org, &project.name, &pr.repository.name, pr.pull_request_id),
        created_by_id: pr
            .created_by
            .as_ref()
            .map(|c| c.id.clone())
            .unwrap_or_default(),
        status: pr.status.clone(),
        merge_status: pr.merge_status.clone(),
        my_vote: my_vote_of(pr, me),
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

fn unknown_actor() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: String::new(),
        display_name: "Unknown".into(),
        unique_name: None,
    }
}

fn build(
    ctx: &SyncContext,
    event_type: &str,
    id: String,
    occurred: DateTime<Utc>,
    t: &TrackedPr,
    title: String,
) -> Event {
    let mut e = Event::new(
        ctx.connection.clone(),
        event_type,
        occurred,
        ctx.server_now,
        title,
        subject::pr(&ctx.connection.id, &t.repo_id, t.pr_id),
    )
    .with_id(id);
    e.catch_up = ctx.catch_up;
    e.url = Some(t.url.clone());
    e.subtitle = Some(t.repo_name.clone());
    e.repo = Some(repo_ref(&t.repo_id, &t.repo_name, &t.project));
    e.pr = Some(PrRef {
        id: t.pr_id,
        title: t.title.clone(),
        source_branch: t.source_ref.clone(),
        target_branch: t.target_ref.clone(),
        is_draft: t.is_draft,
        role: t.role,
    });
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::auth::AdoAuth;
    use crate::client::AdoClient;

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
            notify: Default::default(),
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

    fn source(server: &MockServer) -> PrLifecycleSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PrLifecycleSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn pr(id: i64, creator: &str, created: DateTime<Utc>) -> Value {
        json!({
            "pullRequestId": id,
            "repository": { "id": "Repo-1", "name": "widgets", "project": { "id": "p1", "name": "Alpha" } },
            "status": "active",
            "createdBy": { "id": creator, "displayName": "Pat", "uniqueName": "pat@example.test" },
            "creationDate": created,
            "title": format!("change {id}"),
            "description": "plain",
            "sourceRefName": "refs/heads/topic",
            "targetRefName": "refs/heads/main",
            "mergeStatus": "succeeded",
            "isDraft": false,
            "reviewers": [],
        })
    }

    async fn serve_active(server: &MockServer, prs: Vec<Value>) {
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/_apis/git/pullrequests"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": prs })))
            .mount(server)
            .await;
    }

    async fn run(
        src: &mut PrLifecycleSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        catch_up: bool,
    ) -> Vec<Event> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = now;
        c.catch_up = catch_up;
        src.poll(&c).await.unwrap().events
    }

    fn types(events: &[Event]) -> Vec<&str> {
        events.iter().map(|e| e.event_type.as_str()).collect()
    }

    #[tokio::test]
    async fn seed_writes_snapshot_and_emits_nothing() {
        let server = MockServer::start().await;
        serve_active(
            &server,
            vec![pr(1, ME, at(10, 0)), pr(2, "other", at(10, 0))],
        )
        .await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        assert!(run(&mut src, &ctx, None, at(10, 1), false).await.is_empty());
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.prs.len(), 1);
        assert_eq!(snap.prs[0].pr_id, 1);
        assert_eq!(snap.prs[0].status, "active");
        assert_eq!(snap.prs[0].merge_status.as_deref(), Some("succeeded"));
    }

    #[tokio::test]
    async fn roles_are_derived_case_insensitively() {
        let server = MockServer::start().await;
        let mut as_reviewer = pr(2, "other", at(10, 0));
        as_reviewer["reviewers"] =
            json!([{ "id": ME.to_uppercase(), "isContainer": false, "vote": 10 }]);
        let mut as_group = pr(3, "other", at(10, 0));
        as_group["reviewers"] = json!([{ "id": GROUP.to_uppercase(), "isContainer": true }]);
        let mut my_id_as_group = pr(4, "other", at(10, 0));
        my_id_as_group["reviewers"] = json!([{ "id": "zzzz-other-group", "isContainer": true }]);
        serve_active(
            &server,
            vec![
                pr(1, &ME.to_uppercase(), at(10, 0)),
                as_reviewer,
                as_group,
                my_id_as_group,
            ],
        )
        .await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        let roles: Vec<(i64, PrRole)> = snap.prs.iter().map(|p| (p.pr_id, p.role)).collect();
        assert_eq!(
            roles,
            [
                (1, PrRole::Author),
                (2, PrRole::Reviewer),
                (3, PrRole::GroupReviewer)
            ]
        );
        let votes: Vec<Option<i32>> = snap.prs.iter().map(|p| p.my_vote).collect();
        assert_eq!(votes, [None, Some(10), None]);
    }

    #[tokio::test]
    async fn created_only_within_window_and_catch_up_propagates() {
        let server = MockServer::start().await;
        serve_active(&server, vec![]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 0), false).await;

        let fresh = pr(1, ME, at(10, 11));
        let stale = pr(2, ME, at(1, 0));
        serve_active(&server, vec![fresh, stale]).await;
        let events = run(&mut src, &ctx, Some(at(10, 10)), at(10, 12), true).await;
        assert_eq!(types(&events), ["pr.created"]);
        let e = &events[0];
        assert_eq!(e.id, "c1:pr:Repo-1:1:created");
        assert_eq!(e.occurred_utc, at(10, 11));
        assert_eq!(e.subject_key, "c1:pr:Repo-1:1");
        assert!(e.catch_up);
        assert_eq!(e.connection.id, "c1");
        assert_eq!(e.repo.as_ref().unwrap().namespace.as_deref(), Some("Alpha"));
        assert_eq!(e.pr.as_ref().unwrap().role, PrRole::Author);
        assert_eq!(e.actor.kind, ActorKind::Me);
        assert!(e.url.as_deref().unwrap().ends_with("/pullrequest/1"));
    }

    #[tokio::test]
    async fn vanished_pr_is_fetched_and_completed_with_closer() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1, ME, at(10, 0))]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        serve_active(&server, vec![]).await;
        let mut closed = pr(1, ME, at(10, 0));
        closed["status"] = json!("completed");
        closed["closedDate"] = json!(at(10, 3));
        closed["closedBy"] =
            json!({ "id": "closer", "displayName": "Cam", "uniqueName": "cam@example.test" });
        Mock::given(method("GET"))
            .and(path("/p1/_apis/git/repositories/Repo-1/pullRequests/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(closed))
            .mount(&server)
            .await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 4), false).await;
        assert_eq!(types(&events), ["pr.completed"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:completed");
        assert_eq!(events[0].occurred_utc, at(10, 3));
        assert_eq!(events[0].actor.id, "closer");
        assert_eq!(events[0].actor.kind, ActorKind::Human);

        // Reappearing as active is a reactivation, once.
        serve_active(&server, vec![pr(1, ME, at(10, 0))]).await;
        let events = run(&mut src, &ctx, Some(at(10, 5)), at(10, 6), false).await;
        assert_eq!(types(&events), ["pr.reactivated"]);
        assert!(events[0].inferred);
        let events = run(&mut src, &ctx, Some(at(10, 7)), at(10, 8), false).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn conflicts_detected_then_cleared_and_queued_is_ignored() {
        let server = MockServer::start().await;
        let with_status = |s: &str| {
            let mut p = pr(1, ME, at(10, 0));
            p["mergeStatus"] = json!(s);
            p
        };
        serve_active(&server, vec![with_status("succeeded")]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        serve_active(&server, vec![with_status("conflicts")]).await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false).await;
        assert_eq!(types(&events), ["pr.conflicts.detected"]);
        assert!(events[0].inferred);
        assert_eq!(events[0].actor.kind, ActorKind::System);
        assert!(events[0].id.starts_with("c1:pr:Repo-1:1:merge:conflicts:"));

        serve_active(&server, vec![with_status("queued")]).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false)
                .await
                .is_empty()
        );

        serve_active(&server, vec![with_status("succeeded")]).await;
        let events = run(&mut src, &ctx, Some(at(10, 6)), at(10, 7), false).await;
        assert_eq!(types(&events), ["pr.conflicts.cleared"]);
    }

    #[tokio::test]
    async fn description_mention_fires_once() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1, "other", at(10, 0))]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        let mut p = pr(1, "other", at(10, 0));
        p["description"] = json!(format!("ping @<{}>", ME.to_uppercase()));
        serve_active(&server, vec![p]).await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false).await;
        assert_eq!(types(&events), ["pr.mentioned"]);
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:description-mention");
        assert_eq!(events[0].actor.id, "other");
        assert_eq!(events[0].pr.as_ref().unwrap().role, PrRole::Mentioned);
        assert!(
            run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false)
                .await
                .is_empty()
        );
    }

    async fn serve_pr(server: &MockServer, id: i64, body: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(format!(
                "/p1/_apis/git/repositories/Repo-1/pullRequests/{id}"
            )))
            .respond_with(body)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn list_hiccup_keeps_tracking_and_raises_nothing() {
        let server = MockServer::start().await;
        let mention = format!("ping @<{ME}>");
        let mut mine = pr(1, ME, at(10, 0));
        mine["description"] = json!(mention);
        let mut review = pr(2, "other", at(1, 0));
        review["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(&server, vec![mine.clone(), review.clone()]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        serve_active(&server, vec![]).await;
        let mut renamed = mine.clone();
        renamed["title"] = json!("renamed");
        serve_pr(
            &server,
            1,
            ResponseTemplate::new(200).set_body_json(renamed),
        )
        .await;
        serve_pr(
            &server,
            2,
            ResponseTemplate::new(200).set_body_json(review.clone()),
        )
        .await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false)
                .await
                .is_empty()
        );
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        let titles: Vec<&str> = snap.prs.iter().map(|p| p.title.as_str()).collect();
        assert_eq!(titles, ["renamed", "change 2"]);

        serve_active(&server, vec![mine, review]).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn truncated_description_is_fetched_once_and_mention_found_in_full_text() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1, "other", at(10, 0))]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        let mut listed = pr(1, "other", at(10, 0));
        listed["description"] = json!("x".repeat(450));
        let mut full = pr(1, "other", at(10, 0));
        full["description"] = json!(format!("{} @<{ME}>", "x".repeat(450)));
        serve_active(&server, vec![listed]).await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/git/repositories/Repo-1/pullRequests/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(full))
            .expect(1)
            .mount(&server)
            .await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false).await;
        assert_eq!(types(&events), ["pr.mentioned"]);
        assert!(
            run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn review_requested_on_older_pr_is_inferred_created() {
        let server = MockServer::start().await;
        serve_active(&server, vec![]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 0), false).await;

        let mut p = pr(2, "other", at(1, 0));
        p["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(&server, vec![p]).await;
        let events = run(&mut src, &ctx, Some(at(10, 10)), at(10, 12), false).await;
        assert_eq!(types(&events), ["pr.created"]);
        let e = &events[0];
        assert_eq!(e.id, "c1:pr:Repo-1:2:created");
        assert_eq!(e.occurred_utc, at(10, 12));
        assert!(e.inferred);
        assert_eq!(e.title, "Review requested: change 2");
    }

    #[tokio::test]
    async fn failing_single_get_is_skipped_without_stalling_others() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1, ME, at(10, 0)), pr(2, ME, at(10, 0))]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        serve_active(&server, vec![]).await;
        serve_pr(&server, 1, ResponseTemplate::new(400)).await;
        let mut closed = pr(2, ME, at(10, 0));
        closed["status"] = json!("abandoned");
        serve_pr(&server, 2, ResponseTemplate::new(200).set_body_json(closed)).await;
        let events = run(&mut src, &ctx, Some(at(10, 2)), at(10, 4), false).await;
        assert_eq!(types(&events), ["pr.abandoned"]);
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        let ids: Vec<i64> = snap.prs.iter().map(|p| p.pr_id).collect();
        assert_eq!(ids, [1]);
    }

    #[tokio::test]
    async fn skipped_get_during_list_hiccup_keeps_reviewer_pr_without_review_requested() {
        let server = MockServer::start().await;
        let mut review = pr(2, "other", at(1, 0));
        review["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(&server, vec![review.clone()]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        serve_active(&server, vec![]).await;
        serve_pr(&server, 2, ResponseTemplate::new(403)).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false)
                .await
                .is_empty()
        );
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.prs.len(), 1);

        serve_active(&server, vec![review]).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false)
                .await
                .is_empty()
        );
    }

    /// Seeds one tracked reviewer PR (id 2) that then drops out of the active list.
    async fn tracked_pr_missing_from_list(server: &MockServer) -> (PrLifecycleSource, SyncContext) {
        let mut review = pr(2, "other", at(1, 0));
        review["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(server, vec![review]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        let mut src = source(server);
        run(&mut src, &ctx, None, at(10, 0), false).await;
        serve_active(server, vec![]).await;
        (src, ctx)
    }

    async fn tracked_ids(ctx: &SyncContext) -> Vec<i64> {
        let snap: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await
            .unwrap()
            .unwrap();
        snap.prs.iter().map(|p| p.pr_id).collect()
    }

    async fn forbidden_count(ctx: &SyncContext) -> usize {
        let state: Lifecycle = ctx
            .snapshots
            .get(SNAPSHOT_LIFECYCLE)
            .await
            .unwrap()
            .unwrap();
        state.forbidden_since.len()
    }

    #[tokio::test]
    async fn forbidden_get_keeps_tracking_until_threshold_then_drops_quietly() {
        let server = MockServer::start().await;
        let (mut src, ctx) = tracked_pr_missing_from_list(&server).await;
        serve_pr(&server, 2, ResponseTemplate::new(403)).await;

        assert!(
            run(&mut src, &ctx, Some(at(10, 1)), at(10, 2), false)
                .await
                .is_empty()
        );
        assert!(
            run(&mut src, &ctx, Some(at(10, 3)), at(11, 1), false)
                .await
                .is_empty()
        );
        assert_eq!(tracked_ids(&ctx).await, [2]);
        assert_eq!(forbidden_count(&ctx).await, 1);

        assert!(
            run(&mut src, &ctx, Some(at(11, 2)), at(11, 3), false)
                .await
                .is_empty()
        );
        assert!(tracked_ids(&ctx).await.is_empty());
        assert_eq!(forbidden_count(&ctx).await, 0);
    }

    #[tokio::test]
    async fn successful_get_clears_forbidden_marker() {
        let server = MockServer::start().await;
        let (mut src, ctx) = tracked_pr_missing_from_list(&server).await;
        serve_pr(&server, 2, ResponseTemplate::new(403)).await;
        run(&mut src, &ctx, Some(at(10, 1)), at(10, 2), false).await;
        assert_eq!(forbidden_count(&ctx).await, 1);

        let mut review = pr(2, "other", at(1, 0));
        review["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(&server, vec![]).await;
        serve_pr(&server, 2, ResponseTemplate::new(200).set_body_json(review)).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 3)), at(10, 4), false)
                .await
                .is_empty()
        );
        assert_eq!(forbidden_count(&ctx).await, 0);

        serve_active(&server, vec![]).await;
        serve_pr(&server, 2, ResponseTemplate::new(403)).await;
        run(&mut src, &ctx, Some(at(11, 1)), at(11, 2), false).await;
        assert_eq!(tracked_ids(&ctx).await, [2]);
    }

    #[tokio::test]
    async fn long_transient_failure_never_drops_or_starts_the_clock() {
        let server = MockServer::start().await;
        let (mut src, ctx) = tracked_pr_missing_from_list(&server).await;
        serve_pr(&server, 2, ResponseTemplate::new(500)).await;

        let mut c = ctx.clone();
        c.watermark = Some(at(10, 1));
        c.server_now = at(14, 0);
        assert!(src.poll(&c).await.is_err());
        assert_eq!(tracked_ids(&ctx).await, [2]);
        assert_eq!(forbidden_count(&ctx).await, 0);
    }

    #[tokio::test]
    async fn seeded_old_reviewer_pr_raises_nothing_on_next_poll() {
        let server = MockServer::start().await;
        let mut review = pr(2, "other", at(1, 0));
        review["reviewers"] = json!([{ "id": ME, "isContainer": false }]);
        serve_active(&server, vec![review]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false)
                .await
                .is_empty()
        );
    }

    #[test]
    fn truncation_is_measured_in_utf16_units() {
        // 200 astral chars are 200 scalars but 400 UTF-16 units.
        assert!(is_truncated(&"\u{1F600}".repeat(200)));
        assert!(!is_truncated(&"x".repeat(399)));
    }

    #[tokio::test]
    async fn transient_full_fetch_is_non_fatal_and_retried_next_poll() {
        let server = MockServer::start().await;
        serve_active(&server, vec![pr(1, "other", at(10, 0))]).await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 1), None, false);
        let mut src = source(&server);
        run(&mut src, &ctx, None, at(10, 1), false).await;

        let mut listed = pr(1, "other", at(10, 0));
        listed["description"] = json!("x".repeat(450));
        let mut full = pr(1, "other", at(10, 0));
        full["description"] = json!(format!("{} @<{ME}>", "x".repeat(450)));
        serve_active(&server, vec![listed.clone()]).await;
        serve_pr(&server, 1, ResponseTemplate::new(500)).await;
        assert!(
            run(&mut src, &ctx, Some(at(10, 2)), at(10, 3), false)
                .await
                .is_empty()
        );

        serve_active(&server, vec![listed]).await;
        serve_pr(&server, 1, ResponseTemplate::new(200).set_body_json(full)).await;
        let events = run(&mut src, &ctx, Some(at(10, 4)), at(10, 5), false).await;
        assert_eq!(types(&events), ["pr.mentioned"]);
    }
}
