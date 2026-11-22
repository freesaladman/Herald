//! Slow-tier source: pushes to my branches (and my own branch creations) as branch events.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream;
use herald_core::event::{Actor, ActorKind, BranchRef, Event, event_types, ids, subject};
use herald_core::provider::{PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde_json::json;

use super::{
    MyBranch, MyBranches, SNAPSHOT_MY_BRANCHES, actor_from, enc, is_fatal, project_ref, repo_ref,
    window_start,
};
use crate::client::Project;
use crate::connection::AdoShared;
use crate::dto_git::{GitPush, GitRef, GitRefUpdate, GitRepository};

const CONCURRENCY: usize = 4;
const ZERO_ID: &str = "0000000000000000000000000000000000000000";
const HEADS: &str = "refs/heads/";
/// Last time each branch was observed in a repo's refs; a branch carried over from a failing
/// repo is dropped once this is older than `CARRY_KEEP`.
const SNAPSHOT_LAST_SEEN: &str = "branches:last_seen";
const CARRY_KEEP: Duration = Duration::days(14);

type LastSeen = BTreeMap<String, DateTime<Utc>>;

fn seen_key(repo_id: &str, ref_name: &str) -> String {
    format!("{}|{ref_name}", repo_id.to_ascii_lowercase())
}

/// Carries previous branches whose repo couldn't be read, unless unseen for longer than the lookback.
fn carry<'a>(
    prior: impl Iterator<Item = &'a MyBranch>,
    prev: &LastSeen,
    next: &mut LastSeen,
    now: DateTime<Utc>,
    out: &mut Vec<MyBranch>,
) {
    for b in prior {
        let key = seen_key(&b.repo_id, &b.ref_name);
        let last = prev.get(&key).copied().unwrap_or(now);
        if last >= now - CARRY_KEEP {
            next.insert(key, last);
            out.push(b.clone());
        }
    }
}

pub struct BranchesSource {
    shared: Arc<AdoShared>,
}

struct RepoWork {
    project: Project,
    repo: GitRepository,
}

struct RepoData {
    refs: Vec<GitRef>,
    pushes: Vec<GitPush>,
}

type Fetched = (usize, Option<RepoData>);

impl BranchesSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    /// `Ok(None)` means skip this repo (logged); errors that mean the poll cannot proceed are returned.
    async fn fetch(
        &self,
        i: usize,
        work: &RepoWork,
        ctx: &SyncContext,
    ) -> Result<Fetched, ProviderError> {
        let project = project_ref(&work.project.id, &work.project.name);
        let client = &self.shared.client;
        let both = tokio::try_join!(
            client.refs_heads(project, &work.repo.id),
            client.pushes_since(project, &work.repo.id, window_start(ctx)),
        );
        match both.map_err(ProviderError::from) {
            Ok((refs, pushes)) => Ok((i, Some(RepoData { refs, pushes }))),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!("skipping branches of repo {}: {e}", work.repo.name);
                Ok((i, None))
            }
        }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let previous: MyBranches = ctx
            .snapshots
            .get(SNAPSHOT_MY_BRANCHES)
            .await?
            .unwrap_or_default();
        let last_seen_prev: LastSeen = ctx
            .snapshots
            .get(SNAPSHOT_LAST_SEEN)
            .await?
            .unwrap_or_default();
        let mut last_seen = LastSeen::new();
        let now = ctx.server_now;

        let mut work = Vec::new();
        let mut kept: Vec<MyBranch> = Vec::new();
        for project in self.shared.projects().await? {
            let pref = project_ref(&project.id, &project.name);
            match self.shared.client.repositories(pref).await {
                Ok(repos) => work.extend(repos.into_iter().filter(|r| !r.is_disabled).map(
                    |repo| RepoWork {
                        project: project.clone(),
                        repo,
                    },
                )),
                Err(e) => {
                    let e = ProviderError::from(e);
                    if is_fatal(&e) {
                        return Err(e);
                    }
                    tracing::warn!("skipping repositories of project {}: {e}", project.name);
                    carry(
                        previous
                            .branches
                            .iter()
                            .filter(|b| b.project == project.name),
                        &last_seen_prev,
                        &mut last_seen,
                        now,
                        &mut kept,
                    );
                }
            }
        }

        let fetches: Vec<BoxFuture<'_, Result<Fetched, ProviderError>>> = work
            .iter()
            .enumerate()
            .map(|(i, w)| -> BoxFuture<'_, _> { Box::pin(self.fetch(i, w, ctx)) })
            .collect();
        // try_collect drops the stream on the first fatal error, cancelling in-flight requests.
        let mut results: Vec<Fetched> = stream::iter(fetches)
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        results.sort_by_key(|(i, _)| *i);

        let seeding = ctx.watermark.is_none();
        let org = &self.shared.organization;
        let mut events = Vec::new();
        let mut current = kept;
        for (i, fetched) in results {
            let w = &work[i];
            let prior = previous
                .branches
                .iter()
                .filter(|b| b.repo_id.eq_ignore_ascii_case(&w.repo.id));
            let Some(data) = fetched else {
                carry(prior, &last_seen_prev, &mut last_seen, now, &mut current);
                continue;
            };
            let mine: Vec<String> = data
                .refs
                .iter()
                .filter(|r| {
                    r.creator
                        .as_ref()
                        .is_some_and(|c| c.id.eq_ignore_ascii_case(&ctx.me.id))
                })
                .map(|r| r.name.clone())
                .collect();
            let mut known: Vec<&str> = mine.iter().map(String::as_str).collect();
            known.extend(prior.map(|b| b.ref_name.as_str()));
            if !seeding {
                for push in &data.pushes {
                    for update in &push.ref_updates {
                        let owned = known.contains(&update.name.as_str());
                        if let Some(e) = self.push_event(ctx, org, w, push, update, owned).await? {
                            events.push(e);
                        }
                    }
                }
            }
            for ref_name in mine {
                last_seen.insert(seen_key(&w.repo.id, &ref_name), now);
                current.push(MyBranch {
                    repo_id: w.repo.id.clone(),
                    project: w.project.name.clone(),
                    ref_name,
                });
            }
        }

        current.sort_by(|a, b| (&a.repo_id, &a.ref_name).cmp(&(&b.repo_id, &b.ref_name)));
        current.dedup();
        ctx.snapshots
            .put(SNAPSHOT_MY_BRANCHES, &MyBranches { branches: current })
            .await?;
        ctx.snapshots.put(SNAPSHOT_LAST_SEEN, &last_seen).await?;
        Ok(PollOutcome { events })
    }

    /// `owned`: the ref is in my previous or current branch set.
    async fn push_event(
        &self,
        ctx: &SyncContext,
        org: &str,
        w: &RepoWork,
        push: &GitPush,
        update: &GitRefUpdate,
        owned: bool,
    ) -> Result<Option<Event>, ProviderError> {
        if !update.name.starts_with(HEADS) {
            return Ok(None);
        }
        let by_me = push
            .pushed_by
            .as_ref()
            .is_some_and(|p| p.id.eq_ignore_ascii_case(&ctx.me.id));
        let created = update.old_object_id == ZERO_ID;
        let deleted = update.new_object_id == ZERO_ID;
        let event_type = if created && by_me {
            event_types::BRANCH_CREATED
        } else if !owned || created {
            return Ok(None);
        } else if deleted {
            event_types::BRANCH_DELETED
        } else if by_me {
            event_types::BRANCH_PUSHED
        } else if self.is_force_push(w, update).await? {
            event_types::BRANCH_FORCEPUSHED
        } else {
            event_types::BRANCH_PUSHED
        };
        Ok(Some(build_event(ctx, org, w, push, update, event_type)))
    }

    /// A non-fatal diff failure counts as a plain push.
    async fn is_force_push(
        &self,
        w: &RepoWork,
        update: &GitRefUpdate,
    ) -> Result<bool, ProviderError> {
        let project = project_ref(&w.project.id, &w.project.name);
        match self
            .shared
            .client
            .commit_diff_counts(
                project,
                &w.repo.id,
                &update.old_object_id,
                &update.new_object_id,
            )
            .await
            .map_err(ProviderError::from)
        {
            Ok(d) => Ok(d.behind_count > 0),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!("force-push check failed for {}: {e}", update.name);
                Ok(false)
            }
        }
    }
}

impl Source for BranchesSource {
    fn name(&self) -> &'static str {
        "branches"
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

fn build_event(
    ctx: &SyncContext,
    org: &str,
    w: &RepoWork,
    push: &GitPush,
    update: &GitRefUpdate,
    event_type: &'static str,
) -> Event {
    let short = update.name.strip_prefix(HEADS).unwrap_or(&update.name);
    let verb = match event_type {
        event_types::BRANCH_CREATED => "created",
        event_types::BRANCH_DELETED => "deleted",
        event_types::BRANCH_FORCEPUSHED => "force pushed",
        _ => "pushed",
    };
    let mut e = Event::new(
        ctx.connection.clone(),
        event_type,
        push.date.unwrap_or(ctx.server_now),
        ctx.server_now,
        format!("Branch {short} {verb}"),
        subject::branch(&ctx.connection.id, &w.repo.id, &update.name),
    )
    .with_id(ids::branch(
        &ctx.connection.id,
        &w.repo.id,
        &push.push_id.to_string(),
        &update.name,
    ));
    e.inferred = push.date.is_none();
    e.catch_up = ctx.catch_up;
    e.actor = push
        .pushed_by
        .as_ref()
        .map(|p| actor_from(p, &ctx.me))
        .unwrap_or_else(|| Actor {
            kind: ActorKind::Human,
            id: String::new(),
            display_name: "Unknown".into(),
            unique_name: None,
        });
    e.subtitle = Some(w.repo.name.clone());
    e.url = Some(format!(
        "https://dev.azure.com/{}/{}/_git/{}?version=GB{}",
        enc(org),
        enc(&w.project.name),
        enc(&w.repo.name),
        enc(short)
    ));
    e.repo = Some(repo_ref(&w.repo.id, &w.repo.name, &w.project.name));
    e.branch = Some(BranchRef {
        name: update.name.clone(),
        short_name: short.to_string(),
    });
    e.data = json!({
        "pushId": push.push_id,
        "oldObjectId": update.old_object_id,
        "newObjectId": update.new_object_id,
    });
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AdoAuth;
    use crate::client::AdoClient;
    use chrono::{DateTime, TimeZone, Utc};
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::provider::Identity;
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::Value;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ME: &str = "aaaa-me";
    const OLD: &str = "1111111111111111111111111111111111111111";
    const NEW: &str = "2222222222222222222222222222222222222222";

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

    fn source(server: &MockServer) -> BranchesSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        BranchesSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn ident(id: &str) -> Value {
        json!({ "id": id, "displayName": id, "uniqueName": format!("{id}@example.test") })
    }

    fn head(name: &str, creator: &str) -> Value {
        json!({ "name": format!("refs/heads/{name}"), "objectId": NEW, "creator": ident(creator) })
    }

    fn push(id: i64, by: &str, name: &str, old: &str, new: &str) -> Value {
        json!({
            "pushId": id, "date": at(20, 11), "pushedBy": ident(by),
            "refUpdates": [{ "name": format!("refs/heads/{name}"), "oldObjectId": old, "newObjectId": new }],
        })
    }

    fn ok(items: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": items }))
    }

    fn repo(id: &str, disabled: bool) -> Value {
        json!({ "id": id, "name": format!("name-{id}"), "isDisabled": disabled,
                "project": { "id": "p1", "name": "Alpha" } })
    }

    async fn serve_repos(server: &MockServer, repos: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path("/_apis/projects"))
            .respond_with(ok(vec![json!({ "id": "p1", "name": "Alpha" })]))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/git/repositories"))
            .respond_with(ok(repos))
            .mount(server)
            .await;
    }

    async fn serve_repo(
        server: &MockServer,
        id: &str,
        refs: ResponseTemplate,
        pushes: ResponseTemplate,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/p1/_apis/git/repositories/{id}/refs")))
            .respond_with(refs)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/p1/_apis/git/repositories/{id}/pushes")))
            .respond_with(pushes)
            .mount(server)
            .await;
    }

    async fn serve_diff(server: &MockServer, behind: u32) {
        Mock::given(method("GET"))
            .and(path("/p1/_apis/git/repositories/r1/diffs/commits"))
            .and(query_param("baseVersion", OLD))
            .and(query_param("targetVersion", NEW))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "aheadCount": 1, "behindCount": behind })),
            )
            .expect(1)
            .mount(server)
            .await;
    }

    async fn run(
        src: &mut BranchesSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = at(20, 12);
        src.poll(&c).await.map(|o| o.events)
    }

    async fn snapshot(ctx: &SyncContext) -> Vec<(String, String)> {
        let s: MyBranches = ctx
            .snapshots
            .get(SNAPSHOT_MY_BRANCHES)
            .await
            .unwrap()
            .unwrap();
        s.branches
            .into_iter()
            .map(|b| (b.repo_id, b.ref_name))
            .collect()
    }

    async fn seed_snapshot(ctx: &SyncContext, repo_id: &str, name: &str) {
        ctx.snapshots
            .put(
                SNAPSHOT_MY_BRANCHES,
                &MyBranches {
                    branches: vec![MyBranch {
                        repo_id: repo_id.into(),
                        project: "Alpha".into(),
                        ref_name: format!("refs/heads/{name}"),
                    }],
                },
            )
            .await
            .unwrap();
    }

    fn live_ctx() -> SyncContext {
        sync_context(cfg(), me(), at(20, 12), Some(at(20, 11)), false).0
    }

    #[tokio::test]
    async fn seed_writes_snapshot_and_emits_nothing() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ok(vec![head("mine", ME), head("theirs", "other")]),
            ok(vec![push(1, "other", "mine", OLD, NEW)]),
        )
        .await;
        let ctx = sync_context(cfg(), me(), at(20, 12), None, false).0;
        let events = run(&mut source(&server), &ctx, None).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(
            snapshot(&ctx).await,
            vec![("r1".into(), "refs/heads/mine".into())]
        );
    }

    #[tokio::test]
    async fn pushes_on_my_branch_are_reported_and_other_branches_ignored() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ok(vec![head("mine", ME)]),
            ok(vec![
                push(1, "other", "mine", OLD, NEW),
                push(2, ME, "mine", OLD, NEW),
                push(3, "other", "unrelated", OLD, NEW),
            ]),
        )
        .await;
        serve_diff(&server, 0).await;
        let ctx = live_ctx();
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        let first = &events[0];
        assert_eq!(first.event_type, event_types::BRANCH_PUSHED);
        assert_eq!(first.actor.id, "other");
        assert_eq!(first.actor.kind, ActorKind::Human);
        assert_eq!(first.id, "c1:branch:r1:1:refs/heads/mine");
        assert_eq!(first.subject_key, "c1:branch:r1:refs/heads/mine");
        assert_eq!(first.branch.as_ref().unwrap().short_name, "mine");
        assert_eq!(
            first.url.as_deref(),
            Some("https://dev.azure.com/org/Alpha/_git/name-r1?version=GBmine")
        );
        assert_eq!(first.data["pushId"], 1);
        assert_eq!(events[1].event_type, event_types::BRANCH_PUSHED);
        assert_eq!(events[1].actor.kind, ActorKind::Me);
    }

    #[tokio::test]
    async fn force_push_detected_via_behind_count() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ok(vec![head("mine", ME)]),
            ok(vec![push(1, "other", "mine", OLD, NEW)]),
        )
        .await;
        serve_diff(&server, 3).await;
        let ctx = live_ctx();
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, event_types::BRANCH_FORCEPUSHED);
    }

    #[tokio::test]
    async fn delete_by_other_and_create_by_me() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        // "gone" is only in the previous snapshot; "fresh" is mine and just created.
        serve_repo(
            &server,
            "r1",
            ok(vec![head("fresh", ME)]),
            ok(vec![
                push(1, "other", "gone", OLD, ZERO_ID),
                push(2, ME, "fresh", ZERO_ID, NEW),
            ]),
        )
        .await;
        let ctx = live_ctx();
        seed_snapshot(&ctx, "r1", "gone").await;
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, event_types::BRANCH_DELETED);
        assert_eq!(events[0].actor.id, "other");
        assert_eq!(events[1].event_type, event_types::BRANCH_CREATED);
        assert_eq!(events[1].actor.kind, ActorKind::Me);
        assert_eq!(
            snapshot(&ctx).await,
            vec![("r1".into(), "refs/heads/fresh".into())]
        );
    }

    #[tokio::test]
    async fn disabled_repo_skipped_and_failed_repo_keeps_previous_branches() {
        let server = MockServer::start().await;
        serve_repos(
            &server,
            vec![repo("r1", false), repo("r2", true), repo("r3", false)],
        )
        .await;
        serve_repo(
            &server,
            "r1",
            ResponseTemplate::new(404),
            ResponseTemplate::new(404),
        )
        .await;
        serve_repo(&server, "r3", ok(vec![head("c", ME)]), ok(vec![])).await;
        let ctx = live_ctx();
        seed_snapshot(&ctx, "r1", "kept").await;
        run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        let mut got = snapshot(&ctx).await;
        got.sort();
        assert_eq!(
            got,
            vec![
                ("r1".into(), "refs/heads/kept".into()),
                ("r3".into(), "refs/heads/c".into())
            ]
        );
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().all(|r| !r.url.path().contains("/r2/")));
    }

    #[tokio::test]
    async fn carried_branch_of_failing_repo_is_pruned_after_the_lookback() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ResponseTemplate::new(404),
            ResponseTemplate::new(404),
        )
        .await;
        let ctx = live_ctx();
        seed_snapshot(&ctx, "r1", "stale").await;
        let key = seen_key("r1", "refs/heads/stale");
        let mut seen = LastSeen::new();
        seen.insert(key.clone(), at(20, 12) - Duration::days(15));
        ctx.snapshots.put(SNAPSHOT_LAST_SEEN, &seen).await.unwrap();
        run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert!(snapshot(&ctx).await.is_empty());

        // Within the lookback it is still carried and its stamp is preserved.
        seed_snapshot(&ctx, "r1", "stale").await;
        seen.insert(key.clone(), at(20, 12) - Duration::days(13));
        ctx.snapshots.put(SNAPSHOT_LAST_SEEN, &seen).await.unwrap();
        run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(snapshot(&ctx).await.len(), 1);
        let after: LastSeen = ctx
            .snapshots
            .get(SNAPSHOT_LAST_SEEN)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after[&key], seen[&key]);
    }

    #[tokio::test]
    async fn observed_delete_drops_branch_from_next_snapshot() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ok(vec![]),
            ok(vec![push(1, "other", "gone", OLD, ZERO_ID)]),
        )
        .await;
        let ctx = live_ctx();
        seed_snapshot(&ctx, "r1", "gone").await;
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events[0].event_type, event_types::BRANCH_DELETED);
        assert!(snapshot(&ctx).await.is_empty());
        let seen: LastSeen = ctx
            .snapshots
            .get(SNAPSHOT_LAST_SEEN)
            .await
            .unwrap()
            .unwrap();
        assert!(seen.is_empty());
    }

    #[tokio::test]
    async fn transient_failure_aborts_poll() {
        let server = MockServer::start().await;
        serve_repos(&server, vec![repo("r1", false)]).await;
        serve_repo(
            &server,
            "r1",
            ResponseTemplate::new(500),
            ResponseTemplate::new(500),
        )
        .await;
        let ctx = live_ctx();
        let err = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::Transient(_)), "{err:?}");
    }
}
