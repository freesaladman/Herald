//! Pr-tier source: policy evaluations and PR statuses of my own and reviewed PRs, as state changes.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream;
use herald_core::event::{Actor, ActorKind, Event, PrRef, PrRole, event_types, ids, subject};
use herald_core::provider::{PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    SNAPSHOT_TRACKED_PRS, TrackedPr, TrackedPrs, is_fatal, pr_key, pr_web_url, project_ref,
    repo_ref,
};
use crate::connection::AdoShared;
use crate::dto_git::{GitPullRequestStatus, PolicyEvaluationRecord};

const CONCURRENCY: usize = 4;
const SNAPSHOT_SEEN: &str = "pr_policies:seen";
const TERMINAL: [&str; 6] = [
    "approved",
    "succeeded",
    "rejected",
    "failed",
    "broken",
    "error",
];
const BAD: [&str; 4] = ["rejected", "failed", "broken", "error"];
const PENDING: [&str; 3] = ["running", "queued", "pending"];

pub struct PrPoliciesSource {
    shared: Arc<AdoShared>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SeenState {
    /// `pr_key` -> policy key -> normalised state. Keys are `eval:{id}` or `status:{genre/name}`.
    prs: BTreeMap<String, BTreeMap<String, String>>,
}

struct PrData {
    evaluations: Vec<PolicyEvaluationRecord>,
    statuses: Vec<GitPullRequestStatus>,
}

type Fetched = (usize, Option<PrData>);

/// One policy's current state, with what an event needs.
struct Observed {
    key: String,
    id: String,
    name: String,
    state: String,
    occurred: Option<DateTime<Utc>>,
    target_url: Option<String>,
}

impl PrPoliciesSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    /// `Ok(None)` means skip this PR (logged); errors that mean the poll cannot proceed are returned.
    async fn fetch(&self, i: usize, pr: &TrackedPr) -> Result<Fetched, ProviderError> {
        let project = project_ref(&pr.project_id, &pr.project);
        let client = &self.shared.client;
        let both = tokio::try_join!(
            client.policy_evaluations(project, &pr.project_id, pr.pr_id),
            client.pr_statuses(project, &pr.repo_id, pr.pr_id),
        );
        match both.map_err(ProviderError::from) {
            Ok((evaluations, statuses)) => Ok((
                i,
                Some(PrData {
                    evaluations,
                    statuses,
                }),
            )),
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::warn!(
                    "skipping policies of PR {} in {}: {e}",
                    pr.pr_id,
                    pr.repo_id
                );
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

        // Policy noise on PRs I only watch (group review, commenter, mention) is not wanted.
        let wanted: Vec<&TrackedPr> = tracked
            .prs
            .iter()
            .filter(|p| matches!(p.role, PrRole::Author | PrRole::Reviewer))
            .collect();
        let fetches: Vec<BoxFuture<'_, Result<Fetched, ProviderError>>> = wanted
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
        let org = &self.shared.organization;
        let mut state = SeenState::default();
        let mut events = Vec::new();
        for (i, fetched) in results {
            let pr = wanted[i];
            let key = pr_key(&pr.repo_id, pr.pr_id);
            let previous = old.prs.get(&key);
            let Some(data) = fetched else {
                // Keep the previous state so the next successful poll diffs against it.
                if let Some(p) = previous {
                    state.prs.insert(key, p.clone());
                }
                continue;
            };
            let observed = observe(data, ctx.server_now);
            // A PR seen for the first time is recorded silently.
            if let (Some(prev), false) = (previous, seeding) {
                for o in &observed {
                    let before = prev.get(&o.key).map(String::as_str);
                    if should_emit(before, &o.state) {
                        events.push(policy_event(ctx, org, pr, o, before));
                    }
                }
            }
            state.prs.insert(
                key,
                observed.into_iter().map(|o| (o.key, o.state)).collect(),
            );
        }
        ctx.snapshots.put(SNAPSHOT_SEEN, &state).await?;
        Ok(PollOutcome { events })
    }
}

impl Source for PrPoliciesSource {
    fn name(&self) -> &'static str {
        "pr_policies"
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

fn normalise(raw: &str, empty: &str) -> String {
    let s = raw.trim();
    if s.is_empty() {
        return empty.into();
    }
    match s.to_ascii_lowercase().as_str() {
        "notapplicable" => "notApplicable".into(),
        "notset" => "notSet".into(),
        // A status post that partly succeeded is not a pass; treat it as a failure.
        "partiallysucceeded" => "failed".into(),
        other => other.into(),
    }
}

fn observe(data: PrData, now: DateTime<Utc>) -> Vec<Observed> {
    let mut out: Vec<Observed> = data
        .evaluations
        .into_iter()
        .map(|e| Observed {
            key: format!("eval:{}", e.evaluation_id),
            // ADO keeps the evaluation id across runs of a policy; the start time tells runs apart.
            id: format!(
                "{}-{}",
                e.evaluation_id,
                e.started_date
                    .or(e.completed_date)
                    .unwrap_or(now)
                    .timestamp_millis()
            ),
            name: e.configuration.policy_type.display_name,
            state: normalise(&e.status, "broken"),
            occurred: e.completed_date.or(e.started_date),
            target_url: None,
        })
        .collect();

    // ADO keeps every status post per context; only the latest one is current.
    let when = |s: &GitPullRequestStatus| (s.updated_date.or(s.creation_date), s.id);
    let mut latest: BTreeMap<String, GitPullRequestStatus> = BTreeMap::new();
    for s in data.statuses {
        let name = if s.context.name.is_empty() {
            s.description.clone().unwrap_or_default()
        } else {
            match s.context.genre.as_deref().filter(|g| !g.is_empty()) {
                Some(g) => format!("{g}/{}", s.context.name),
                None => s.context.name.clone(),
            }
        };
        match latest.get(&name) {
            Some(cur) if when(cur) >= when(&s) => {}
            _ => {
                latest.insert(name, s);
            }
        }
    }
    out.extend(latest.into_iter().map(|(name, s)| Observed {
        key: format!("status:{name}"),
        id: format!("status-{}", s.id),
        occurred: s.updated_date.or(s.creation_date),
        state: normalise(&s.state, "notSet"),
        target_url: s.target_url,
        name,
    }));
    out
}

/// Terminal results always report; a return to running/queued/pending only after a bad result
/// (a rebuild). A first appearance in a non-terminal state, and everything else, stays silent.
fn should_emit(previous: Option<&str>, state: &str) -> bool {
    if previous == Some(state) {
        return false;
    }
    if TERMINAL.contains(&state) {
        return true;
    }
    PENDING.contains(&state) && previous.is_some_and(|p| BAD.contains(&p))
}

fn http_url(raw: &str) -> bool {
    url::Url::parse(raw).is_ok_and(|u| matches!(u.scheme(), "http" | "https"))
}

fn policy_event(
    ctx: &SyncContext,
    org: &str,
    pr: &TrackedPr,
    o: &Observed,
    previous: Option<&str>,
) -> Event {
    let name = if o.name.is_empty() { "Policy" } else { &o.name };
    let mut e = Event::new(
        ctx.connection.clone(),
        event_types::PR_POLICY_CHANGED,
        o.occurred.unwrap_or(ctx.server_now),
        ctx.server_now,
        format!("{name}: {}", o.state),
        subject::pr(&ctx.connection.id, &pr.repo_id, pr.pr_id),
    )
    .with_id(ids::pr_policy(
        &ctx.connection.id,
        &pr.repo_id,
        pr.pr_id,
        &o.id,
        &o.state,
    ));
    e.inferred = o.occurred.is_none();
    e.catch_up = ctx.catch_up;
    e.actor = Actor {
        kind: ActorKind::System,
        id: String::new(),
        display_name: "System".into(),
        unique_name: None,
    };
    e.url = Some(o.target_url.as_deref().filter(|u| http_url(u)).map_or_else(
        || pr_web_url(org, &pr.project, &pr.repo_name, pr.pr_id),
        str::to_string,
    ));
    e.subtitle = Some(pr.title.clone());
    e.repo = Some(repo_ref(&pr.repo_id, &pr.repo_name, &pr.project));
    e.pr = Some(PrRef {
        id: pr.pr_id,
        title: pr.title.clone(),
        source_branch: pr.source_ref.clone(),
        target_branch: pr.target_ref.clone(),
        is_draft: pr.is_draft,
        role: pr.role,
    });
    e.data = json!({
        "policyName": name,
        "state": o.state,
        "previousState": previous,
    });
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
    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            id: "aaaa-me".into(),
            display_name: "Me".into(),
            unique_name: "me@example.test".into(),
            group_ids: Vec::new(),
        }
    }

    fn source(server: &MockServer) -> PrPoliciesSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PrPoliciesSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn evaluation(id: &str, status: &str, completed: Option<DateTime<Utc>>) -> Value {
        json!({
            "evaluationId": id, "status": status, "startedDate": at(1, 0),
            "completedDate": completed,
            "configuration": { "id": 48, "type": { "displayName": "Build" } },
        })
    }

    fn status(id: i64, state: Option<&str>, when: DateTime<Utc>, target: Option<&str>) -> Value {
        let mut v = json!({
            "id": id, "description": "cov",
            "context": { "name": "codecoverage", "genre": "ci.build" },
            "creationDate": when, "updatedDate": when,
        });
        if let Some(s) = state {
            v["state"] = json!(s);
        }
        if let Some(t) = target {
            v["targetUrl"] = json!(t);
        }
        v
    }

    fn ok(items: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": items }))
    }

    async fn serve(server: &MockServer, evals: ResponseTemplate, statuses: ResponseTemplate) {
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/policy/evaluations"))
            .respond_with(evals)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/p1/_apis/git/repositories/Repo-1/pullRequests/1/statuses",
            ))
            .respond_with(statuses)
            .mount(server)
            .await;
    }

    async fn track(ctx: &SyncContext, role: PrRole) {
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
                        ..Default::default()
                    }],
                },
            )
            .await
            .unwrap();
    }

    async fn run(
        src: &mut PrPoliciesSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        c.server_now = now;
        src.poll(&c).await.map(|o| o.events)
    }

    /// Seeds with `before`, then polls with `after`.
    async fn seeded(
        before: (Vec<Value>, Vec<Value>),
        after: (Vec<Value>, Vec<Value>),
    ) -> Vec<Event> {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Author).await;
        let mut src = source(&server);
        serve(&server, ok(before.0), ok(before.1)).await;
        assert!(
            run(&mut src, &ctx, None, at(10, 0))
                .await
                .unwrap()
                .is_empty()
        );
        serve(&server, ok(after.0), ok(after.1)).await;
        run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn seed_and_first_sight_are_silent_then_queued_to_approved_emits() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Author).await;
        let mut src = source(&server);
        serve(
            &server,
            ok(vec![evaluation("e1", "queued", None)]),
            ok(vec![]),
        )
        .await;
        assert!(
            run(&mut src, &ctx, None, at(10, 0))
                .await
                .unwrap()
                .is_empty()
        );
        let done = evaluation("e1", "approved", Some(at(10, 5)));
        serve(&server, ok(vec![done]), ok(vec![])).await;
        let events = run(&mut src, &ctx, Some(at(10, 0)), at(10, 12))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event_type, "pr.policy.changed");
        assert_eq!(
            e.id,
            format!(
                "c1:pr:Repo-1:1:policy:e1-{}:approved",
                at(1, 0).timestamp_millis()
            )
        );
        assert_eq!(e.title, "Build: approved");
        assert_eq!(e.occurred_utc, at(10, 5));
        assert!(!e.inferred);
        assert_eq!(e.actor.kind, ActorKind::System);
        assert_eq!(
            e.data,
            json!({ "policyName": "Build", "state": "approved", "previousState": "queued" })
        );
        assert_eq!(
            e.url.as_deref(),
            Some("https://dev.azure.com/org/Alpha/_git/widgets/pullrequest/1")
        );
        // A repeat poll has nothing new.
        assert!(
            run(&mut src, &ctx, Some(at(10, 12)), at(10, 13))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn latest_status_post_wins_and_missing_state_is_not_set() {
        let first = status(1, None, at(9, 1), None);
        let failed = status(
            2,
            Some("failed"),
            at(10, 3),
            Some("https://ci.example.com/run/9"),
        );
        let events = seeded(
            (vec![], vec![first.clone()]),
            // The older succeeded post must not mask the newer failed one.
            (
                vec![],
                vec![failed, status(3, Some("succeeded"), at(9, 5), None)],
            ),
        )
        .await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data["policyName"], "ci.build/codecoverage");
        assert_eq!(events[0].data["state"], "failed");
        assert_eq!(events[0].data["previousState"], "notSet");
        assert_eq!(events[0].id, "c1:pr:Repo-1:1:policy:status-2:failed");
        assert_eq!(
            events[0].url.as_deref(),
            Some("https://ci.example.com/run/9")
        );
    }

    #[tokio::test]
    async fn non_http_target_url_falls_back_to_pr_url() {
        let events = seeded(
            (vec![], vec![status(1, Some("pending"), at(9, 1), None)]),
            (
                vec![],
                vec![status(
                    2,
                    Some("succeeded"),
                    at(10, 3),
                    Some("vstfs:///Build/Build/5"),
                )],
            ),
        )
        .await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].url.as_deref(),
            Some("https://dev.azure.com/org/Alpha/_git/widgets/pullrequest/1")
        );
    }

    #[tokio::test]
    async fn rebuild_after_failure_emits_but_new_queued_does_not() {
        let events = seeded(
            (vec![evaluation("e1", "rejected", Some(at(9, 1)))], vec![]),
            (
                vec![
                    evaluation("e1", "queued", None),
                    evaluation("e2", "queued", None),
                ],
                vec![],
            ),
        )
        .await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data["state"], "queued");
        assert_eq!(events[0].data["previousState"], "rejected");
        assert_eq!(events[0].occurred_utc, at(1, 0));
    }

    #[tokio::test]
    async fn repeated_failure_of_a_stable_evaluation_gets_a_new_id() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), None, false);
        track(&ctx, PrRole::Author).await;
        let mut src = source(&server);
        let run_of = |status: &str, started: DateTime<Utc>, done: Option<DateTime<Utc>>| {
            let mut v = evaluation("e1", status, done);
            v["startedDate"] = json!(started);
            v
        };
        serve(
            &server,
            ok(vec![run_of("approved", at(1, 0), Some(at(1, 1)))]),
            ok(vec![]),
        )
        .await;
        run(&mut src, &ctx, None, at(10, 0)).await.unwrap();
        let mut events = Vec::new();
        let mut wm = at(10, 0);
        for (i, e) in [
            run_of("rejected", at(2, 0), Some(at(2, 1))),
            run_of("queued", at(3, 0), None),
            run_of("rejected", at(3, 0), Some(at(3, 1))),
        ]
        .into_iter()
        .enumerate()
        {
            serve(&server, ok(vec![e]), ok(vec![])).await;
            let now = at(11 + i as u32, 0);
            events.extend(run(&mut src, &ctx, Some(wm), now).await.unwrap());
            wm = now;
        }
        let rejected: Vec<&Event> = events
            .iter()
            .filter(|e| e.data["state"] == "rejected")
            .collect();
        assert_eq!(rejected.len(), 2);
        assert_ne!(rejected[0].id, rejected[1].id);
    }

    #[tokio::test]
    async fn group_reviewer_prs_are_not_polled() {
        let server = MockServer::start().await;
        let (ctx, _store) = sync_context(cfg(), me(), at(10, 0), Some(at(9, 0)), false);
        track(&ctx, PrRole::GroupReviewer).await;
        let mut src = source(&server);
        serve(
            &server,
            ResponseTemplate::new(500),
            ResponseTemplate::new(500),
        )
        .await;
        assert!(
            run(&mut src, &ctx, Some(at(9, 0)), at(10, 0))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(server.received_requests().await.unwrap().is_empty());
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
}
