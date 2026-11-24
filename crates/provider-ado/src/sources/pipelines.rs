//! Fast-tier source: my builds (completed runs, failed stages) and YAML pipeline approvals.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, TryStreamExt, stream};
use herald_core::event::{
    Actor, ActorKind, ApprovalRef, Event, PrRef, PrRole, RunRef, event_types, ids, subject,
};
use herald_core::provider::{Identity, PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};

use super::{
    MyBranches, MyBuilds, SNAPSHOT_MY_BRANCHES, SNAPSHOT_MY_BUILDS, SNAPSHOT_TRACKED_PRS,
    TrackedPr, TrackedPrs, is_fatal, project_ref, repo_ref, window_start,
};
use crate::client::{AdoError, Project};
use crate::connection::AdoShared;
use crate::pipelines::{Build, PipelineApproval, TimelineRecord};

const CONCURRENCY: usize = 4;
const BUILD_RETENTION_DAYS: i64 = 30;
const KIND: &str = "ado-yaml";
const SNAPSHOT_STAGES: &str = "pipelines:stages";
const SNAPSHOT_APPROVALS: &str = "pipelines:approvals";

pub struct PipelinesSource {
    shared: Arc<AdoShared>,
}

/// Stage failures already emitted (or seen while seeding), so a re-polled build stays quiet.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct StageSnapshot {
    emitted: Vec<StageKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StageKey {
    project: String,
    build_id: i64,
    record_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ApprovalSnapshot {
    pending: Vec<PendingApproval>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingApproval {
    id: String,
    project: String,
    pipeline: String,
    run_id: Option<i64>,
    url: Option<String>,
    #[serde(default)]
    via_group: bool,
}

/// What decides whether a build is mine.
struct Mine<'a> {
    me: &'a Identity,
    branches: &'a MyBranches,
    prs: &'a TrackedPrs,
}

impl Mine<'_> {
    fn by_person(&self, b: &Build) -> bool {
        [&b.requested_for, &b.requested_by]
            .into_iter()
            .flatten()
            .any(|i| i.id.eq_ignore_ascii_case(&self.me.id))
    }

    /// Builds without repository info match on branch or PR number alone.
    fn same_repo(build_repo: &str, repo_id: &str) -> bool {
        build_repo.is_empty() || build_repo.eq_ignore_ascii_case(repo_id)
    }

    fn on_my_branch(&self, b: &Build) -> bool {
        self.branches
            .branches
            .iter()
            .any(|m| m.ref_name == b.source_branch && Self::same_repo(&b.repository.id, &m.repo_id))
    }

    /// The tracked PR I authored whose merge ref this build ran on.
    fn my_pr(&self, b: &Build) -> Option<&TrackedPr> {
        let id: i64 = b
            .source_branch
            .strip_prefix("refs/pull/")?
            .strip_suffix("/merge")?
            .parse()
            .ok()?;
        self.prs.prs.iter().find(|p| {
            p.pr_id == id
                && p.role == PrRole::Author
                && Self::same_repo(&b.repository.id, &p.repo_id)
        })
    }

    fn is_mine(&self, b: &Build) -> bool {
        self.by_person(b) || self.on_my_branch(b) || self.my_pr(b).is_some()
    }
}

/// Per-project result; sections that failed non-fatally keep the previous state.
struct ProjectOut {
    events: Vec<Event>,
    mine_builds: Vec<i64>,
    stages: Vec<StageKey>,
    approvals: Vec<PendingApproval>,
}

/// Fatal errors propagate; anything else is logged and becomes `None`.
fn soft<T>(r: Result<T, AdoError>, what: &str, project: &str) -> Result<Option<T>, ProviderError> {
    match r.map_err(ProviderError::from) {
        Ok(v) => Ok(Some(v)),
        Err(e) if is_fatal(&e) => Err(e),
        Err(e) => {
            tracing::warn!("skipping {what} of project {project}: {e}");
            Ok(None)
        }
    }
}

impl PipelinesSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let branches: MyBranches = ctx
            .snapshots
            .get(SNAPSHOT_MY_BRANCHES)
            .await?
            .unwrap_or_default();
        let prs: TrackedPrs = ctx
            .snapshots
            .get(SNAPSHOT_TRACKED_PRS)
            .await?
            .unwrap_or_default();
        let prev_stages: StageSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_STAGES)
            .await?
            .unwrap_or_default();
        let prev_approvals: ApprovalSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_APPROVALS)
            .await?
            .unwrap_or_default();
        let mine = Mine {
            me: &ctx.me,
            branches: &branches,
            prs: &prs,
        };

        let projects = self.shared.projects().await?;
        let work: Vec<BoxFuture<'_, Result<(usize, ProjectOut), ProviderError>>> = projects
            .iter()
            .enumerate()
            .map(|(i, p)| -> BoxFuture<'_, _> {
                let (mine, stages, approvals) = (&mine, &prev_stages, &prev_approvals);
                Box::pin(async move {
                    Ok((i, self.poll_project(ctx, p, mine, stages, approvals).await?))
                })
            })
            .collect();
        // try_collect drops the stream on the first fatal error, cancelling in-flight requests.
        let mut results: Vec<(usize, ProjectOut)> = stream::iter(work)
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        results.sort_by_key(|(i, _)| *i);

        let mut events = Vec::new();
        let mut builds: MyBuilds = ctx
            .snapshots
            .get(SNAPSHOT_MY_BUILDS)
            .await?
            .unwrap_or_default();
        let mut stages = StageSnapshot::default();
        let mut approvals = ApprovalSnapshot::default();
        for (_, out) in results {
            events.extend(out.events);
            for id in out.mine_builds {
                builds.insert(id, ctx.server_now);
            }
            stages.emitted.extend(out.stages);
            approvals.pending.extend(out.approvals);
        }
        builds.prune_older_than(ctx.server_now - Duration::days(BUILD_RETENTION_DAYS));
        ctx.snapshots.put(SNAPSHOT_MY_BUILDS, &builds).await?;
        ctx.snapshots.put(SNAPSHOT_STAGES, &stages).await?;
        ctx.snapshots.put(SNAPSHOT_APPROVALS, &approvals).await?;
        Ok(PollOutcome { events })
    }

    async fn poll_project(
        &self,
        ctx: &SyncContext,
        project: &Project,
        mine: &Mine<'_>,
        prev_stages: &StageSnapshot,
        prev_approvals: &ApprovalSnapshot,
    ) -> Result<ProjectOut, ProviderError> {
        let pname = project.name.as_str();
        let pref = project_ref(&project.id, pname);
        let client = &self.shared.client;
        let seeding = ctx.watermark.is_none();
        let mut out = ProjectOut {
            events: Vec::new(),
            mine_builds: Vec::new(),
            stages: prev_stages
                .emitted
                .iter()
                .filter(|k| k.project == pname)
                .cloned()
                .collect(),
            approvals: prev_approvals
                .pending
                .iter()
                .filter(|a| a.project == pname)
                .cloned()
                .collect(),
        };

        let completed = soft(
            client.builds_completed_since(pref, window_start(ctx)).await,
            "completed builds",
            pname,
        )?
        .unwrap_or_default();
        for b in completed.iter().filter(|b| mine.is_mine(b)) {
            out.mine_builds.push(b.id);
            if !seeding {
                out.events.push(completed_event(ctx, pname, mine, b));
            }
        }

        if let Some(running) = soft(
            client.builds_in_progress(pref).await,
            "running builds",
            pname,
        )? {
            let mut kept = Vec::new();
            // A build that finished between the two calls is covered by run.completed.
            for b in running
                .iter()
                .filter(|b| mine.is_mine(b) && !completed.iter().any(|c| c.id == b.id))
            {
                kept.extend(out.stages.iter().filter(|k| k.build_id == b.id).cloned());
                let Some(timeline) =
                    soft(client.build_timeline(pref, b.id).await, "timeline", pname)?
                else {
                    continue;
                };
                for rec in timeline.stages().filter(|r| is_failed(r)) {
                    let key = StageKey {
                        project: pname.to_string(),
                        build_id: b.id,
                        record_id: rec.id.clone(),
                    };
                    if kept.contains(&key) {
                        continue;
                    }
                    if !seeding {
                        out.events
                            .push(stage_failed_event(ctx, pname, mine, b, rec));
                    }
                    kept.push(key);
                }
            }
            out.stages = kept;
        }

        if let Some(pending) = soft(
            client.pending_pipeline_approvals(pref).await,
            "approvals",
            pname,
        )? {
            self.diff_approvals(ctx, pname, pref, &pending, &mut out)
                .await?;
        }
        Ok(out)
    }

    /// `out.approvals` holds the previous state for the project on entry.
    async fn diff_approvals(
        &self,
        ctx: &SyncContext,
        pname: &str,
        pref: &str,
        pending: &[PipelineApproval],
        out: &mut ProjectOut,
    ) -> Result<(), ProviderError> {
        let seeding = ctx.watermark.is_none();
        let previous = std::mem::take(&mut out.approvals);
        let mine_now: Vec<&PipelineApproval> = pending
            .iter()
            .filter(|a| approval_is_mine(a, &ctx.me))
            .collect();

        for a in &mine_now {
            let known = previous.iter().find(|p| p.id.eq_ignore_ascii_case(&a.id));
            let entry = known.cloned().unwrap_or_else(|| PendingApproval {
                id: a.id.clone(),
                project: pname.to_string(),
                pipeline: a
                    .pipeline
                    .as_ref()
                    .map(|p| p.name.clone())
                    .unwrap_or_default(),
                run_id: a.run_id(),
                url: a.web_url().map(str::to_string),
                via_group: approval_via_group(a, &ctx.me),
            });
            if known.is_none() && !seeding {
                out.events
                    .push(approval_event(ctx, &entry, "pending", a.created_on));
            }
            out.approvals.push(entry);
        }

        let gone: Vec<PendingApproval> = previous
            .into_iter()
            .filter(|p| !mine_now.iter().any(|a| a.id.eq_ignore_ascii_case(&p.id)))
            .collect();
        if gone.is_empty() || seeding {
            return Ok(());
        }
        let ids: Vec<String> = gone.iter().map(|g| g.id.clone()).collect();
        let finals = soft(
            self.shared
                .client
                .pipeline_approvals_by_id(pref, &ids)
                .await,
            "approval status",
            pname,
        )?;
        let Some(finals) = finals else {
            out.approvals.extend(gone);
            return Ok(());
        };
        for g in gone {
            let Some(done) = finals.iter().find(|f| f.id.eq_ignore_ascii_case(&g.id)) else {
                tracing::warn!("approval {} left pending but was not found by id", g.id);
                continue;
            };
            let status = normalise_status(&done.status);
            if status == "pending" {
                out.approvals.push(g);
            } else {
                out.events
                    .push(approval_event(ctx, &g, &status, done.last_modified_on));
            }
        }
        Ok(())
    }
}

fn is_failed(r: &TimelineRecord) -> bool {
    r.result
        .as_deref()
        .is_some_and(|s| s.eq_ignore_ascii_case("failed"))
}

fn approval_is_mine(a: &PipelineApproval, me: &Identity) -> bool {
    a.steps.iter().any(|s| {
        let id = &s.assigned_approver.id;
        id.eq_ignore_ascii_case(&me.id) || me.group_ids.iter().any(|g| g.eq_ignore_ascii_case(id))
    })
}

/// Mine only through a group: no step is assigned to me directly.
fn approval_via_group(a: &PipelineApproval, me: &Identity) -> bool {
    approval_is_mine(a, me)
        && !a
            .steps
            .iter()
            .any(|s| s.assigned_approver.id.eq_ignore_ascii_case(&me.id))
}

/// ADO reports approval states in assorted casing; map onto the pinned set.
fn normalise_status(raw: &str) -> String {
    match raw.to_ascii_lowercase().as_str() {
        "approved" => "approved".into(),
        "rejected" => "rejected".into(),
        "canceled" | "cancelled" => "canceled".into(),
        "skipped" => "skipped".into(),
        "timedout" => "timedOut".into(),
        "pending" => "pending".into(),
        _ => raw.to_string(),
    }
}

fn system_actor() -> Actor {
    Actor {
        kind: ActorKind::System,
        id: String::new(),
        display_name: "Azure Pipelines".into(),
        unique_name: None,
    }
}

/// What differs between the run events.
struct RunEventSpec<'a> {
    event_type: &'static str,
    title: String,
    id: String,
    occurred: Option<DateTime<Utc>>,
    stage: Option<&'a str>,
}

fn run_event(
    ctx: &SyncContext,
    pname: &str,
    mine: &Mine<'_>,
    b: &Build,
    spec: RunEventSpec<'_>,
) -> Event {
    let mut e = Event::new(
        ctx.connection.clone(),
        spec.event_type,
        spec.occurred.unwrap_or(ctx.server_now),
        ctx.server_now,
        spec.title,
        subject::run(&ctx.connection.id, &b.id.to_string()),
    )
    .with_id(spec.id);
    e.inferred = spec.occurred.is_none();
    e.catch_up = ctx.catch_up;
    e.actor = system_actor();
    e.url = b.web_url().map(str::to_string);
    e.run = Some(RunRef {
        id: b.id.to_string(),
        definition: b.definition.name.clone(),
        source_branch: b.source_branch.clone(),
        reason: b.reason.clone(),
        status: b.status.clone(),
        result: b.result.clone().unwrap_or_default(),
        stage: spec.stage.map(str::to_string),
    });
    if let Some(pr) = mine.my_pr(b) {
        e.pr = Some(PrRef {
            id: pr.pr_id,
            title: pr.title.clone(),
            source_branch: pr.source_ref.clone(),
            target_branch: pr.target_ref.clone(),
            is_draft: pr.is_draft,
            role: pr.role,
        });
        e.repo = Some(repo_ref(&pr.repo_id, &pr.repo_name, pname));
        e.subtitle = Some(pr.title.clone());
    }
    e
}

fn completed_event(ctx: &SyncContext, pname: &str, mine: &Mine<'_>, b: &Build) -> Event {
    let result = b.result.as_deref().unwrap_or("completed");
    let spec = RunEventSpec {
        event_type: event_types::RUN_COMPLETED,
        title: format!("{} #{} {result}", b.definition.name, b.build_number),
        id: ids::run_completed(&ctx.connection.id, &b.id.to_string()),
        occurred: b.finish_time,
        stage: None,
    };
    run_event(ctx, pname, mine, b, spec)
}

fn stage_failed_event(
    ctx: &SyncContext,
    pname: &str,
    mine: &Mine<'_>,
    b: &Build,
    rec: &TimelineRecord,
) -> Event {
    let spec = RunEventSpec {
        event_type: event_types::RUN_STAGE_FAILED,
        title: format!(
            "{} #{} stage {} failed",
            b.definition.name, b.build_number, rec.name
        ),
        id: ids::run_stage_failed(&ctx.connection.id, &b.id.to_string(), &rec.id),
        occurred: rec.finish_time,
        stage: Some(&rec.name),
    };
    run_event(ctx, pname, mine, b, spec)
}

fn approval_event(
    ctx: &SyncContext,
    a: &PendingApproval,
    status: &str,
    occurred: Option<DateTime<Utc>>,
) -> Event {
    let event_type = if status == "pending" {
        event_types::APPROVAL_PENDING
    } else {
        event_types::APPROVAL_COMPLETED
    };
    let mut e = Event::new(
        ctx.connection.clone(),
        event_type,
        occurred.unwrap_or(ctx.server_now),
        ctx.server_now,
        format!("Approval {status}: {}", a.pipeline),
        subject::approval(&ctx.connection.id, KIND, &a.id),
    )
    .with_id(ids::approval(&ctx.connection.id, KIND, &a.id, status));
    e.inferred = occurred.is_none();
    e.catch_up = ctx.catch_up;
    e.actor = system_actor();
    e.url = a.url.clone();
    e.via_group = a.via_group;
    e.approval = Some(ApprovalRef {
        id: a.id.clone(),
        kind: KIND.into(),
        stage: a.pipeline.clone(),
        status: status.to_string(),
    });
    if let Some(run_id) = a.run_id {
        e.run = Some(RunRef {
            id: run_id.to_string(),
            definition: a.pipeline.clone(),
            source_branch: String::new(),
            reason: String::new(),
            status: String::new(),
            result: String::new(),
            stage: None,
        });
    }
    e
}

impl Source for PipelinesSource {
    fn name(&self) -> &'static str {
        "pipelines"
    }

    fn tier(&self) -> Tier {
        Tier::Fast
    }

    fn poll<'a>(
        &'a mut self,
        ctx: &'a SyncContext,
    ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>> {
        Box::pin(self.poll_inner(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AdoAuth;
    use crate::client::AdoClient;
    use crate::dto_git::IdentityRef;
    use crate::pipelines::ApprovalStep;
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ME: &str = "aaaa-me";
    const GROUP: &str = "grp-1";

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
            group_ids: vec![GROUP.into()],
        }
    }

    fn live_ctx() -> SyncContext {
        sync_context(cfg(), me(), at(20, 12), Some(at(20, 11)), false).0
    }

    fn source(server: &MockServer) -> PipelinesSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        PipelinesSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn ident(id: &str) -> Value {
        json!({ "id": id, "displayName": id, "uniqueName": format!("{id}@example.test") })
    }

    fn build(id: i64, requester: &str, branch: &str, done: bool) -> Value {
        json!({
            "id": id, "buildNumber": format!("2026062{id}.1"),
            "status": if done { "completed" } else { "inProgress" },
            "result": if done { json!("failed") } else { Value::Null },
            "finishTime": if done { json!(at(20, 11)) } else { Value::Null },
            "sourceBranch": branch, "reason": "manual",
            "definition": { "id": 1, "name": "Def" },
            "requestedFor": ident(requester), "requestedBy": ident(requester),
            "repository": { "id": "r1", "type": "TfsGit" },
            "_links": { "web": { "href": format!("https://example.test/build/{id}") } },
        })
    }

    fn list(items: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": items }))
    }

    fn approval(id: &str, approver: &str) -> Value {
        json!({
            "id": id, "status": "pending", "createdOn": at(20, 10),
            "steps": [{ "assignedApprover": { "id": approver, "isContainer": true }, "status": "pending" }],
            "pipeline": {
                "id": "9", "name": "Deploy pipeline",
                "owner": { "id": 77, "name": "20260620.1",
                           "_links": { "web": { "href": "https://example.test/run/77" } } },
            },
        })
    }

    async fn serve(
        server: &MockServer,
        completed: Vec<Value>,
        running: Vec<Value>,
        approvals: Vec<Value>,
    ) {
        Mock::given(method("GET"))
            .and(path("/_apis/projects"))
            .respond_with(list(vec![json!({ "id": "p1", "name": "Alpha" })]))
            .mount(server)
            .await;
        for (status, items) in [("completed", completed), ("inProgress", running)] {
            Mock::given(method("GET"))
                .and(path("/p1/_apis/build/builds"))
                .and(query_param("statusFilter", status))
                .respond_with(list(items))
                .mount(server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/p1/_apis/pipelines/approvals"))
            .and(query_param("state", "pending"))
            .respond_with(list(approvals))
            .mount(server)
            .await;
    }

    async fn serve_timeline(server: &MockServer, build_id: i64) {
        Mock::given(method("GET"))
            .and(path(format!("/p1/_apis/build/builds/{build_id}/timeline")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "records": [
                { "id": "st-ok", "type": "Stage", "name": "Build", "result": "succeeded" },
                { "id": "st-bad", "type": "Stage", "name": "Deploy", "result": "failed",
                  "finishTime": at(20, 11) },
                { "id": "job-bad", "type": "Job", "name": "Job", "result": "failed" },
            ] })),
            )
            .mount(server)
            .await;
    }

    async fn serve_by_id(server: &MockServer, id: &str, status: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path("/p1/_apis/pipelines/approvals"))
            .and(query_param("approvalIds", id))
            .respond_with(status)
            .mount(server)
            .await;
    }

    async fn run(
        src: &mut PipelinesSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        src.poll(&c).await.map(|o| o.events)
    }

    async fn my_builds(ctx: &SyncContext) -> MyBuilds {
        ctx.snapshots
            .get(SNAPSHOT_MY_BUILDS)
            .await
            .unwrap()
            .unwrap()
    }

    fn tracked(pr_id: i64, role: PrRole) -> TrackedPr {
        TrackedPr {
            repo_id: "r1".into(),
            repo_name: "repo".into(),
            project: "Alpha".into(),
            pr_id,
            role,
            title: "My PR".into(),
            source_ref: "refs/heads/feat".into(),
            target_ref: "refs/heads/main".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn seed_records_builds_and_emits_nothing() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![build(1, ME, "refs/heads/x", true)],
            vec![build(5, ME, "refs/heads/x", false)],
            vec![approval("ap1", GROUP)],
        )
        .await;
        serve_timeline(&server, 5).await;
        let ctx = live_ctx();
        let events = run(&mut source(&server), &ctx, None).await.unwrap();
        assert!(events.is_empty());
        assert!(my_builds(&ctx).await.contains(1));
        let a: ApprovalSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_APPROVALS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.pending.len(), 1);
        // The seeded stage failure stays quiet on the next poll.
        let again = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert!(
            again
                .iter()
                .all(|e| e.event_type != event_types::RUN_STAGE_FAILED)
        );
    }

    #[tokio::test]
    async fn completed_builds_are_mine_by_person_or_branch_only() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![
                build(1, ME, "refs/heads/other", true),
                build(2, "someone", "refs/heads/other", true),
                build(3, "someone", "refs/heads/mine", true),
            ],
            vec![],
            vec![],
        )
        .await;
        let ctx = live_ctx();
        ctx.snapshots
            .put(
                SNAPSHOT_MY_BRANCHES,
                &MyBranches {
                    branches: vec![crate::sources::MyBranch {
                        repo_id: "r1".into(),
                        project: "Alpha".into(),
                        ref_name: "refs/heads/mine".into(),
                    }],
                },
            )
            .await
            .unwrap();
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        let got: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(got, ["c1:run:1:completed", "c1:run:3:completed"]);
        let e = &events[0];
        assert_eq!(e.event_type, event_types::RUN_COMPLETED);
        assert_eq!(e.actor.kind, ActorKind::System);
        assert_eq!(e.occurred_utc, at(20, 11));
        assert_eq!(e.title, "Def #20260621.1 failed");
        assert_eq!(e.url.as_deref(), Some("https://example.test/build/1"));
        assert_eq!(e.run.as_ref().unwrap().result, "failed");
        assert!(e.pr.is_none());
        let mine = my_builds(&ctx).await;
        assert!(mine.contains(1) && mine.contains(3) && !mine.contains(2));
    }

    #[tokio::test]
    async fn pr_build_of_my_authored_pr_carries_pr_ref() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![
                build(1, "someone", "refs/pull/7/merge", true),
                build(2, "someone", "refs/pull/8/merge", true),
            ],
            vec![],
            vec![],
        )
        .await;
        let ctx = live_ctx();
        ctx.snapshots
            .put(
                SNAPSHOT_TRACKED_PRS,
                &TrackedPrs {
                    prs: vec![tracked(7, PrRole::Author), tracked(8, PrRole::Reviewer)],
                },
            )
            .await
            .unwrap();
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let pr = events[0].pr.as_ref().unwrap();
        assert_eq!((pr.id, pr.role), (7, PrRole::Author));
        assert_eq!(events[0].repo.as_ref().unwrap().name, "repo");
    }

    #[tokio::test]
    async fn failed_stage_in_running_build_emits_once() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![],
            vec![
                build(5, ME, "refs/heads/x", false),
                build(6, "someone", "refs/heads/y", false),
            ],
            vec![],
        )
        .await;
        serve_timeline(&server, 5).await;
        let ctx = live_ctx();
        let first = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].event_type, event_types::RUN_STAGE_FAILED);
        assert_eq!(first[0].id, "c1:run:5:stage:st-bad:failed");
        assert_eq!(
            first[0].run.as_ref().unwrap().stage.as_deref(),
            Some("Deploy")
        );
        let second = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert!(second.is_empty());
    }

    /// Polls once with the approval pending, then once with it gone and `final_status` by id.
    async fn approval_round(final_status: &str) -> (Vec<Event>, Vec<Event>) {
        let ctx = live_ctx();
        let first_server = MockServer::start().await;
        serve(
            &first_server,
            vec![],
            vec![],
            vec![approval("ap1", GROUP), approval("ap2", "other-group")],
        )
        .await;
        let first = run(&mut source(&first_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();

        let second_server = MockServer::start().await;
        serve(&second_server, vec![], vec![], vec![]).await;
        let mut done = approval("ap1", GROUP);
        done["status"] = json!(final_status);
        done["lastModifiedOn"] = json!(at(20, 11));
        serve_by_id(&second_server, "ap1", list(vec![done])).await;
        let second = run(&mut source(&second_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        (first, second)
    }

    #[tokio::test]
    async fn group_approval_goes_pending_then_completed() {
        let (first, second) = approval_round("approved").await;
        assert_eq!(first.len(), 1);
        let p = &first[0];
        assert_eq!(p.event_type, event_types::APPROVAL_PENDING);
        assert_eq!(p.id, "c1:approval:ado-yaml:ap1:pending");
        assert_eq!(p.actor.kind, ActorKind::System);
        assert_eq!(p.approval.as_ref().unwrap().stage, "Deploy pipeline");
        assert_eq!(p.run.as_ref().unwrap().id, "77");
        assert_eq!(p.url.as_deref(), Some("https://example.test/run/77"));
        assert!(p.via_group);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].event_type, event_types::APPROVAL_COMPLETED);
        assert!(second[0].via_group);
        assert_eq!(second[0].approval.as_ref().unwrap().status, "approved");
    }

    #[test]
    fn approval_via_group_table() {
        let step = |id: &str| ApprovalStep {
            assigned_approver: IdentityRef {
                id: id.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let case = |ids: &[&str]| PipelineApproval {
            steps: ids.iter().map(|i| step(i)).collect(),
            ..Default::default()
        };
        let me = me();
        assert!(!approval_via_group(&case(&["AAAA-ME"]), &me));
        assert!(approval_via_group(&case(&[GROUP]), &me));
        assert!(!approval_via_group(&case(&[GROUP, ME]), &me));
        assert!(!approval_via_group(&case(&["other"]), &me));
        assert!(!approval_via_group(&case(&[]), &me));
    }

    #[test]
    fn snapshot_without_via_group_defaults_false() {
        let s: ApprovalSnapshot = serde_json::from_value(json!({
            "pending": [{ "id": "ap1", "project": "Alpha", "pipeline": "P", "runId": 1, "url": null }],
        }))
        .unwrap();
        assert!(!s.pending[0].via_group);
    }

    #[tokio::test]
    async fn final_status_is_normalised() {
        let (_, second) = approval_round("timedout").await;
        assert_eq!(second[0].approval.as_ref().unwrap().status, "timedOut");
        assert_eq!(second[0].id, "c1:approval:ado-yaml:ap1:timedOut");
    }

    #[tokio::test]
    async fn failed_status_lookup_keeps_approval_pending() {
        let ctx = live_ctx();
        let first_server = MockServer::start().await;
        serve(&first_server, vec![], vec![], vec![approval("ap1", GROUP)]).await;
        run(&mut source(&first_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();

        let server = MockServer::start().await;
        serve(&server, vec![], vec![], vec![]).await;
        serve_by_id(&server, "ap1", ResponseTemplate::new(404)).await;
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert!(events.is_empty());
        let a: ApprovalSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_APPROVALS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.pending.len(), 1);
    }

    #[tokio::test]
    async fn my_builds_older_than_30_days_are_pruned() {
        let server = MockServer::start().await;
        serve(&server, vec![], vec![], vec![]).await;
        let ctx = live_ctx();
        let mut seeded = MyBuilds::default();
        seeded.insert(98, ctx.server_now - Duration::days(10));
        seeded.insert(99, ctx.server_now - Duration::days(31));
        ctx.snapshots
            .put(SNAPSHOT_MY_BUILDS, &seeded)
            .await
            .unwrap();
        run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        let mine = my_builds(&ctx).await;
        assert!(mine.contains(98) && !mine.contains(99));
    }

    #[tokio::test]
    async fn transient_error_aborts_the_poll() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_apis/projects"))
            .respond_with(list(vec![json!({ "id": "p1", "name": "Alpha" })]))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/build/builds"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let ctx = live_ctx();
        let err = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::Transient(_)), "{err:?}");
    }
}
