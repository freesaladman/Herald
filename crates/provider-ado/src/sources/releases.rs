//! Fast-tier source: classic releases that are mine, their deployments, and classic approvals.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, TryStreamExt, stream};
use herald_core::event::{
    Actor, ActorKind, ApprovalRef, DeploymentRef, Event, event_types, ids, subject,
};
use herald_core::provider::{Identity, PollOutcome, ProviderError, Source, SyncContext, Tier};
use serde::{Deserialize, Serialize};

use super::{MyBuilds, SNAPSHOT_MY_BUILDS, enc, is_fatal, project_ref, window_start};
use crate::client::{AdoError, Project};
use crate::connection::AdoShared;
use crate::releases::{Deployment, Release, ReleaseApproval};

const CONCURRENCY: usize = 4;
const RETENTION_DAYS: i64 = 30;
const KIND: &str = "ado-classic";
const SNAPSHOT_MINE: &str = "releases:mine";
const SNAPSHOT_APPROVALS: &str = "releases:approvals";

pub struct ReleasesSource {
    shared: Arc<AdoShared>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct MineReleases {
    releases: Vec<MineRelease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MineRelease {
    id: i64,
    project: String,
    seen_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ApprovalSnapshot {
    pending: Vec<PendingApproval>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingApproval {
    id: i64,
    project: String,
    release_id: i64,
    release_name: String,
    definition: String,
    environment: String,
    url: String,
}

/// Per-project result; sections that failed non-fatally keep the previous state.
struct ProjectOut {
    events: Vec<Event>,
    mine: Vec<MineRelease>,
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

impl ReleasesSource {
    pub fn new(shared: Arc<AdoShared>) -> Self {
        Self { shared }
    }

    async fn poll_inner(&self, ctx: &SyncContext) -> Result<PollOutcome, ProviderError> {
        let prev_mine: MineReleases = ctx.snapshots.get(SNAPSHOT_MINE).await?.unwrap_or_default();
        let prev_approvals: ApprovalSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_APPROVALS)
            .await?
            .unwrap_or_default();
        let builds: MyBuilds = ctx
            .snapshots
            .get(SNAPSHOT_MY_BUILDS)
            .await?
            .unwrap_or_default();

        let projects = self.shared.projects().await?;
        let work: Vec<BoxFuture<'_, Result<(usize, ProjectOut), ProviderError>>> = projects
            .iter()
            .enumerate()
            .map(|(i, p)| -> BoxFuture<'_, _> {
                let (builds, mine, approvals) = (&builds, &prev_mine, &prev_approvals);
                Box::pin(async move {
                    Ok((i, self.poll_project(ctx, p, builds, mine, approvals).await?))
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
        let mut mine = MineReleases::default();
        let mut approvals = ApprovalSnapshot::default();
        for (_, out) in results {
            events.extend(out.events);
            mine.releases.extend(out.mine);
            approvals.pending.extend(out.approvals);
        }
        let cutoff = ctx.server_now - Duration::days(RETENTION_DAYS);
        mine.releases.retain(|r| r.seen_utc >= cutoff);
        ctx.snapshots.put(SNAPSHOT_MINE, &mine).await?;
        ctx.snapshots.put(SNAPSHOT_APPROVALS, &approvals).await?;
        Ok(PollOutcome { events })
    }

    async fn poll_project(
        &self,
        ctx: &SyncContext,
        project: &Project,
        builds: &MyBuilds,
        prev_mine: &MineReleases,
        prev_approvals: &ApprovalSnapshot,
    ) -> Result<ProjectOut, ProviderError> {
        let pname = project.name.as_str();
        let pref = project_ref(&project.id, pname);
        let client = &self.shared.client;
        let seeding = ctx.watermark.is_none();
        let mut out = ProjectOut {
            events: Vec::new(),
            mine: prev_mine
                .releases
                .iter()
                .filter(|r| r.project == pname)
                .cloned()
                .collect(),
            approvals: prev_approvals
                .pending
                .iter()
                .filter(|a| a.project == pname)
                .cloned()
                .collect(),
        };

        if let Some(releases) = soft(
            client.releases_since(pref, window_start(ctx)).await,
            "releases",
            pname,
        )? {
            for r in releases.iter().filter(|r| is_mine(r, &ctx.me, builds)) {
                if out.mine.iter().any(|m| m.id == r.id) {
                    continue;
                }
                out.mine.push(MineRelease {
                    id: r.id,
                    project: pname.to_string(),
                    seen_utc: ctx.server_now,
                });
                if !seeding {
                    out.events.push(self.created_event(ctx, pname, r));
                }
            }
        }

        if !seeding
            && !out.mine.is_empty()
            && let Some(deployments) = soft(
                client.deployments_since(pref, window_start(ctx)).await,
                "deployments",
                pname,
            )?
        {
            for d in deployments
                .iter()
                .filter(|d| out.mine.iter().any(|m| m.id == d.release.id))
            {
                if let Some(e) = self.deploy_event(ctx, pname, d) {
                    out.events.push(e);
                }
            }
        }

        self.diff_approvals(ctx, pname, pref, &mut out).await?;
        Ok(out)
    }

    /// `out.approvals` holds the previous state for the project on entry.
    async fn diff_approvals(
        &self,
        ctx: &SyncContext,
        pname: &str,
        pref: &str,
        out: &mut ProjectOut,
    ) -> Result<(), ProviderError> {
        let client = &self.shared.client;
        let seeding = ctx.watermark.is_none();
        let Some(release_ids) = soft(client.pending_release_ids(pref).await, "approvals", pname)?
        else {
            return Ok(());
        };
        let now = if release_ids.is_empty() {
            Vec::new()
        } else {
            let Some(found) = soft(
                client
                    .my_pending_approvals(pref, &release_ids, &ctx.me.id)
                    .await,
                "approvals",
                pname,
            )?
            else {
                return Ok(());
            };
            found
        };

        let previous = std::mem::take(&mut out.approvals);
        for a in &now {
            let known = previous.iter().find(|p| p.id == a.id);
            let entry = known
                .cloned()
                .unwrap_or_else(|| self.pending_entry(pname, a));
            if known.is_none() && !seeding {
                out.events
                    .push(approval_event(ctx, &entry, "pending", a.created_on));
            }
            out.approvals.push(entry);
        }

        let gone = previous
            .into_iter()
            .filter(|p| !now.iter().any(|a| a.id == p.id));
        for g in gone {
            if seeding {
                continue;
            }
            let Some(done) = soft(
                client.release_approval(pref, g.id).await,
                "approval status",
                pname,
            )?
            else {
                out.approvals.push(g);
                continue;
            };
            let status = normalise_status(&done.status);
            if status == "pending" {
                out.approvals.push(g);
            } else {
                out.events
                    .push(approval_event(ctx, &g, &status, done.modified_on));
            }
        }
        Ok(())
    }

    fn release_url(&self, project: &str, release_id: i64) -> String {
        format!(
            "https://dev.azure.com/{}/{}/_releaseProgress?releaseId={release_id}&_a=release-summary",
            enc(&self.shared.organization),
            enc(project)
        )
    }

    fn pending_entry(&self, project: &str, a: &ReleaseApproval) -> PendingApproval {
        PendingApproval {
            id: a.id,
            project: project.to_string(),
            release_id: a.release.id,
            release_name: a.release.name.clone(),
            definition: a.release_definition.name.clone(),
            environment: a.release_environment.name.clone(),
            url: self.release_url(project, a.release.id),
        }
    }

    fn created_event(&self, ctx: &SyncContext, pname: &str, r: &Release) -> Event {
        let id = r.id.to_string();
        let def = &r.release_definition.name;
        let mut e = base_event(
            ctx,
            event_types::RELEASE_CREATED,
            r.created_on,
            format!("Release {} created ({def})", r.name),
            subject::release(&ctx.connection.id, &id),
            ids::release_created(&ctx.connection.id, &id),
        );
        e.url = r
            .web_url()
            .map(str::to_string)
            .or_else(|| Some(self.release_url(pname, r.id)));
        e.deployment = Some(DeploymentRef {
            release_id: id,
            release_name: r.name.clone(),
            definition: def.clone(),
            environment: None,
            deployment_id: None,
            status: None,
        });
        e
    }

    fn deploy_event(&self, ctx: &SyncContext, pname: &str, d: &Deployment) -> Option<Event> {
        let (event_type, status) = deploy_kind(d)?;
        let release_id = d.release.id.to_string();
        let env = &d.release_environment;
        let occurred = d.completed_on.or(d.started_on).or(d.last_modified_on);
        let mut e = base_event(
            ctx,
            event_type,
            occurred,
            format!("{} -> {}: {status}", d.release.name, env.name),
            subject::release(&ctx.connection.id, &release_id),
            ids::deploy(
                &ctx.connection.id,
                &release_id,
                &env.id.to_string(),
                &d.id.to_string(),
                status,
            ),
        );
        e.url = Some(self.release_url(pname, d.release.id));
        e.deployment = Some(DeploymentRef {
            release_id,
            release_name: d.release.name.clone(),
            definition: d.release_definition.name.clone(),
            environment: Some(env.name.clone()),
            deployment_id: Some(d.id.to_string()),
            status: Some(status.to_string()),
        });
        Some(e)
    }
}

fn is_mine(r: &Release, me: &Identity, builds: &MyBuilds) -> bool {
    r.created_by
        .as_ref()
        .is_some_and(|c| c.id.eq_ignore_ascii_case(&me.id))
        || r.artifacts
            .iter()
            .filter_map(|a| a.build_id())
            .any(|id| builds.contains(id))
}

/// Event type and `deployment.status` for a deployment; queued or undefined states are ignored.
fn deploy_kind(d: &Deployment) -> Option<(&'static str, &'static str)> {
    let status = d.deployment_status.to_ascii_lowercase();
    let started = d.started_on.is_some();
    match status.as_str() {
        "inprogress" => Some((event_types::DEPLOY_STARTED, "inProgress")),
        "succeeded" => Some((event_types::DEPLOY_SUCCEEDED, "succeeded")),
        "failed" => Some((event_types::DEPLOY_FAILED, "failed")),
        "partiallysucceeded" => Some((event_types::DEPLOY_FAILED, "partiallySucceeded")),
        "canceled" if started => Some((event_types::DEPLOY_CANCELED, "canceled")),
        "notdeployed" if started && d.operation_status.eq_ignore_ascii_case("canceled") => {
            Some((event_types::DEPLOY_CANCELED, "canceled"))
        }
        _ => None,
    }
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
        display_name: "Azure Releases".into(),
        unique_name: None,
    }
}

fn base_event(
    ctx: &SyncContext,
    event_type: &'static str,
    occurred: Option<DateTime<Utc>>,
    title: String,
    subject_key: String,
    id: String,
) -> Event {
    let mut e = Event::new(
        ctx.connection.clone(),
        event_type,
        occurred.unwrap_or(ctx.server_now),
        ctx.server_now,
        title,
        subject_key,
    )
    .with_id(id);
    e.inferred = occurred.is_none();
    e.catch_up = ctx.catch_up;
    e.actor = system_actor();
    e
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
    let approval_id = a.id.to_string();
    let mut e = base_event(
        ctx,
        event_type,
        occurred,
        format!("Approval {status}: {} -> {}", a.release_name, a.environment),
        subject::approval(&ctx.connection.id, KIND, &approval_id),
        ids::approval(&ctx.connection.id, KIND, &approval_id, status),
    );
    e.url = Some(a.url.clone());
    e.approval = Some(ApprovalRef {
        id: approval_id,
        kind: KIND.into(),
        stage: a.environment.clone(),
        status: status.to_string(),
    });
    e.deployment = Some(DeploymentRef {
        release_id: a.release_id.to_string(),
        release_name: a.release_name.clone(),
        definition: a.definition.clone(),
        environment: Some(a.environment.clone()),
        deployment_id: None,
        status: None,
    });
    e
}

impl Source for ReleasesSource {
    fn name(&self) -> &'static str {
        "releases"
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
    use chrono::TimeZone;
    use herald_core::config::{AuthConfig, ConnectionConfig};
    use herald_core::secrets::{MemorySecretStore, SecretStore};
    use herald_core::testing::sync_context;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ME: &str = "aaaa-me";
    const GROUP: &str = "grp-1";

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

    fn live_ctx() -> SyncContext {
        sync_context(cfg(), me(), at(20, 12), Some(at(20, 11)), false).0
    }

    fn source(server: &MockServer) -> ReleasesSource {
        let secrets = MemorySecretStore::new();
        secrets.set("c1", "pat").unwrap();
        let auth = AdoAuth::from_config(&cfg(), Arc::new(secrets)).unwrap();
        let u = server.uri();
        let client = AdoClient::with_base_urls("org", auth, &u, &u, &u);
        ReleasesSource::new(AdoShared::new(Arc::new(client), "org".into(), Vec::new()))
    }

    fn release(id: i64, creator: &str, build_id: &str) -> Value {
        json!({
            "id": id, "name": format!("R-{id}"), "status": "active",
            "createdOn": at(20, 11),
            "createdBy": { "id": creator, "displayName": creator, "uniqueName": creator },
            "releaseDefinition": { "id": 11, "name": "Def" },
            "artifacts": [{
                "type": "Build", "alias": "a",
                "definitionReference": { "version": { "id": build_id, "name": "1" } },
            }],
            "_links": { "web": { "href": format!("https://example.test/rel/{id}") } },
        })
    }

    fn deployment(id: i64, release_id: i64, status: &str, started: Value, done: Value) -> Value {
        json!({
            "id": id, "release": { "id": release_id, "name": format!("R-{release_id}") },
            "releaseDefinition": { "id": 11, "name": "Def" },
            "releaseEnvironment": { "id": 5, "name": "prod" },
            "deploymentStatus": status, "operationStatus": "Approved",
            "startedOn": started, "completedOn": done,
            "lastModifiedOn": at(20, 11),
        })
    }

    fn approval(id: i64, release_id: i64, status: &str) -> Value {
        json!({
            "id": id, "status": status, "approvalType": "preDeploy",
            "createdOn": at(20, 10), "modifiedOn": at(20, 11),
            "approver": { "id": GROUP, "isContainer": true },
            "release": { "id": release_id, "name": format!("R-{release_id}") },
            "releaseDefinition": { "id": 11, "name": "Def" },
            "releaseEnvironment": { "id": 5, "name": "qa" },
        })
    }

    fn list(items: Vec<Value>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({ "value": items }))
    }

    async fn serve(
        server: &MockServer,
        releases: Vec<Value>,
        deployments: Vec<Value>,
        pending: Vec<Value>,
    ) {
        let get = |p: &str| Mock::given(method("GET")).and(path(p.to_string()));
        get("/_apis/projects")
            .respond_with(list(vec![json!({ "id": "p1", "name": "Alpha" })]))
            .mount(server)
            .await;
        get("/p1/_apis/release/releases")
            .respond_with(list(releases))
            .mount(server)
            .await;
        get("/p1/_apis/release/deployments")
            .respond_with(list(deployments))
            .mount(server)
            .await;
        // Step 1 (all pending) and step 2 (mine) share one path; the same list answers both.
        get("/p1/_apis/release/approvals")
            .respond_with(list(pending))
            .mount(server)
            .await;
    }

    async fn run(
        src: &mut ReleasesSource,
        ctx: &SyncContext,
        watermark: Option<DateTime<Utc>>,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut c = ctx.clone();
        c.watermark = watermark;
        src.poll(&c).await.map(|o| o.events)
    }

    async fn with_my_build(ctx: &SyncContext, id: i64) {
        let mut b = MyBuilds::default();
        b.insert(id, ctx.server_now);
        ctx.snapshots.put(SNAPSHOT_MY_BUILDS, &b).await.unwrap();
    }

    async fn mine(ctx: &SyncContext) -> MineReleases {
        ctx.snapshots.get(SNAPSHOT_MINE).await.unwrap().unwrap()
    }

    async fn track_release_1(ctx: &SyncContext) {
        let tracked = MineReleases {
            releases: vec![MineRelease {
                id: 1,
                project: "Alpha".into(),
                seen_utc: at(19, 0),
            }],
        };
        ctx.snapshots.put(SNAPSHOT_MINE, &tracked).await.unwrap();
    }

    #[tokio::test]
    async fn seed_records_state_and_emits_nothing() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![release(1, ME, "9")],
            vec![deployment(
                10,
                1,
                "succeeded",
                json!(at(20, 10)),
                json!(at(20, 11)),
            )],
            vec![approval(50, 1, "pending")],
        )
        .await;
        let ctx = live_ctx();
        let events = run(&mut source(&server), &ctx, None).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(mine(&ctx).await.releases.len(), 1);
        let a: ApprovalSnapshot = ctx
            .snapshots
            .get(SNAPSHOT_APPROVALS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.pending.len(), 1);
    }

    #[tokio::test]
    async fn only_my_releases_emit_created() {
        let server = MockServer::start().await;
        serve(
            &server,
            vec![
                release(1, "someone", "77"),
                release(2, ME, "88"),
                release(3, "someone", "99"),
            ],
            vec![],
            vec![],
        )
        .await;
        let ctx = live_ctx();
        with_my_build(&ctx, 77).await;
        let mut src = source(&server);
        let events = run(&mut src, &ctx, Some(at(20, 11))).await.unwrap();
        let got: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(got, ["c1:release:1:created", "c1:release:2:created"]);
        let e = &events[0];
        assert_eq!(e.event_type, event_types::RELEASE_CREATED);
        assert_eq!(e.actor.kind, ActorKind::System);
        assert_eq!(e.occurred_utc, at(20, 11));
        assert_eq!(e.url.as_deref(), Some("https://example.test/rel/1"));
        let d = e.deployment.as_ref().unwrap();
        assert_eq!(
            (d.release_name.as_str(), d.definition.as_str()),
            ("R-1", "Def")
        );
        assert!(d.environment.is_none());
        // Already tracked releases do not re-emit.
        let again = run(&mut src, &ctx, Some(at(20, 11))).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn deployments_of_my_release_map_statuses() {
        let server = MockServer::start().await;
        let unset = json!("0001-01-01T00:00:00");
        serve(
            &server,
            vec![],
            vec![
                deployment(10, 1, "inProgress", json!(at(20, 10)), unset.clone()),
                deployment(11, 1, "succeeded", json!(at(20, 10)), json!(at(20, 11))),
                deployment(
                    12,
                    1,
                    "partiallySucceeded",
                    json!(at(20, 10)),
                    json!(at(20, 11)),
                ),
                deployment(13, 1, "notDeployed", unset.clone(), unset.clone()),
                deployment(14, 2, "failed", json!(at(20, 10)), json!(at(20, 11))),
            ],
            vec![],
        )
        .await;
        let ctx = live_ctx();
        track_release_1(&ctx).await;
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        let got: Vec<(&str, &str)> = events
            .iter()
            .map(|e| (e.event_type.as_str(), e.id.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                ("deploy.started", "c1:release:1:env:5:deploy:10:inProgress"),
                ("deploy.succeeded", "c1:release:1:env:5:deploy:11:succeeded"),
                (
                    "deploy.failed",
                    "c1:release:1:env:5:deploy:12:partiallySucceeded"
                ),
            ]
        );
        let d = events[0].deployment.as_ref().unwrap();
        assert_eq!(d.environment.as_deref(), Some("prod"));
        assert_eq!(d.status.as_deref(), Some("inProgress"));
        assert_eq!(events[0].title, "R-1 -> prod: inProgress");
        assert_eq!(events[0].occurred_utc, at(20, 10));
        assert_eq!(events[1].occurred_utc, at(20, 11));
    }

    #[tokio::test]
    async fn sentinel_dates_fall_back_to_modified_time() {
        let server = MockServer::start().await;
        let unset = json!("0001-01-01T00:00:00");
        let mut canceled = deployment(10, 1, "notDeployed", json!(at(20, 10)), unset.clone());
        canceled["operationStatus"] = json!("Canceled");
        let mut bare = deployment(11, 1, "succeeded", unset.clone(), unset.clone());
        bare["lastModifiedOn"] = Value::Null;
        serve(&server, vec![], vec![canceled, bare], vec![]).await;
        let ctx = live_ctx();
        track_release_1(&ctx).await;
        let events = run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(events[0].event_type, event_types::DEPLOY_CANCELED);
        assert_eq!(events[0].occurred_utc, at(20, 10));
        assert!(!events[0].inferred);
        assert!(events[1].inferred);
        assert_eq!(events[1].occurred_utc, ctx.server_now);
    }

    #[tokio::test]
    async fn approval_goes_pending_then_completed() {
        let ctx = live_ctx();
        let first_server = MockServer::start().await;
        serve(
            &first_server,
            vec![],
            vec![],
            vec![approval(50, 1, "pending")],
        )
        .await;
        let first = run(&mut source(&first_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        let p = &first[0];
        assert_eq!(p.event_type, event_types::APPROVAL_PENDING);
        assert_eq!(p.id, "c1:approval:ado-classic:50:pending");
        assert_eq!(p.actor.kind, ActorKind::System);
        let a = p.approval.as_ref().unwrap();
        assert_eq!((a.kind.as_str(), a.stage.as_str()), ("ado-classic", "qa"));
        assert_eq!(p.deployment.as_ref().unwrap().release_id, "1");
        assert!(p.url.as_deref().unwrap().contains("releaseId=1"));

        let second_server = MockServer::start().await;
        serve(&second_server, vec![], vec![], vec![]).await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/release/approvals/50"))
            .respond_with(ResponseTemplate::new(200).set_body_json(approval(50, 1, "approved")))
            .mount(&second_server)
            .await;
        let second = run(&mut source(&second_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].event_type, event_types::APPROVAL_COMPLETED);
        assert_eq!(second[0].approval.as_ref().unwrap().status, "approved");
        assert_eq!(second[0].id, "c1:approval:ado-classic:50:approved");
    }

    #[tokio::test]
    async fn failed_status_lookup_keeps_approval_pending() {
        let ctx = live_ctx();
        let first_server = MockServer::start().await;
        serve(
            &first_server,
            vec![],
            vec![],
            vec![approval(50, 1, "pending")],
        )
        .await;
        run(&mut source(&first_server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();

        let server = MockServer::start().await;
        serve(&server, vec![], vec![], vec![]).await;
        Mock::given(method("GET"))
            .and(path("/p1/_apis/release/approvals/50"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
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
    async fn mine_older_than_30_days_is_pruned() {
        let server = MockServer::start().await;
        serve(&server, vec![], vec![], vec![]).await;
        let ctx = live_ctx();
        let old = |id, days| MineRelease {
            id,
            project: "Alpha".into(),
            seen_utc: ctx.server_now - Duration::days(days),
        };
        let seeded = MineReleases {
            releases: vec![old(1, 10), old(2, 31)],
        };
        ctx.snapshots.put(SNAPSHOT_MINE, &seeded).await.unwrap();
        run(&mut source(&server), &ctx, Some(at(20, 11)))
            .await
            .unwrap();
        let ids: Vec<i64> = mine(&ctx).await.releases.iter().map(|r| r.id).collect();
        assert_eq!(ids, [1]);
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
            .and(path("/p1/_apis/release/releases"))
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
