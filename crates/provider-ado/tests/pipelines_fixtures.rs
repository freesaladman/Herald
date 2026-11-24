use std::sync::Arc;

use chrono::{TimeZone, Utc};
use herald_core::config::{AuthConfig, ConnectionConfig};
use herald_core::secrets::{MemorySecretStore, SecretStore};
use herald_provider_ado::auth::AdoAuth;
use herald_provider_ado::client::{AdoClient, ListResponse};
use herald_provider_ado::pipelines::{Build, PipelineApproval, Timeline};
use herald_provider_ado::releases::{Deployment, Release, ReleaseApproval};
use serde::de::DeserializeOwned;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn list<T: DeserializeOwned>(name: &str) -> Vec<T> {
    serde_json::from_str::<ListResponse<T>>(&fixture(name))
        .unwrap()
        .value
}

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
    let dev = format!("{}/dev", server.uri());
    let vsrm = format!("{}/vsrm", server.uri());
    AdoClient::with_base_urls("org", auth, &dev, &vsrm, &server.uri())
}

#[test]
fn builds_deserialize() {
    let builds: Vec<Build> = list("builds_completed");
    assert_eq!(builds.len(), 3);
    let b = &builds[0];
    assert_eq!(b.id, 48567);
    assert_eq!(b.status, "completed");
    assert_eq!(b.result.as_deref(), Some("failed"));
    assert_eq!(b.definition.id, 101);
    assert_eq!(b.project.name, "Project One");
    assert!(b.finish_time.unwrap() > b.start_time.unwrap());
    assert!(b.source_branch.starts_with("refs/heads/"));
    assert!(!b.requested_for.as_ref().unwrap().id.is_empty());
    assert!(!b.repository.repo_type.is_empty());
    assert!(b.trigger_info.is_object());
    assert!(b.web_url().unwrap().contains("buildId=48567"));
}

#[test]
fn timeline_exposes_stage_record() {
    let t: Timeline = serde_json::from_str(&fixture("build_timeline")).unwrap();
    let stage = t.stages().next().unwrap();
    assert_eq!(stage.result.as_deref(), Some("failed"));
    assert!(stage.parent_id.is_none());
    assert!(
        t.records
            .iter()
            .any(|r| r.parent_id.as_deref() == Some(stage.id.as_str()))
    );
}

#[test]
fn pipeline_approval_deserializes() {
    let a: Vec<PipelineApproval> = list("pipeline_approvals");
    assert_eq!(a[0].status, "pending");
    let pipeline = a[0].pipeline.as_ref().unwrap();
    assert_eq!(pipeline.id, "89");
    assert_eq!(a[0].run_id(), Some(47181));
    assert!(a[0].web_url().unwrap().ends_with("buildId=47181"));
    assert!(a[0].steps[0].assigned_approver.is_container);
    assert!(!a[1].steps[0].assigned_approver.is_container);
    assert!(a[0].created_on.is_some());
}

#[test]
fn releases_expose_artifact_build_ids() {
    let releases: Vec<Release> = list("releases");
    let art = &releases[0].artifacts[0];
    assert_eq!(art.build_id(), Some(51125));
    assert_eq!(art.version_name(), Some("2026.11.20.43"));
    assert_eq!(art.definition_id(), Some(43));
    assert_eq!(art.definition_name(), Some("repo-alpha"));
    assert!(releases[0].created_on.is_some());
    assert!(releases[0].created_by.is_some());
    assert!(releases[0].web_url().is_some());
}

#[test]
fn single_release_has_environments_with_deploy_steps() {
    let r: Release = serde_json::from_str(&fixture("release_single")).unwrap();
    assert_eq!(r.environments[0].name, "ci");
    assert_eq!(r.environments[0].deploy_steps[0].deployment_id, 23289);
    assert_eq!(
        r.environments[1].deploy_steps[0].operation_status,
        "Pending"
    );
}

#[test]
fn deployments_map_unset_dates_to_none() {
    let d: Vec<Deployment> = list("deployments");
    assert_eq!(d[0].release.id, 8375);
    assert_eq!(d[0].release_environment.name, "prod");
    assert_eq!(d[0].deployment_status, "notDeployed");
    assert_eq!(d[0].operation_status, "Canceled");
    assert!(d[0].started_on.is_none());
    assert!(d[0].completed_on.is_none());
    assert!(d[0].last_modified_on.is_some());
}

#[test]
fn release_approvals_deserialize() {
    let a: Vec<ReleaseApproval> = list("release_approvals");
    assert_eq!(a[0].status, "pending");
    assert_eq!(a[0].approval_type, "preDeploy");
    assert!(a[0].approver.as_ref().unwrap().is_container);
    assert_eq!(a[0].release.id, 8679);
    assert_eq!(a[0].release_environment.name, "qa");
}

#[tokio::test]
async fn completed_builds_query_is_project_scoped_with_min_time_and_order() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dev/Project%20One/_apis/build/builds"))
        .and(query_param("statusFilter", "completed"))
        .and(query_param("queryOrder", "finishTimeAscending"))
        .and(query_param("minTime", "2026-11-18T01:02:03Z"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("builds_completed")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dev/Project%20One/_apis/pipelines/approvals"))
        .and(query_param("state", "pending"))
        .and(query_param("$expand", "steps"))
        .and(query_param("api-version", "7.1-preview.1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("pipeline_approvals")))
        .expect(1)
        .mount(&server)
        .await;
    let c = client(&server);
    let since = Utc.with_ymd_and_hms(2026, 11, 18, 1, 2, 3).unwrap();
    let builds = c
        .builds_completed_since("Project One", since)
        .await
        .unwrap();
    assert_eq!(builds.len(), 3);
    let approvals = c.pending_pipeline_approvals("Project One").await.unwrap();
    assert_eq!(approvals.len(), 2);
}

#[tokio::test]
async fn release_queries_use_the_vsrm_host_with_expected_params() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vsrm/P/_apis/release/releases"))
        .and(query_param("minCreatedTime", "2026-11-18T01:02:03Z"))
        .and(query_param("$expand", "artifacts"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("releases")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/vsrm/P/_apis/release/deployments"))
        .and(query_param("minModifiedTime", "2026-11-18T01:02:03Z"))
        .and(query_param("queryOrder", "ascending"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("deployments")))
        .expect(1)
        .mount(&server)
        .await;
    let c = client(&server);
    let t = Utc.with_ymd_and_hms(2026, 11, 18, 1, 2, 3).unwrap();
    assert_eq!(c.releases_since("P", t).await.unwrap().len(), 2);
    assert_eq!(c.deployments_since("P", t).await.unwrap().len(), 3);
}

#[tokio::test]
async fn classic_approvals_are_two_step_and_chunk_release_ids() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vsrm/P/_apis/release/approvals"))
        .and(query_param("statusFilter", "pending"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "value": [
            { "id": 1, "release": { "id": 30 } },
            { "id": 2, "release": { "id": 10 } },
            { "id": 3, "release": { "id": 30 } }
        ]})))
        .expect(1)
        .mount(&server)
        .await;
    let c = client(&server);
    assert_eq!(c.pending_release_ids("P").await.unwrap(), vec![10, 30]);

    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/vsrm/P/_apis/release/approvals"))
        .and(query_param("assignedToFilter", "me-id"))
        .and(query_param("statusFilter", "Pending"))
        .and(query_param("includeMyGroupApprovals", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("release_approvals")))
        .expect(2)
        .mount(&server)
        .await;
    let ids: Vec<i64> = (1..=51).collect();
    let mine = c.my_pending_approvals("P", &ids, "me-id").await.unwrap();
    assert_eq!(mine.len(), 6);
    let requests = server.received_requests().await.unwrap();
    let filters: Vec<String> = requests
        .iter()
        .map(|r| {
            r.url
                .query_pairs()
                .find(|(k, _)| k == "releaseIdsFilter")
                .unwrap()
                .1
                .into_owned()
        })
        .collect();
    assert_eq!(filters[0].split(',').count(), 50);
    assert_eq!(filters[1], "51");
}

#[tokio::test]
async fn pipeline_approvals_by_id_use_the_comma_joined_filter() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/dev/Project%20One/_apis/pipelines/approvals"))
        .and(query_param("approvalIds", "a,b"))
        .and(query_param("$expand", "steps"))
        .and(query_param("api-version", "7.1-preview.1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("pipeline_approvals")))
        .expect(1)
        .mount(&server)
        .await;
    let ids = vec!["a".to_string(), "b".to_string()];
    let found = client(&server)
        .pipeline_approvals_by_id("Project One", &ids)
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[tokio::test]
async fn single_release_approval_is_fetched_by_id_from_vsrm() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/vsrm/P/_apis/release/approvals/77"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "id": 77, "status": "approved", "release": { "id": 5 } })),
        )
        .expect(1)
        .mount(&server)
        .await;
    let a = client(&server).release_approval("P", 77).await.unwrap();
    assert_eq!((a.id, a.status.as_str(), a.release.id), (77, "approved", 5));
}
