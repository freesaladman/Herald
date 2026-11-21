//! Builds, timelines and YAML pipeline approvals. All endpoints are project-scoped.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::client::{AdoClient, AdoError, Host};
use crate::dto_git::{IdentityRef, ProjectRef};

const APPROVALS_API_VERSION: &str = "7.1-preview.1";
const APPROVAL_ID_CHUNK: usize = 25;

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BuildDefinitionRef {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct BuildRepository {
    pub id: String,
    #[serde(rename = "type")]
    pub repo_type: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Link {
    pub href: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct WebLinks {
    pub web: Option<Link>,
}

impl WebLinks {
    pub fn web_href(&self) -> Option<&str> {
        self.web
            .as_ref()
            .map(|l| l.href.as_str())
            .filter(|h| !h.is_empty())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Build {
    pub id: i64,
    pub build_number: String,
    pub status: String,
    pub result: Option<String>,
    pub queue_time: Option<DateTime<Utc>>,
    pub start_time: Option<DateTime<Utc>>,
    pub finish_time: Option<DateTime<Utc>>,
    pub source_branch: String,
    pub source_version: String,
    pub reason: String,
    pub definition: BuildDefinitionRef,
    pub requested_for: Option<IdentityRef>,
    pub requested_by: Option<IdentityRef>,
    pub project: ProjectRef,
    pub repository: BuildRepository,
    pub trigger_info: Value,
    #[serde(rename = "_links")]
    pub links: WebLinks,
}

impl Build {
    pub fn web_url(&self) -> Option<&str> {
        self.links.web_href()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TimelineRecord {
    pub id: String,
    pub parent_id: Option<String>,
    #[serde(rename = "type")]
    pub record_type: String,
    pub name: String,
    pub state: Option<String>,
    pub result: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub finish_time: Option<DateTime<Utc>>,
    pub order: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Timeline {
    pub records: Vec<TimelineRecord>,
}

impl Timeline {
    pub fn stages(&self) -> impl Iterator<Item = &TimelineRecord> {
        self.records.iter().filter(|r| r.record_type == "Stage")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ApprovalStep {
    pub assigned_approver: IdentityRef,
    pub status: String,
    pub actual_approver: Option<IdentityRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct ApprovalPipeline {
    #[serde(deserialize_with = "string_or_number")]
    pub id: String,
    pub name: String,
    pub owner: Option<ApprovalOwner>,
}

/// The build (run) a YAML approval belongs to.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct ApprovalOwner {
    pub id: i64,
    pub name: String,
    #[serde(rename = "_links")]
    pub links: WebLinks,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PipelineApproval {
    pub id: String,
    pub status: String,
    pub created_on: Option<DateTime<Utc>>,
    pub last_modified_on: Option<DateTime<Utc>>,
    pub steps: Vec<ApprovalStep>,
    pub pipeline: Option<ApprovalPipeline>,
}

impl PipelineApproval {
    pub fn run_id(&self) -> Option<i64> {
        self.pipeline
            .as_ref()?
            .owner
            .as_ref()
            .map(|o| o.id)
            .filter(|id| *id != 0)
    }

    pub fn web_url(&self) -> Option<&str> {
        self.pipeline.as_ref()?.owner.as_ref()?.links.web_href()
    }
}

fn string_or_number<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(match Value::deserialize(d)? {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    })
}

fn enc(s: &str) -> String {
    crate::client::encode_segment(s)
}

fn ts(t: DateTime<Utc>) -> String {
    enc(&t.to_rfc3339_opts(SecondsFormat::Secs, true))
}

impl AdoClient {
    /// Oldest first, so a caller can advance its window to the last `finishTime`.
    pub async fn builds_completed_since(
        &self,
        project: &str,
        min_time: DateTime<Utc>,
    ) -> Result<Vec<Build>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!(
                "{}/_apis/build/builds?statusFilter=completed&queryOrder=finishTimeAscending&minTime={}",
                enc(project),
                ts(min_time)
            ),
        )
        .await
    }

    pub async fn builds_in_progress(&self, project: &str) -> Result<Vec<Build>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!(
                "{}/_apis/build/builds?statusFilter=inProgress",
                enc(project)
            ),
        )
        .await
    }

    pub async fn build_timeline(&self, project: &str, build_id: i64) -> Result<Timeline, AdoError> {
        self.get_json(
            Host::Dev,
            &format!("{}/_apis/build/builds/{build_id}/timeline", enc(project)),
        )
        .await
    }

    pub async fn pending_pipeline_approvals(
        &self,
        project: &str,
    ) -> Result<Vec<PipelineApproval>, AdoError> {
        self.get_paged(
            Host::Dev,
            &format!(
                "{}/_apis/pipelines/approvals?state=pending&$expand=steps&api-version={APPROVALS_API_VERSION}",
                enc(project)
            ),
        )
        .await
    }

    /// Looks up approvals by id (any state), e.g. to learn the final status of one that left pending.
    pub async fn pipeline_approvals_by_id(
        &self,
        project: &str,
        ids: &[String],
    ) -> Result<Vec<PipelineApproval>, AdoError> {
        let mut all = Vec::new();
        for chunk in ids.chunks(APPROVAL_ID_CHUNK) {
            let page: Vec<PipelineApproval> = self
                .get_paged(
                    Host::Dev,
                    &format!(
                        "{}/_apis/pipelines/approvals?approvalIds={}&$expand=steps&api-version={APPROVALS_API_VERSION}",
                        enc(project),
                        chunk.iter().map(|i| enc(i)).collect::<Vec<_>>().join(",")
                    ),
                )
                .await?;
            all.extend(page);
        }
        Ok(all)
    }
}
