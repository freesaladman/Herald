//! Classic release endpoints on the vsrm host. All are project-scoped.

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::client::{AdoClient, AdoError, Host};
use crate::dto_git::IdentityRef;
use crate::pipelines::WebLinks;

const APPROVAL_ID_CHUNK: usize = 50;

/// ADO sends unset dates as `0001-01-01T00:00:00` (no offset): read offset-less values as UTC
/// and map the unset sentinel to `None`.
fn lenient_date<'de, D: Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
    let Some(s) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };
    let parsed = DateTime::parse_from_rfc3339(&s)
        .map(|t| t.with_timezone(&Utc))
        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f").map(|n| n.and_utc()))
        .map_err(serde::de::Error::custom)?;
    Ok((parsed.timestamp() > 0).then_some(parsed))
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct NamedRef {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReleaseArtifact {
    #[serde(rename = "type")]
    pub artifact_type: String,
    pub alias: String,
    pub definition_reference: Value,
}

impl ReleaseArtifact {
    fn reference(&self, key: &str, field: &str) -> Option<&str> {
        self.definition_reference
            .get(key)?
            .get(field)?
            .as_str()
            .filter(|s| !s.is_empty())
    }

    /// Build id of a `Build` artifact (the version id), else `None`.
    pub fn build_id(&self) -> Option<i64> {
        if self.artifact_type != "Build" {
            return None;
        }
        self.reference("version", "id")?.parse().ok()
    }

    pub fn version_name(&self) -> Option<&str> {
        self.reference("version", "name")
    }

    pub fn definition_id(&self) -> Option<i64> {
        self.reference("definition", "id")?.parse().ok()
    }

    pub fn definition_name(&self) -> Option<&str> {
        self.reference("definition", "name")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DeployStep {
    pub id: i64,
    pub deployment_id: i64,
    pub attempt: i64,
    pub status: String,
    pub operation_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReleaseEnvironment {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub deploy_steps: Vec<DeployStep>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Release {
    pub id: i64,
    pub name: String,
    pub status: String,
    #[serde(deserialize_with = "lenient_date")]
    pub created_on: Option<DateTime<Utc>>,
    pub created_by: Option<IdentityRef>,
    pub release_definition: NamedRef,
    pub artifacts: Vec<ReleaseArtifact>,
    pub environments: Vec<ReleaseEnvironment>,
    #[serde(rename = "_links")]
    pub links: WebLinks,
}

impl Release {
    pub fn web_url(&self) -> Option<&str> {
        self.links.web_href()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Deployment {
    pub id: i64,
    pub release: NamedRef,
    pub release_definition: NamedRef,
    pub release_environment: NamedRef,
    pub deployment_status: String,
    pub operation_status: String,
    #[serde(deserialize_with = "lenient_date")]
    pub started_on: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "lenient_date")]
    pub completed_on: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "lenient_date")]
    pub last_modified_on: Option<DateTime<Utc>>,
    pub requested_by: Option<IdentityRef>,
    pub requested_for: Option<IdentityRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReleaseApproval {
    pub id: i64,
    pub status: String,
    pub approval_type: String,
    #[serde(deserialize_with = "lenient_date")]
    pub created_on: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "lenient_date")]
    pub modified_on: Option<DateTime<Utc>>,
    pub approver: Option<IdentityRef>,
    pub release: NamedRef,
    pub release_definition: NamedRef,
    pub release_environment: NamedRef,
}

fn enc(s: &str) -> String {
    crate::client::encode_segment(s)
}

fn ts(t: DateTime<Utc>) -> String {
    enc(&t.to_rfc3339_opts(SecondsFormat::Secs, true))
}

impl AdoClient {
    pub async fn releases_since(
        &self,
        project: &str,
        min_created: DateTime<Utc>,
    ) -> Result<Vec<Release>, AdoError> {
        self.get_paged(
            Host::Vsrm,
            &format!(
                "{}/_apis/release/releases?minCreatedTime={}&$expand=artifacts",
                enc(project),
                ts(min_created)
            ),
        )
        .await
    }

    /// Includes environments and their deploy steps.
    pub async fn release(&self, project: &str, id: i64) -> Result<Release, AdoError> {
        self.get_json(
            Host::Vsrm,
            &format!("{}/_apis/release/releases/{id}", enc(project)),
        )
        .await
    }

    pub async fn deployments_since(
        &self,
        project: &str,
        min_modified: DateTime<Utc>,
    ) -> Result<Vec<Deployment>, AdoError> {
        self.get_paged(
            Host::Vsrm,
            &format!(
                "{}/_apis/release/deployments?minModifiedTime={}&queryOrder=ascending",
                enc(project),
                ts(min_modified)
            ),
        )
        .await
    }

    /// Step 1 of the classic approvals query: ids of releases with any pending approval
    /// (all assignees), sorted and deduplicated.
    pub async fn pending_release_ids(&self, project: &str) -> Result<Vec<i64>, AdoError> {
        let approvals: Vec<ReleaseApproval> = self
            .get_paged(
                Host::Vsrm,
                &format!(
                    "{}/_apis/release/approvals?statusFilter=pending",
                    enc(project)
                ),
            )
            .await?;
        let mut ids: Vec<i64> = approvals.into_iter().map(|a| a.release.id).collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// Step 2: pending approvals assigned to `me_id` or a group of theirs, limited to
    /// `release_ids` (without the id filter ADO ignores `assignedToFilter`).
    pub async fn my_pending_approvals(
        &self,
        project: &str,
        release_ids: &[i64],
        me_id: &str,
    ) -> Result<Vec<ReleaseApproval>, AdoError> {
        let mut all = Vec::new();
        for chunk in release_ids.chunks(APPROVAL_ID_CHUNK) {
            let ids = chunk
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let page: Vec<ReleaseApproval> = self
                .get_paged(
                    Host::Vsrm,
                    &format!(
                        "{}/_apis/release/approvals?releaseIdsFilter={ids}&assignedToFilter={}&statusFilter=Pending&includeMyGroupApprovals=true",
                        enc(project),
                        enc(me_id)
                    ),
                )
                .await?;
            all.extend(page);
        }
        Ok(all)
    }

    pub async fn release_approval(
        &self,
        project: &str,
        id: i64,
    ) -> Result<ReleaseApproval, AdoError> {
        self.get_json(
            Host::Vsrm,
            &format!("{}/_apis/release/approvals/{id}", enc(project)),
        )
        .await
    }
}
