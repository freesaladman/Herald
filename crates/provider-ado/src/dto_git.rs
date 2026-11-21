//! Serde DTOs for the Azure DevOps git REST endpoints. Everything is optional or defaulted
//! so a missing or extra field never fails a poll.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::BTreeMap;

/// Treats an explicit JSON `null` like a missing field (`serde(default)` only covers missing).
fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IdentityRef {
    #[serde(deserialize_with = "null_default")]
    pub id: String,
    #[serde(deserialize_with = "null_default")]
    pub display_name: String,
    #[serde(deserialize_with = "null_default")]
    pub unique_name: String,
    pub is_container: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProjectRef {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitRepository {
    pub id: String,
    pub name: String,
    pub project: ProjectRef,
    pub is_disabled: bool,
    pub web_url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IdentityRefWithVote {
    pub id: String,
    pub display_name: String,
    pub unique_name: String,
    pub is_container: bool,
    pub vote: i32,
    pub is_required: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitPullRequest {
    pub pull_request_id: i64,
    pub repository: GitRepository,
    pub status: String,
    pub created_by: Option<IdentityRef>,
    pub creation_date: Option<DateTime<Utc>>,
    pub closed_date: Option<DateTime<Utc>>,
    pub closed_by: Option<IdentityRef>,
    pub title: String,
    pub description: Option<String>,
    pub source_ref_name: String,
    pub target_ref_name: String,
    pub is_draft: bool,
    pub merge_status: Option<String>,
    pub reviewers: Vec<IdentityRefWithVote>,
    pub auto_complete_set_by: Option<IdentityRef>,
    pub url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Comment {
    pub id: i64,
    pub parent_comment_id: i64,
    pub author: Option<IdentityRef>,
    pub content: Option<String>,
    pub published_date: Option<DateTime<Utc>>,
    pub last_updated_date: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "null_default")]
    pub comment_type: String,
    #[serde(deserialize_with = "null_default")]
    pub is_deleted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitPullRequestCommentThread {
    pub id: i64,
    pub published_date: Option<DateTime<Utc>>,
    pub last_updated_date: Option<DateTime<Utc>>,
    pub status: Option<String>,
    #[serde(deserialize_with = "null_default")]
    pub is_deleted: bool,
    pub properties: Value,
    #[serde(deserialize_with = "null_default")]
    pub comments: Vec<Comment>,
    /// Keyed by the placeholder ids that identity-valued properties point at; `null` when absent.
    #[serde(deserialize_with = "null_default")]
    pub identities: BTreeMap<String, IdentityRef>,
}

impl GitPullRequestCommentThread {
    /// `CodeReviewThreadType` is set on system threads only (VoteUpdate, ReviewersUpdate, ...).
    pub fn thread_type(&self) -> Option<&str> {
        self.property_str("CodeReviewThreadType")
    }

    pub fn property_str(&self, name: &str) -> Option<&str> {
        self.properties.get(name)?.get("$value")?.as_str()
    }

    /// Resolves an identity-valued property (e.g. `CodeReviewVotedByIdentity`) through `identities`.
    pub fn property_identity(&self, name: &str) -> Option<&IdentityRef> {
        self.identities.get(self.property_str(name)?)
    }

    /// For properties holding a JSON array string of identity keys, e.g. `["1"]`.
    /// Unparseable values and unknown keys are skipped.
    pub fn property_identity_list(&self, name: &str) -> Vec<&IdentityRef> {
        let Some(raw) = self.property_str(name) else {
            return Vec::new();
        };
        let keys: Vec<String> = serde_json::from_str(raw).unwrap_or_default();
        keys.iter()
            .filter_map(|k| self.identities.get(k.as_str()))
            .collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CommitRef {
    pub commit_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitPullRequestIteration {
    pub id: i64,
    pub author: Option<IdentityRef>,
    pub created_date: Option<DateTime<Utc>>,
    pub updated_date: Option<DateTime<Utc>>,
    pub source_ref_commit: Option<CommitRef>,
    pub pushed_by: Option<IdentityRef>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StatusContext {
    pub name: String,
    pub genre: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitPullRequestStatus {
    pub id: i64,
    pub state: String,
    pub description: Option<String>,
    pub context: StatusContext,
    pub creation_date: Option<DateTime<Utc>>,
    pub updated_date: Option<DateTime<Utc>>,
    pub created_by: Option<IdentityRef>,
    pub target_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyType {
    pub display_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyConfiguration {
    pub id: i64,
    #[serde(rename = "type")]
    pub policy_type: PolicyType,
    pub settings: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyEvaluationRecord {
    pub evaluation_id: String,
    pub status: String,
    pub started_date: Option<DateTime<Utc>>,
    pub completed_date: Option<DateTime<Utc>>,
    pub configuration: PolicyConfiguration,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitRef {
    pub name: String,
    pub object_id: String,
    pub creator: Option<IdentityRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitRefUpdate {
    pub name: String,
    pub old_object_id: String,
    pub new_object_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitPush {
    pub push_id: i64,
    pub date: Option<DateTime<Utc>>,
    pub pushed_by: Option<IdentityRef>,
    pub ref_updates: Vec<GitRefUpdate>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GitCommitDiffs {
    pub ahead_count: u32,
    pub behind_count: u32,
}
