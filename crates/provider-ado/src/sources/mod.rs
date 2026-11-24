//! Shared helpers and snapshot types for the ADO sources.

use chrono::{DateTime, Duration, Utc};
use herald_core::event::{
    Actor, ActorKind, ConnectionRef, Event, PrRole, RepoRef, event_types as t,
};
use herald_core::provider::{Identity, ProviderError, SyncContext};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::connection::AdoShared;
use crate::dto_git::{GitPullRequest, IdentityRef};

pub mod branches;
pub mod pipelines;
pub mod pr_discovery;
pub mod pr_policies;
pub mod pr_reviews;
pub mod pr_threads;
pub mod prs;
pub mod releases;

pub const SNAPSHOT_TRACKED_PRS: &str = "prs:tracked";
pub const SNAPSHOT_TRACKED_THREADS: &str = "prs:threads";
pub const SNAPSHOT_MY_BRANCHES: &str = "branches:mine";
pub const SNAPSHOT_MY_BUILDS: &str = "pipelines:mine";

/// Overlap re-read on every poll so late-arriving items are not missed; event ids dedupe.
const WINDOW_OVERLAP: Duration = Duration::minutes(2);

const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TrackedPrs {
    pub prs: Vec<TrackedPr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TrackedPr {
    pub repo_id: String,
    pub repo_name: String,
    pub project: String,
    pub project_id: String,
    pub pr_id: i64,
    pub role: PrRole,
    pub source_ref: String,
    pub target_ref: String,
    pub is_draft: bool,
    pub title: String,
    pub url: String,
    pub created_by_id: String,
    pub status: String,
    pub merge_status: Option<String>,
    /// My vote from the PR list; `None` until a list poll has seen me as an individual reviewer.
    pub my_vote: Option<i32>,
}

/// `PrRole` has no natural default; `Author` only fills the blank value.
impl Default for TrackedPr {
    fn default() -> Self {
        Self {
            repo_id: String::new(),
            repo_name: String::new(),
            project: String::new(),
            project_id: String::new(),
            pr_id: 0,
            role: PrRole::Author,
            source_ref: String::new(),
            target_ref: String::new(),
            is_draft: false,
            title: String::new(),
            url: String::new(),
            created_by_id: String::new(),
            status: String::new(),
            merge_status: None,
            my_vote: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TrackedThreads {
    pub threads: Vec<TrackedThread>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThreadReason {
    Commenter,
    Mentioned,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackedThread {
    pub repo_id: String,
    pub repo_name: String,
    pub project: String,
    pub project_id: String,
    pub pr_id: i64,
    pub thread_id: i64,
    pub since_utc: DateTime<Utc>,
    pub reason: ThreadReason,
    pub title: String,
    pub source_ref: String,
    pub target_ref: String,
    pub is_draft: bool,
    pub url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MyBranches {
    pub branches: Vec<MyBranch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyBranch {
    pub repo_id: String,
    pub project: String,
    pub ref_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MyBuilds {
    pub builds: Vec<MyBuild>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyBuild {
    pub id: i64,
    pub seen_utc: DateTime<Utc>,
}

impl MyBuilds {
    pub fn contains(&self, id: i64) -> bool {
        self.builds.iter().any(|b| b.id == id)
    }

    /// Adds the id if absent; an existing entry keeps its original age.
    pub fn insert(&mut self, id: i64, now: DateTime<Utc>) {
        if !self.contains(id) {
            self.builds.push(MyBuild { id, seen_utc: now });
        }
    }

    pub fn prune_older_than(&mut self, cutoff: DateTime<Utc>) {
        self.builds.retain(|b| b.seen_utc >= cutoff);
    }
}

/// Start of the poll window: watermark minus the overlap, never older than the lookback.
pub fn window_start(ctx: &SyncContext) -> DateTime<Utc> {
    let floor = ctx.server_now - Duration::days(ctx.config.polling.max_lookback_days);
    match ctx.watermark {
        Some(w) => (w - WINDOW_OVERLAP).max(floor),
        None => floor,
    }
}

/// Sources set `System` themselves for system event types.
pub fn actor_from(identity: &IdentityRef, me: &Identity) -> Actor {
    let kind = if !identity.id.is_empty() && identity.id.eq_ignore_ascii_case(&me.id) {
        ActorKind::Me
    } else {
        ActorKind::Human
    };
    Actor {
        kind,
        id: identity.id.clone(),
        display_name: identity.display_name.clone(),
        unique_name: Some(identity.unique_name.clone()).filter(|s| !s.is_empty()),
    }
}

pub(super) fn enc(s: &str) -> String {
    utf8_percent_encode(s, PATH_SEGMENT).to_string()
}

pub fn pr_web_url(org: &str, project: &str, repo: &str, pr_id: i64) -> String {
    format!(
        "https://dev.azure.com/{}/{}/_git/{}/pullrequest/{pr_id}",
        enc(org),
        enc(project),
        enc(repo)
    )
}

pub fn repo_ref(repo_id: &str, repo_name: &str, project: &str) -> RepoRef {
    RepoRef {
        id: repo_id.to_string(),
        name: repo_name.to_string(),
        namespace: Some(project.to_string()),
    }
}

pub fn connection_ref(ctx: &SyncContext) -> ConnectionRef {
    ctx.connection.clone()
}

/// Mentions render as `@<identity-id>` in comment and description text.
pub fn mentions(text: &str, me_id: &str) -> bool {
    let needle = format!("@<{me_id}>").to_lowercase();
    text.to_lowercase().contains(&needle)
}

/// Active PRs across the org, limited to the configured projects when any are set.
pub async fn active_prs(shared: &AdoShared) -> Result<Vec<GitPullRequest>, ProviderError> {
    let mut prs = shared.client.active_pull_requests(None).await?;
    if !shared.configured_projects.is_empty() {
        let projects = shared.projects().await?;
        prs.retain(|pr| {
            projects
                .iter()
                .any(|p| p.name.eq_ignore_ascii_case(&pr.repository.project.name))
        });
    }
    Ok(prs)
}

pub fn pr_key(repo_id: &str, pr_id: i64) -> String {
    format!("{}:{pr_id}", repo_id.to_lowercase())
}

/// The project id is stable across renames; fall back to the name when absent.
pub fn project_ref<'a>(id: &'a str, name: &'a str) -> &'a str {
    if id.is_empty() { name } else { id }
}

/// Errors that mean the poll cannot proceed, as opposed to a single item to skip.
pub fn is_fatal(e: &ProviderError) -> bool {
    matches!(
        e,
        ProviderError::Transient(_)
            | ProviderError::Throttled { .. }
            | ProviderError::Unauthorized
            | ProviderError::MissingCredentials
    )
}

/// On a PR I did not author, only review asks, mentions and thread activity are wanted.
/// Thread events need no check here: `pr_threads` only watches my threads on such PRs.
pub fn wanted_on_pr(e: &Event) -> bool {
    let Some(pr) = &e.pr else {
        return true;
    };
    if pr.role == PrRole::Author {
        return true;
    }
    let flag = |k: &str| e.data.get(k).and_then(Value::as_bool).unwrap_or(false);
    match e.event_type.as_str() {
        t::PR_CREATED
        | t::PR_PUBLISHED
        | t::PR_MENTIONED
        | t::PR_REREVIEW_NEEDED
        | t::PR_COMMENT_ADDED
        | t::PR_COMMENT_REPLIED
        | t::PR_THREAD_STATUS_CHANGED => true,
        t::PR_REVIEWER_ADDED | t::PR_REVIEWER_REMOVED => flag("isMe") || flag("isMyGroup"),
        _ => false,
    }
}

/// True when a PR event reached me only through a group I belong to.
pub fn via_group(e: &Event) -> bool {
    let flag = |k: &str| e.data.get(k).and_then(Value::as_bool).unwrap_or(false);
    match e.event_type.as_str() {
        t::PR_CREATED | t::PR_PUBLISHED | t::PR_REREVIEW_NEEDED => {
            e.pr.as_ref()
                .is_some_and(|p| p.role == PrRole::GroupReviewer)
        }
        t::PR_REVIEWER_ADDED | t::PR_REVIEWER_REMOVED => flag("isMyGroup") && !flag("isMe"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use herald_core::config::Config;
    use herald_core::testing::sync_context;

    fn me() -> Identity {
        Identity {
            id: "me-id".into(),
            display_name: "Me".into(),
            unique_name: "me@example.test".into(),
            group_ids: vec![],
        }
    }

    fn ctx(now: DateTime<Utc>, watermark: Option<DateTime<Utc>>) -> SyncContext {
        let cfg = Config::from_toml_str(
            "[[connections]]\nid = \"c1\"\nprovider = \"ado\"\norganization = \"contoso\"\n",
        )
        .unwrap();
        sync_context(cfg.connections[0].clone(), me(), now, watermark, false).0
    }

    fn at(day: u32, h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, day, h, m, 0).unwrap()
    }

    #[test]
    fn window_start_seed_normal_and_clamped() {
        let now = at(20, 12, 0);
        let lookback = Duration::days(14);
        assert_eq!(window_start(&ctx(now, None)), now - lookback);
        assert_eq!(window_start(&ctx(now, Some(at(20, 11, 0)))), at(20, 10, 58));
        assert_eq!(window_start(&ctx(now, Some(at(1, 0, 0)))), now - lookback);
    }

    #[test]
    fn wanted_on_pr_keeps_review_asks_on_prs_i_did_not_author() {
        use herald_core::event::PrRef;
        let event = |ty: &str, role: PrRole, data: Value| {
            let mut e = Event::new(
                ConnectionRef {
                    id: "c1".into(),
                    provider: "ado".into(),
                },
                ty,
                at(1, 0, 0),
                at(1, 0, 0),
                "t",
                "c1:pr:r:1",
            );
            e.pr = Some(PrRef {
                id: 1,
                title: "p".into(),
                source_branch: String::new(),
                target_branch: String::new(),
                is_draft: false,
                role,
            });
            e.data = data;
            e
        };
        let none = Value::Null;
        for ty in [
            t::PR_CONFLICTS_DETECTED,
            t::PR_COMPLETED,
            t::PR_VOTE_CHANGED,
            t::PR_POLICY_CHANGED,
        ] {
            assert!(
                wanted_on_pr(&event(ty, PrRole::Author, none.clone())),
                "{ty}"
            );
            assert!(
                !wanted_on_pr(&event(ty, PrRole::Reviewer, none.clone())),
                "{ty}"
            );
        }
        for ty in [
            t::PR_CREATED,
            t::PR_REREVIEW_NEEDED,
            t::PR_MENTIONED,
            t::PR_COMMENT_REPLIED,
        ] {
            assert!(
                wanted_on_pr(&event(ty, PrRole::GroupReviewer, none.clone())),
                "{ty}"
            );
        }
        let added = |data| event(t::PR_REVIEWER_ADDED, PrRole::Reviewer, data);
        assert!(wanted_on_pr(&added(serde_json::json!({ "isMe": true }))));
        assert!(wanted_on_pr(&added(
            serde_json::json!({ "isMyGroup": true })
        )));
        assert!(!wanted_on_pr(&added(serde_json::json!({ "isMe": false }))));
    }

    #[test]
    fn via_group_marks_events_that_reached_me_only_through_a_group() {
        use herald_core::event::PrRef;
        let event = |ty: &str, role: PrRole, data: Value| {
            let mut e = Event::new(
                ConnectionRef {
                    id: "c1".into(),
                    provider: "ado".into(),
                },
                ty,
                at(1, 0, 0),
                at(1, 0, 0),
                "t",
                "c1:pr:r:1",
            );
            e.pr = Some(PrRef {
                id: 1,
                title: "p".into(),
                source_branch: String::new(),
                target_branch: String::new(),
                is_draft: false,
                role,
            });
            e.data = data;
            e
        };
        let none = Value::Null;
        for ty in [t::PR_CREATED, t::PR_PUBLISHED, t::PR_REREVIEW_NEEDED] {
            assert!(
                via_group(&event(ty, PrRole::GroupReviewer, none.clone())),
                "{ty}"
            );
            assert!(
                !via_group(&event(ty, PrRole::Reviewer, none.clone())),
                "{ty}"
            );
            assert!(!via_group(&event(ty, PrRole::Author, none.clone())), "{ty}");
        }
        for ty in [
            t::PR_MENTIONED,
            t::PR_COMMENT_ADDED,
            t::PR_COMMENT_REPLIED,
            t::PR_THREAD_STATUS_CHANGED,
        ] {
            assert!(
                !via_group(&event(ty, PrRole::GroupReviewer, none.clone())),
                "{ty}"
            );
        }
        for ty in [t::PR_REVIEWER_ADDED, t::PR_REVIEWER_REMOVED] {
            let with = |data| event(ty, PrRole::Reviewer, data);
            assert!(
                via_group(&with(serde_json::json!({ "isMyGroup": true }))),
                "{ty}"
            );
            assert!(
                !via_group(&with(
                    serde_json::json!({ "isMe": true, "isMyGroup": true })
                )),
                "{ty}"
            );
            assert!(
                !via_group(&with(serde_json::json!({ "isMe": true }))),
                "{ty}"
            );
            assert!(!via_group(&with(none.clone())), "{ty}");
        }
    }

    #[test]
    fn mentions_is_case_insensitive() {
        assert!(mentions("hi @<ABC-123> please", "abc-123"));
        assert!(!mentions("hi @abc-123", "abc-123"));
        assert!(!mentions("hi @<other>", "abc-123"));
    }

    #[test]
    fn actor_from_marks_me_only_for_my_id() {
        let mine = IdentityRef {
            id: "me-id".into(),
            display_name: "Me".into(),
            unique_name: "me@example.test".into(),
            is_container: false,
        };
        let other = IdentityRef {
            id: "other".into(),
            unique_name: String::new(),
            ..mine.clone()
        };
        assert_eq!(actor_from(&mine, &me()).kind, ActorKind::Me);
        let upper = IdentityRef {
            id: "ME-ID".into(),
            ..mine.clone()
        };
        assert_eq!(actor_from(&upper, &me()).kind, ActorKind::Me);
        let empty = IdentityRef {
            id: String::new(),
            ..mine.clone()
        };
        let mut nobody = me();
        nobody.id = String::new();
        assert_eq!(actor_from(&empty, &nobody).kind, ActorKind::Human);
        let a = actor_from(&other, &me());
        assert_eq!(a.kind, ActorKind::Human);
        assert_eq!(a.unique_name, None);
    }

    #[test]
    fn my_builds_ages_ids_individually() {
        let mut b = MyBuilds::default();
        b.insert(1, at(1, 0, 0));
        b.insert(2, at(5, 0, 0));
        b.insert(1, at(9, 0, 0));
        b.prune_older_than(at(3, 0, 0));
        assert!(!b.contains(1));
        assert!(b.contains(2));
    }

    #[test]
    fn pr_web_url_encodes_segments() {
        assert_eq!(
            pr_web_url("contoso", "My Proj", "repo", 7),
            "https://dev.azure.com/contoso/My%20Proj/_git/repo/pullrequest/7"
        );
    }
}
