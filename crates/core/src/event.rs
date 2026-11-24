//! The provider-neutral event model, event type names and deterministic id builders.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub connection: ConnectionRef,
    /// Provider timestamp; detection time when the event is inferred.
    pub occurred_utc: DateTime<Utc>,
    pub detected_utc: DateTime<Utc>,
    pub inferred: bool,
    pub catch_up: bool,
    pub test: bool,
    pub severity: Severity,
    pub actor: Actor,
    /// Set by the classifier; `Event::new` leaves it unclassified (false). After classify,
    /// `alert == (level == Level::Alert)`.
    pub alert: bool,
    /// Set by the classifier; `Event::new` leaves the default until it runs.
    #[serde(default)]
    pub level: Level,
    /// Set by the provider: the event reached me only through a group.
    #[serde(default)]
    pub via_group: bool,
    pub title: String,
    pub subtitle: Option<String>,
    pub url: Option<String>,
    pub subject_key: String,
    pub repo: Option<RepoRef>,
    pub pr: Option<PrRef>,
    pub branch: Option<BranchRef>,
    pub run: Option<RunRef>,
    pub deployment: Option<DeploymentRef>,
    pub comment: Option<CommentRef>,
    pub approval: Option<ApprovalRef>,
    pub local: Option<LocalPaths>,
    /// Provider extras plus the pinned per-type keys; `Null` when none.
    #[serde(default)]
    pub data: serde_json::Value,
    /// Store-assigned sequence number; set only on events read from the store (or published
    /// after being stored), never part of the stored json.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

impl Event {
    /// Builds an event with defaults: flags false, severity Info, actor kind Human with
    /// an empty identity, no optional refs, `data` Null. The id is left empty; see `with_id`.
    pub fn new(
        connection: ConnectionRef,
        event_type: &str,
        occurred_utc: DateTime<Utc>,
        detected_utc: DateTime<Utc>,
        title: impl Into<String>,
        subject_key: impl Into<String>,
    ) -> Self {
        Self {
            id: String::new(),
            event_type: event_type.to_string(),
            connection,
            occurred_utc,
            detected_utc,
            inferred: false,
            catch_up: false,
            test: false,
            severity: Severity::Info,
            actor: Actor {
                kind: ActorKind::Human,
                id: String::new(),
                display_name: String::new(),
                unique_name: None,
            },
            alert: false,
            level: Level::Alert,
            via_group: false,
            title: title.into(),
            subtitle: None,
            url: None,
            subject_key: subject_key.into(),
            repo: None,
            pr: None,
            branch: None,
            run: None,
            deployment: None,
            comment: None,
            approval: None,
            local: None,
            data: serde_json::Value::Null,
            seq: None,
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Listed in the pane; meaningful after classify.
    pub fn in_pane(&self) -> bool {
        self.level != Level::Ignore
            && self.actor.kind != ActorKind::Me
            && self.event_type != event_types::RECORD_ACTIVATED
    }
}

/// Notification level stamped on each event by the classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    #[default]
    Alert,
    Pane,
    Ignore,
}

impl Level {
    pub const ALL: [Level; 3] = [Level::Alert, Level::Pane, Level::Ignore];

    pub fn as_str(self) -> &'static str {
        match self {
            Level::Alert => "alert",
            Level::Pane => "pane",
            Level::Ignore => "ignore",
        }
    }

    pub fn parse(s: &str) -> Option<Level> {
        Self::ALL.into_iter().find(|l| l.as_str() == s)
    }

    /// The quieter of self and cap (order: Alert > Pane > Ignore).
    pub fn at_most(self, cap: Level) -> Level {
        fn loudness(l: Level) -> u8 {
            match l {
                Level::Alert => 2,
                Level::Pane => 1,
                Level::Ignore => 0,
            }
        }
        if loudness(self) <= loudness(cap) {
            self
        } else {
            cap
        }
    }
}

/// Configurable event category: the event type prefix before the first '.'.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Category {
    Pr,
    Branch,
    Run,
    Release,
    Deploy,
    Approval,
}

impl Category {
    pub const ALL: [Category; 6] = [
        Category::Pr,
        Category::Branch,
        Category::Run,
        Category::Release,
        Category::Deploy,
        Category::Approval,
    ];

    pub fn key(self) -> &'static str {
        match self {
            Category::Pr => "pr",
            Category::Branch => "branch",
            Category::Run => "run",
            Category::Release => "release",
            Category::Deploy => "deploy",
            Category::Approval => "approval",
        }
    }

    pub fn from_key(s: &str) -> Option<Category> {
        Self::ALL.into_iter().find(|c| c.key() == s)
    }

    pub fn of(event_type: &str) -> Option<Category> {
        Self::from_key(event_type.split('.').next()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionRef {
    pub id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Critical,
    Warn,
    Success,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub kind: ActorKind,
    pub id: String,
    pub display_name: String,
    pub unique_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorKind {
    Me,
    Human,
    Bot,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoRef {
    pub id: String,
    pub name: String,
    /// Provider grouping of the repo (ADO: project).
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrRef {
    pub id: i64,
    pub title: String,
    pub source_branch: String,
    pub target_branch: String,
    pub is_draft: bool,
    pub role: PrRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PrRole {
    Author,
    Reviewer,
    GroupReviewer,
    Commenter,
    Mentioned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchRef {
    pub name: String,
    pub short_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRef {
    pub id: String,
    pub definition: String,
    pub source_branch: String,
    pub reason: String,
    pub status: String,
    pub result: String,
    pub stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentRef {
    pub release_id: String,
    pub release_name: String,
    pub definition: String,
    pub environment: Option<String>,
    pub deployment_id: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRef {
    pub thread_id: String,
    pub comment_id: String,
    /// At most 200 characters.
    pub excerpt: String,
    pub thread_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRef {
    pub id: String,
    /// Provider-defined, e.g. "ado-classic" or "ado-yaml".
    pub kind: String,
    pub stage: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalPaths {
    pub repo_path: Option<String>,
    pub worktree_path: Option<String>,
}

pub mod event_types {
    pub const PR_CREATED: &str = "pr.created";
    pub const PR_PUBLISHED: &str = "pr.published";
    pub const PR_COMPLETED: &str = "pr.completed";
    pub const PR_ABANDONED: &str = "pr.abandoned";
    pub const PR_REACTIVATED: &str = "pr.reactivated";
    pub const PR_ITERATION_PUSHED: &str = "pr.iteration.pushed";
    pub const PR_REVIEWER_ADDED: &str = "pr.reviewer.added";
    pub const PR_REVIEWER_REMOVED: &str = "pr.reviewer.removed";
    pub const PR_VOTE_CHANGED: &str = "pr.vote.changed";
    pub const PR_VOTES_RESET: &str = "pr.votes.reset";
    pub const PR_REREVIEW_NEEDED: &str = "pr.rereview.needed";
    pub const PR_COMMENT_ADDED: &str = "pr.comment.added";
    pub const PR_COMMENT_REPLIED: &str = "pr.comment.replied";
    pub const PR_THREAD_STATUS_CHANGED: &str = "pr.thread.status.changed";
    pub const PR_MENTIONED: &str = "pr.mentioned";
    pub const PR_POLICY_CHANGED: &str = "pr.policy.changed";
    pub const PR_CONFLICTS_DETECTED: &str = "pr.conflicts.detected";
    pub const PR_CONFLICTS_CLEARED: &str = "pr.conflicts.cleared";
    pub const PR_AUTOCOMPLETE_SET: &str = "pr.autocomplete.set";
    pub const PR_AUTOCOMPLETE_CANCELLED: &str = "pr.autocomplete.cancelled";
    pub const BRANCH_CREATED: &str = "branch.created";
    pub const BRANCH_PUSHED: &str = "branch.pushed";
    pub const BRANCH_FORCEPUSHED: &str = "branch.forcepushed";
    pub const BRANCH_DELETED: &str = "branch.deleted";
    pub const RUN_COMPLETED: &str = "run.completed";
    pub const RUN_STAGE_FAILED: &str = "run.stage.failed";
    pub const RELEASE_CREATED: &str = "release.created";
    pub const DEPLOY_STARTED: &str = "deploy.started";
    pub const DEPLOY_SUCCEEDED: &str = "deploy.succeeded";
    pub const DEPLOY_FAILED: &str = "deploy.failed";
    pub const DEPLOY_CANCELED: &str = "deploy.canceled";
    pub const APPROVAL_PENDING: &str = "approval.pending";
    pub const APPROVAL_COMPLETED: &str = "approval.completed";
    pub const CATCHUP_SUMMARY: &str = "catchup.summary";
    pub const AUTH_FAILED: &str = "auth.failed";
    pub const SOURCE_DISABLED: &str = "source.disabled";
    pub const RECORD_ACTIVATED: &str = "record.activated";

    /// True for system-originated types (the classifier forces actor kind System):
    /// run.*, release.*, deploy.*, approval.*, pr.policy.changed, pr.conflicts.*,
    /// pr.rereview.needed, catchup.summary, auth.failed, source.disabled.
    pub fn is_system_type(t: &str) -> bool {
        const PREFIXES: [&str; 5] = ["run.", "release.", "deploy.", "approval.", "pr.conflicts."];
        const EXACT: [&str; 5] = [
            PR_POLICY_CHANGED,
            PR_REREVIEW_NEEDED,
            CATCHUP_SUMMARY,
            AUTH_FAILED,
            SOURCE_DISABLED,
        ];
        PREFIXES.iter().any(|p| t.starts_with(p)) || EXACT.contains(&t)
    }
}

pub mod subject {
    pub const APP: &str = "app";

    pub fn pr(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{c}:pr:{repo_id}:{pr_id}")
    }
    pub fn branch(c: &str, repo_id: &str, ref_name: &str) -> String {
        format!("{c}:branch:{repo_id}:{ref_name}")
    }
    pub fn run(c: &str, id: &str) -> String {
        format!("{c}:run:{id}")
    }
    pub fn release(c: &str, id: &str) -> String {
        format!("{c}:release:{id}")
    }
    pub fn approval(c: &str, kind: &str, id: &str) -> String {
        format!("{c}:approval:{kind}:{id}")
    }
    pub fn connection(c: &str) -> String {
        format!("{c}:connection")
    }
}

/// Deterministic event ids. Timestamps are RFC3339 millis UTC where the rule says iso,
/// epoch millis where it says millis.
pub mod ids {
    use super::{DateTime, SecondsFormat, Utc};

    fn iso(t: DateTime<Utc>) -> String {
        t.to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    fn pr(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{c}:pr:{repo_id}:{pr_id}")
    }

    pub fn pr_created(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{}:created", pr(c, repo_id, pr_id))
    }
    pub fn pr_completed(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{}:completed", pr(c, repo_id, pr_id))
    }
    pub fn pr_abandoned(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{}:abandoned", pr(c, repo_id, pr_id))
    }
    pub fn pr_reactivated(c: &str, repo_id: &str, pr_id: i64, at: DateTime<Utc>) -> String {
        format!("{}:reactivated:{}", pr(c, repo_id, pr_id), iso(at))
    }
    pub fn pr_comment(
        c: &str,
        repo_id: &str,
        pr_id: i64,
        thread_id: &str,
        comment_id: &str,
    ) -> String {
        format!(
            "{}:thread:{thread_id}:comment:{comment_id}",
            pr(c, repo_id, pr_id)
        )
    }
    pub fn pr_mention(
        c: &str,
        repo_id: &str,
        pr_id: i64,
        thread_id: &str,
        comment_id: &str,
    ) -> String {
        format!(
            "{}:thread:{thread_id}:mention:{comment_id}",
            pr(c, repo_id, pr_id)
        )
    }
    pub fn pr_description_mention(c: &str, repo_id: &str, pr_id: i64) -> String {
        format!("{}:description-mention", pr(c, repo_id, pr_id))
    }
    pub fn pr_thread_status(
        c: &str,
        repo_id: &str,
        pr_id: i64,
        thread_id: &str,
        status: &str,
        last_updated: DateTime<Utc>,
    ) -> String {
        format!(
            "{}:thread:{thread_id}:status:{status}:{}",
            pr(c, repo_id, pr_id),
            iso(last_updated)
        )
    }
    pub fn pr_system(c: &str, repo_id: &str, pr_id: i64, thread_id: &str) -> String {
        format!("{}:system:{thread_id}", pr(c, repo_id, pr_id))
    }
    pub fn pr_iteration(c: &str, repo_id: &str, pr_id: i64, iteration_id: &str) -> String {
        format!("{}:iteration:{iteration_id}", pr(c, repo_id, pr_id))
    }
    pub fn pr_rereview(c: &str, repo_id: &str, pr_id: i64, iteration_id: &str) -> String {
        format!("{}:rereview:{iteration_id}", pr(c, repo_id, pr_id))
    }
    pub fn pr_rereview_reset(c: &str, repo_id: &str, pr_id: i64, thread_id: &str) -> String {
        format!("{}:rereview:reset:{thread_id}", pr(c, repo_id, pr_id))
    }
    pub fn pr_policy(
        c: &str,
        repo_id: &str,
        pr_id: i64,
        evaluation_id: &str,
        state: &str,
    ) -> String {
        format!("{}:policy:{evaluation_id}:{state}", pr(c, repo_id, pr_id))
    }
    pub fn pr_merge(
        c: &str,
        repo_id: &str,
        pr_id: i64,
        merge_status: &str,
        detected: DateTime<Utc>,
    ) -> String {
        format!(
            "{}:merge:{merge_status}:{}",
            pr(c, repo_id, pr_id),
            detected.timestamp_millis()
        )
    }
    pub fn branch(c: &str, repo_id: &str, push_id: &str, ref_name: &str) -> String {
        format!("{c}:branch:{repo_id}:{push_id}:{ref_name}")
    }
    pub fn run_completed(c: &str, build_id: &str) -> String {
        format!("{c}:run:{build_id}:completed")
    }
    pub fn run_stage_failed(c: &str, build_id: &str, record_id: &str) -> String {
        format!("{c}:run:{build_id}:stage:{record_id}:failed")
    }
    pub fn release_created(c: &str, release_id: &str) -> String {
        format!("{c}:release:{release_id}:created")
    }
    pub fn deploy(
        c: &str,
        release_id: &str,
        environment_id: &str,
        deployment_id: &str,
        status: &str,
    ) -> String {
        format!("{c}:release:{release_id}:env:{environment_id}:deploy:{deployment_id}:{status}")
    }
    pub fn approval(c: &str, kind: &str, approval_id: &str, status: &str) -> String {
        format!("{c}:approval:{kind}:{approval_id}:{status}")
    }
    pub fn auth_failed(c: &str, detected: DateTime<Utc>) -> String {
        format!("{c}:auth:failed:{}", detected.timestamp_millis())
    }
    pub fn source_disabled(c: &str, source: &str, detected: DateTime<Utc>) -> String {
        format!(
            "{c}:source-disabled:{source}:{}",
            detected.timestamp_millis()
        )
    }
    pub fn catchup(at: DateTime<Utc>) -> String {
        format!("catchup:{}", at.timestamp_millis())
    }
    pub fn ui_activated(original_id: &str, at: DateTime<Utc>) -> String {
        format!("ui:activated:{original_id}:{}", at.timestamp_millis())
    }
    pub fn test(uuid: &str) -> String {
        format!("test:{uuid}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use pretty_assertions::assert_eq;

    fn t() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 20, 17, 52, 29).unwrap() + chrono::Duration::milliseconds(7)
    }

    fn full_event() -> Event {
        let mut e = Event::new(
            ConnectionRef {
                id: "conn-a".into(),
                provider: "ado".into(),
            },
            event_types::PR_COMMENT_ADDED,
            t(),
            t(),
            "Title",
            subject::pr("conn-a", "r1", 42),
        )
        .with_id("x");
        e.actor = Actor {
            kind: ActorKind::Me,
            id: "u1".into(),
            display_name: "Me".into(),
            unique_name: Some("me@example.com".into()),
        };
        e.severity = Severity::Warn;
        e.subtitle = Some("sub".into());
        e.url = Some("https://example.com".into());
        e.repo = Some(RepoRef {
            id: "r1".into(),
            name: "repo".into(),
            namespace: Some("p".into()),
        });
        e.pr = Some(PrRef {
            id: 42,
            title: "t".into(),
            source_branch: "refs/heads/a".into(),
            target_branch: "refs/heads/main".into(),
            is_draft: false,
            role: PrRole::GroupReviewer,
        });
        e.branch = Some(BranchRef {
            name: "refs/heads/a".into(),
            short_name: "a".into(),
        });
        e.run = Some(RunRef {
            id: "1".into(),
            definition: "d".into(),
            source_branch: "b".into(),
            reason: "manual".into(),
            status: "completed".into(),
            result: "failed".into(),
            stage: Some("s".into()),
        });
        e.deployment = Some(DeploymentRef {
            release_id: "9".into(),
            release_name: "Release-9".into(),
            definition: "d".into(),
            environment: Some("prod".into()),
            deployment_id: Some("3".into()),
            status: Some("succeeded".into()),
        });
        e.comment = Some(CommentRef {
            thread_id: "7".into(),
            comment_id: "1".into(),
            excerpt: "hi".into(),
            thread_status: "active".into(),
        });
        e.approval = Some(ApprovalRef {
            id: "a".into(),
            kind: "ado-yaml".into(),
            stage: "prod".into(),
            status: "pending".into(),
        });
        e.local = Some(LocalPaths {
            repo_path: Some("/r".into()),
            worktree_path: Some("/w".into()),
        });
        e.data = serde_json::json!({ "vote": 10 });
        e
    }

    #[test]
    fn serde_round_trips_and_uses_camel_case_keys() {
        let e = full_event();
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["type"], "pr.comment.added");
        assert_eq!(v["occurredUtc"], "2026-11-20T17:52:29.007Z");
        assert_eq!(v["subjectKey"], "conn-a:pr:r1:42");
        assert_eq!(v["pr"]["role"], "groupReviewer");
        assert_eq!(v["actor"]["kind"], "me");
        assert_eq!(v["severity"], "warn");
        assert_eq!(v["local"]["worktreePath"], "/w");
        let back: Event = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn level_and_via_group_round_trip_and_default() {
        let mut e = full_event();
        e.level = Level::Pane;
        e.via_group = true;
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["level"], "pane");
        assert_eq!(v["viaGroup"], true);
        let back: Event = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(back, e);

        let mut old = v;
        let o = old.as_object_mut().unwrap();
        o.remove("level");
        o.remove("viaGroup");
        let e: Event = serde_json::from_value(old).unwrap();
        assert_eq!(e.level, Level::Alert);
        assert!(!e.via_group);
    }

    #[test]
    fn level_and_category_parse_and_reject_unknowns() {
        for l in Level::ALL {
            assert_eq!(Level::parse(l.as_str()), Some(l));
        }
        for c in Category::ALL {
            assert_eq!(Category::from_key(c.key()), Some(c));
        }
        for bad in ["Alert", "loud", "", "ALERT"] {
            assert_eq!(Level::parse(bad), None, "{bad}");
        }
        for bad in ["Pr", "prs", "", "pr.vote"] {
            assert_eq!(Category::from_key(bad), None, "{bad}");
        }
    }

    #[test]
    fn at_most_returns_the_quieter_level() {
        use Level::*;
        let cases = [
            (Alert, Alert, Alert),
            (Alert, Pane, Pane),
            (Alert, Ignore, Ignore),
            (Pane, Alert, Pane),
            (Pane, Pane, Pane),
            (Pane, Ignore, Ignore),
            (Ignore, Alert, Ignore),
            (Ignore, Pane, Ignore),
            (Ignore, Ignore, Ignore),
        ];
        for (level, cap, want) in cases {
            assert_eq!(level.at_most(cap), want, "{level:?} capped at {cap:?}");
        }
    }

    #[test]
    fn category_of_uses_the_type_prefix() {
        let cases = [
            ("pr.vote.changed", Some(Category::Pr)),
            ("branch.created", Some(Category::Branch)),
            ("run.completed", Some(Category::Run)),
            ("release.created", Some(Category::Release)),
            ("deploy.failed", Some(Category::Deploy)),
            ("approval.pending", Some(Category::Approval)),
            ("auth.failed", None),
            ("source.disabled", None),
            ("catchup.summary", None),
            ("record.activated", None),
            ("", None),
        ];
        for (ty, want) in cases {
            assert_eq!(Category::of(ty), want, "{ty}");
        }
    }

    #[test]
    fn in_pane_excludes_ignored_me_and_record_activated() {
        let mut e = full_event();
        e.actor.kind = ActorKind::Human;
        e.level = Level::Pane;
        assert!(e.in_pane());
        e.actor.kind = ActorKind::Bot;
        assert!(e.in_pane());
        e.level = Level::Alert;
        assert!(e.in_pane());

        e.level = Level::Ignore;
        assert!(!e.in_pane());
        e.level = Level::Pane;
        e.actor.kind = ActorKind::Me;
        assert!(!e.in_pane());
        e.actor.kind = ActorKind::Human;
        e.event_type = event_types::RECORD_ACTIVATED.to_string();
        assert!(!e.in_pane());
    }

    #[test]
    fn missing_data_defaults_to_null() {
        let mut v = serde_json::to_value(full_event()).unwrap();
        v.as_object_mut().unwrap().remove("data");
        let e: Event = serde_json::from_value(v).unwrap();
        assert_eq!(e.data, serde_json::Value::Null);
    }

    #[test]
    fn id_builders_produce_documented_strings() {
        assert_eq!(
            ids::pr_reactivated("c", "r", 5, t()),
            "c:pr:r:5:reactivated:2026-11-20T17:52:29.007Z"
        );
        assert_eq!(
            ids::pr_mention("c", "r", 5, "t1", "k2"),
            "c:pr:r:5:thread:t1:mention:k2"
        );
        assert_eq!(
            ids::deploy("c", "1", "2", "3", "failed"),
            "c:release:1:env:2:deploy:3:failed"
        );
        assert_eq!(
            ids::catchup(t()),
            format!("catchup:{}", t().timestamp_millis())
        );
        assert_eq!(subject::connection("c"), "c:connection");
    }

    #[test]
    fn every_id_builder_produces_its_documented_format() {
        let at = t();
        let ms = at.timestamp_millis();
        let iso = "2026-11-20T17:52:29.007Z";
        let cases: Vec<(&str, String, String)> = vec![
            (
                "pr_created",
                ids::pr_created("c", "r", 5),
                "c:pr:r:5:created".into(),
            ),
            (
                "pr_completed",
                ids::pr_completed("c", "r", 5),
                "c:pr:r:5:completed".into(),
            ),
            (
                "pr_abandoned",
                ids::pr_abandoned("c", "r", 5),
                "c:pr:r:5:abandoned".into(),
            ),
            (
                "pr_reactivated",
                ids::pr_reactivated("c", "r", 5, at),
                format!("c:pr:r:5:reactivated:{iso}"),
            ),
            (
                "pr_comment",
                ids::pr_comment("c", "r", 5, "t1", "k2"),
                "c:pr:r:5:thread:t1:comment:k2".into(),
            ),
            (
                "pr_mention",
                ids::pr_mention("c", "r", 5, "t1", "k2"),
                "c:pr:r:5:thread:t1:mention:k2".into(),
            ),
            (
                "pr_description_mention",
                ids::pr_description_mention("c", "r", 5),
                "c:pr:r:5:description-mention".into(),
            ),
            (
                "pr_thread_status",
                ids::pr_thread_status("c", "r", 5, "t1", "fixed", at),
                format!("c:pr:r:5:thread:t1:status:fixed:{iso}"),
            ),
            (
                "pr_system",
                ids::pr_system("c", "r", 5, "t1"),
                "c:pr:r:5:system:t1".into(),
            ),
            (
                "pr_iteration",
                ids::pr_iteration("c", "r", 5, "3"),
                "c:pr:r:5:iteration:3".into(),
            ),
            (
                "pr_rereview",
                ids::pr_rereview("c", "r", 5, "3"),
                "c:pr:r:5:rereview:3".into(),
            ),
            (
                "pr_rereview_reset",
                ids::pr_rereview_reset("c", "r", 5, "t1"),
                "c:pr:r:5:rereview:reset:t1".into(),
            ),
            (
                "pr_policy",
                ids::pr_policy("c", "r", 5, "e1", "rejected"),
                "c:pr:r:5:policy:e1:rejected".into(),
            ),
            (
                "pr_merge",
                ids::pr_merge("c", "r", 5, "conflicts", at),
                format!("c:pr:r:5:merge:conflicts:{ms}"),
            ),
            (
                "branch",
                ids::branch("c", "r", "p9", "refs/heads/a"),
                "c:branch:r:p9:refs/heads/a".into(),
            ),
            (
                "run_completed",
                ids::run_completed("c", "77"),
                "c:run:77:completed".into(),
            ),
            (
                "run_stage_failed",
                ids::run_stage_failed("c", "77", "rec"),
                "c:run:77:stage:rec:failed".into(),
            ),
            (
                "release_created",
                ids::release_created("c", "9"),
                "c:release:9:created".into(),
            ),
            (
                "deploy",
                ids::deploy("c", "1", "2", "3", "failed"),
                "c:release:1:env:2:deploy:3:failed".into(),
            ),
            (
                "approval",
                ids::approval("c", "ado-yaml", "a1", "pending"),
                "c:approval:ado-yaml:a1:pending".into(),
            ),
            (
                "auth_failed",
                ids::auth_failed("c", at),
                format!("c:auth:failed:{ms}"),
            ),
            (
                "source_disabled",
                ids::source_disabled("c", "prs", at),
                format!("c:source-disabled:prs:{ms}"),
            ),
            ("catchup", ids::catchup(at), "catchup:1795197149007".into()),
            (
                "ui_activated",
                ids::ui_activated("orig", at),
                format!("ui:activated:orig:{ms}"),
            ),
            ("test", ids::test("u-1"), "test:u-1".into()),
        ];
        for (name, got, want) in cases {
            assert_eq!(got, want, "{name}");
        }
    }

    #[test]
    fn every_system_family_resolves_as_system() {
        use event_types::*;
        let system = [
            RUN_COMPLETED,
            RUN_STAGE_FAILED,
            RELEASE_CREATED,
            DEPLOY_STARTED,
            DEPLOY_SUCCEEDED,
            DEPLOY_FAILED,
            DEPLOY_CANCELED,
            APPROVAL_PENDING,
            APPROVAL_COMPLETED,
            PR_POLICY_CHANGED,
            PR_CONFLICTS_DETECTED,
            PR_CONFLICTS_CLEARED,
            PR_REREVIEW_NEEDED,
            CATCHUP_SUMMARY,
            AUTH_FAILED,
            SOURCE_DISABLED,
        ];
        for s in system {
            assert!(is_system_type(s), "{s}");
        }
        let not_system = [
            PR_CREATED,
            PR_PUBLISHED,
            PR_COMPLETED,
            PR_ABANDONED,
            PR_REACTIVATED,
            PR_ITERATION_PUSHED,
            PR_REVIEWER_ADDED,
            PR_REVIEWER_REMOVED,
            PR_VOTE_CHANGED,
            PR_VOTES_RESET,
            PR_COMMENT_ADDED,
            PR_COMMENT_REPLIED,
            PR_THREAD_STATUS_CHANGED,
            PR_MENTIONED,
            PR_AUTOCOMPLETE_SET,
            PR_AUTOCOMPLETE_CANCELLED,
            BRANCH_CREATED,
            BRANCH_PUSHED,
            BRANCH_FORCEPUSHED,
            BRANCH_DELETED,
            RECORD_ACTIVATED,
        ];
        for s in not_system {
            assert!(!is_system_type(s), "{s}");
        }
    }

    #[test]
    fn system_type_matching() {
        for s in [
            "run.completed",
            "deploy.failed",
            "pr.conflicts.cleared",
            "pr.policy.changed",
            "auth.failed",
        ] {
            assert!(event_types::is_system_type(s), "{s}");
        }
        for s in [
            "pr.comment.added",
            "pr.created",
            "branch.pushed",
            "record.activated",
        ] {
            assert!(!event_types::is_system_type(s), "{s}");
        }
    }
}
