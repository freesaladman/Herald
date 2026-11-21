//! Sets actor kind, severity and alert on events before they are stored or broadcast.

use std::collections::BTreeMap;

use globset::GlobMatcher;
use serde_json::Value;

use crate::config::{Config, build_glob};
use crate::event::{ActorKind, Event, PrRole, Severity, event_types as t};
use crate::provider::Identity;

pub struct Classifier {
    me: BTreeMap<String, String>,
    bots: BTreeMap<String, Vec<GlobMatcher>>,
    mute_types: Vec<GlobMatcher>,
    group_reviewer_types: Vec<GlobMatcher>,
}

fn compile(patterns: &[String]) -> Vec<GlobMatcher> {
    patterns
        .iter()
        .filter_map(|p| match build_glob(p) {
            Ok(g) => Some(g.compile_matcher()),
            Err(e) => {
                tracing::warn!(pattern = %p, error = %e, "skipping invalid glob");
                None
            }
        })
        .collect()
}

fn any_match(globs: &[GlobMatcher], s: &str) -> bool {
    globs.iter().any(|g| g.is_match(s))
}

fn data_str<'a>(e: &'a Event, key: &str) -> Option<&'a str> {
    e.data.get(key).and_then(Value::as_str)
}

impl Classifier {
    pub fn new(config: &Config, identities: &BTreeMap<String, Identity>) -> Self {
        Self {
            me: identities
                .iter()
                .map(|(c, i)| (c.clone(), i.id.clone()))
                .collect(),
            bots: config
                .connections
                .iter()
                .map(|c| (c.id.clone(), compile(&c.bots)))
                .collect(),
            mute_types: compile(&config.alerts.mute_types),
            group_reviewer_types: compile(&config.roles.group_reviewer.alert_types),
        }
    }

    pub fn classify(&self, e: &mut Event) {
        e.actor.kind = self.actor_kind(e);
        if e.event_type != t::CATCHUP_SUMMARY {
            e.severity = severity(e);
        }
        e.alert = self.alert(e);
    }

    fn actor_kind(&self, e: &Event) -> ActorKind {
        if t::is_system_type(&e.event_type) {
            return ActorKind::System;
        }
        if !e.actor.id.is_empty()
            && self
                .me
                .get(&e.connection.id)
                .is_some_and(|id| id.eq_ignore_ascii_case(&e.actor.id))
        {
            return ActorKind::Me;
        }
        if let Some(globs) = self.bots.get(&e.connection.id) {
            let unique = e.actor.unique_name.as_deref().unwrap_or("");
            let names = [unique, e.actor.display_name.as_str()];
            if names.iter().any(|n| !n.is_empty() && any_match(globs, n)) {
                return ActorKind::Bot;
            }
        }
        e.actor.kind
    }

    fn alert(&self, e: &Event) -> bool {
        matches!(e.actor.kind, ActorKind::Human | ActorKind::System)
            && e.event_type != t::RECORD_ACTIVATED
            && !any_match(&self.mute_types, &e.event_type)
            && (e.pr.as_ref().map(|p| p.role) != Some(PrRole::GroupReviewer)
                || any_match(&self.group_reviewer_types, &e.event_type))
    }
}

fn severity(e: &Event) -> Severity {
    use Severity::{Critical, Info, Success, Warn};
    let flag = |k: &str| e.data.get(k).and_then(Value::as_bool).unwrap_or(false);
    match e.event_type.as_str() {
        t::RUN_STAGE_FAILED | t::DEPLOY_FAILED | t::PR_CONFLICTS_DETECTED | t::AUTH_FAILED => {
            Critical
        }
        t::RUN_COMPLETED => match e.run.as_ref().map(|r| r.result.as_str()) {
            Some("failed") => Critical,
            Some("canceled" | "partiallySucceeded") => Warn,
            Some("succeeded") => Success,
            _ => Info,
        },
        t::PR_VOTE_CHANGED => match e.data.get("vote").and_then(Value::as_i64) {
            Some(-10) => Critical,
            Some(-5) => Warn,
            Some(5 | 10) => Success,
            _ => Info,
        },
        t::PR_POLICY_CHANGED => match data_str(e, "state") {
            Some("rejected" | "failed" | "broken" | "error") => Critical,
            Some("approved" | "succeeded") => Success,
            _ => Info,
        },
        t::PR_REREVIEW_NEEDED
        | t::APPROVAL_PENDING
        | t::PR_MENTIONED
        | t::DEPLOY_CANCELED
        | t::SOURCE_DISABLED => Warn,
        t::PR_REVIEWER_ADDED if flag("isMe") || flag("isMyGroup") => Warn,
        t::PR_COMPLETED | t::DEPLOY_SUCCEEDED | t::PR_CONFLICTS_CLEARED => Success,
        t::APPROVAL_COMPLETED if e.approval.as_ref().is_some_and(|a| a.status == "approved") => {
            Success
        }
        _ => Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ApprovalRef, ConnectionRef, DeploymentRef, PrRef, RunRef};
    use chrono::{DateTime, Utc};
    use serde_json::json;

    fn identity() -> Identity {
        Identity {
            id: "me-id".into(),
            display_name: "Me".into(),
            unique_name: "me@example.com".into(),
            group_ids: vec![],
        }
    }

    fn classifier(extra: &str) -> Classifier {
        let toml = format!(
            "{extra}\n[[connections]]\nid = \"conn-a\"\nprovider = \"fake\"\nbots = [\"Build\\\\*\", \"svc-*@example.com\"]\n"
        );
        let cfg = Config::from_toml_str(&toml).expect("config");
        let ids = BTreeMap::from([("conn-a".to_string(), identity())]);
        Classifier::new(&cfg, &ids)
    }

    fn event(ty: &str, actor_id: &str, name: &str) -> Event {
        let now: DateTime<Utc> = DateTime::UNIX_EPOCH;
        let mut e = Event::new(
            ConnectionRef {
                id: "conn-a".into(),
                provider: "fake".into(),
            },
            ty,
            now,
            now,
            "t",
            "conn-a:connection",
        );
        e.actor.id = actor_id.into();
        e.actor.display_name = name.into();
        e
    }

    fn pr(role: PrRole) -> PrRef {
        PrRef {
            id: 1,
            title: "p".into(),
            source_branch: "a".into(),
            target_branch: "b".into(),
            is_draft: false,
            role,
        }
    }

    fn run_ref(result: &str) -> RunRef {
        RunRef {
            id: "1".into(),
            definition: "d".into(),
            source_branch: "b".into(),
            reason: "manual".into(),
            status: "completed".into(),
            result: result.into(),
            stage: None,
        }
    }

    #[test]
    fn actor_rules() {
        let c = classifier("");
        let mut e = event(t::PR_COMMENT_ADDED, "me-id", "Someone");
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Me);

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Build\\Service");
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Bot);

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        e.actor.unique_name = Some("SVC-scan@example.com".into());
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Bot);

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Human);

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        e.actor.kind = ActorKind::Bot;
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Bot);

        let mut e = event(t::RUN_COMPLETED, "me-id", "Me");
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::System);

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        e.actor.kind = ActorKind::System;
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::System);

        let mut e = event(t::PR_COMMENT_ADDED, "me-id", "Me");
        e.connection.id = "conn-b".into();
        c.classify(&mut e);
        assert_eq!(e.actor.kind, ActorKind::Human);
    }

    fn deployment(status: &str) -> DeploymentRef {
        DeploymentRef {
            release_id: "1".into(),
            release_name: "r".into(),
            definition: "d".into(),
            environment: Some("prod".into()),
            deployment_id: Some("2".into()),
            status: Some(status.into()),
        }
    }

    fn approval(status: &str) -> ApprovalRef {
        ApprovalRef {
            id: "1".into(),
            kind: "k".into(),
            stage: "s".into(),
            status: status.into(),
        }
    }

    type Setup = fn(&mut Event);

    #[test]
    fn severity_table() {
        let rows: Vec<(&str, &str, Setup, Severity)> = vec![
            (
                "run failed",
                t::RUN_COMPLETED,
                |e| e.run = Some(run_ref("failed")),
                Severity::Critical,
            ),
            (
                "run canceled",
                t::RUN_COMPLETED,
                |e| e.run = Some(run_ref("canceled")),
                Severity::Warn,
            ),
            (
                "run partiallySucceeded",
                t::RUN_COMPLETED,
                |e| e.run = Some(run_ref("partiallySucceeded")),
                Severity::Warn,
            ),
            (
                "run succeeded",
                t::RUN_COMPLETED,
                |e| e.run = Some(run_ref("succeeded")),
                Severity::Success,
            ),
            (
                "run unknown result",
                t::RUN_COMPLETED,
                |e| e.run = Some(run_ref("weird")),
                Severity::Info,
            ),
            (
                "run stage failed",
                t::RUN_STAGE_FAILED,
                |_| {},
                Severity::Critical,
            ),
            (
                "deploy failed",
                t::DEPLOY_FAILED,
                |e| e.deployment = Some(deployment("failed")),
                Severity::Critical,
            ),
            (
                "deploy canceled",
                t::DEPLOY_CANCELED,
                |e| e.deployment = Some(deployment("canceled")),
                Severity::Warn,
            ),
            (
                "deploy succeeded",
                t::DEPLOY_SUCCEEDED,
                |e| e.deployment = Some(deployment("succeeded")),
                Severity::Success,
            ),
            (
                "vote -10",
                t::PR_VOTE_CHANGED,
                |e| e.data = json!({"vote": -10}),
                Severity::Critical,
            ),
            (
                "vote -5",
                t::PR_VOTE_CHANGED,
                |e| e.data = json!({"vote": -5}),
                Severity::Warn,
            ),
            (
                "vote 5",
                t::PR_VOTE_CHANGED,
                |e| e.data = json!({"vote": 5}),
                Severity::Success,
            ),
            (
                "vote 10",
                t::PR_VOTE_CHANGED,
                |e| e.data = json!({"vote": 10}),
                Severity::Success,
            ),
            (
                "vote 0",
                t::PR_VOTE_CHANGED,
                |e| e.data = json!({"vote": 0}),
                Severity::Info,
            ),
            (
                "conflicts detected",
                t::PR_CONFLICTS_DETECTED,
                |_| {},
                Severity::Critical,
            ),
            (
                "conflicts cleared",
                t::PR_CONFLICTS_CLEARED,
                |_| {},
                Severity::Success,
            ),
            (
                "policy rejected",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "rejected"}),
                Severity::Critical,
            ),
            (
                "policy failed",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "failed"}),
                Severity::Critical,
            ),
            (
                "policy broken",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "broken"}),
                Severity::Critical,
            ),
            (
                "policy error",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "error"}),
                Severity::Critical,
            ),
            (
                "policy approved",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "approved"}),
                Severity::Success,
            ),
            (
                "policy succeeded",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "succeeded"}),
                Severity::Success,
            ),
            (
                "policy pending",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "pending"}),
                Severity::Info,
            ),
            (
                "policy running",
                t::PR_POLICY_CHANGED,
                |e| e.data = json!({"state": "running"}),
                Severity::Info,
            ),
            ("auth failed", t::AUTH_FAILED, |_| {}, Severity::Critical),
            (
                "rereview needed",
                t::PR_REREVIEW_NEEDED,
                |_| {},
                Severity::Warn,
            ),
            (
                "approval pending",
                t::APPROVAL_PENDING,
                |e| e.approval = Some(approval("pending")),
                Severity::Warn,
            ),
            (
                "approval approved",
                t::APPROVAL_COMPLETED,
                |e| e.approval = Some(approval("approved")),
                Severity::Success,
            ),
            (
                "approval rejected",
                t::APPROVAL_COMPLETED,
                |e| e.approval = Some(approval("rejected")),
                Severity::Info,
            ),
            (
                "reviewer isMe",
                t::PR_REVIEWER_ADDED,
                |e| e.data = json!({"isMe": true, "isMyGroup": false}),
                Severity::Warn,
            ),
            (
                "reviewer isMyGroup",
                t::PR_REVIEWER_ADDED,
                |e| e.data = json!({"isMe": false, "isMyGroup": true}),
                Severity::Warn,
            ),
            (
                "reviewer neither",
                t::PR_REVIEWER_ADDED,
                |e| e.data = json!({"isMe": false, "isMyGroup": false}),
                Severity::Info,
            ),
            ("pr mentioned", t::PR_MENTIONED, |_| {}, Severity::Warn),
            (
                "source disabled",
                t::SOURCE_DISABLED,
                |_| {},
                Severity::Warn,
            ),
            ("pr completed", t::PR_COMPLETED, |_| {}, Severity::Success),
            ("comment added", t::PR_COMMENT_ADDED, |_| {}, Severity::Info),
            (
                "catchup keeps preset",
                t::CATCHUP_SUMMARY,
                |e| e.severity = Severity::Critical,
                Severity::Critical,
            ),
        ];
        let c = classifier("");
        for (name, ty, setup, expected) in rows {
            let mut e = event(ty, "x", "P");
            setup(&mut e);
            c.classify(&mut e);
            assert_eq!(e.severity, expected, "row: {name}");
        }
    }

    #[test]
    fn alert_rules() {
        let c = classifier("[alerts]\nmute_types = [\"pr.iteration.*\"]\n");
        let alert = |mut e: Event| {
            c.classify(&mut e);
            e.alert
        };
        assert!(alert(event(t::PR_COMMENT_ADDED, "x", "Pat")));
        assert!(!alert(event(t::PR_COMMENT_ADDED, "me-id", "Me")));
        assert!(!alert(event(t::PR_COMMENT_ADDED, "x", "Build\\Svc")));
        assert!(!alert(event(t::PR_ITERATION_PUSHED, "x", "Pat")));
        assert!(!alert(event(t::RECORD_ACTIVATED, "x", "Pat")));
        assert!(alert(event(t::RUN_COMPLETED, "x", "Pat")));

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        e.pr = Some(pr(PrRole::GroupReviewer));
        assert!(!alert(e));
        let mut e = event(t::PR_CREATED, "x", "Pat");
        e.pr = Some(pr(PrRole::GroupReviewer));
        assert!(alert(e));

        let mut e = event(t::PR_COMMENT_ADDED, "x", "Pat");
        e.catch_up = true;
        e.test = true;
        assert!(alert(e));
    }
}
