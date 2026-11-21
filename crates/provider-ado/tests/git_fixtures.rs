use herald_provider_ado::client::ListResponse;
use herald_provider_ado::dto_git::*;
use serde::de::DeserializeOwned;

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

#[test]
fn pull_requests_deserialize() {
    let prs: Vec<GitPullRequest> = list("active_prs");
    assert_eq!(prs.len(), 3);
    let pr = &prs[0];
    assert_eq!(pr.pull_request_id, 7123);
    assert_eq!(pr.status, "active");
    assert_eq!(pr.repository.name, "repo-alpha");
    assert_eq!(pr.repository.project.name, "Project One");
    assert!(pr.source_ref_name.starts_with("refs/heads/"));
    assert!(pr.creation_date.is_some());
    assert!(!pr.created_by.as_ref().unwrap().id.is_empty());
    assert!(!pr.reviewers.is_empty());
}

#[test]
fn threads_expose_system_thread_type() {
    let threads: Vec<GitPullRequestCommentThread> = list("pr_threads");
    let vote = threads
        .iter()
        .find(|t| t.thread_type() == Some("VoteUpdate"))
        .unwrap();
    assert_eq!(vote.property_str("CodeReviewVoteResult"), Some("10"));
    assert_eq!(vote.comments[0].comment_type, "system");
    assert!(vote.published_date.is_some());
    assert!(
        threads
            .iter()
            .any(|t| t.thread_type() == Some("ReviewersUpdate"))
    );
    assert_eq!(vote.property_str("Missing"), None);
    assert_eq!(
        vote.property_identity("CodeReviewVotedByIdentity")
            .map(|i| i.id.as_str()),
        Some("2936be1a-54e6-4454-a922-5a7d3e21b31d")
    );
    assert!(vote.property_identity("Missing").is_none());

    let added = threads
        .iter()
        .find(|t| t.thread_type() == Some("ReviewersUpdate"))
        .unwrap();
    assert_eq!(
        added
            .property_identity("CodeReviewReviewersUpdatedAddedIdentity")
            .map(|i| i.id.as_str()),
        Some("2936be1a-54e6-4454-a922-5a7d3e21b31d")
    );
}

#[test]
fn identity_list_property_resolves_known_keys_only() {
    let t: GitPullRequestCommentThread = serde_json::from_value(serde_json::json!({
        "properties": {
            "Voters": { "$value": "[\"1\",\"9\"]" },
            "Bad": { "$value": "not json" }
        },
        "identities": { "1": { "id": "guid-1" } }
    }))
    .unwrap();
    let ids: Vec<&str> = t
        .property_identity_list("Voters")
        .iter()
        .map(|i| i.id.as_str())
        .collect();
    assert_eq!(ids, ["guid-1"]);
    assert!(t.property_identity_list("Bad").is_empty());
    assert!(t.property_identity_list("Missing").is_empty());
}

#[test]
fn iterations_statuses_and_policies_deserialize() {
    let iters: Vec<GitPullRequestIteration> = list("pr_iterations");
    assert_eq!(iters[0].id, 1);
    assert!(iters[0].created_date.is_some());
    assert!(
        !iters[0]
            .source_ref_commit
            .as_ref()
            .unwrap()
            .commit_id
            .is_empty()
    );

    let statuses: Vec<GitPullRequestStatus> = list("pr_statuses");
    assert_eq!(statuses[0].context.genre.as_deref(), Some("ci/coverage"));
    assert!(!statuses[0].context.name.is_empty());
    assert_eq!(statuses[0].state, "");
    assert!(statuses.iter().any(|s| s.state == "pending"));

    let evals: Vec<PolicyEvaluationRecord> = list("policy_evaluations");
    assert!(!evals[0].evaluation_id.is_empty());
    assert!(!evals[0].configuration.policy_type.display_name.is_empty());
    assert!(evals[0].configuration.settings.is_object());
}

#[test]
fn repos_refs_pushes_and_diffs_deserialize() {
    let repos: Vec<GitRepository> = list("repositories");
    assert!(!repos[0].id.is_empty() && !repos[0].project.id.is_empty());

    let refs: Vec<GitRef> = list("refs_heads");
    assert!(refs[0].name.starts_with("refs/heads/"));
    assert!(!refs[0].creator.as_ref().unwrap().id.is_empty());

    let pushes: Vec<GitPush> = list("pushes");
    assert!(pushes[0].push_id > 0);
    assert!(pushes[0].ref_updates[0].old_object_id.starts_with("0000"));

    let d: GitCommitDiffs = serde_json::from_str(&fixture("commit_diffs")).unwrap();
    assert_eq!((d.ahead_count, d.behind_count), (3, 1));
}
