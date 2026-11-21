//! Event type and field matching, and `{path}` placeholder expansion, shared by the hook
//! runner and the stream server. Paths are dotted camelCase into the event's JSON.

use globset::GlobMatcher;
use serde_json::Value;

use crate::config::{MatchTable, build_glob};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unresolved placeholder '{0}'")]
pub struct MissingPlaceholder(pub String);

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

pub fn type_matches(globs: &[String], t: &str) -> bool {
    compile(globs).iter().any(|g| g.is_match(t))
}

pub fn match_table(m: &MatchTable, v: &Value) -> bool {
    CompiledTable::new(m).matches(v)
}

/// Resolves a dotted path to a scalar rendered as text.
pub fn lookup(path: &str, v: &Value) -> Option<String> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    match cur {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Replaces `{dotted.path}` tokens; `{{` and `}}` are literal braces.
pub fn expand(template: &str, v: &Value) -> Result<String, MissingPlaceholder> {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            '{' => {
                let mut path = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '}' {
                        closed = true;
                        break;
                    }
                    path.push(c);
                }
                let path = path.trim();
                if !closed {
                    return Err(MissingPlaceholder(path.to_string()));
                }
                let value = lookup(path, v).ok_or_else(|| MissingPlaceholder(path.to_string()))?;
                out.push_str(&value);
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

struct CompiledTable {
    entries: Vec<(String, Vec<GlobMatcher>)>,
}

impl CompiledTable {
    fn new(m: &MatchTable) -> Self {
        Self {
            entries: m
                .iter()
                .map(|(k, val)| (k.clone(), compile(val.patterns())))
                .collect(),
        }
    }

    fn matches(&self, v: &Value) -> bool {
        self.entries.iter().all(|(path, globs)| {
            lookup(path, v).is_some_and(|s| globs.iter().any(|g| g.is_match(&s)))
        })
    }
}

/// A hook's `on` globs plus `match` table, compiled once.
pub struct CompiledHookFilter {
    on: Vec<GlobMatcher>,
    table: CompiledTable,
}

impl CompiledHookFilter {
    pub fn new(on: &[String], m: &MatchTable) -> Self {
        Self {
            on: compile(on),
            table: CompiledTable::new(m),
        }
    }

    pub fn matches(&self, event_type: &str, v: &Value) -> bool {
        self.on.iter().any(|g| g.is_match(event_type)) && self.table.matches(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MatchValue;
    use crate::event::{ConnectionRef, Event, LocalPaths, PrRef, PrRole};
    use chrono::Utc;

    fn event() -> Value {
        let now = Utc::now();
        let mut e = Event::new(
            ConnectionRef {
                id: "conn-a".into(),
                provider: "ado".into(),
            },
            "pr.created",
            now,
            now,
            "t",
            "conn-a:pr:r:42",
        );
        e.pr = Some(PrRef {
            id: 42,
            title: "x".into(),
            source_branch: "refs/heads/f".into(),
            target_branch: "refs/heads/main".into(),
            is_draft: false,
            role: PrRole::GroupReviewer,
        });
        e.local = Some(LocalPaths {
            repo_path: None,
            worktree_path: Some("C:\\wt\\f".into()),
        });
        serde_json::to_value(&e).unwrap()
    }

    fn table(entries: &[(&str, MatchValue)]) -> MatchTable {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn one(s: &str) -> MatchValue {
        MatchValue::One(s.into())
    }

    #[test]
    fn type_globs() {
        let g = vec!["pr.*".to_string()];
        assert!(type_matches(&g, "pr.created"));
        assert!(type_matches(&["run.completed".into()], "run.completed"));
        assert!(!type_matches(&g, "run.completed"));
        assert!(!type_matches(&[], "pr.created"));
    }

    #[test]
    fn match_fields() {
        let v = event();
        let t = table(&[
            ("actor.kind", one("human")),
            ("pr.role", one("groupReviewer")),
            ("connection.provider", one("ado")),
            ("pr.id", one("42")),
        ]);
        assert!(match_table(&t, &v));
        assert!(match_table(&MatchTable::new(), &v));
        assert!(!match_table(&table(&[("pr.role", one("author"))]), &v));
    }

    #[test]
    fn match_any_missing_and_case() {
        let v = event();
        let any = MatchValue::Any(vec!["author".into(), "group*".into()]);
        assert!(match_table(&table(&[("pr.role", any)]), &v));
        assert!(!match_table(&table(&[("run.id", one("*"))]), &v));
        assert!(!match_table(&table(&[("data", one("*"))]), &v));
        assert!(match_table(&table(&[("connection.id", one("CONN-A"))]), &v));
    }

    #[test]
    fn compiled_filter() {
        let v = event();
        let f = CompiledHookFilter::new(
            &["pr.*".into()],
            &table(&[("pr.role", one("groupReviewer"))]),
        );
        assert!(f.matches("pr.created", &v));
        assert!(!f.matches("run.completed", &v));
        let f = CompiledHookFilter::new(&["pr.*".into()], &table(&[("pr.role", one("author"))]));
        assert!(!f.matches("pr.created", &v));
    }

    #[test]
    fn expand_placeholders() {
        let v = event();
        assert_eq!(
            expand(
                "cd {local.worktreePath} # PR {pr.id} {{raw}} on {connection.id}",
                &v
            )
            .unwrap(),
            "cd C:\\wt\\f # PR 42 {raw} on conn-a"
        );
        assert_eq!(
            expand("{local.repoPath}", &v),
            Err(MissingPlaceholder("local.repoPath".into()))
        );
        assert!(expand("{pr.id", &v).is_err());
    }

    #[test]
    fn expand_never_rescans_values() {
        let mut v = event();
        v["pr"]["title"] = "{connection.id} {{x}}".into();
        assert_eq!(expand("{pr.title}", &v).unwrap(), "{connection.id} {{x}}");
        v["pr"]["title"] = "{nope}".into();
        assert_eq!(expand("{pr.title}", &v).unwrap(), "{nope}");
    }

    #[test]
    fn invalid_glob_is_skipped() {
        let f = CompiledHookFilter::new(&["[".into(), "pr.*".into()], &MatchTable::new());
        assert!(f.matches("pr.created", &event()));
    }
}
