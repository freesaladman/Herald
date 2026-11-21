//! Index of local git clones and worktrees, read straight from `.git` files (no git binary).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::config::LocalConfig;
use crate::event::{Event, LocalPaths, RepoRef};
use crate::provider::{ProviderRegistry, RemoteRepo};

const SKIP_DIRS: &[&str] = &["node_modules", "target"];

#[derive(Debug, Clone)]
struct Checkout {
    path: PathBuf,
    /// None when HEAD is detached or unreadable.
    branch: Option<String>,
}

#[derive(Debug, Clone)]
struct LocalRepo {
    remotes: Vec<RemoteRepo>,
    main: Checkout,
    worktrees: Vec<Checkout>,
}

#[derive(Debug, Clone, Default)]
pub struct LocalIndex {
    repos: Vec<LocalRepo>,
}

impl LocalIndex {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn build(cfg: &LocalConfig, registry: &ProviderRegistry) -> Self {
        // Keyed by the common git dir so a main clone and its worktrees group together.
        let mut found: BTreeMap<PathBuf, Vec<Checkout>> = BTreeMap::new();
        for root in &cfg.scan_roots {
            walk(Path::new(root), 0, cfg.scan_depth, &mut found);
        }

        let mut repos = Vec::new();
        for (common_dir, checkouts) in found {
            let Some(main_path) = common_dir.parent() else {
                continue;
            };
            let remotes: Vec<RemoteRepo> = read_remote_urls(&common_dir.join("config"))
                .iter()
                .flat_map(|url| registry.all().iter().filter_map(|f| f.parse_remote(url)))
                .collect();
            if remotes.is_empty() {
                continue;
            }
            let main = Checkout {
                path: main_path.to_path_buf(),
                branch: read_head_branch(&common_dir.join("HEAD")),
            };
            let worktrees = checkouts
                .into_iter()
                .filter(|c| c.path != main.path)
                .collect();
            repos.push(LocalRepo {
                remotes,
                main,
                worktrees,
            });
        }
        Self { repos }
    }

    pub fn enrich(&self, e: &mut Event) {
        let Some(repo_ref) = &e.repo else { return };
        let Some(repo) = self.repos.iter().find(|r| {
            r.remotes
                .iter()
                .any(|rm| remote_matches(rm, &e.connection.provider, repo_ref))
        }) else {
            return;
        };

        let wanted: Vec<&str> =
            e.pr.as_ref()
                .map(|p| short_ref(&p.source_branch))
                .into_iter()
                .chain(e.branch.as_ref().map(|b| short_ref(&b.name)))
                .collect();
        let worktree = wanted.iter().find_map(|name| {
            std::iter::once(&repo.main)
                .chain(&repo.worktrees)
                .find(|c| c.branch.as_deref() == Some(name))
        });

        e.local = Some(LocalPaths {
            repo_path: Some(repo.main.path.display().to_string()),
            worktree_path: worktree.map(|c| c.path.display().to_string()),
        });
    }
}

fn remote_matches(rm: &RemoteRepo, provider: &str, repo: &RepoRef) -> bool {
    rm.provider == provider
        && rm.repo_name.eq_ignore_ascii_case(&repo.name)
        && match (&rm.namespace, &repo.namespace) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            _ => true,
        }
}

fn short_ref(r: &str) -> &str {
    r.strip_prefix("refs/heads/").unwrap_or(r)
}

fn walk(dir: &Path, depth: usize, max_depth: usize, found: &mut BTreeMap<PathBuf, Vec<Checkout>>) {
    if let Some((common_dir, checkout)) = inspect_checkout(dir) {
        found.entry(common_dir).or_default().push(checkout);
        return;
    }
    if depth >= max_depth {
        return;
    }
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            tracing::debug!(dir = %dir.display(), %err, "skipping unreadable dir");
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || SKIP_DIRS.contains(&name.as_ref()) {
            continue;
        }
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            walk(&entry.path(), depth + 1, max_depth, found);
        }
    }
}

/// Returns (common git dir, checkout) when `dir` is a clone or a worktree.
fn inspect_checkout(dir: &Path) -> Option<(PathBuf, Checkout)> {
    let dot_git = dir.join(".git");
    let meta = fs::metadata(&dot_git).ok()?;
    if meta.is_dir() {
        let branch = read_head_branch(&dot_git.join("HEAD"));
        return Some((
            dot_git,
            Checkout {
                path: dir.to_path_buf(),
                branch,
            },
        ));
    }
    let text = fs::read_to_string(&dot_git).ok()?;
    let target = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("gitdir:"))?;
    let gitdir = resolve(dir, target.trim());
    let common = fs::read_to_string(gitdir.join("commondir")).ok()?;
    let common_dir = resolve(&gitdir, common.trim());
    Some((
        common_dir,
        Checkout {
            path: dir.to_path_buf(),
            branch: read_head_branch(&gitdir.join("HEAD")),
        },
    ))
}

/// Joins and lexically collapses `..` so paths read back from git files compare equal.
fn resolve(base: &Path, p: &str) -> PathBuf {
    let p = Path::new(p);
    let joined = if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    };
    let mut out = PathBuf::new();
    for c in joined.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

fn read_head_branch(head: &Path) -> Option<String> {
    let text = fs::read_to_string(head).ok()?;
    text.trim()
        .strip_prefix("ref:")?
        .trim()
        .strip_prefix("refs/heads/")
        .map(str::to_string)
}

fn read_remote_urls(config: &Path) -> Vec<String> {
    let Ok(text) = fs::read_to_string(config) else {
        tracing::debug!(config = %config.display(), "unreadable git config");
        return Vec::new();
    };
    let mut in_remote = false;
    let mut urls = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_remote = line.starts_with("[remote ");
        } else if in_remote
            && let Some((key, value)) = line.split_once('=')
            && key.trim().eq_ignore_ascii_case("url")
        {
            urls.push(value.trim().trim_matches('"').to_string());
        }
    }
    urls
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{BranchRef, ConnectionRef, PrRef, PrRole};
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use chrono::Utc;
    use std::sync::Arc;

    fn registry() -> ProviderRegistry {
        ProviderRegistry::new(vec![Arc::new(
            FakeProviderFactory::new(FakeProvider::new()),
        )])
    }

    fn cfg(root: &Path, depth: usize) -> LocalConfig {
        LocalConfig {
            scan_roots: vec![root.display().to_string()],
            scan_depth: depth,
        }
    }

    fn write(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    fn make_clone(dir: &Path, url: &str, head: &str) {
        write(
            &dir.join(".git/config"),
            &format!("[core]\n\tbare = false\n[remote \"origin\"]\n\turl = {url}\n"),
        );
        write(&dir.join(".git/HEAD"), head);
    }

    fn make_worktree(dir: &Path, main: &Path, name: &str, head: &str) {
        let gitdir = main.join(".git/worktrees").join(name);
        write(&gitdir.join("HEAD"), head);
        write(&gitdir.join("commondir"), "../..\n");
        write(
            &dir.join(".git"),
            &format!("gitdir: {}\n", gitdir.display()),
        );
    }

    fn event(repo: &str, source_branch: Option<&str>) -> Event {
        let now = Utc::now();
        let mut e = Event::new(
            ConnectionRef {
                id: "c".into(),
                provider: "fake".into(),
            },
            "pr.created",
            now,
            now,
            "t",
            "c:pr:1:1",
        );
        e.repo = Some(RepoRef {
            id: "1".into(),
            name: repo.into(),
            namespace: Some("proj".into()),
        });
        if let Some(b) = source_branch {
            e.pr = Some(PrRef {
                id: 1,
                title: "t".into(),
                source_branch: b.into(),
                target_branch: "refs/heads/main".into(),
                is_draft: false,
                role: PrRole::Author,
            });
        }
        e
    }

    const URL: &str = "fake://host/proj/Widgets";

    #[test]
    fn enrich_main_clone_matched_by_repo_name_case_insensitively() {
        let tmp = tempfile::tempdir().unwrap();
        let clone = tmp.path().join("widgets");
        make_clone(&clone, URL, "ref: refs/heads/main\n");
        let idx = LocalIndex::build(&cfg(tmp.path(), 2), &registry());

        let mut e = event("WIDGETS", Some("refs/heads/feature"));
        idx.enrich(&mut e);
        let local = e.local.unwrap();
        assert_eq!(local.repo_path, Some(clone.display().to_string()));
        assert_eq!(local.worktree_path, None);
    }

    #[test]
    fn enrich_worktree_on_source_branch_sets_worktree_path() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("widgets");
        make_clone(&main, URL, "ref: refs/heads/main\n");
        let wt = tmp.path().join("widgets-feat");
        make_worktree(&wt, &main, "feat", "ref: refs/heads/feature/x\n");
        let idx = LocalIndex::build(&cfg(tmp.path(), 2), &registry());

        let mut e = event("Widgets", Some("refs/heads/feature/x"));
        idx.enrich(&mut e);
        let local = e.local.unwrap();
        assert_eq!(local.repo_path, Some(main.display().to_string()));
        assert_eq!(local.worktree_path, Some(wt.display().to_string()));

        let mut e = event("Widgets", None);
        e.branch = Some(BranchRef {
            name: "refs/heads/main".into(),
            short_name: "main".into(),
        });
        idx.enrich(&mut e);
        assert_eq!(
            e.local.unwrap().worktree_path,
            Some(main.display().to_string())
        );
    }

    #[test]
    fn enrich_detached_head_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("widgets");
        make_clone(&main, URL, "ref: refs/heads/main\n");
        let wt = tmp.path().join("wt");
        make_worktree(
            &wt,
            &main,
            "wt",
            "0123456789abcdef0123456789abcdef01234567\n",
        );
        let idx = LocalIndex::build(&cfg(tmp.path(), 2), &registry());

        let mut e = event("widgets", Some("refs/heads/feature"));
        idx.enrich(&mut e);
        assert_eq!(e.local.unwrap().worktree_path, None);
    }

    #[test]
    fn build_respects_scan_depth() {
        let tmp = tempfile::tempdir().unwrap();
        make_clone(
            &tmp.path().join("a/b/widgets"),
            URL,
            "ref: refs/heads/main\n",
        );

        let mut e = event("widgets", None);
        LocalIndex::build(&cfg(tmp.path(), 2), &registry()).enrich(&mut e);
        assert!(e.local.is_none());

        LocalIndex::build(&cfg(tmp.path(), 3), &registry()).enrich(&mut e);
        assert!(e.local.is_some());
    }

    #[test]
    fn enrich_leaves_local_none_without_match() {
        let tmp = tempfile::tempdir().unwrap();
        make_clone(&tmp.path().join("widgets"), URL, "ref: refs/heads/main\n");
        make_clone(
            &tmp.path().join("node_modules/other"),
            "fake://host/proj/Hidden",
            "ref: refs/heads/main\n",
        );
        let idx = LocalIndex::build(&cfg(tmp.path(), 3), &registry());

        let mut other = event("gadgets", None);
        idx.enrich(&mut other);
        assert!(other.local.is_none());

        let mut skipped = event("hidden", None);
        idx.enrich(&mut skipped);
        assert!(skipped.local.is_none());

        let mut wrong_ns = event("widgets", None);
        wrong_ns.repo.as_mut().unwrap().namespace = Some("elsewhere".into());
        idx.enrich(&mut wrong_ns);
        assert!(wrong_ns.local.is_none());

        let mut wrong_provider = event("widgets", None);
        wrong_provider.connection.provider = "other".into();
        idx.enrich(&mut wrong_provider);
        assert!(wrong_provider.local.is_none());

        let mut e = event("widgets", None);
        LocalIndex::empty().enrich(&mut e);
        assert!(e.local.is_none());
    }
}
