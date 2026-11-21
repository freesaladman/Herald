//! Ties child processes to Herald's lifetime.
//!
//! On Windows, children assigned to a process-wide kill-on-close Job Object die with every
//! descendant when Herald exits for any reason, including a crash or kill. The job handle is
//! never closed on purpose: the OS closes it at process exit. Elsewhere this does nothing.
//!
//! Assignment is per child rather than putting Herald itself in the job, so URL handlers and
//! detached hooks (which are simply never assigned) are not caught by it.
//!
//! The job allows breakaway (`JOB_OBJECT_LIMIT_BREAKAWAY_OK`), so a descendant that explicitly
//! asks to break away (some browsers and launchers do) escapes the job. That is intended.

/// Adds `child` to the kill-on-exit job. Failures are logged once and otherwise ignored.
pub fn assign_to_kill_on_exit_job(child: &tokio::process::Child) {
    #[cfg(windows)]
    imp::assign(child);
    #[cfg(not(windows))]
    let _ = child;
}

use std::process::Stdio;
use std::time::Duration;

const KILLER_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(any(windows, unix))]
fn run_killer(cmd: &mut std::process::Command) {
    let spawned = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    let mut child = match spawned {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "could not start the process tree killer");
            return;
        }
    };
    let deadline = std::time::Instant::now() + KILLER_TIMEOUT;
    while matches!(child.try_wait(), Ok(None)) {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Kills `pid` and its descendants (taskkill /T on Windows; on Unix the pid must be a
/// process group leader). Blocks for at most a few seconds.
#[cfg(windows)]
pub fn kill_tree(pid: u32) {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let root = std::env::var_os("SystemRoot").map_or_else(
        || std::path::PathBuf::from(r"C:\Windows"),
        std::path::PathBuf::from,
    );
    run_killer(
        std::process::Command::new(root.join("System32").join("taskkill.exe"))
            .args(["/T", "/F", "/PID", &pid.to_string()])
            .creation_flags(CREATE_NO_WINDOW),
    );
}

/// Children are spawned as their own process group leader, so the pid is the group id.
#[cfg(unix)]
pub fn kill_tree(pid: u32) {
    run_killer(std::process::Command::new("/bin/kill").args(["-KILL", &format!("-{pid}")]));
}

#[cfg(not(any(windows, unix)))]
pub fn kill_tree(_pid: u32) {}

#[cfg(windows)]
mod imp {
    use std::sync::{Once, OnceLock};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject,
    };

    static JOB: OnceLock<Option<isize>> = OnceLock::new();
    static WARNED: Once = Once::new();

    fn warn_once(what: &str, error: &windows::core::Error) {
        WARNED.call_once(|| {
            tracing::warn!(%error, "{what}; child processes may outlive Herald if it crashes");
        });
    }

    fn create() -> Option<isize> {
        // SAFETY: plain Win32 calls; the info struct outlives the call and its size is passed.
        unsafe {
            let job = match CreateJobObjectW(None, windows::core::PCWSTR::null()) {
                Ok(j) => j,
                Err(e) => {
                    warn_once("could not create the kill-on-exit job object", &e);
                    return None;
                }
            };
            let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            info.BasicLimitInformation.LimitFlags =
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK;
            let set = SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&info).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            if let Err(e) = set {
                warn_once("could not configure the kill-on-exit job object", &e);
                return None;
            }
            Some(job.0 as isize)
        }
    }

    pub fn job_handle() -> Option<HANDLE> {
        JOB.get_or_init(create).map(|h| HANDLE(h as *mut _))
    }

    pub fn assign(child: &tokio::process::Child) {
        let (Some(job), Some(process)) = (job_handle(), child.raw_handle()) else {
            return;
        };
        // SAFETY: both handles are valid for the call; the child handle is owned by `child`.
        let result = unsafe { AssignProcessToJobObject(job, HANDLE(process)) };
        if let Err(e) = result {
            warn_once("could not add a child to the kill-on-exit job object", &e);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::process::Stdio;
        use windows::Win32::System::JobObjects::IsProcessInJob;
        use windows::core::BOOL;

        fn sleeper() -> tokio::process::Command {
            let mut cmd = tokio::process::Command::new("ping");
            cmd.args(["-n", "30", "127.0.0.1"])
                .stdout(Stdio::null())
                .kill_on_drop(true);
            cmd
        }

        fn in_job(child: &tokio::process::Child) -> bool {
            let job = job_handle().expect("job created");
            let mut result = BOOL(0);
            // SAFETY: valid process and job handles; `result` outlives the call.
            unsafe {
                IsProcessInJob(
                    HANDLE(child.raw_handle().expect("handle")),
                    Some(job),
                    &mut result,
                )
                .expect("IsProcessInJob");
            }
            result.as_bool()
        }

        #[tokio::test]
        async fn assigned_child_is_in_the_job_and_unassigned_is_not() {
            let assigned = sleeper().spawn().expect("spawn");
            let detached = sleeper().spawn().expect("spawn");
            crate::process::assign_to_kill_on_exit_job(&assigned);
            assert!(in_job(&assigned));
            assert!(!in_job(&detached));
        }
    }
}

#[cfg(all(test, windows))]
mod kill_tests {
    use super::*;

    #[tokio::test]
    async fn kill_tree_ends_the_child() {
        let mut child = tokio::process::Command::new("cmd")
            .args(["/D", "/C", "ping -n 30 127.0.0.1 >NUL"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn");
        let pid = child.id().expect("pid");
        tokio::task::spawn_blocking(move || kill_tree(pid))
            .await
            .expect("join");
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
        assert!(status.is_ok(), "child still running after kill_tree");
    }
}
