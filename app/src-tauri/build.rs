const COMMANDS: &[&str] = &[
    "list_unviewed",
    "unviewed_count",
    "mark_viewed",
    "mark_all_viewed",
    "activate_record",
    "get_ui_state",
    "set_pinned",
    "list_connections",
    "set_secret",
    "clear_secret",
    "test_connection",
    "toast_clicked",
    "toast_hover",
    "toast_resize",
    "hide_pane",
];

fn main() {
    tauri_build::try_build(
        tauri_build::Attributes::new()
            .app_manifest(tauri_build::AppManifest::new().commands(COMMANDS)),
    )
    .expect("failed to run tauri-build");
}
