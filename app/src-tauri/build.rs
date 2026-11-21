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
    "list_providers",
    "get_connection_draft",
    "test_draft_connection",
    "list_draft_scopes",
    "save_connection",
    "remove_connection",
];

fn main() {
    tauri_build::try_build(
        tauri_build::Attributes::new()
            .app_manifest(tauri_build::AppManifest::new().commands(COMMANDS)),
    )
    .expect("failed to run tauri-build");
}
