//! Tray icon: dot/warning variants, tooltip and the context menu.

use std::sync::{Mutex, PoisonError};

use chrono::{DateTime, Duration, Local, NaiveTime, SecondsFormat, TimeZone, Utc};
use herald_core::provider::AuthState;
use image::{Pixel, Rgba, RgbaImage};
use tauri::image::Image;
use tauri::menu::{CheckMenuItem, Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use tauri::tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Listener, Manager};
use tauri_plugin_autostart::ManagerExt as _;
use tauri_plugin_opener::OpenerExt as _;
use tokio::sync::watch;

use crate::theme::{self, TaskbarTheme};
use crate::ui_state::{MUTED_KEY, SNOOZE_KEY, UiStateCache};
use crate::{EngineHandle, commands, windows};

const TRAY_ID: &str = "herald";
const ACCENT: [u8; 3] = [0x3B, 0x82, 0xF6];
const AMBER: [u8; 3] = [0xF5, 0xA5, 0x24];
const DOT_DIAMETER: f32 = 0.35;
const FALLBACK_ICON_SIZE: u32 = 32;
const THEME_POLL: std::time::Duration = std::time::Duration::from_secs(10);
// Artwork is named by the taskbar it suits: a dark taskbar gets the light-coloured set.
const ART_FOR_DARK_TASKBAR: &[u8] = include_bytes!("../icons/tray/tray-light.png");
const ART_FOR_LIGHT_TASKBAR: &[u8] = include_bytes!("../icons/tray/tray-dark.png");
const TOMORROW_HOUR: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IconVariant {
    Base,
    Dot,
    Warning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnoozeChoice {
    Minutes(i64),
    UntilTomorrow,
}

fn icon_variant(auth_needed: bool, unviewed: u32) -> IconVariant {
    if auth_needed {
        IconVariant::Warning
    } else if unviewed > 0 {
        IconVariant::Dot
    } else {
        IconVariant::Base
    }
}

fn tooltip_text(variant: IconVariant, unviewed: u32) -> String {
    match variant {
        IconVariant::Warning => "Herald: credentials needed".into(),
        _ if unviewed > 0 => format!("Herald: {unviewed} unviewed"),
        _ => "Herald: no new activity".into(),
    }
}

/// Generic over the zone so tests can use a fixed offset.
fn snooze_until<Tz: TimeZone>(choice: SnoozeChoice, now: DateTime<Tz>) -> DateTime<Utc> {
    match choice {
        SnoozeChoice::Minutes(m) => now.with_timezone(&Utc) + Duration::minutes(m),
        SnoozeChoice::UntilTomorrow => {
            let tomorrow = now.date_naive() + Duration::days(1);
            let at =
                tomorrow.and_time(NaiveTime::from_hms_opt(TOMORROW_HOUR, 0, 0).unwrap_or_default());
            // A DST gap at 08:00 has no local time; fall back to a fixed 16 hours.
            now.timezone()
                .from_local_datetime(&at)
                .earliest()
                .map_or_else(
                    || now.with_timezone(&Utc) + Duration::hours(16),
                    |t| t.with_timezone(&Utc),
                )
        }
    }
}

fn format_utc(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn is_snoozed(value: Option<&str>, now: DateTime<Utc>) -> bool {
    value
        .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
        .is_some_and(|until| until.with_timezone(&Utc) > now)
}

/// Time left until the snooze ends; `None` when unset, unparsable or already over.
fn snooze_remaining(value: Option<&str>, now: DateTime<Utc>) -> Option<std::time::Duration> {
    let until = DateTime::parse_from_rfc3339(value?).ok()?;
    (until.with_timezone(&Utc) - now)
        .to_std()
        .ok()
        .filter(|d| !d.is_zero())
}

/// Draws a filled circle in the bottom-right of the RGBA buffer, edge-blended.
fn composite_dot(rgba: &[u8], width: u32, height: u32, color: [u8; 3]) -> Vec<u8> {
    let Some(mut img) = RgbaImage::from_raw(width, height, rgba.to_vec()) else {
        return rgba.to_vec();
    };
    let size = width.min(height) as f32;
    let radius = size * DOT_DIAMETER / 2.0;
    let margin = size * 0.03;
    let cx = width as f32 - radius - margin;
    let cy = height as f32 - radius - margin;
    for (x, y, px) in img.enumerate_pixels_mut() {
        let d = ((x as f32 + 0.5 - cx).powi(2) + (y as f32 + 0.5 - cy).powi(2)).sqrt();
        let coverage = (radius + 0.5 - d).clamp(0.0, 1.0);
        if coverage > 0.0 {
            let alpha = (coverage * 255.0).round() as u8;
            px.blend(&Rgba([color[0], color[1], color[2], alpha]));
        }
    }
    img.into_raw()
}

fn art_for(theme: TaskbarTheme) -> &'static [u8] {
    match theme {
        TaskbarTheme::Dark => ART_FOR_DARK_TASKBAR,
        TaskbarTheme::Light => ART_FOR_LIGHT_TASKBAR,
    }
}

/// Index into `Icons::images`: theme-major, then base/dot/warning.
fn icon_slot(theme: TaskbarTheme, variant: IconVariant) -> usize {
    let t = match theme {
        TaskbarTheme::Dark => 0,
        TaskbarTheme::Light => 3,
    };
    t + match variant {
        IconVariant::Base => 0,
        IconVariant::Dot => 1,
        IconVariant::Warning => 2,
    }
}

fn decode_art(png: &[u8]) -> Option<(Vec<u8>, u32, u32)> {
    let img = image::load_from_memory(png).ok()?.into_rgba8();
    let (w, h) = img.dimensions();
    Some((img.into_raw(), w, h))
}

fn theme_images(theme: TaskbarTheme) -> [Image<'static>; 3] {
    let (rgba, w, h) = decode_art(art_for(theme)).unwrap_or_else(|| {
        tracing::warn!("tray artwork failed to decode; using a plain tray icon");
        let n = FALLBACK_ICON_SIZE;
        (vec![0x80; (n * n * 4) as usize], n, n)
    });
    let variant = |color| Image::new_owned(composite_dot(&rgba, w, h, color), w, h);
    [
        Image::new_owned(rgba.clone(), w, h),
        variant(ACCENT),
        variant(AMBER),
    ]
}

struct Icons {
    images: [Image<'static>; 6],
}

impl Icons {
    fn build() -> Self {
        let [a, b, c] = theme_images(TaskbarTheme::Dark);
        let [d, e, f] = theme_images(TaskbarTheme::Light);
        Self {
            images: [a, b, c, d, e, f],
        }
    }

    fn get(&self, theme: TaskbarTheme, v: IconVariant) -> Image<'static> {
        self.images[icon_slot(theme, v)].clone()
    }
}

/// Menu items whose state follows the store; kept for refreshes. Managed state.
pub struct TrayState {
    _tray: TrayIcon,
    mute: CheckMenuItem<tauri::Wry>,
    autostart: CheckMenuItem<tauri::Wry>,
    resume: MenuItem<tauri::Wry>,
    task: Mutex<Option<tauri::async_runtime::JoinHandle<()>>>,
}

fn write_ui_state(app: &AppHandle, key: &'static str, value: &str) {
    if let Some(cache) = app.try_state::<UiStateCache>() {
        cache.set(key, value);
    }
    commands::emit_ui_state(app);
    refresh_menu(app);
}

fn refresh_menu(app: &AppHandle) {
    let Some(state) = app.try_state::<TrayState>() else {
        return;
    };
    let cache = app.try_state::<UiStateCache>();
    let _ = state
        .mute
        .set_checked(cache.as_ref().is_some_and(|c| c.muted()));
    let snoozed = cache.as_ref().and_then(|c| c.get(SNOOZE_KEY));
    let _ = state
        .resume
        .set_enabled(is_snoozed(snoozed.as_deref(), Utc::now()));
    let autostart = app.autolaunch().is_enabled().unwrap_or(false);
    let _ = state.autostart.set_checked(autostart);
}

fn snooze(app: &AppHandle, choice: SnoozeChoice) {
    write_ui_state(
        app,
        SNOOZE_KEY,
        &format_utc(snooze_until(choice, Local::now())),
    );
}

fn toggle_mute(app: &AppHandle) {
    let muted = app.try_state::<UiStateCache>().is_some_and(|c| c.muted());
    write_ui_state(app, MUTED_KEY, if muted { "false" } else { "true" });
}

fn toggle_autostart(app: &AppHandle) {
    let manager = app.autolaunch();
    let result = match manager.is_enabled() {
        Ok(true) => manager.disable(),
        Ok(false) => manager.enable(),
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        tracing::warn!(error = %e, "toggling autostart failed");
    }
    refresh_menu(app);
}

fn open_config_folder(app: &AppHandle) {
    let Some(engine) = app.try_state::<EngineHandle>().and_then(|h| h.get()) else {
        return;
    };
    let home = engine.paths().home.to_string_lossy().into_owned();
    if let Err(e) = app.opener().open_path(home, None::<&str>) {
        tracing::warn!(error = %e, "opening config folder failed");
    }
}

fn on_menu_event(app: &AppHandle, event: MenuEvent) {
    match event.id().as_ref() {
        "open" => windows::show_pane(app),
        "snooze_30" => snooze(app, SnoozeChoice::Minutes(30)),
        "snooze_60" => snooze(app, SnoozeChoice::Minutes(60)),
        "snooze_tomorrow" => snooze(app, SnoozeChoice::UntilTomorrow),
        "resume" => write_ui_state(app, SNOOZE_KEY, ""),
        "mute" => toggle_mute(app),
        "autostart" => toggle_autostart(app),
        "config_folder" => open_config_folder(app),
        "reload" => crate::reload_config_now(app),
        "poll" => {
            if let Some(engine) = app.try_state::<EngineHandle>().and_then(|h| h.get()) {
                engine.poll_now();
            }
        }
        "quit" => crate::request_quit(app),
        _ => {}
    }
}

fn auth_needed(statuses: &[herald_core::scheduler::ConnectionStatus]) -> bool {
    statuses
        .iter()
        .any(|s| matches!(s.auth_state, AuthState::Missing | AuthState::Invalid))
}

fn apply(
    tray: &TrayIcon,
    icons: &Icons,
    statuses: &[herald_core::scheduler::ConnectionStatus],
    unviewed: u32,
    theme: TaskbarTheme,
    shown: &mut Option<(TaskbarTheme, IconVariant)>,
) {
    let variant = icon_variant(auth_needed(statuses), unviewed);
    if *shown != Some((theme, variant)) {
        match tray.set_icon(Some(icons.get(theme, variant))) {
            Ok(()) => *shown = Some((theme, variant)),
            Err(e) => tracing::warn!(error = %e, "setting tray icon failed"),
        }
    }
    if let Err(e) = tray.set_tooltip(Some(tooltip_text(variant, unviewed))) {
        tracing::warn!(error = %e, "setting tray tooltip failed");
    }
}

pub fn setup(app: &AppHandle) -> tauri::Result<()> {
    let item = |id: &str, text: &str, enabled: bool| {
        MenuItem::with_id(app, id, text, enabled, None::<&str>)
    };
    let open = item("open", "Open", true)?;
    let resume = item("resume", "Resume", false)?;
    let snooze_menu = Submenu::with_items(
        app,
        "Snooze",
        true,
        &[
            &item("snooze_30", "30 minutes", true)?,
            &item("snooze_60", "1 hour", true)?,
            &item("snooze_tomorrow", "Until tomorrow 08:00", true)?,
            &resume,
        ],
    )?;
    let mute = CheckMenuItem::with_id(app, "mute", "Mute sound", true, false, None::<&str>)?;
    let autostart = CheckMenuItem::with_id(
        app,
        "autostart",
        "Start at login",
        true,
        false,
        None::<&str>,
    )?;
    let menu = Menu::with_items(
        app,
        &[
            &open,
            &snooze_menu,
            &mute,
            &autostart,
            &item("config_folder", "Open config folder", true)?,
            &item("reload", "Reload config", true)?,
            &item("poll", "Poll now", true)?,
            &PredefinedMenuItem::separator(app)?,
            &item("quit", "Quit", true)?,
        ],
    )?;

    let icons = Icons::build();
    let initial_theme = theme::current();
    let tray = TrayIconBuilder::with_id(TRAY_ID)
        .icon(icons.get(initial_theme, IconVariant::Base))
        .tooltip("Herald")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(on_menu_event)
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                windows::toggle_pane(tray.app_handle());
            }
        })
        .build(app)?;

    // The refresher emits the real count once the listener below exists.
    let (unviewed_tx, unviewed_rx) = watch::channel(0);
    app.listen(commands::EVENT_UNVIEWED_CHANGED, move |ev| {
        if let Ok(n) = serde_json::from_str::<u32>(ev.payload()) {
            let _ = unviewed_tx.send(n);
        }
    });
    let (ui_tx, ui_rx) = watch::channel(());
    app.listen(commands::EVENT_UI_STATE, move |_| {
        let _ = ui_tx.send(());
    });
    let task = app
        .try_state::<EngineHandle>()
        .and_then(|h| h.get())
        .map(|engine| {
            let status_rx = engine.status_changes();
            spawn_updater(
                app.clone(),
                tray.clone(),
                icons,
                status_rx,
                unviewed_rx,
                ui_rx,
                initial_theme,
            )
        });

    app.manage(TrayState {
        _tray: tray,
        mute,
        autostart,
        resume,
        task: Mutex::new(task),
    });
    refresh_menu(app);
    Ok(())
}

fn spawn_updater(
    app: AppHandle,
    tray: TrayIcon,
    icons: Icons,
    mut status_rx: watch::Receiver<Vec<herald_core::scheduler::ConnectionStatus>>,
    mut unviewed_rx: watch::Receiver<u32>,
    mut ui_rx: watch::Receiver<()>,
    initial_theme: TaskbarTheme,
) -> tauri::async_runtime::JoinHandle<()> {
    tauri::async_runtime::spawn(async move {
        let mut theme_tick = tokio::time::interval(THEME_POLL);
        let mut shown = Some((initial_theme, IconVariant::Base));
        let mut dirty = true;
        loop {
            if dirty {
                let unviewed = *unviewed_rx.borrow_and_update();
                apply(
                    &tray,
                    &icons,
                    &status_rx.borrow_and_update(),
                    unviewed,
                    theme::current(),
                    &mut shown,
                );
                refresh_menu(&app);
                ui_rx.borrow_and_update();
            }
            dirty = true;
            let snoozed_until = app
                .try_state::<UiStateCache>()
                .and_then(|c| c.get(SNOOZE_KEY));
            let wake = snooze_remaining(snoozed_until.as_deref(), Utc::now())
                .map(|d| tokio::time::Instant::now() + d);
            tokio::select! {
                r = status_rx.changed() => if r.is_err() { break },
                r = unviewed_rx.changed() => if r.is_err() { break },
                r = ui_rx.changed() => if r.is_err() { break },
                _ = theme_tick.tick() => {
                    // Cheap registry read; only re-apply when the theme actually moved.
                    dirty = shown.is_some_and(|(t, _)| t != theme::current());
                }
                () = sleep_until_or_pending(wake) => {
                    refresh_menu(&app);
                    commands::emit_ui_state(&app);
                }
            }
        }
    })
}

async fn sleep_until_or_pending(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

pub fn abort(app: &AppHandle) {
    if let Some(state) = app.try_state::<TrayState>()
        && let Some(h) = state
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    {
        h.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::FixedOffset;

    #[test]
    fn icon_variant_prefers_warning_then_dot() {
        assert_eq!(icon_variant(true, 5), IconVariant::Warning);
        assert_eq!(icon_variant(false, 1), IconVariant::Dot);
        assert_eq!(icon_variant(false, 0), IconVariant::Base);
    }

    #[test]
    fn icon_slot_is_unique_per_theme_and_variant() {
        let mut slots = Vec::new();
        for t in [TaskbarTheme::Dark, TaskbarTheme::Light] {
            for v in [IconVariant::Base, IconVariant::Dot, IconVariant::Warning] {
                slots.push(icon_slot(t, v));
            }
        }
        slots.sort_unstable();
        assert_eq!(slots, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn embedded_art_decodes_to_64x64_rgba_and_differs_per_theme() {
        for theme in [TaskbarTheme::Dark, TaskbarTheme::Light] {
            let (rgba, w, h) = decode_art(art_for(theme)).unwrap();
            assert_eq!((w, h), (64, 64));
            assert_eq!(rgba.len(), 64 * 64 * 4);
        }
        assert_ne!(art_for(TaskbarTheme::Dark), art_for(TaskbarTheme::Light));
    }

    #[test]
    fn tooltip_text_matches_variant() {
        assert_eq!(tooltip_text(IconVariant::Dot, 3), "Herald: 3 unviewed");
        assert_eq!(
            tooltip_text(IconVariant::Base, 0),
            "Herald: no new activity"
        );
        assert_eq!(
            tooltip_text(IconVariant::Warning, 3),
            "Herald: credentials needed"
        );
    }

    #[test]
    fn snooze_until_adds_minutes_and_finds_tomorrow_8_local() {
        let tz = FixedOffset::west_opt(5 * 3600).unwrap();
        let now = tz.with_ymd_and_hms(2026, 11, 20, 22, 15, 0).unwrap();
        assert_eq!(
            format_utc(snooze_until(SnoozeChoice::Minutes(30), now)),
            "2026-11-21T03:45:00.000Z"
        );
        // 08:00 at UTC-5 on the 21st
        assert_eq!(
            format_utc(snooze_until(SnoozeChoice::UntilTomorrow, now)),
            "2026-11-21T13:00:00.000Z"
        );
    }

    #[test]
    fn is_snoozed_treats_empty_and_elapsed_as_not_snoozed() {
        let now = Utc.with_ymd_and_hms(2026, 11, 20, 12, 0, 0).unwrap();
        assert!(is_snoozed(Some("2026-11-20T12:30:00.000Z"), now));
        assert!(!is_snoozed(Some("2026-11-20T11:30:00.000Z"), now));
        assert!(!is_snoozed(Some(""), now));
        assert!(!is_snoozed(None, now));
    }

    #[test]
    fn snooze_remaining_is_some_only_for_a_future_end() {
        let now = Utc.with_ymd_and_hms(2026, 11, 20, 12, 0, 0).unwrap();
        assert_eq!(
            snooze_remaining(Some("2026-11-20T12:30:00.000Z"), now),
            Some(std::time::Duration::from_secs(1800))
        );
        assert_eq!(
            snooze_remaining(Some("2026-11-20T12:00:00.000Z"), now),
            None
        );
        assert_eq!(
            snooze_remaining(Some("2026-11-20T11:30:00.000Z"), now),
            None
        );
        assert_eq!(snooze_remaining(Some(""), now), None);
        assert_eq!(snooze_remaining(None, now), None);
    }

    #[test]
    fn composite_dot_paints_bottom_right_and_keeps_size() {
        let (w, h) = (64, 64);
        let base = vec![0u8; (w * h * 4) as usize];
        let out = composite_dot(&base, w, h, ACCENT);
        assert_eq!(out.len(), base.len());
        let size = 64.0_f32;
        let r = size * DOT_DIAMETER / 2.0;
        let c = (size - r - size * 0.03) as usize;
        let i = (c * w as usize + c) * 4;
        assert_eq!(&out[i..i + 4], &[ACCENT[0], ACCENT[1], ACCENT[2], 255]);
        assert_eq!(&out[0..4], &[0, 0, 0, 0]);
    }
}
