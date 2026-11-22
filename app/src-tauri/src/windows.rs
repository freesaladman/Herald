//! Toast and pane windows: anchoring, pane sizing, blur-hide and resize persistence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tauri::{
    AppHandle, LogicalSize, Manager, Monitor, PhysicalPosition, WebviewWindow, WindowEvent,
};

use crate::ui_state::{PANE_SIZE_KEY, UiStateCache};

const PANE: &str = "pane";
const TOAST: &str = "toast";
const TOAST_WIDTH: f64 = 320.0;
const MARGIN: f64 = 12.0;
const MIN_PANE: (f64, f64) = (360.0, 480.0);
const PANE_WIDTH_FRACTION: f64 = 0.182;
const PANE_HEIGHT_FRACTION: f64 = 0.417;
const TOGGLE_DEBOUNCE: Duration = Duration::from_millis(300);
const RESIZE_DEBOUNCE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq)]
struct Rect {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Corner {
    TopRight,
    BottomRight,
}

fn anchor_corner() -> Corner {
    if cfg!(windows) {
        Corner::BottomRight
    } else {
        Corner::TopRight
    }
}

/// Top-left physical position of a `size` window tucked into `corner` of `work`.
fn anchor_position(work: Rect, size: (f64, f64), margin: f64, corner: Corner) -> (f64, f64) {
    let x = work.x + work.w - size.0 - margin;
    let y = match corner {
        Corner::TopRight => work.y + margin,
        Corner::BottomRight => work.y + work.h - size.1 - margin,
    };
    (x, y)
}

/// Parses logical "WxH"; rejects non-positive or non-finite values.
fn parse_size(s: &str) -> Option<(f64, f64)> {
    let (w, h) = s.split_once('x')?;
    let (w, h) = (w.trim().parse::<f64>().ok()?, h.trim().parse::<f64>().ok()?);
    (w.is_finite() && h.is_finite() && w > 0.0 && h > 0.0).then_some((w, h))
}

fn format_size((w, h): (f64, f64)) -> String {
    format!("{}x{}", w.round(), h.round())
}

/// Logical pane size: the saved one, else a fraction of the monitor; always within the work area.
fn pane_size(
    saved: Option<(f64, f64)>,
    monitor_physical: (f64, f64),
    scale: f64,
    work_logical: (f64, f64),
) -> (f64, f64) {
    let (w, h) = saved.unwrap_or((
        PANE_WIDTH_FRACTION * monitor_physical.0 / scale,
        PANE_HEIGHT_FRACTION * monitor_physical.1 / scale,
    ));
    let (w, h) = (w.max(MIN_PANE.0), h.max(MIN_PANE.1));
    (
        w.min(work_logical.0 - 2.0 * MARGIN),
        h.min(work_logical.1 - 2.0 * MARGIN),
    )
}

/// Non-finite counts as 0 (hide); otherwise within the work area minus both margins.
fn clamp_toast_height(height: f64, work_logical_height: f64) -> f64 {
    if !height.is_finite() {
        return 0.0;
    }
    height.clamp(0.0, (work_logical_height - 2.0 * MARGIN).max(0.0))
}

/// A toggle right after a blur-hide is the same click that caused the blur.
fn toggle_should_show(last_blur_hide: Option<Instant>, now: Instant) -> bool {
    last_blur_hide.is_none_or(|t| now.saturating_duration_since(t) >= TOGGLE_DEBOUNCE)
}

#[derive(Default)]
struct PaneState {
    last_blur_hide: Mutex<Option<Instant>>,
    last_applied: Mutex<Option<(f64, f64)>>,
    resize_gen: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn work_rect(m: &Monitor) -> Rect {
    let wa = m.work_area();
    Rect {
        x: f64::from(wa.position.x),
        y: f64::from(wa.position.y),
        w: f64::from(wa.size.width),
        h: f64::from(wa.size.height),
    }
}

fn primary_monitor(app: &AppHandle) -> Option<Monitor> {
    match app.primary_monitor() {
        Ok(Some(m)) => Some(m),
        Ok(None) => {
            tracing::warn!("no primary monitor");
            None
        }
        Err(e) => {
            tracing::warn!(error = %e, "primary monitor lookup failed");
            None
        }
    }
}

/// Sizes (logical) and anchors the window on the monitor's work area.
fn place(win: &WebviewWindow, monitor: &Monitor, size: (f64, f64)) {
    let scale = monitor.scale_factor();
    let phys = (size.0 * scale, size.1 * scale);
    let (x, y) = anchor_position(work_rect(monitor), phys, MARGIN * scale, anchor_corner());
    let sized = win.set_size(LogicalSize::new(size.0, size.1));
    let moved = win.set_position(PhysicalPosition::new(x.round() as i32, y.round() as i32));
    if let Err(e) = sized.and(moved) {
        tracing::warn!(error = %e, "positioning window failed");
    }
}

fn saved_pane_size(app: &AppHandle) -> Option<(f64, f64)> {
    parse_size(&app.try_state::<UiStateCache>()?.get(PANE_SIZE_KEY)?)
}

pub fn show_pane(app: &AppHandle) {
    let Some(pane) = app.get_webview_window(PANE) else {
        return;
    };
    if let Some(monitor) = primary_monitor(app) {
        let scale = monitor.scale_factor();
        let work = work_rect(&monitor);
        let size = pane_size(
            saved_pane_size(app),
            (
                f64::from(monitor.size().width),
                f64::from(monitor.size().height),
            ),
            scale,
            (work.w / scale, work.h / scale),
        );
        *lock(&app.state::<PaneState>().last_applied) = Some(size);
        place(&pane, &monitor, size);
    }
    let _ = pane.show();
    let _ = pane.set_focus();
}

pub fn hide_pane(app: &AppHandle) {
    if let Some(pane) = app.get_webview_window(PANE) {
        let _ = pane.hide();
    }
}

pub fn toggle_pane(app: &AppHandle) {
    let Some(pane) = app.get_webview_window(PANE) else {
        return;
    };
    if pane.is_visible().unwrap_or(false) {
        hide_pane(app);
        return;
    }
    let last = *lock(&app.state::<PaneState>().last_blur_hide);
    if toggle_should_show(last, Instant::now()) {
        show_pane(app);
    }
}

/// Height > 0 shows the toast stack without taking focus; 0 hides it.
pub fn position_toast(app: &AppHandle, height_logical: f64) {
    let Some(toast) = app.get_webview_window(TOAST) else {
        return;
    };
    let Some(monitor) = primary_monitor(app) else {
        if clamp_toast_height(height_logical, f64::MAX) <= 0.0 {
            let _ = toast.hide();
        }
        return;
    };
    let work_h = work_rect(&monitor).h / monitor.scale_factor();
    let height = clamp_toast_height(height_logical, work_h);
    if height <= 0.0 {
        let _ = toast.hide();
        return;
    }
    place(&toast, &monitor, (TOAST_WIDTH, height));
    let _ = toast.show();
}

fn on_pane_event(app: &AppHandle, event: &WindowEvent) {
    match event {
        WindowEvent::Focused(false) => blur_hide(app),
        WindowEvent::Resized(_) => schedule_persist(app),
        _ => {}
    }
}

fn blur_hide(app: &AppHandle) {
    if app.try_state::<UiStateCache>().is_some_and(|c| c.pinned()) {
        return;
    }
    *lock(&app.state::<PaneState>().last_blur_hide) = Some(Instant::now());
    hide_pane(app);
}

fn schedule_persist(app: &AppHandle) {
    let gen_now = app
        .state::<PaneState>()
        .resize_gen
        .fetch_add(1, Ordering::SeqCst)
        + 1;
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(RESIZE_DEBOUNCE).await;
        if app.state::<PaneState>().resize_gen.load(Ordering::SeqCst) == gen_now {
            persist_pane_size(&app);
        }
    });
}

fn persist_pane_size(app: &AppHandle) {
    let Some(pane) = app.get_webview_window(PANE) else {
        return;
    };
    if !pane.is_visible().unwrap_or(false) {
        return;
    }
    let (Ok(size), Ok(scale)) = (pane.inner_size(), pane.scale_factor()) else {
        return;
    };
    let logical = (
        f64::from(size.width) / scale,
        f64::from(size.height) / scale,
    );
    let applied = *lock(&app.state::<PaneState>().last_applied);
    // The resize show_pane applied itself is not a user choice.
    if applied.is_some_and(|a| (a.0 - logical.0).abs() < 1.5 && (a.1 - logical.1).abs() < 1.5) {
        return;
    }
    if let Some(cache) = app.try_state::<UiStateCache>() {
        cache.set(PANE_SIZE_KEY, &format_size(logical));
    }
}

/// Manages the pane state and hooks the pane's window events.
pub fn register(app: &AppHandle) {
    app.manage(PaneState::default());
    match app.get_webview_window(PANE) {
        Some(pane) => {
            let handle = app.clone();
            pane.on_window_event(move |event| on_pane_event(&handle, event));
        }
        None => tracing::warn!("pane window missing; events not registered"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORK: Rect = Rect {
        x: 0.0,
        y: 0.0,
        w: 1920.0,
        h: 1040.0,
    };

    #[test]
    fn parse_size_accepts_wxh_and_rejects_junk() {
        assert_eq!(parse_size("400x600"), Some((400.0, 600.0)));
        assert_eq!(parse_size("400.5x600"), Some((400.5, 600.0)));
        for bad in ["", "400", "0x600", "-1x5", "axb", "400x", "NaNx5"] {
            assert_eq!(parse_size(bad), None, "{bad}");
        }
        assert_eq!(
            parse_size(&format_size((400.4, 599.6))),
            Some((400.0, 600.0))
        );
    }

    #[test]
    fn pane_size_default_uses_fractions_with_minimum() {
        // 1920x1080 @1x: 349 -> min 360 wide, 450 -> min 480 tall.
        assert_eq!(
            pane_size(None, (1920.0, 1080.0), 1.0, (1920.0, 1040.0)),
            MIN_PANE
        );
        let (w, h) = pane_size(None, (3840.0, 2160.0), 1.5, (2560.0, 1400.0));
        assert!((w - 465.92).abs() < 0.01 && (h - 600.48).abs() < 0.01);
    }

    #[test]
    fn pane_size_saved_wins_but_is_clamped_to_work_area() {
        assert_eq!(
            pane_size(
                Some((500.0, 700.0)),
                (1920.0, 1080.0),
                1.0,
                (1920.0, 1040.0)
            ),
            (500.0, 700.0)
        );
        assert_eq!(
            pane_size(
                Some((5000.0, 5000.0)),
                (1920.0, 1080.0),
                1.0,
                (800.0, 600.0)
            ),
            (776.0, 576.0)
        );
        // A tiny work area beats the default minimum.
        assert_eq!(
            pane_size(None, (800.0, 600.0), 1.0, (300.0, 300.0)),
            (276.0, 276.0)
        );
    }

    #[test]
    fn pane_size_saved_gets_the_minimum_too() {
        assert_eq!(
            pane_size(
                Some((100.0, 100.0)),
                (1920.0, 1080.0),
                1.0,
                (1920.0, 1040.0)
            ),
            MIN_PANE
        );
    }

    #[test]
    fn clamp_toast_height_hides_bad_values_and_fits_work_area() {
        assert_eq!(clamp_toast_height(f64::NAN, 1000.0), 0.0);
        assert_eq!(clamp_toast_height(f64::INFINITY, 1000.0), 0.0);
        assert_eq!(clamp_toast_height(-5.0, 1000.0), 0.0);
        assert_eq!(clamp_toast_height(200.0, 1000.0), 200.0);
        assert_eq!(clamp_toast_height(5000.0, 1000.0), 976.0);
        assert_eq!(clamp_toast_height(50.0, 10.0), 0.0);
    }

    #[test]
    fn anchor_position_bottom_right_and_top_right() {
        let size = (640.0, 200.0);
        assert_eq!(
            anchor_position(WORK, size, 24.0, Corner::BottomRight),
            (1920.0 - 640.0 - 24.0, 1040.0 - 200.0 - 24.0)
        );
        assert_eq!(
            anchor_position(WORK, size, 24.0, Corner::TopRight),
            (1920.0 - 640.0 - 24.0, 24.0)
        );
    }

    #[test]
    fn anchor_position_respects_work_area_offset() {
        let work = Rect {
            x: -1920.0,
            y: 40.0,
            w: 1920.0,
            h: 1000.0,
        };
        assert_eq!(
            anchor_position(work, (100.0, 100.0), 10.0, Corner::TopRight),
            (-110.0, 50.0)
        );
    }

    #[test]
    fn toggle_is_ignored_only_shortly_after_a_blur_hide() {
        let t0 = Instant::now();
        assert!(toggle_should_show(None, t0));
        assert!(!toggle_should_show(
            Some(t0),
            t0 + Duration::from_millis(299)
        ));
        assert!(toggle_should_show(
            Some(t0),
            t0 + Duration::from_millis(300)
        ));
    }
}
