//! Taskbar theme detection. Read-only; the tray icon picks its artwork from it.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskbarTheme {
    Light,
    Dark,
}

/// `SystemUsesLightTheme`: 0 = dark taskbar, nonzero = light. Missing is treated as dark.
pub fn parse_uses_light(value: Option<u32>) -> TaskbarTheme {
    match value {
        Some(v) if v != 0 => TaskbarTheme::Light,
        _ => TaskbarTheme::Dark,
    }
}

/// Windows reads the registry. Linux and macOS assume a dark bar (light icon):
/// there is no cheap, reliable signal for the tray background there.
pub fn current() -> TaskbarTheme {
    parse_uses_light(read_system_uses_light())
}

#[cfg(windows)]
fn read_system_uses_light() -> Option<u32> {
    use windows::Win32::System::Registry::{HKEY_CURRENT_USER, RRF_RT_REG_DWORD, RegGetValueW};
    use windows::core::w;

    let mut data = 0u32;
    let mut size = size_of::<u32>() as u32;
    // SAFETY: `data` and `size` outlive the call and `size` matches the buffer.
    let status = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            w!(r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize"),
            w!("SystemUsesLightTheme"),
            RRF_RT_REG_DWORD,
            None,
            Some((&raw mut data).cast()),
            Some(&raw mut size),
        )
    };
    status.is_ok().then_some(data)
}

#[cfg(not(windows))]
fn read_system_uses_light() -> Option<u32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uses_light_maps_zero_one_and_missing() {
        assert_eq!(parse_uses_light(Some(0)), TaskbarTheme::Dark);
        assert_eq!(parse_uses_light(Some(1)), TaskbarTheme::Light);
        assert_eq!(parse_uses_light(None), TaskbarTheme::Dark);
    }
}
