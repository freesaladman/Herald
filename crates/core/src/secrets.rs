//! Secret storage behind a trait so the engine and providers stay OS-agnostic.
//! Values are never logged, formatted into errors or shown by `Debug`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret store failure: {0}")]
    Backend(String),
    #[error("no native secret store is available")]
    NoNativeStore,
}

pub trait SecretStore: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<String>, SecretError>;
    fn set(&self, key: &str, value: &str) -> Result<(), SecretError>;
    /// Deleting a missing key is Ok.
    fn delete(&self, key: &str) -> Result<(), SecretError>;
    fn is_native(&self) -> bool;
}

pub struct KeyringSecretStore {
    service: String,
}

impl KeyringSecretStore {
    pub fn new() -> Self {
        Self::with_service("herald")
    }

    pub fn with_service(service: &str) -> Self {
        Self {
            service: service.to_owned(),
        }
    }

    fn entry(&self, key: &str) -> Result<keyring::Entry, SecretError> {
        keyring::Entry::new(&self.service, key).map_err(map_error)
    }
}

impl Default for KeyringSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for KeyringSecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyringSecretStore")
            .field("service", &self.service)
            .finish()
    }
}

fn map_error(e: keyring::Error) -> SecretError {
    match e {
        keyring::Error::NoDefaultStore => SecretError::NoNativeStore,
        // Drop the payload: it is the raw stored bytes.
        keyring::Error::BadEncoding(_) => {
            SecretError::Backend("stored value is not valid UTF-8".into())
        }
        other => SecretError::Backend(other.to_string()),
    }
}

impl SecretStore for KeyringSecretStore {
    fn get(&self, key: &str) -> Result<Option<String>, SecretError> {
        match self.entry(key)?.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(map_error(e)),
        }
    }

    fn set(&self, key: &str, value: &str) -> Result<(), SecretError> {
        self.entry(key)?.set_password(value).map_err(map_error)
    }

    fn delete(&self, key: &str) -> Result<(), SecretError> {
        match self.entry(key)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(map_error(e)),
        }
    }

    fn is_native(&self) -> bool {
        keyring::Entry::store_status().is_ok()
    }
}

/// In-memory store for tests in any crate.
#[derive(Default)]
pub struct MemorySecretStore {
    values: Mutex<HashMap<String, String>>,
    native: bool,
}

impl MemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_native(native: bool) -> Self {
        Self {
            values: Mutex::default(),
            native,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.values.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl fmt::Debug for MemorySecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySecretStore")
            .field("keys", &self.lock().len())
            .field("native", &self.native)
            .finish()
    }
}

impl SecretStore for MemorySecretStore {
    fn get(&self, key: &str) -> Result<Option<String>, SecretError> {
        Ok(self.lock().get(key).cloned())
    }

    fn set(&self, key: &str, value: &str) -> Result<(), SecretError> {
        self.lock().insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), SecretError> {
        self.lock().remove(key);
        Ok(())
    }

    fn is_native(&self) -> bool {
        self.native
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_set_get_overwrite_delete() {
        let s = MemorySecretStore::new();
        assert_eq!(s.get("a").unwrap(), None);
        s.set("a", "one").unwrap();
        s.set("a", "two").unwrap();
        assert_eq!(s.get("a").unwrap().as_deref(), Some("two"));
        s.delete("a").unwrap();
        assert_eq!(s.get("a").unwrap(), None);
        s.delete("a").unwrap();
        assert!(!s.is_native());
        assert!(MemorySecretStore::with_native(true).is_native());
    }

    #[test]
    fn debug_does_not_print_values() {
        let s = MemorySecretStore::new();
        s.set("k", "hunter2").unwrap();
        assert!(!format!("{s:?}").contains("hunter2"));
    }

    /// Deletes the throwaway entry on drop, so a panicking test leaves nothing behind.
    struct EntryGuard<'a>(&'a KeyringSecretStore, &'a str);

    impl Drop for EntryGuard<'_> {
        fn drop(&mut self) {
            let _ = self.0.delete(self.1);
        }
    }

    #[test]
    fn keyring_round_trip_uses_throwaway_service() {
        let s = KeyringSecretStore::with_service(&format!("herald-test-{}", uuid::Uuid::new_v4()));
        if !s.is_native() {
            return;
        }
        let key = "round-trip";
        let _guard = EntryGuard(&s, key);
        assert_eq!(s.get(key).unwrap(), None);
        s.set(key, "secret-value").unwrap();
        assert_eq!(s.get(key).unwrap().as_deref(), Some("secret-value"));
        s.delete(key).unwrap();
        assert_eq!(s.get(key).unwrap(), None);
        s.delete(key).unwrap();
    }

    #[test]
    fn map_error_maps_each_variant() {
        assert!(matches!(
            map_error(keyring::Error::NoDefaultStore),
            SecretError::NoNativeStore
        ));
        let bad = map_error(keyring::Error::BadEncoding(b"hunter2".to_vec())).to_string();
        assert!(bad.contains("not valid UTF-8"));
        assert!(!bad.contains("hunter2"));
        assert!(matches!(
            map_error(keyring::Error::NoEntry),
            SecretError::Backend(_)
        ));
        assert!(matches!(
            map_error(keyring::Error::Invalid("a".into(), "b".into())),
            SecretError::Backend(_)
        ));
    }
}
