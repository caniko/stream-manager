//! Credential-store abstraction.
//!
//! Rules enforced here:
//! - No secret material in SQLite, logs, Debug output, args, env or exports.
//! - Declarative credentials are referenced by runtime path only.
//! - Rotating OAuth tokens live in a separate writable *encrypted* store,
//!   never in an agenix-managed file (those are read-only inputs).

use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use zeroize::Zeroize;

pub mod oauth;

/// Non-secret reference to a declarative credential (e.g. agenix path
/// or token-store key id). The pointed-to value is never embedded here.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretRef {
    /// Runtime path or key id. Display-safe (no secret content).
    pub path: String,
    /// True when managed by agenix (read-only); false for the writable
    /// rotating OAuth token store.
    pub agenix_managed: bool,
}

/// In-memory secret handle with redacted Debug and zeroizing Drop.
#[derive(Clone)]
pub struct Secret(SecretString);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(SecretString::from(value))
    }

    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        // secrecy::SecretString zeroizes on drop; nothing to do here.
    }
}

/// Writable rotating-token store. Implementations must encrypt at rest
/// with a key outside the store file itself, serialize refreshes across
/// processes, and write atomically (temp file + rename).
pub trait TokenStore: Send + Sync {
    /// Load and decrypt all entries (ids only in errors/logs).
    fn load_ids(&self) -> anyhow::Result<Vec<String>>;
    /// Persist one rotated entry atomically. Must not touch agenix paths.
    fn store_rotated(&self, key_id: &str, refresh_token: Secret) -> anyhow::Result<()>;
}

/// Zeroize helper for callers holding plaintext buffers briefly.
pub fn wipe(buf: &mut String) {
    buf.zeroize();
}

/// Actionable locked-store error. A missing/unreadable runtime key never
/// falls back to plaintext: callers surface this as a locked state.
#[derive(Debug, thiserror::Error)]
#[error("token store locked: {0}")]
pub struct StoreLocked(pub String);

/// Verify a declarative credential reference without exposing its value.
/// Agenix paths must exist, be readable files, and be non-empty. Callers
/// log only the path and this boolean outcome.
pub fn verify_secret_ref(r: &SecretRef) -> anyhow::Result<()> {
    if r.agenix_managed {
        let data = std::fs::read(&r.path)
            .map_err(|e| anyhow::anyhow!("cannot read agenix credential at {}: {e}", r.path))?;
        if data.is_empty() {
            anyhow::bail!("agenix credential at {} is empty", r.path);
        }
        Ok(())
    } else {
        anyhow::bail!(
            "token-store key '{}' resolves only through an unlocked FileTokenStore",
            r.path
        )
    }
}

/// Read a 32-byte runtime key from an operator-provisioned key file. The
/// file may hold 32 raw bytes, 64 hex characters, or base64. Only the byte
/// length is ever reported; key material never reaches logs or errors.
fn load_runtime_key(key_path: &Path) -> Result<[u8; 32], StoreLocked> {
    let raw = std::fs::read(key_path).map_err(|e| {
        StoreLocked(format!(
            "runtime key file {} unreadable ({e}); provision it per docs/chat-handoff.md",
            key_path.display()
        ))
    })?;
    // Exactly 32 bytes is used unchanged: trimming first would mangle a
    // valid random key that happens to start or end in whitespace.
    if raw.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(&raw);
        return Ok(k);
    }
    // Otherwise the file must be text: at most one trailing newline is
    // tolerated, nothing else is stripped or reinterpreted.
    let text = std::str::from_utf8(&raw)
        .map_err(|_| StoreLocked("runtime key file is not valid text or raw bytes".into()))?;
    let trimmed = text.strip_suffix('\n').unwrap_or(text);
    let trimmed = trimmed.strip_suffix('\r').unwrap_or(trimmed);
    if trimmed.len() == 64 {
        let mut k = [0u8; 32];
        for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(chunk)
                .map_err(|_| StoreLocked("runtime key file is not valid hex".into()))?;
            k[i] = u8::from_str_radix(s, 16)
                .map_err(|_| StoreLocked("runtime key file is not valid hex".into()))?;
        }
        return Ok(k);
    }
    // Base64 decoded exactly as written: padding and interior
    // whitespace are the operator's to get right, not ours to reinterpret.
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| {
            base64::engine::general_purpose::URL_SAFE
                .decode(trimmed)
                .map_err(|_| ())
        })
        .map_err(|()| {
            StoreLocked("runtime key file must be 32 raw bytes, 64 hex chars, or base64".into())
        })?;
    if decoded.len() != 32 {
        return Err(StoreLocked(format!(
            "runtime key decodes to {} bytes, need 32",
            decoded.len()
        )));
    }
    let mut k = [0u8; 32];
    k.copy_from_slice(&decoded);
    Ok(k)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredEntry {
    refresh_token: String,
    updated_utc: String,
}

/// Encrypted rotating-token store backed by one file.
///
/// - AES-256-GCM with a fresh random nonce per write; the key lives in an
///   operator-provisioned file outside the store, never beside it.
/// - Atomic replacement: write temp file in the same directory, fsync,
///   chmod 0600, rename over the destination.
/// - Never touches agenix-managed paths (those are read-only bootstrap
///   inputs resolved via [`verify_secret_ref`]).
/// - Refresh serialization across tasks is provided by [`RefreshLocks`];
///   callers hold the per-account lock across refresh + store.
///
/// Process-wide rotation locks keyed by store path. Every token store
/// instance pointing at the same file shares one mutex, so concurrent
/// accounts cannot interleave read-modify-write cycles within this
/// process. Cross-process writers must still coordinate (one writer per
/// account per host); the unique temporary files below at least prevent
/// silent cross-process clobbering of one shared temp name.
static STORE_LOCKS: std::sync::LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn path_lock(path: &Path) -> Arc<Mutex<()>> {
    let key = path
        .canonicalize()
        .unwrap_or_else(|_| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()));
    let mut map = STORE_LOCKS.lock().expect("store lock map");
    map.entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct FileTokenStore {
    store_path: PathBuf,
    key_path: PathBuf,
}

impl FileTokenStore {
    pub fn new(store_path: &str, key_path: &str) -> Self {
        Self {
            store_path: PathBuf::from(shellexpand(store_path)),
            key_path: PathBuf::from(shellexpand(key_path)),
        }
    }

    pub fn store_path(&self) -> &Path {
        &self.store_path
    }

    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    fn read_entries(&self) -> anyhow::Result<HashMap<String, StoredEntry>> {
        let bytes = match std::fs::read(&self.store_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(anyhow::anyhow!("read token store: {e}")),
        };
        let envelope: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("token store corrupt: {e}"))?;
        let key = load_runtime_key(&self.key_path).map_err(|e| anyhow::anyhow!("{e}"))?;
        decrypt_envelope(&key, &envelope)
    }

    fn write_entries(&self, entries: &HashMap<String, StoredEntry>) -> anyhow::Result<()> {
        let key = load_runtime_key(&self.key_path).map_err(|e| anyhow::anyhow!("{e}"))?;
        let plaintext = serde_json::to_vec(entries)?;
        let envelope = encrypt_envelope(&key, &plaintext)?;
        let bytes = serde_json::to_vec(&envelope)?;
        atomic_write_0600(&self.store_path, &bytes)
    }

    /// Load one refresh token into a zeroizing handle. The plaintext is
    /// wiped when the handle drops; never log or persist it elsewhere.
    pub fn load_refresh(&self, key_id: &str) -> anyhow::Result<Secret> {
        let entries = self.read_entries()?;
        entries
            .get(key_id)
            .map(|e| Secret::new(e.refresh_token.clone()))
            .ok_or_else(|| anyhow::anyhow!("no token stored for '{key_id}'"))
    }

    /// Delete one stored refresh token (logout). A missing file or key
    /// is success — the goal is absence. A locked or corrupt store is an
    /// error: it cannot be surgically edited, so report instead of
    /// pretending the token is gone.
    pub fn remove(&self, key_id: &str) -> anyhow::Result<()> {
        let guard = path_lock(&self.store_path);
        let _guard = guard.lock().expect("token store lock");
        if !self.store_path.exists() {
            return Ok(());
        }
        let _file_guard = self.file_guard()?;
        let mut entries = self.read_entries()?;
        if entries.remove(key_id).is_none() {
            return Ok(());
        }
        self.write_entries(&entries)
    }

    /// Exclusive cross-process guard for the store directory. Callers must
    /// already hold the process lock and the async per-account lock.
    fn file_guard(&self) -> anyhow::Result<FileMutex> {
        let dir = self
            .store_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let stem = self
            .store_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("store");
        FileMutex::lock(&dir, &format!(".lock-{stem}"))
    }
}

fn shellexpand(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_string()
}

impl TokenStore for FileTokenStore {
    fn load_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut ids: Vec<String> = self.read_entries()?.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }

    fn store_rotated(&self, key_id: &str, refresh_token: Secret) -> anyhow::Result<()> {
        let guard = path_lock(&self.store_path);
        let _guard = guard.lock().expect("token store lock");
        // Cross-process exclusion for the read-modify-write below. Brief
        // by construction (one small file); callers already hold the
        // async per-account lock across refresh + store.
        let _file_guard = self.file_guard()?;
        let mut entries = self.read_entries()?;
        entries.insert(
            key_id.to_string(),
            StoredEntry {
                refresh_token: refresh_token.expose().to_owned(),
                updated_utc: chrono::Utc::now().to_rfc3339(),
            },
        );
        self.write_entries(&entries)
    }
}

fn encrypt_envelope(key: &[u8; 32], plaintext: &[u8]) -> anyhow::Result<serde_json::Value> {
    use aes_gcm::aead::rand_core::RngCore;
    use aes_gcm::{
        aead::{Aead, KeyInit, OsRng},
        Aes256Gcm, Nonce,
    };
    let cipher = Aes256Gcm::new(key.into());
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext)
        .map_err(|e| anyhow::anyhow!("encrypt token store: {e}"))?;
    use base64::Engine as _;
    Ok(serde_json::json!({
        "v": 1u32,
        "nonce": base64::engine::general_purpose::STANDARD.encode(nonce_bytes),
        "ciphertext": base64::engine::general_purpose::STANDARD.encode(ct),
    }))
}

fn decrypt_envelope(
    key: &[u8; 32],
    envelope: &serde_json::Value,
) -> anyhow::Result<HashMap<String, StoredEntry>> {
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };
    use base64::Engine as _;
    if envelope.get("v").and_then(|v| v.as_u64()) != Some(1) {
        anyhow::bail!("unsupported token store version");
    }
    let nonce_b64 = envelope
        .get("nonce")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("token store missing nonce"))?;
    let ct_b64 = envelope
        .get("ciphertext")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("token store missing ciphertext"))?;
    let nonce = base64::engine::general_purpose::STANDARD
        .decode(nonce_b64)
        .map_err(|e| anyhow::anyhow!("token store nonce: {e}"))?;
    if nonce.len() != 12 {
        anyhow::bail!("token store nonce has wrong length");
    }
    let ct = base64::engine::general_purpose::STANDARD
        .decode(ct_b64)
        .map_err(|e| anyhow::anyhow!("token store ciphertext: {e}"))?;
    let cipher = Aes256Gcm::new(key.into());
    let pt = cipher
        .decrypt(Nonce::from_slice(&nonce), ct.as_ref())
        .map_err(|_| anyhow::anyhow!("token store locked: decryption failed"))?;
    Ok(serde_json::from_slice(&pt)?)
}

/// Cross-process mutual exclusion for read-modify-write cycles, built on
/// a kernel-held `flock` over a persistent anchor file. The anchor is
/// never unlinked: the lock dies with the file descriptor, so a crash
/// cannot leave a stale lock behind (unlike create-then-unlink schemes).
/// Spins briefly, then fails loudly instead of interleaving rotations.
struct FileMutex {
    #[allow(dead_code)]
    file: std::fs::File,
}

impl FileMutex {
    fn lock(dir: &Path, name: &str) -> anyhow::Result<Self> {
        let path = dir.join(name);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            // Anchor files stay private even if the umask is permissive.
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            use std::os::unix::io::AsRawFd as _;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                // SAFETY: flock on our own open file descriptor with a
                // constant operation flag; no memory unsafety possible.
                let r = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if r == 0 {
                    return Ok(Self { file });
                }
                let e = std::io::Error::last_os_error();
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    return Err(e.into());
                }
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!("token store is locked by another process");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        #[cfg(not(unix))]
        {
            // Single-platform flake (x86_64-linux); fail closed elsewhere.
            anyhow::bail!("file locking unsupported on this platform");
            #[allow(unreachable_code)]
            Ok(Self { file })
        }
    }
}

fn atomic_write_0600(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(dir) = parent {
        std::fs::create_dir_all(dir)?;
    }
    // Unique exclusive temporary file: concurrent writers (and a second
    // process) never share a temp name, and `create_new` fails rather
    // than truncating on the vanishingly unlikely collision.
    let n = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let stem = path.file_name().and_then(|s| s.to_str()).unwrap_or("store");
    let tmp_name = format!(".tmp-{}-{n}-{stem}", std::process::id());
    let tmp = path.with_file_name(tmp_name);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        use std::io::Write as _;
        let write_result = (&file).write_all(bytes).and_then(|()| file.sync_all());
        drop(file);
        // Durability failures are real errors: a rotation that is not on
        // disk must not be reported as stored.
        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(e.into());
        }
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&tmp, bytes)?;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.into());
    }
    if let Some(dir) = parent {
        // Directory fsync makes the rename itself crash-durable; its
        // failure is reported, not swallowed.
        let dir_fd =
            std::fs::File::open(dir).map_err(|e| anyhow::anyhow!("fsync store dir: {e}"))?;
        dir_fd
            .sync_all()
            .map_err(|e| anyhow::anyhow!("fsync store dir: {e}"))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Per-account refresh locks. OAuth refresh is check-then-set: without a
/// shared lock two concurrent refreshes race and the loser's rotated token
/// invalidates the winner's. Hold the guard across refresh + store.
#[derive(Debug, Default, Clone)]
pub struct RefreshLocks {
    inner: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl RefreshLocks {
    pub fn lock_for(&self, account_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.inner.lock().expect("refresh lock map");
        map.entry(account_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_secret() {
        let s = Secret::new("super-secret-value".into());
        assert_eq!(format!("{s:?}"), "Secret([redacted])");
        assert_eq!(s.expose(), "super-secret-value");
    }

    fn key_file(dir: &tempfile::TempDir, name: &str) -> String {
        let p = dir.path().join(name);
        std::fs::write(&p, rand_hex_key()).unwrap();
        p.to_str().unwrap().to_owned()
    }

    fn rand_hex_key() -> String {
        use rand::RngCore;
        let mut b = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut b);
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn token_store_roundtrips_and_stays_private() {
        let dir = tempfile::tempdir().unwrap();
        let key = key_file(&dir, "key");
        let store_path = dir.path().join("tokens.json").to_str().unwrap().to_owned();
        let store = FileTokenStore::new(&store_path, &key);
        assert_eq!(store.load_ids().unwrap(), Vec::<String>::new());
        store
            .store_rotated("twitch:123", Secret::new("refresh-abc".into()))
            .unwrap();
        assert_eq!(store.load_ids().unwrap(), vec!["twitch:123".to_string()]);
        let loaded = store.load_refresh("twitch:123").unwrap();
        assert_eq!(loaded.expose(), "refresh-abc");
        // Ciphertext at rest: no plaintext token in the file.
        let raw = std::fs::read(&store_path).unwrap();
        assert!(!String::from_utf8_lossy(&raw).contains("refresh-abc"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&store_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn token_store_locked_without_key_is_actionable() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("tokens.json").to_str().unwrap().to_owned();
        let missing = dir.path().join("no-key").to_str().unwrap().to_owned();
        let store = FileTokenStore::new(&store_path, &missing);
        let err = store
            .store_rotated("x", Secret::new("y".into()))
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("locked") && msg.contains("provision"), "{msg}");
    }

    #[test]
    fn token_store_wrong_key_does_not_decrypt() {
        let dir = tempfile::tempdir().unwrap();
        let key = key_file(&dir, "key");
        let store_path = dir.path().join("tokens.json").to_str().unwrap().to_owned();
        FileTokenStore::new(&store_path, &key)
            .store_rotated("a", Secret::new("tok".into()))
            .unwrap();
        let other = key_file(&dir, "key2");
        let err = FileTokenStore::new(&store_path, &other)
            .load_ids()
            .unwrap_err();
        assert!(format!("{err:#}").contains("locked"));
    }

    #[test]
    fn concurrent_rotations_do_not_corrupt_store() {
        let dir = tempfile::tempdir().unwrap();
        let key = key_file(&dir, "key");
        let store_path = dir.path().join("tokens.json").to_str().unwrap().to_owned();
        // Separate instances per thread (as real call sites construct):
        // the process-wide per-path lock must still serialize them.
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let (p, k) = (store_path.clone(), key.clone());
                std::thread::spawn(move || {
                    FileTokenStore::new(&p, &k)
                        .store_rotated(&format!("acct:{i}"), Secret::new(format!("tok-{i}")))
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap().unwrap();
        }
        let store = FileTokenStore::new(&store_path, &key);
        assert_eq!(store.load_ids().unwrap().len(), 8);
        // No stray temporary files survive concurrent rotations.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".tmp-") || n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn remove_forgets_logout_token() {
        let dir = tempfile::tempdir().unwrap();
        let key = key_file(&dir, "key");
        let store_path = dir.path().join("tokens.json").to_str().unwrap().to_owned();
        let store = FileTokenStore::new(&store_path, &key);
        store
            .store_rotated("twitch:1", Secret::new("r1".into()))
            .unwrap();
        store
            .store_rotated("twitch:2", Secret::new("r2".into()))
            .unwrap();
        store.remove("twitch:1").unwrap();
        assert_eq!(store.load_ids().unwrap(), vec!["twitch:2".to_string()]);
        // Missing files and keys are success (goal is absence).
        store.remove("twitch:1").unwrap();
        let absent = FileTokenStore::new(dir.path().join("nope.json").to_str().unwrap(), &key);
        absent.remove("x").unwrap();
    }

    #[test]
    fn exact_32_bytes_used_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("key");
        // 32 bytes with leading/trailing spaces: valid randomness, used
        // exactly — never trimmed into a different key.
        let mut raw = vec![b' '];
        raw.extend(vec![7u8; 30]);
        raw.push(b' ');
        assert_eq!(raw.len(), 32);
        std::fs::write(&p, &raw).unwrap();
        let k1 = load_runtime_key(p.as_path()).unwrap();
        assert_eq!(&k1[..], &raw[..]);
    }

    #[test]
    fn interior_whitespace_key_is_rejected_not_mangled() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("key");
        // 33 bytes with an interior space: must not be silently stripped
        // to a valid-looking 32-byte key.
        let mut raw = vec![7u8; 16];
        raw.push(b' ');
        raw.extend(vec![7u8; 16]);
        std::fs::write(&p, &raw).unwrap();
        let err = load_runtime_key(p.as_path()).unwrap_err();
        assert!(!format!("{err}").contains('7'));
    }

    #[test]
    fn agenix_ref_verification_checks_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bootstrap");
        std::fs::write(&p, "opaque").unwrap();
        assert!(verify_secret_ref(&SecretRef {
            path: p.to_str().unwrap().into(),
            agenix_managed: true,
        })
        .is_ok());
        assert!(verify_secret_ref(&SecretRef {
            path: dir.path().join("absent").to_str().unwrap().into(),
            agenix_managed: true,
        })
        .is_err());
    }

    #[test]
    fn secret_ref_is_non_secret() {
        let r = SecretRef {
            path: "/run/agenix/youtube-oauth".into(),
            agenix_managed: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("/run/agenix/youtube-oauth"));
    }
}
