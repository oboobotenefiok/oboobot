//! `CursorFile` is for things worth keeping a full history of: every
//! position, every decision. Some state isn't like that: a daily
//! buffer's current high and low, the current True Open level, a status
//! blob for a human to glance at. For those, appending forever just
//! means reading further and further back to find the one line that's
//! still relevant. `SnapshotFile` is the other half of that pair: read
//! the current value (if any), overwrite it with a new one. Same
//! fsync-before-return durability guarantee as `CursorFile`, same
//! "empty/missing means None, not an error" startup behavior, different
//! shape for a different kind of state.

use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::cursor::PersistenceError;

pub struct SnapshotFile<T> {
    path: PathBuf,
    _marker: PhantomData<T>,
}

impl<T> SnapshotFile<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new(path: impl Into<PathBuf>) -> Self {
        SnapshotFile { path: path.into(), _marker: PhantomData }
    }

    /// The current value, or `None` if this snapshot has never been
    /// written (a fresh state directory, or a buffer that hasn't reset
    /// and captured anything yet).
    pub async fn read(&self) -> Result<Option<T>, PersistenceError> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(self.io_err(source)),
        };

        if contents.trim().is_empty() {
            return Ok(None);
        }

        let value: T = serde_json::from_str(contents.trim()).map_err(|source| self.serde_err(source))?;
        Ok(Some(value))
    }

    /// Overwrite the current value. Written to a temp file in the same
    /// directory and renamed into place, which on every platform this is
    /// meant to run on (Linux, via GitHub Actions runners) is an atomic
    /// operation: a reader can never observe a half-written file, only
    /// the old value or the new one, never a torn mix of both. Still
    /// fsync'd before the function returns, same as `CursorFile::append`.
    pub async fn write(&self, value: &T) -> Result<(), PersistenceError> {
        let json = serde_json::to_string(value).map_err(|source| self.serde_err(source))?;

        let tmp_path = self.path.with_extension("tmp");
        let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|source| self.io_err(source))?;
        file.write_all(json.as_bytes()).await.map_err(|source| self.io_err(source))?;
        file.sync_all().await.map_err(|source| self.io_err(source))?;
        drop(file);

        tokio::fs::rename(&tmp_path, &self.path).await.map_err(|source| self.io_err(source))?;
        Ok(())
    }

    fn io_err(&self, source: std::io::Error) -> PersistenceError {
        PersistenceError::Io { path: self.path.display().to_string(), source }
    }

    fn serde_err(&self, source: serde_json::Error) -> PersistenceError {
        PersistenceError::Serde { path: self.path.display().to_string(), source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        value: u32,
    }

    #[tokio::test]
    async fn missing_snapshot_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        assert_eq!(snap.read().await.unwrap(), None);
    }

    #[tokio::test]
    async fn write_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        snap.write(&Sample { value: 7 }).await.unwrap();
        assert_eq!(snap.read().await.unwrap(), Some(Sample { value: 7 }));
    }

    #[tokio::test]
    async fn a_second_write_replaces_rather_than_appends() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        snap.write(&Sample { value: 1 }).await.unwrap();
        snap.write(&Sample { value: 2 }).await.unwrap();
        assert_eq!(snap.read().await.unwrap(), Some(Sample { value: 2 }));
    }
}
