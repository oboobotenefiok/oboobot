//! An append-only, newline-delimited JSON file with a byte-offset cursor,
//! which is the persistence pattern this whole workspace borrows from
//! bruh. The one rule that actually matters here: `append` does not
//! return successfully until the write has been `fsync`'d to disk. A
//! write that only makes it as far as the OS page cache can still be
//! lost on a power cut or an OOM kill, and "the daemon thinks this order
//! was recorded, but it wasn't really durable yet" is exactly the kind of
//! gap that turns into an orphaned position after a crash.

use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("I/O error on cursor file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("could not read or write a record in {path}: {source}")]
    Serde {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

pub struct CursorFile<T> {
    path: PathBuf,
    _marker: PhantomData<T>,
}

impl<T> CursorFile<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new(path: impl Into<PathBuf>) -> Self {
        CursorFile {
            path: path.into(),
            _marker: PhantomData,
        }
    }

    /// Append one record as a new line and return the file's new byte
    /// length (the new cursor position), only after the write has been
    /// fsync'd. See the module docs for why the fsync isn't optional.
    pub async fn append(&self, record: &T) -> Result<u64, PersistenceError> {
        let mut line =
            serde_json::to_string(record).map_err(|source| self.serde_err(source))?;
        line.push('\n');

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|source| self.io_err(source))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|source| self.io_err(source))?;

        // The durability guarantee this whole type exists for. Everything
        // above this line could, in principle, still be sitting in a
        // buffer somewhere; after this line returns `Ok`, the record is
        // actually on disk.
        file.sync_all().await.map_err(|source| self.io_err(source))?;

        let metadata = file.metadata().await.map_err(|source| self.io_err(source))?;
        Ok(metadata.len())
    }

    /// Every record in the file, from the beginning. Used at startup
    /// before a cursor offset has been established.
    pub async fn read_all(&self) -> Result<Vec<T>, PersistenceError> {
        self.read_from(0).await
    }

    /// Every record starting at a given byte offset, which is how a
    /// daemon resumes from a previously saved cursor instead of
    /// re-reading its entire history on every restart.
    pub async fn read_from(&self, offset: u64) -> Result<Vec<T>, PersistenceError> {
        let file = match tokio::fs::OpenOptions::new().read(true).open(&self.path).await {
            Ok(file) => file,
            // A cursor file that hasn't been created yet just means
            // "nothing's been recorded so far," not an error condition.
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(self.io_err(source)),
        };

        let mut file = file;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| self.io_err(source))?;

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(|source| self.io_err(source))?;
            if bytes_read == 0 {
                break;
            }
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                continue;
            }
            let record: T =
                serde_json::from_str(trimmed).map_err(|source| self.serde_err(source))?;
            records.push(record);
        }

        Ok(records)
    }

    /// The current end-of-file byte offset, i.e. what a cursor should be
    /// set to right now if you wanted to skip everything already
    /// recorded.
    pub async fn current_offset(&self) -> Result<u64, PersistenceError> {
        match tokio::fs::metadata(&self.path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(source) => Err(self.io_err(source)),
        }
    }

    fn io_err(&self, source: std::io::Error) -> PersistenceError {
        PersistenceError::Io {
            path: self.path.display().to_string(),
            source,
        }
    }

    fn serde_err(&self, source: serde_json::Error) -> PersistenceError {
        PersistenceError::Serde {
            path: self.path.display().to_string(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SampleRecord {
        id: u32,
        label: String,
    }

    #[tokio::test]
    async fn append_then_read_all_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();

        let records = cursor.read_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, 1);
        assert_eq!(records[1].id, 2);
    }

    #[tokio::test]
    async fn read_from_a_saved_offset_only_returns_newer_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        let offset_after_first = cursor.current_offset().await.unwrap();
        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();

        let records = cursor.read_from(offset_after_first).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, 2);
    }

    #[tokio::test]
    async fn reading_a_file_that_does_not_exist_yet_is_an_empty_list_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("never_written.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        let records = cursor.read_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn current_offset_grows_with_each_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        assert_eq!(cursor.current_offset().await.unwrap(), 0);
        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        let after_one = cursor.current_offset().await.unwrap();
        assert!(after_one > 0);

        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();
        let after_two = cursor.current_offset().await.unwrap();
        assert!(after_two > after_one);
    }
}
