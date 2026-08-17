use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, TerminatingWrite, WatchCallback,
    WatchHandle, WritePtr,
};
use tantivy::directory::MmapDirectory;

/// A file writer that flushes to the OS page cache but never calls
/// `sync_data`/`fsync` on terminate.
///
/// The MmapDirectory equivalent syncs every segment component file when the
/// writer terminates, which on spinning disks costs hundreds of milliseconds
/// per commit. This variant keeps writes visible to readers immediately
/// (flush + close), deferring durability to an explicit `sync_all` call.
struct NoSyncFileWriter(File);

impl NoSyncFileWriter {
    fn new(file: File) -> NoSyncFileWriter {
        NoSyncFileWriter(file)
    }
}

impl Write for NoSyncFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TerminatingWrite for NoSyncFileWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.0.flush()?;
        Ok(())
    }
}

/// A `Directory` backed by `MmapDirectory` that skips every durability sync.
///
/// Reads, deletes, existence checks, and atomic reads delegate to the inner
/// `MmapDirectory` (so search keeps memory-mapping segment files). Writes
/// (segment components and small files such as `meta.json` and `managed.json`)
/// are flushed to the page cache and atomically renamed, but never fsynced.
///
/// A `sync_all` pass over the generation directory restores full durability
/// on demand; the caller decides which commits must survive a crash.
#[derive(Clone)]
pub struct NoSyncDirectory {
    inner: MmapDirectory,
    root_path: PathBuf,
}

impl fmt::Debug for NoSyncDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NoSyncDirectory({:?})", self.root_path)
    }
}

impl NoSyncDirectory {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<NoSyncDirectory> {
        Ok(NoSyncDirectory {
            inner: MmapDirectory::open(&path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?,
            root_path: path.as_ref().to_path_buf(),
        })
    }

    fn resolve_path(&self, path: &Path) -> PathBuf {
        self.root_path.join(path)
    }
}

impl Directory for NoSyncDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let full_path = self.resolve_path(path);

        let open_res = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full_path);

        let mut file = open_res.map_err(|io_err| {
            if io_err.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_path_buf())
            } else {
                OpenWriteError::wrap_io_error(io_err, path.to_path_buf())
            }
        })?;

        file.flush()
            .map_err(|io_error| OpenWriteError::wrap_io_error(io_error, path.to_path_buf()))?;

        let writer = NoSyncFileWriter::new(file);
        Ok(BufWriter::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, content: &[u8]) -> io::Result<()> {
        let full_path = self.resolve_path(path);
        let parent_path = full_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Path {full_path:?} does not have parent directory."),
            )
        })?;
        let mut tempfile = tempfile::Builder::new().tempfile_in(parent_path)?;
        tempfile.write_all(content)?;
        tempfile.flush()?;
        tempfile.into_temp_path().persist(&full_path)?;
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

/// Fsyncs every file under `root` and the root directory itself.
///
/// This restores the durability a stock `MmapDirectory` commit provides, so
/// callers can choose which commits must survive a crash.
pub fn sync_all(root: &Path) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    sync_tree(root)?;
    let dir_handle = File::open(root)?;
    dir_handle.sync_all()?;
    Ok(())
}

fn sync_tree(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_tree(&path)?;
        } else {
            let handle = File::open(&path)?;
            handle.sync_all()?;
        }
    }
    Ok(())
}
