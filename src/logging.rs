//! Запись серверных логов (`tracing`) в файлы с ротацией по размеру.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::AppConfig;

/// Максимальный размер одного файла лога (2 МБ).
const LOG_FILE_MAX_BYTES: u64 = 2 * 1024 * 1024;

/// Максимальное число архивных файлов (плюс активный `scheduler.log`).
const LOG_FILE_MAX_FILES: usize = 10;

const LOG_FILE_NAME: &str = "scheduler.log";

#[derive(Clone)]
struct RotatingWriter {
    inner: Arc<Mutex<RotatingFile>>,
}

impl RotatingWriter {
    fn new(dir: PathBuf, file_name: &str, max_size: u64, max_files: usize) -> io::Result<Self> {
        let file = RotatingFile::new(dir, file_name.to_string(), max_size, max_files)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(file)),
        })
    }
}

impl Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("mutex poisoned"))?;
        guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("mutex poisoned"))?;
        guard.flush()
    }
}

struct RotatingFile {
    dir: PathBuf,
    file_name: String,
    max_size: u64,
    max_files: usize,
    file: File,
    current_size: u64,
}

impl RotatingFile {
    fn new(dir: PathBuf, file_name: String, max_size: u64, max_files: usize) -> io::Result<Self> {
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }

        let path = dir.join(&file_name);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            dir,
            file_name,
            max_size,
            max_files,
            file,
            current_size,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;

        for i in (1..self.max_files).rev() {
            let src = self.log_path(i);
            let dst = self.log_path(i + 1);
            if src.exists() {
                let _ = fs::rename(src, dst);
            }
        }

        let base = self.log_path(0);
        if base.exists() {
            let _ = fs::rename(&base, self.log_path(1));
        }

        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&base)?;
        self.current_size = 0;
        Ok(())
    }

    fn log_path(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.dir.join(&self.file_name)
        } else {
            let stem = Path::new(&self.file_name)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("app");
            let ext = Path::new(&self.file_name)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("log");
            self.dir.join(format!("{stem}.{index}.{ext}"))
        }
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_size + buf.len() as u64 > self.max_size {
            self.rotate()?;
        }
        let written = self.file.write(buf)?;
        self.current_size += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Инициализирует `tracing`: консоль + ротируемый файл в `AJS_LOG_DIR` (относительно каталога запуска).
pub fn init_logging(config: &AppConfig) -> io::Result<()> {
    let log_dir = PathBuf::from(&config.log_dir);
    let rotating_writer =
        RotatingWriter::new(log_dir, LOG_FILE_NAME, LOG_FILE_MAX_BYTES, LOG_FILE_MAX_FILES)?;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    let file_layer = fmt::layer()
        .with_target(false)
        .with_line_number(true)
        .with_ansi(false)
        .with_writer(move || rotating_writer.clone());

    let stdout_layer = fmt::layer()
        .with_target(false)
        .with_line_number(true)
        .with_writer(std::io::stdout);

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    Ok(())
}
