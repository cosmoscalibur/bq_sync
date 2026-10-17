//! Minimal stderr logger mirroring the Python CLI output format.
//!
//! Non-verbose: ``<message>`` at INFO and above.
//! Verbose: ``<LEVEL>: <message>`` at DEBUG and above.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use log::{Level, LevelFilter, Log, Metadata, Record};

struct StderrLogger;

static VERBOSE: AtomicBool = AtomicBool::new(false);
static LOGGER: StderrLogger = StderrLogger;

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Error => "ERROR",
        Level::Warn => "WARNING",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "TRACE",
    }
}

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // Silence chatty dependencies; only bq-sync's own records are shown.
        metadata.target().starts_with("bq_sync")
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) || record.level() > log::max_level() {
            return;
        }
        let mut stderr = std::io::stderr().lock();
        let _ = if VERBOSE.load(Ordering::Relaxed) {
            writeln!(stderr, "{}: {}", level_name(record.level()), record.args())
        } else {
            writeln!(stderr, "{}", record.args())
        };
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Install the logger (idempotent) and set the verbosity.
pub fn init(verbose: bool) {
    VERBOSE.store(verbose, Ordering::Relaxed);
    // Ignore the error when a logger is already installed (e.g. repeated
    // invocations from Python in the same process).
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    });
}
