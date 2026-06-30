use std::fs::OpenOptions;
use std::io::Write;
use std::panic;
use std::path::{Path, PathBuf};

use mutsuki_service_config::ServiceConfig;
use tracing_subscriber::EnvFilter;

pub struct ObserveGuard {
    _file_guard: tracing_appender::non_blocking::WorkerGuard,
}

pub fn init_observe(config: &ServiceConfig) -> ObserveGuard {
    let file_appender =
        tracing_appender::rolling::never(&config.service.log_dir, &config.observe.log_file);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(file_writer)
        .json()
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    if config.observe.console {
        tracing::info!(
            instance_id = %config.service.instance_id,
            profile = %config.service.profile,
            "mutsuki service host starting"
        );
    }
    install_panic_hook(config.service.log_dir.join(&config.observe.panic_file));
    ObserveGuard {
        _file_guard: file_guard,
    }
}

pub fn install_panic_hook(path: PathBuf) {
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let _ = append_panic(&path, info.to_string());
        previous(info);
    }));
}

fn append_panic(path: &Path, message: String) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{message}")?;
    Ok(())
}
