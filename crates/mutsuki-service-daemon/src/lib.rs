use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result};
use mutsuki_service_config::ServiceConfig;

pub const WINDOWS_SERVICE_RUN_COMMAND: &str = "windows-service-run";

#[derive(Clone, Debug)]
pub struct DaemonLaunchOptions {
    pub executable_path: PathBuf,
    pub config_file: Option<PathBuf>,
    pub home_dir: PathBuf,
    pub profile: String,
    pub persist_control_token: bool,
}

impl DaemonLaunchOptions {
    pub fn from_config(
        config: &ServiceConfig,
        config_file: Option<PathBuf>,
        persist_control_token: bool,
    ) -> Result<Self> {
        Ok(Self {
            executable_path: std::env::current_exe()
                .context("failed to resolve current executable")?,
            config_file,
            home_dir: config.service.home_dir.clone(),
            profile: config.service.profile.clone(),
            persist_control_token,
        })
    }
}

pub fn install(config: &ServiceConfig, launch: &DaemonLaunchOptions) -> Result<()> {
    platform::install(config, launch)
}

pub fn uninstall(config: &ServiceConfig) -> Result<()> {
    platform::uninstall(config)
}

pub fn start(config: &ServiceConfig) -> Result<()> {
    platform::start(config)
}

pub fn run_windows_service(config: ServiceConfig) -> Result<()> {
    platform::run_windows_service(config)
}

pub fn windows_service_name(config: &ServiceConfig) -> String {
    format!(
        "mutsuki-service-{}",
        sanitized_service_suffix(&config.service.instance_id)
    )
}

pub fn windows_service_display_name(config: &ServiceConfig) -> String {
    format!("Mutsuki Service ({})", config.service.instance_id)
}

pub fn windows_service_launch_arguments(launch: &DaemonLaunchOptions) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("--home"),
        launch.home_dir.as_os_str().to_owned(),
        OsString::from("--profile"),
        OsString::from(&launch.profile),
    ];
    if let Some(config_file) = &launch.config_file {
        args.push(OsString::from("--config"));
        args.push(config_file.as_os_str().to_owned());
    }
    args.push(OsString::from(WINDOWS_SERVICE_RUN_COMMAND));
    args
}

fn sanitized_service_suffix(instance_id: &str) -> String {
    let mut suffix = String::new();
    let mut last_was_dash = false;
    for ch in instance_id.chars() {
        let mapped = if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' {
            if last_was_dash {
                continue;
            }
            last_was_dash = true;
        } else {
            last_was_dash = false;
        }
        suffix.push(mapped);
    }
    let suffix = suffix.trim_matches('-');
    if suffix.is_empty() {
        "default".into()
    } else {
        suffix.into()
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::OsString;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result, anyhow};
    use mutsuki_service_config::ServiceConfig;
    use windows_service::define_windows_service;
    use windows_service::service::{
        ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
        ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_dispatcher;
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    use super::{
        DaemonLaunchOptions, windows_service_display_name, windows_service_launch_arguments,
        windows_service_name,
    };

    const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;
    const ERROR_SERVICE_DOES_NOT_EXIST: i32 = 1060;
    const ERROR_SERVICE_EXISTS: i32 = 1073;
    const ERROR_SERVICE_ALREADY_RUNNING: i32 = 1056;

    static SERVICE_CONFIG: OnceLock<ServiceConfig> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub fn install(config: &ServiceConfig, launch: &DaemonLaunchOptions) -> Result<()> {
        if launch.persist_control_token {
            persist_control_token(config)?;
        }

        let service_name = windows_service_name(config);
        let service_info = ServiceInfo {
            name: OsString::from(&service_name),
            display_name: OsString::from(windows_service_display_name(config)),
            service_type: SERVICE_TYPE,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: launch.executable_path.clone(),
            launch_arguments: windows_service_launch_arguments(launch),
            dependencies: Vec::new(),
            account_name: None,
            account_password: None,
        };
        let manager_access = ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE;
        let service_manager = ServiceManager::local_computer(None::<&str>, manager_access)
            .context("failed to connect to Windows service manager")?;
        let service = match service_manager.create_service(
            &service_info,
            ServiceAccess::CHANGE_CONFIG | ServiceAccess::QUERY_STATUS,
        ) {
            Ok(service) => service,
            Err(error) if raw_os_error(&error) == Some(ERROR_SERVICE_EXISTS) => {
                return Err(anyhow!("Windows service {service_name} already exists"));
            }
            Err(error) => return Err(error).context("failed to create Windows service"),
        };
        service
            .set_description("MutsukiCore headless service host")
            .context("failed to set Windows service description")?;
        Ok(())
    }

    pub fn uninstall(config: &ServiceConfig) -> Result<()> {
        let service_name = windows_service_name(config);
        let service_manager =
            ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                .context("failed to connect to Windows service manager")?;
        let service = match service_manager.open_service(
            &service_name,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        ) {
            Ok(service) => service,
            Err(error) if raw_os_error(&error) == Some(ERROR_SERVICE_DOES_NOT_EXIST) => {
                return Ok(());
            }
            Err(error) => return Err(error).context("failed to open Windows service"),
        };

        let status = service
            .query_status()
            .context("failed to query Windows service status")?;
        if !matches!(
            status.current_state,
            ServiceState::Stopped | ServiceState::StopPending
        ) {
            service
                .stop()
                .context("failed to request Windows service stop")?;
            wait_until_stopped(&service, Duration::from_secs(15))?;
        }
        service
            .delete()
            .context("failed to delete Windows service")?;
        drop(service);
        wait_until_deleted(&service_manager, &service_name, Duration::from_secs(5))?;
        Ok(())
    }

    pub fn start(config: &ServiceConfig) -> Result<()> {
        let service_name = windows_service_name(config);
        let service_manager =
            ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                .context("failed to connect to Windows service manager")?;
        let service = service_manager
            .open_service(
                &service_name,
                ServiceAccess::QUERY_STATUS | ServiceAccess::START,
            )
            .context("failed to open Windows service")?;
        if service
            .query_status()
            .context("failed to query Windows service status")?
            .current_state
            == ServiceState::Running
        {
            return Ok(());
        }
        match service.start::<&str>(&[]) {
            Ok(()) => Ok(()),
            Err(error) if raw_os_error(&error) == Some(ERROR_SERVICE_ALREADY_RUNNING) => Ok(()),
            Err(error) => Err(error).context("failed to start Windows service"),
        }
    }

    pub fn run_windows_service(config: ServiceConfig) -> Result<()> {
        let service_name = windows_service_name(&config);
        SERVICE_CONFIG
            .set(config)
            .map_err(|_| anyhow!("Windows service config was already initialized"))?;
        service_dispatcher::start(service_name, ffi_service_main)
            .context("failed to start Windows service dispatcher")
    }

    fn service_main(_arguments: Vec<OsString>) {
        if let Err(error) = run_service() {
            eprintln!("Windows service failed: {error:#}");
        }
    }

    fn run_service() -> Result<()> {
        let config = SERVICE_CONFIG
            .get()
            .cloned()
            .ok_or_else(|| anyhow!("Windows service config is not initialized"))?;
        let service_name = windows_service_name(&config);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
        let event_handler = {
            let shutdown_tx = shutdown_tx.clone();
            move |control_event| -> ServiceControlHandlerResult {
                match control_event {
                    ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                    ServiceControl::Stop => {
                        if let Some(tx) = shutdown_tx.lock().expect("shutdown sender").take() {
                            let _ = tx.send(());
                        }
                        ServiceControlHandlerResult::NoError
                    }
                    _ => ServiceControlHandlerResult::NotImplemented,
                }
            }
        };
        let status_handle = service_control_handler::register(service_name, event_handler)
            .context("failed to register Windows service control handler")?;
        status_handle
            .set_service_status(service_status(
                ServiceState::StartPending,
                ServiceControlAccept::empty(),
            ))
            .context("failed to report Windows service start pending")?;

        let runtime = tokio::runtime::Runtime::new().context("failed to create Tokio runtime")?;
        let result = runtime.block_on(async move {
            let service = mutsuki_service_runtime::ServiceRuntime::start(config).await?;
            status_handle
                .set_service_status(service_status(
                    ServiceState::Running,
                    ServiceControlAccept::STOP,
                ))
                .context("failed to report Windows service running")?;
            let status_handle_for_stop = status_handle.clone();
            let shutdown_signal = async move {
                let _ = shutdown_rx.await;
                let _ = status_handle_for_stop.set_service_status(service_status(
                    ServiceState::StopPending,
                    ServiceControlAccept::empty(),
                ));
                "windows-service-control".to_string()
            };
            service.run_until_shutdown_signal(shutdown_signal).await?;
            status_handle
                .set_service_status(service_status(
                    ServiceState::Stopped,
                    ServiceControlAccept::empty(),
                ))
                .context("failed to report Windows service stopped")?;
            Ok::<(), anyhow::Error>(())
        });
        if result.is_err() {
            let _ = status_handle.set_service_status(service_status(
                ServiceState::Stopped,
                ServiceControlAccept::empty(),
            ));
        }
        result
    }

    fn persist_control_token(config: &ServiceConfig) -> Result<()> {
        std::fs::create_dir_all(&config.service.run_dir).with_context(|| {
            format!(
                "failed to create run dir {}",
                config.service.run_dir.display()
            )
        })?;
        let token_path = config.service.run_dir.join("control.token");
        std::fs::write(&token_path, config.control_token())
            .with_context(|| format!("failed to write control token {}", token_path.display()))?;
        Ok(())
    }

    fn wait_until_stopped(
        service: &windows_service::service::Service,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let status = service
                .query_status()
                .context("failed to query Windows service status")?;
            if status.current_state == ServiceState::Stopped {
                return Ok(());
            }
            sleep(Duration::from_millis(250));
        }
        Err(anyhow!("Windows service did not stop within {:?}", timeout))
    }

    fn wait_until_deleted(
        service_manager: &ServiceManager,
        service_name: &str,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            match service_manager.open_service(service_name, ServiceAccess::QUERY_STATUS) {
                Ok(_) => sleep(Duration::from_millis(250)),
                Err(error) if raw_os_error(&error) == Some(ERROR_SERVICE_DOES_NOT_EXIST) => {
                    return Ok(());
                }
                Err(error) => {
                    return Err(error).context("failed to confirm Windows service deletion");
                }
            }
        }
        Err(anyhow!(
            "Windows service {service_name} is marked for deletion but still exists"
        ))
    }

    fn service_status(
        current_state: ServiceState,
        controls_accepted: ServiceControlAccept,
    ) -> ServiceStatus {
        ServiceStatus {
            service_type: SERVICE_TYPE,
            current_state,
            controls_accepted,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        }
    }

    fn raw_os_error(error: &windows_service::Error) -> Option<i32> {
        match error {
            windows_service::Error::Winapi(error) => error.raw_os_error(),
            _ => None,
        }
    }
}

#[cfg(not(windows))]
mod platform {
    use anyhow::{Result, bail};
    use mutsuki_service_config::ServiceConfig;

    use super::DaemonLaunchOptions;

    pub fn install(_config: &ServiceConfig, _launch: &DaemonLaunchOptions) -> Result<()> {
        unsupported("install")
    }

    pub fn uninstall(_config: &ServiceConfig) -> Result<()> {
        unsupported("uninstall")
    }

    pub fn start(_config: &ServiceConfig) -> Result<()> {
        unsupported("start")
    }

    pub fn run_windows_service(_config: ServiceConfig) -> Result<()> {
        unsupported("run")
    }

    fn unsupported(operation: &str) -> Result<()> {
        bail!(
            "Windows service {operation} is unsupported on {}",
            std::env::consts::OS
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use mutsuki_service_config::ServiceConfig;

    use super::*;

    #[test]
    fn service_name_is_derived_from_sanitized_instance_id() {
        let mut config = ServiceConfig::default();
        config.service.instance_id = "Prod/Main 01".into();

        assert_eq!(
            windows_service_name(&config),
            "mutsuki-service-prod-main-01"
        );

        config.service.instance_id = "\\/:*?".into();
        assert_eq!(windows_service_name(&config), "mutsuki-service-default");
    }

    #[test]
    fn launch_arguments_preserve_config_selection_without_token() {
        let launch = DaemonLaunchOptions {
            executable_path: PathBuf::from("mutsuki-service.exe"),
            config_file: Some(PathBuf::from("C:/mutsuki/service.toml")),
            home_dir: PathBuf::from("C:/mutsuki/home"),
            profile: "prod".into(),
            persist_control_token: true,
        };

        let args = windows_service_launch_arguments(&launch);

        assert!(args.contains(&OsString::from(WINDOWS_SERVICE_RUN_COMMAND)));
        assert!(args.contains(&OsString::from("--home")));
        assert!(args.contains(&OsString::from("C:/mutsuki/home")));
        assert!(args.contains(&OsString::from("--profile")));
        assert!(args.contains(&OsString::from("prod")));
        assert!(args.contains(&OsString::from("--config")));
        assert!(args.contains(&OsString::from("C:/mutsuki/service.toml")));
        assert!(!args.contains(&OsString::from("--token")));
    }
}
