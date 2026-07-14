use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use mutsuki_service_config::ServiceConfig;

pub const WINDOWS_SERVICE_RUN_COMMAND: &str = "windows-service-run";
pub const FOREGROUND_RUN_COMMAND: &str = "run";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DaemonScope {
    User,
    System,
}

impl DaemonScope {
    pub const fn platform_default() -> Self {
        if cfg!(windows) {
            Self::System
        } else {
            Self::User
        }
    }
}

impl FromStr for DaemonScope {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "user" => Ok(Self::User),
            "system" => Ok(Self::System),
            _ => anyhow::bail!("unsupported daemon scope {value}; expected user or system"),
        }
    }
}

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
    install_with_scope(config, launch, DaemonScope::platform_default(), None)
}

pub fn uninstall(config: &ServiceConfig) -> Result<()> {
    uninstall_with_scope(config, DaemonScope::platform_default())
}

pub fn start(config: &ServiceConfig) -> Result<()> {
    start_with_scope(config, DaemonScope::platform_default())
}

pub fn install_with_scope(
    config: &ServiceConfig,
    launch: &DaemonLaunchOptions,
    scope: DaemonScope,
    service_user: Option<&str>,
) -> Result<()> {
    platform::install(config, launch, scope, service_user)
}

pub fn uninstall_with_scope(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
    platform::uninstall(config, scope)
}

pub fn start_with_scope(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
    platform::start(config, scope)
}

pub fn run_windows_service(config: ServiceConfig) -> Result<()> {
    platform::run_windows_service(config)
}

pub fn windows_service_name(config: &ServiceConfig) -> String {
    service_name(config)
}

pub fn service_name(config: &ServiceConfig) -> String {
    format!(
        "mutsuki-service-{}",
        sanitized_service_suffix(&config.service.instance_id)
    )
}

pub fn launchd_label(config: &ServiceConfig) -> String {
    format!(
        "io.github.sena-nana.mutsuki-service.{}",
        sanitized_service_suffix(&config.service.instance_id)
    )
}

pub fn foreground_launch_arguments(launch: &DaemonLaunchOptions) -> Vec<OsString> {
    launch_arguments(launch, FOREGROUND_RUN_COMMAND)
}

pub fn windows_service_display_name(config: &ServiceConfig) -> String {
    format!("Mutsuki Service ({})", config.service.instance_id)
}

pub fn windows_service_launch_arguments(launch: &DaemonLaunchOptions) -> Vec<OsString> {
    launch_arguments(launch, WINDOWS_SERVICE_RUN_COMMAND)
}

fn launch_arguments(launch: &DaemonLaunchOptions, command: &str) -> Vec<OsString> {
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
    args.push(OsString::from(command));
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
        DaemonLaunchOptions, DaemonScope, windows_service_display_name,
        windows_service_launch_arguments, windows_service_name,
    };

    const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;
    const ERROR_SERVICE_DOES_NOT_EXIST: i32 = 1060;
    const ERROR_SERVICE_EXISTS: i32 = 1073;
    const ERROR_SERVICE_ALREADY_RUNNING: i32 = 1056;

    static SERVICE_CONFIG: OnceLock<ServiceConfig> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub fn install(
        config: &ServiceConfig,
        launch: &DaemonLaunchOptions,
        scope: DaemonScope,
        _service_user: Option<&str>,
    ) -> Result<()> {
        ensure_system_scope(scope)?;
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

    pub fn uninstall(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
        ensure_system_scope(scope)?;
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

    pub fn start(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
        ensure_system_scope(scope)?;
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

    fn ensure_system_scope(scope: DaemonScope) -> Result<()> {
        if scope == DaemonScope::System {
            Ok(())
        } else {
            Err(anyhow!("Windows Service supports only system scope"))
        }
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

#[cfg(unix)]
mod platform {
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use anyhow::{Context, Result, anyhow, bail};
    use mutsuki_service_config::ServiceConfig;
    #[cfg(any(target_os = "macos", test))]
    use plist::{Dictionary, Value};

    #[cfg(any(target_os = "macos", test))]
    use super::launchd_label;
    #[cfg(any(target_os = "linux", test))]
    use super::service_name;
    use super::{DaemonLaunchOptions, DaemonScope, foreground_launch_arguments};

    pub fn install(
        config: &ServiceConfig,
        launch: &DaemonLaunchOptions,
        scope: DaemonScope,
        service_user: Option<&str>,
    ) -> Result<()> {
        let service_user = validate_scope(scope, service_user)?;
        if launch.persist_control_token {
            persist_control_token(config, service_user)?;
        }
        #[cfg(target_os = "linux")]
        return systemd::install(config, launch, scope, service_user);
        #[cfg(target_os = "macos")]
        return launchd::install(config, launch, scope, service_user);
        #[allow(unreachable_code)]
        unsupported("install")
    }

    pub fn uninstall(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
        #[cfg(target_os = "linux")]
        return systemd::uninstall(config, scope);
        #[cfg(target_os = "macos")]
        return launchd::uninstall(config, scope);
        #[allow(unreachable_code)]
        unsupported("uninstall")
    }

    pub fn start(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
        #[cfg(target_os = "linux")]
        return systemd::start(config, scope);
        #[cfg(target_os = "macos")]
        return launchd::start(config, scope);
        #[allow(unreachable_code)]
        unsupported("start")
    }

    pub fn run_windows_service(_config: ServiceConfig) -> Result<()> {
        bail!(
            "Windows service run is unsupported on {}",
            std::env::consts::OS
        )
    }

    fn unsupported(operation: &str) -> Result<()> {
        bail!(
            "daemon {operation} is unsupported on {}",
            std::env::consts::OS
        )
    }

    fn validate_scope(scope: DaemonScope, service_user: Option<&str>) -> Result<Option<&str>> {
        if scope == DaemonScope::User {
            if service_user.is_some() {
                bail!("service_user is valid only for system scope");
            }
            return Ok(None);
        }
        let service_user = service_user
            .map(str::trim)
            .filter(|user| !user.is_empty())
            .ok_or_else(|| anyhow!("system scope requires an explicit non-root service_user"))?;
        validate_text(service_user, "service_user")?;
        if service_user == "root" {
            bail!("system scope service_user must not be root");
        }
        Ok(Some(service_user))
    }

    fn validate_text(value: &str, field: &str) -> Result<()> {
        if value.contains(['\0', '\n', '\r']) {
            bail!("{field} contains an unsupported control character");
        }
        Ok(())
    }

    fn home_dir() -> Result<PathBuf> {
        dirs::home_dir().ok_or_else(|| anyhow!("failed to resolve user home directory"))
    }

    fn persist_control_token(config: &ServiceConfig, service_user: Option<&str>) -> Result<()> {
        fs::create_dir_all(&config.service.run_dir).with_context(|| {
            format!(
                "failed to create run dir {}",
                config.service.run_dir.display()
            )
        })?;
        let token_path = config.service.run_dir.join("control.token");
        fs::write(&token_path, config.control_token())
            .with_context(|| format!("failed to write control token {}", token_path.display()))?;
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure control token {}", token_path.display()))?;
        if let Some(service_user) = service_user {
            let mut command = Command::new("chown");
            command.arg(service_user).arg(&token_path);
            run_command(command, "assign control token ownership")?;
        }
        Ok(())
    }

    fn write_definition(path: &Path, bytes: &[u8]) -> Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("service definition has no parent: {}", path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create service directory {}", parent.display()))?;
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, bytes).with_context(|| {
            format!("failed to write service definition {}", temporary.display())
        })?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o644)).with_context(|| {
            format!(
                "failed to set service definition permissions {}",
                temporary.display()
            )
        })?;
        fs::rename(&temporary, path)
            .with_context(|| format!("failed to install service definition {}", path.display()))
    }

    fn run_command(mut command: Command, action: &str) -> Result<()> {
        let output = command
            .output()
            .with_context(|| format!("failed to {action}"))?;
        if output.status.success() {
            return Ok(());
        }
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!("failed to {action}: {detail}")
    }

    #[cfg(target_os = "macos")]
    fn command_succeeds(mut command: Command) -> bool {
        command
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn ignore_command_failure(mut command: Command) {
        let _ = command.status();
    }

    fn launch_program_arguments(launch: &DaemonLaunchOptions) -> Result<Vec<String>> {
        let arguments = foreground_launch_arguments(launch);
        std::iter::once(launch.executable_path.as_os_str())
            .chain(arguments.iter().map(OsString::as_os_str))
            .map(|value| os_string(value, "launch argument"))
            .collect()
    }

    fn os_string(value: &OsStr, field: &str) -> Result<String> {
        let value = value
            .to_str()
            .ok_or_else(|| anyhow!("{field} is not valid UTF-8"))?
            .to_string();
        validate_text(&value, field)?;
        Ok(value)
    }

    #[cfg(any(target_os = "macos", test))]
    fn launchd_plist(
        config: &ServiceConfig,
        launch: &DaemonLaunchOptions,
        scope: DaemonScope,
        service_user: Option<&str>,
    ) -> Result<Vec<u8>> {
        let mut root = Dictionary::new();
        root.insert("Label".into(), Value::String(launchd_label(config)));
        root.insert(
            "ProgramArguments".into(),
            Value::Array(
                launch_program_arguments(launch)?
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            ),
        );
        root.insert(
            "WorkingDirectory".into(),
            Value::String(os_string(launch.home_dir.as_os_str(), "working directory")?),
        );
        root.insert("RunAtLoad".into(), Value::Boolean(true));
        root.insert("ProcessType".into(), Value::String("Background".into()));
        let mut keep_alive = Dictionary::new();
        keep_alive.insert("SuccessfulExit".into(), Value::Boolean(false));
        root.insert("KeepAlive".into(), Value::Dictionary(keep_alive));
        if scope == DaemonScope::System {
            root.insert(
                "UserName".into(),
                Value::String(service_user.expect("validated system service user").into()),
            );
        }
        let mut bytes = Vec::new();
        Value::Dictionary(root)
            .to_writer_xml(&mut bytes)
            .context("failed to serialize launchd plist")?;
        Ok(bytes)
    }

    #[cfg(any(target_os = "linux", test))]
    fn systemd_unit(
        config: &ServiceConfig,
        launch: &DaemonLaunchOptions,
        scope: DaemonScope,
        service_user: Option<&str>,
    ) -> Result<String> {
        let arguments = launch_program_arguments(launch)?
            .into_iter()
            .map(|argument| systemd_quote(&argument))
            .collect::<Result<Vec<_>>>()?
            .join(" ");
        let working_directory = systemd_quote(&os_string(
            launch.home_dir.as_os_str(),
            "working directory",
        )?)?;
        let wanted_by = if scope == DaemonScope::User {
            "default.target"
        } else {
            "multi-user.target"
        };
        let user = service_user
            .map(|user| format!("User={}\n", systemd_atom(user)))
            .unwrap_or_default();
        let timeout_secs = config.core.shutdown_timeout_ms.div_ceil(1000).max(1);
        Ok(format!(
            "[Unit]\nDescription=Mutsuki Service ({})\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\n{user}WorkingDirectory={working_directory}\nExecStart={arguments}\nRestart=on-failure\nRestartSec=2\nKillSignal=SIGTERM\nTimeoutStopSec={timeout_secs}\n\n[Install]\nWantedBy={wanted_by}\n",
            config.service.instance_id.replace(['\n', '\r'], " ")
        ))
    }

    #[cfg(any(target_os = "linux", test))]
    fn systemd_quote(value: &str) -> Result<String> {
        validate_text(value, "systemd argument")?;
        Ok(format!(
            "\"{}\"",
            value
                .replace('%', "%%")
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        ))
    }

    #[cfg(any(target_os = "linux", test))]
    fn systemd_atom(value: &str) -> String {
        value.replace('%', "%%")
    }

    #[cfg(target_os = "linux")]
    mod systemd {
        use super::*;

        pub fn install(
            config: &ServiceConfig,
            launch: &DaemonLaunchOptions,
            scope: DaemonScope,
            service_user: Option<&str>,
        ) -> Result<()> {
            let path = definition_path(config, scope)?;
            write_definition(
                &path,
                systemd_unit(config, launch, scope, service_user)?.as_bytes(),
            )?;
            run_systemctl(scope, ["daemon-reload"], "reload systemd units")?;
            run_systemctl(
                scope,
                ["enable", &service_name(config)],
                "enable systemd service",
            )
        }

        pub fn start(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
            run_systemctl(
                scope,
                ["start", &service_name(config)],
                "start systemd service",
            )
        }

        pub fn uninstall(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
            ignore_systemctl(scope, ["stop", &service_name(config)]);
            ignore_systemctl(scope, ["disable", &service_name(config)]);
            let path = definition_path(config, scope)?;
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to remove systemd unit {}", path.display())
                    });
                }
            }
            run_systemctl(scope, ["daemon-reload"], "reload systemd units")
        }

        fn definition_path(config: &ServiceConfig, scope: DaemonScope) -> Result<PathBuf> {
            let directory = if scope == DaemonScope::User {
                home_dir()?.join(".config/systemd/user")
            } else {
                PathBuf::from("/etc/systemd/system")
            };
            Ok(directory.join(format!("{}.service", service_name(config))))
        }

        fn run_systemctl<const N: usize>(
            scope: DaemonScope,
            args: [&str; N],
            action: &str,
        ) -> Result<()> {
            let mut command = systemctl(scope);
            command.args(args);
            run_command(command, action)
        }

        fn ignore_systemctl<const N: usize>(scope: DaemonScope, args: [&str; N]) {
            let mut command = systemctl(scope);
            command.args(args);
            ignore_command_failure(command);
        }

        fn systemctl(scope: DaemonScope) -> Command {
            let mut command = Command::new("systemctl");
            if scope == DaemonScope::User {
                command.arg("--user");
            }
            command
        }
    }

    #[cfg(target_os = "macos")]
    mod launchd {
        use super::*;

        pub fn install(
            config: &ServiceConfig,
            launch: &DaemonLaunchOptions,
            scope: DaemonScope,
            service_user: Option<&str>,
        ) -> Result<()> {
            let path = definition_path(config, scope)?;
            write_definition(&path, &launchd_plist(config, launch, scope, service_user)?)?;
            let mut command = Command::new("launchctl");
            command.arg("enable").arg(service_target(config, scope)?);
            run_command(command, "enable launchd service")
        }

        pub fn start(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
            let target = service_target(config, scope)?;
            let mut print = Command::new("launchctl");
            print.arg("print").arg(&target);
            if command_succeeds(print) {
                let mut kickstart = Command::new("launchctl");
                kickstart.arg("kickstart").arg("-k").arg(&target);
                return run_command(kickstart, "start launchd service");
            }
            let mut bootstrap = Command::new("launchctl");
            bootstrap
                .arg("bootstrap")
                .arg(domain(scope)?)
                .arg(definition_path(config, scope)?);
            run_command(bootstrap, "bootstrap launchd service")
        }

        pub fn uninstall(config: &ServiceConfig, scope: DaemonScope) -> Result<()> {
            let mut bootout = Command::new("launchctl");
            bootout.arg("bootout").arg(service_target(config, scope)?);
            ignore_command_failure(bootout);
            let path = definition_path(config, scope)?;
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error)
                    .with_context(|| format!("failed to remove launchd plist {}", path.display())),
            }
        }

        fn definition_path(config: &ServiceConfig, scope: DaemonScope) -> Result<PathBuf> {
            let directory = if scope == DaemonScope::User {
                home_dir()?.join("Library/LaunchAgents")
            } else {
                PathBuf::from("/Library/LaunchDaemons")
            };
            Ok(directory.join(format!("{}.plist", launchd_label(config))))
        }

        fn service_target(config: &ServiceConfig, scope: DaemonScope) -> Result<String> {
            Ok(format!("{}/{}", domain(scope)?, launchd_label(config)))
        }

        fn domain(scope: DaemonScope) -> Result<String> {
            if scope == DaemonScope::System {
                return Ok("system".into());
            }
            let output = Command::new("id")
                .arg("-u")
                .output()
                .context("failed to resolve current uid")?;
            if !output.status.success() {
                bail!("failed to resolve current uid");
            }
            let uid = String::from_utf8(output.stdout)
                .context("current uid is not UTF-8")?
                .trim()
                .to_string();
            validate_text(&uid, "uid")?;
            Ok(format!("gui/{uid}"))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn fixture() -> (ServiceConfig, DaemonLaunchOptions) {
            let mut config = ServiceConfig::default();
            config.service.instance_id = "Prod/Main 01".into();
            config.core.shutdown_timeout_ms = 3_500;
            let launch = DaemonLaunchOptions {
                executable_path: PathBuf::from("/opt/mutsuki/bin/mutsuki-service"),
                config_file: Some(PathBuf::from("/opt/mutsuki/config/service.toml")),
                home_dir: PathBuf::from("/opt/mutsuki/home"),
                profile: "prod".into(),
                persist_control_token: true,
            };
            (config, launch)
        }

        #[test]
        fn system_scope_requires_non_root_service_user() {
            assert!(validate_scope(DaemonScope::System, None).is_err());
            assert!(validate_scope(DaemonScope::System, Some("root")).is_err());
            assert_eq!(
                validate_scope(DaemonScope::System, Some("mutsuki")).unwrap(),
                Some("mutsuki")
            );
            assert!(validate_scope(DaemonScope::User, Some("mutsuki")).is_err());
        }

        #[test]
        fn launchd_definition_uses_argument_array_and_failure_only_restart() {
            let (config, launch) = fixture();
            let bytes =
                launchd_plist(&config, &launch, DaemonScope::System, Some("mutsuki")).unwrap();
            let value = Value::from_reader_xml(bytes.as_slice()).unwrap();
            let dictionary = value.as_dictionary().unwrap();
            assert_eq!(
                dictionary.get("Label").and_then(Value::as_string),
                Some("io.github.sena-nana.mutsuki-service.prod-main-01")
            );
            assert_eq!(
                dictionary.get("UserName").and_then(Value::as_string),
                Some("mutsuki")
            );
            let arguments = dictionary
                .get("ProgramArguments")
                .and_then(Value::as_array)
                .unwrap();
            assert!(
                arguments
                    .iter()
                    .any(|value| value.as_string() == Some("run"))
            );
            assert!(
                !arguments
                    .iter()
                    .any(|value| value.as_string() == Some("--token"))
            );
            assert_eq!(
                dictionary
                    .get("KeepAlive")
                    .and_then(Value::as_dictionary)
                    .and_then(|keep_alive| keep_alive.get("SuccessfulExit"))
                    .and_then(Value::as_boolean),
                Some(false)
            );
        }

        #[test]
        fn systemd_definition_is_safe_and_runs_as_explicit_user() {
            let (config, launch) = fixture();
            let unit =
                systemd_unit(&config, &launch, DaemonScope::System, Some("mutsuki")).unwrap();
            assert!(unit.contains("User=mutsuki"));
            assert!(unit.contains("Restart=on-failure"));
            assert!(unit.contains("KillSignal=SIGTERM"));
            assert!(unit.contains("TimeoutStopSec=4"));
            assert!(unit.contains("WantedBy=multi-user.target"));
            assert!(!unit.contains("--token"));
        }

        #[test]
        fn unsafe_service_arguments_are_rejected() {
            let (config, mut launch) = fixture();
            launch.profile = "bad\nprofile".into();
            assert!(systemd_unit(&config, &launch, DaemonScope::User, None).is_err());
            assert!(launchd_plist(&config, &launch, DaemonScope::User, None).is_err());
        }
    }
}

#[cfg(all(not(windows), not(unix)))]
mod platform {
    use anyhow::{Result, bail};
    use mutsuki_service_config::ServiceConfig;

    use super::{DaemonLaunchOptions, DaemonScope};

    pub fn install(
        _config: &ServiceConfig,
        _launch: &DaemonLaunchOptions,
        _scope: DaemonScope,
        _service_user: Option<&str>,
    ) -> Result<()> {
        unsupported("install")
    }

    pub fn uninstall(_config: &ServiceConfig, _scope: DaemonScope) -> Result<()> {
        unsupported("uninstall")
    }

    pub fn start(_config: &ServiceConfig, _scope: DaemonScope) -> Result<()> {
        unsupported("start")
    }

    pub fn run_windows_service(_config: ServiceConfig) -> Result<()> {
        unsupported("run")
    }

    fn unsupported(operation: &str) -> Result<()> {
        bail!(
            "daemon {operation} is unsupported on {}",
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
        assert_eq!(service_name(&config), "mutsuki-service-default");
    }

    #[test]
    fn platform_default_scope_matches_service_manager_model() {
        assert_eq!(
            DaemonScope::platform_default(),
            if cfg!(windows) {
                DaemonScope::System
            } else {
                DaemonScope::User
            }
        );
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
