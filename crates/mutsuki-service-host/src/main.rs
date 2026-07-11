use std::path::PathBuf;

use clap::{Parser, Subcommand};
use mutsuki_service_config::{ConfigOverrides, ServiceConfig};
use mutsuki_service_control::{ControlMethod, ControlRequest, IdParam};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(name = "mutsuki-service")]
#[command(about = "MutsukiCore foreground daemon host and local control client")]
struct Cli {
    #[arg(long, global = true)]
    profile: Option<String>,
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    home: Option<PathBuf>,
    #[arg(long, env = "MUTSUKI_CONTROL_TOKEN", global = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run,
    Status,
    Stop,
    Health,
    Install,
    Uninstall,
    Start,
    #[command(name = "windows-service-run", hide = true)]
    WindowsServiceRun,
    #[command(subcommand)]
    Plugin(PluginCommand),
    #[command(subcommand)]
    Runner(RunnerCommand),
    #[command(subcommand)]
    EventSource(EventSourceCommand),
    #[command(subcommand)]
    Task(TaskCommand),
}

#[derive(Subcommand)]
enum PluginCommand {
    List,
    Reload,
}

#[derive(Subcommand)]
enum RunnerCommand {
    List,
    Restart { id: String },
    Stop { id: String },
}

#[derive(Subcommand)]
enum EventSourceCommand {
    List,
    Restart { id: String },
}

#[derive(Subcommand)]
enum TaskCommand {
    List,
    Cancel { id: String },
    Outcome { id: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Cli {
        profile,
        config: config_file,
        home,
        token,
        command,
    } = Cli::parse();
    let token_override = token.is_some();
    let config = ServiceConfig::load(ConfigOverrides {
        profile,
        config_file: config_file.clone(),
        home_dir: home,
        control_token: token,
    })?;

    match command {
        Command::Run => {
            let runtime = mutsuki_service_runtime::ServiceRuntime::start(config).await?;
            runtime.run_foreground().await?;
        }
        Command::Status => {
            request_and_print(&config, ControlMethod::ServiceStatus, Value::Null).await?
        }
        Command::Stop => {
            request_and_print(&config, ControlMethod::ServiceShutdown, Value::Null).await?
        }
        Command::Health => {
            request_and_print(&config, ControlMethod::HealthCheck, Value::Null).await?
        }
        Command::Install => {
            let launch = mutsuki_service_daemon::DaemonLaunchOptions::from_config(
                &config,
                config_file,
                token_override,
            )?;
            mutsuki_service_daemon::install(&config, &launch)?;
            print_daemon_action("installed", &config);
        }
        Command::Uninstall => {
            mutsuki_service_daemon::uninstall(&config)?;
            print_daemon_action("uninstalled", &config);
        }
        Command::Start => {
            mutsuki_service_daemon::start(&config)?;
            print_daemon_action("started", &config);
        }
        Command::WindowsServiceRun => {
            mutsuki_service_daemon::run_windows_service(config)?;
        }
        Command::Plugin(command) => match command {
            PluginCommand::List => {
                request_and_print(&config, ControlMethod::PluginList, Value::Null).await?
            }
            PluginCommand::Reload => {
                request_and_print(&config, ControlMethod::PluginReload, Value::Null).await?
            }
        },
        Command::Runner(command) => match command {
            RunnerCommand::List => {
                request_and_print(&config, ControlMethod::RunnerList, Value::Null).await?
            }
            RunnerCommand::Restart { id } => {
                request_and_print(
                    &config,
                    ControlMethod::RunnerRestart,
                    serde_json::to_value(IdParam { id })?,
                )
                .await?
            }
            RunnerCommand::Stop { id } => {
                request_and_print(
                    &config,
                    ControlMethod::RunnerStop,
                    serde_json::to_value(IdParam { id })?,
                )
                .await?
            }
        },
        Command::EventSource(command) => match command {
            EventSourceCommand::List => {
                request_and_print(&config, ControlMethod::EventSourceList, Value::Null).await?
            }
            EventSourceCommand::Restart { id } => {
                request_and_print(
                    &config,
                    ControlMethod::EventSourceRestart,
                    serde_json::to_value(IdParam { id })?,
                )
                .await?
            }
        },
        Command::Task(command) => match command {
            TaskCommand::List => {
                request_and_print(&config, ControlMethod::TaskList, Value::Null).await?
            }
            TaskCommand::Cancel { id } => {
                request_and_print(
                    &config,
                    ControlMethod::TaskCancel,
                    serde_json::to_value(IdParam { id })?,
                )
                .await?
            }
            TaskCommand::Outcome { id } => {
                request_and_print(
                    &config,
                    ControlMethod::TaskOutcome,
                    serde_json::to_value(IdParam { id })?,
                )
                .await?
            }
        },
    }
    Ok(())
}

fn print_daemon_action(action: &str, config: &ServiceConfig) {
    println!(
        "{action} {}",
        mutsuki_service_daemon::windows_service_name(config)
    );
}

async fn request_and_print(
    config: &ServiceConfig,
    method: ControlMethod,
    params: Value,
) -> anyhow::Result<()> {
    let response = mutsuki_service_ipc::request(
        config,
        ControlRequest {
            token: config.control_token().to_string(),
            method,
            params,
        },
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&json!(response))?);
    Ok(())
}
