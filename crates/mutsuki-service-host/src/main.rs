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
    #[command(subcommand)]
    Plugin(PluginCommand),
    #[command(subcommand)]
    Runner(RunnerCommand),
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
enum TaskCommand {
    List,
    Cancel { id: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = ServiceConfig::load(ConfigOverrides {
        profile: cli.profile,
        config_file: cli.config,
        home_dir: cli.home,
        control_token: cli.token,
    })?;

    match cli.command {
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
        Command::Install => unsupported_daemon("install")?,
        Command::Uninstall => unsupported_daemon("uninstall")?,
        Command::Start => unsupported_daemon("start")?,
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
        },
    }
    Ok(())
}

fn unsupported_daemon(operation: &'static str) -> anyhow::Result<()> {
    anyhow::bail!(
        "{operation} is not implemented for {} in ServiceHost 0.1.0; use foreground run mode",
        std::env::consts::OS
    )
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
