use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use mutsuki_service_config::ServiceConfig;
use mutsuki_service_control::{
    ControlErrorBody, ControlMethod, ControlRequest, ConversationHistoryResponse,
    ConversationSendParams, ConversationSendResponse, ConversationTurn, LogTailParams,
    LogTailResponse, PluginCallParams, TerminalTuiStatus,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const CONVERSATION_PLUGIN_ID: &str = "mutsuki.conversation.sim";
const TUI_PLUGIN_ID: &str = "mutsuki.terminal.tui";
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_LOG_LINES: usize = 200;
const MAX_TURNS: usize = 40;

pub async fn run(config: ServiceConfig) -> anyhow::Result<()> {
    let status: TerminalTuiStatus =
        plugin_call(&config, TUI_PLUGIN_ID, "status", Value::Null).await?;
    if !status.available {
        bail!("terminal TUI plugin is not available");
    }
    let history: ConversationHistoryResponse =
        plugin_call(&config, CONVERSATION_PLUGIN_ID, "history", Value::Null).await?;

    let mut app = TuiApp::default();
    app.apply_history(history);

    enable_raw_mode().context("enable terminal raw mode")?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("create terminal")?;

    let result = run_loop(&config, &mut terminal, &mut app).await;

    disable_raw_mode().ok();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();
    result
}

async fn run_loop(
    config: &ServiceConfig,
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut TuiApp,
) -> anyhow::Result<()> {
    let mut last_log_poll = Instant::now() - LOG_POLL_INTERVAL;
    while !app.should_quit {
        if last_log_poll.elapsed() >= LOG_POLL_INTERVAL {
            let response = request_log_tail(config, app.log_cursor, Some(100)).await?;
            app.apply_log_tail(response);
            last_log_poll = Instant::now();
        }

        terminal.draw(|frame| render(frame, app))?;

        if event::poll(Duration::from_millis(50))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            match app.handle_key_code(key.code, key.modifiers) {
                AppAction::None => {}
                AppAction::Quit => app.should_quit = true,
                AppAction::Send(message) => {
                    let response: ConversationSendResponse = plugin_call(
                        config,
                        CONVERSATION_PLUGIN_ID,
                        "send",
                        json!(ConversationSendParams { message }),
                    )
                    .await?;
                    app.apply_send_response(response);
                }
            }
        }
    }
    Ok(())
}

fn render(frame: &mut ratatui::Frame<'_>, app: &TuiApp) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(8),
            Constraint::Length(8),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(area);

    let log_lines = app
        .logs
        .iter()
        .map(|line| Line::from(line.as_str()))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(log_lines)
            .block(Block::default().title("Logs").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[0],
    );

    let turn_lines = app
        .turns
        .iter()
        .map(|turn| {
            Line::from(vec![
                Span::styled(
                    format!("{} ", turn.role),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(turn.content.as_str()),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(turn_lines)
            .block(Block::default().title("Conversation").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[1],
    );

    frame.render_widget(
        Paragraph::new(app.latest_reply.as_str())
            .block(Block::default().title("Reply").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[2],
    );

    frame.render_widget(
        Paragraph::new(format!("> {}", app.input))
            .block(Block::default().title("Input").borders(Borders::ALL)),
        chunks[3],
    );
}

async fn request_log_tail(
    config: &ServiceConfig,
    cursor: Option<u64>,
    lines: Option<usize>,
) -> anyhow::Result<LogTailResponse> {
    request_control(
        config,
        ControlMethod::LogTail,
        json!(LogTailParams {
            cursor,
            lines,
            filters: Default::default(),
        }),
    )
    .await
}

async fn plugin_call<T: DeserializeOwned>(
    config: &ServiceConfig,
    plugin_id: &str,
    operation: &str,
    payload: Value,
) -> anyhow::Result<T> {
    request_control(
        config,
        ControlMethod::PluginCall,
        json!(PluginCallParams {
            plugin_id: plugin_id.into(),
            operation: operation.into(),
            payload,
        }),
    )
    .await
}

async fn request_control<T: DeserializeOwned>(
    config: &ServiceConfig,
    method: ControlMethod,
    params: Value,
) -> anyhow::Result<T> {
    let response = mutsuki_service_ipc::request(
        config,
        ControlRequest {
            token: config.control_token().to_string(),
            method,
            params,
        },
    )
    .await?;
    if !response.ok {
        return Err(control_error(response.error));
    }
    serde_json::from_value(response.result.unwrap_or(Value::Null)).map_err(Into::into)
}

fn control_error(error: Option<ControlErrorBody>) -> anyhow::Error {
    match error {
        Some(error) => anyhow::anyhow!("{}: {}", error.code, error.message),
        None => anyhow::anyhow!("control request failed"),
    }
}

#[derive(Clone, Debug, Default)]
pub struct TuiApp {
    input: String,
    logs: Vec<String>,
    turns: Vec<ConversationTurn>,
    latest_reply: String,
    log_cursor: Option<u64>,
    should_quit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppAction {
    None,
    Quit,
    Send(String),
}

impl TuiApp {
    pub fn handle_key_code(&mut self, code: KeyCode, modifiers: KeyModifiers) -> AppAction {
        if code == KeyCode::Esc
            || (code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL))
        {
            return AppAction::Quit;
        }
        match code {
            KeyCode::Char(character) => {
                self.input.push(character);
                AppAction::None
            }
            KeyCode::Backspace => {
                self.input.pop();
                AppAction::None
            }
            KeyCode::Enter => {
                let message = self.input.trim().to_string();
                self.input.clear();
                if message.is_empty() {
                    AppAction::None
                } else {
                    AppAction::Send(message)
                }
            }
            _ => AppAction::None,
        }
    }

    pub fn apply_log_tail(&mut self, response: LogTailResponse) {
        self.log_cursor = Some(response.cursor);
        self.logs
            .extend(response.entries.into_iter().map(|entry| entry.line));
        if self.logs.len() > MAX_LOG_LINES {
            let start = self.logs.len() - MAX_LOG_LINES;
            self.logs.drain(0..start);
        }
    }

    pub fn apply_send_response(&mut self, response: ConversationSendResponse) {
        self.latest_reply = response.reply.content;
        self.turns = response.turns;
        self.trim_turns();
    }

    pub fn apply_history(&mut self, response: ConversationHistoryResponse) {
        self.latest_reply = response
            .turns
            .iter()
            .rev()
            .find(|turn| turn.role == "assistant")
            .map(|turn| turn.content.clone())
            .unwrap_or_default();
        self.turns = response.turns;
        self.trim_turns();
    }

    fn trim_turns(&mut self) {
        if self.turns.len() > MAX_TURNS {
            let start = self.turns.len() - MAX_TURNS;
            self.turns.drain(0..start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutsuki_service_control::LogTailEntry;

    #[test]
    fn enter_returns_send_action_and_clears_input() {
        let mut app = TuiApp::default();
        assert_eq!(
            app.handle_key_code(KeyCode::Char('h'), KeyModifiers::empty()),
            AppAction::None
        );
        assert_eq!(
            app.handle_key_code(KeyCode::Enter, KeyModifiers::empty()),
            AppAction::Send("h".into())
        );
        assert!(app.input.is_empty());
    }

    #[test]
    fn empty_input_is_not_sent() {
        let mut app = TuiApp::default();
        app.input = "  ".into();

        assert_eq!(
            app.handle_key_code(KeyCode::Enter, KeyModifiers::empty()),
            AppAction::None
        );
        assert!(app.input.is_empty());
    }

    #[test]
    fn log_tail_updates_cursor_and_keeps_recent_lines() {
        let mut app = TuiApp::default();
        app.apply_log_tail(LogTailResponse {
            cursor: 8,
            entries: vec![LogTailEntry {
                offset: 0,
                line: "one".into(),
            }],
        });

        assert_eq!(app.log_cursor, Some(8));
        assert_eq!(app.logs, vec!["one"]);
    }

    #[test]
    fn send_response_updates_latest_reply_and_turns() {
        let mut app = TuiApp::default();
        let reply = ConversationTurn {
            sequence: 2,
            role: "assistant".into(),
            content: "reply".into(),
        };

        app.apply_send_response(ConversationSendResponse {
            reply: reply.clone(),
            turns: vec![
                ConversationTurn {
                    sequence: 1,
                    role: "user".into(),
                    content: "hello".into(),
                },
                reply,
            ],
        });

        assert_eq!(app.latest_reply, "reply");
        assert_eq!(app.turns.len(), 2);
    }
}
