# Control API Skill

用于本地控制 API、IPC transport、CLI 客户端和鉴权。

## 边界

- 第一版默认只允许本地控制面。
- Windows 使用 Named Pipe；Linux/macOS 使用 Unix Domain Socket；调试 TCP 必须显式启用。
- API 只做 service/core/plugin/runner/task/log/health 控制和查询，不实现业务管理后台。

## 方法

必须保持结构化请求/响应：

- `service.status`
- `service.shutdown`
- `core.status`
- `plugin.list`
- `plugin.reload`
- `runner.list`
- `runner.restart`
- `runner.stop`
- `task.list`
- `task.cancel`
- `health.check`

所有请求必须携带 control token，除非显式处于测试模式。
