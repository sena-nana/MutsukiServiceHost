# MutsukiServiceHost 工作规范

本仓库是 **MutsukiCore 的无界面常驻服务 Host**。它负责进程生命周期、配置、插件发现、外部 Runner 监督、本地控制面、日志观测和服务化集成；不得实现 AgentLoop、Bot 路由、QQBot 网关、模型 Provider、Python SDK 或业务插件逻辑。

## 阅读顺序

按改动方向选择对应 skill，并先读完该 skill 的 `SKILL.md`：

1. `skills/runtime/SKILL.md`：Core bootstrap、HostServices、生命周期、shutdown/drain。
2. `skills/plugin-loader/SKILL.md`：plugin.toml、builtin/native/ABI/external 插件发现与加载。
3. `skills/runner-supervisor/SKILL.md`：外部 Runner 进程、Runner Link、重启、stdio、环境变量隔离。
4. `skills/control-api/SKILL.md`：本地控制 API、IPC、CLI 命令、鉴权。
5. `skills/observe/SKILL.md`：日志、trace、health、panic/crash 记录。
6. `skills/config-security/SKILL.md`：profile、配置层级、secret、权限边界。
7. `skills/daemon/SKILL.md`：Windows Service/systemd/launchd 安装和长期运行。

## Hard Rules

- ServiceHost 管运行环境；Core 管任务系统；插件管领域能力。
- 不在 Host 中加入 Agent/Bot/Provider/Python SDK 业务逻辑。
- Core、StdPlugins、AgentKit、BotPlugins、Runner Kit、Core adapter、SDK 和业务插件必须由所属仓库实现；Host 只能接入其公开 API、协议或真实 crate，禁止复制、重写、内建替代或生产 fallback/shim。
- 上游能力缺失时必须 fail loud/unavailable，先补齐上游并更新依赖，不得把职责下沉到 Host；test double 仅限测试路径。
- 插件和 Runner 能力必须真实接入对应后端；禁止做看似可用但未接线的 UI/CLI 输出。
- 修复问题先定位根因，选择正确层级修正，禁止只为绕过症状打补丁。
- 配置、控制 API、Runner 环境、secret 和日志必须默认安全：本地访问、token 鉴权、secret 不进普通日志、外部 Runner 不默认继承完整环境。
- 禁止仓库外 Cargo `path`/本地 `[patch]`；跨仓库依赖使用远端 Git URL 和固定 `rev`，并在独立 checkout 验证。
- 新测试必须验证功能行为；禁止低价值字符串/日志硬匹配测试。

## CodeGraph

如果仓库根目录存在 `.codegraph/`，需要理解或定位代码时先使用 CodeGraph，再使用 `rg` 或直接读文件。没有 `.codegraph/` 时跳过。

## 验证

Rust 代码改动至少运行：

```powershell
cargo fmt --check
cargo check
```

涉及控制面、Runner、配置解析或插件加载时，优先补充行为测试或运行对应定向测试。最终说明必须列出实际执行过的验证命令与结果。
