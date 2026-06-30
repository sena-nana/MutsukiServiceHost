# Runtime Skill

用于 Core bootstrap、HostServices、生命周期、shutdown/drain、panic 边界和前台 service loop。

## 边界

- 可以创建 Tokio runtime、读取 service profile、初始化 HostServices、启动 MutsukiCore、进入 service loop。
- 不实现 Core 调度、TaskPool、RunnerRegistry、AgentLoop 或 Bot 路由。
- Core 相关能力必须通过 `mutsuki-runtime-host` / `mutsuki-runtime-core` 的公开 API 接入。

## 实现要求

- Core 启动失败必须 fail loud，不能降级为“服务已运行”。
- shutdown 必须通知 control plane、停止 IPC、停止 runner supervisor，再释放 HostRuntime。
- drain 是生命周期动作，不是插件业务逻辑。
- HostServices 只暴露 OS/运行环境能力，标准协议包装属于 StdPlugins。
