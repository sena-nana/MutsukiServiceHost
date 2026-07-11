# Runner Supervisor Skill

用于外部 Runner 进程生命周期、Runner Link、stdio 收集、重启策略和环境隔离。

## 边界

- ServiceHost 负责启动、监控、重启、停止 external runner。
- Python/Rust/ABI Runner 的协议实现属于对应 runner kit 或 Core adapter。
- 外部 Runner 不默认继承完整宿主环境。
- SDK、Runner Kit 和 Core adapter 由所属仓库实现；能力缺失时先补齐上游并更新依赖，Host 只报告 unsupported/unavailable，不得提供本地替代或私有协议。

## 要求

- 启动时传入 session token 和必要的运行目录。
- stdout/stderr 必须被收集，避免子进程阻塞。
- 重启必须有限速；超过限制进入 failed 状态。
- shutdown 先 graceful，再超时 kill。
- 与 Core 接入时必须使用明确 Runner Link 协议，例如 `jsonl-stdio`。
