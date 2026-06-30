# Config And Security Skill

用于配置层级、profile、目录解析、secret backend 和权限边界。

## 配置层级

默认配置 -> profile 配置 -> 本地 `service.toml` -> 环境变量 -> 命令行参数。

## 安全要求

- control API 默认本地访问并需要 token。
- secret 第一版支持环境变量 backend；明文文件只能作为后续扩展。
- 插件目录、数据目录、日志目录、run 目录必须隔离。
- 外部 Runner 只接收 allowlist 环境变量和 Host 注入变量。
