# Plugin Loader Skill

用于插件发现、manifest 校验、builtin registry、native/ABI/external 插件运行环境。

## 边界

- ServiceHost 扫描目录、读取 `plugin.toml`、校验 API 版本和部署类型、生成加载计划输入。
- 不在本仓库实现 StdPlugins、AgentKit、BotPlugins 或模型插件。
- Builtin 插件只能通过编译期 feature/注册表接入真实 crate；缺失时必须报告 unavailable。
- 业务插件由所属仓库实现；能力缺失时先补齐上游并更新依赖，不得在 loader 中替代实现或伪造可用能力。

## Manifest

- `plugin.toml` 必须映射到 Mutsuki contracts 的 `PluginManifest`。
- external runner manifest 可以声明 `runtime.command`、`runtime.args`、`runtime.env`、`runtime.cwd`、`runtime.runner_link`。
- secret/token 不允许写入 manifest。

## 热重载

- reload 必须走 scan -> validate -> generation/load-plan 比较 -> drain -> swap。
- 不允许原地替换正在占用的 breaking surface。
