# Observe Skill

用于结构化日志、trace、health、panic hook、runner stdout/stderr 和 crash report。

## 边界

- ServiceHost 统一聚合运行级观测。
- 插件领域健康信息由插件提供，Host 只聚合。
- secret/token 不进入普通日志。

## 要求

- 日志目录来自配置，默认写入 `<MUTSUKI_HOME>/logs`。
- panic hook 必须落文件并保留原 hook 行为。
- health 至少包含 service、core、plugin、runner、recent error。
