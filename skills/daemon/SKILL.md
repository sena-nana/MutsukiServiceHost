# Daemon Skill

用于 Windows Service、systemd、launchd 和长期运行安装。

## 第一版

- `run` 前台模式必须完整可用。
- install/uninstall/start/stop 命令可以先提供平台能力探测和明确 unsupported 错误。
- Windows Service 优先级高于 systemd/launchd。

## 边界

- 服务化只管理 ServiceHost 进程。
- 不把 GUI、插件市场或业务配置界面放进本仓库。
