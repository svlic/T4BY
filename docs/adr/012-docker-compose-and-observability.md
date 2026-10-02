# ADR-012：以 Docker Compose 部署并暴露 Prometheus 指标

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

开发早期文档计划使用 virtualenv 直接运行，并建议由 systemd 管理。随后部署目标明确为单机 Docker 环境，同时首次 Telegram 登录仍需要交互终端。

## 决策

- 使用 `python:3.11-slim` 构建镜像，并通过安装后的 `t4by` entry point 启动。
- 容器以 UID 10001 的非 root 用户运行。
- Reader/Writer session 和 SQLite 统一保存在 `/app/data`，由 `t4by-data` named volume 持久化。
- Compose 启用 TTY/stdin 以支持首次登录，并设置 `restart: unless-stopped`。
- Prometheus HTTP 服务在容器内监听配置地址；Compose 只将端口绑定到宿主 `127.0.0.1:9464`。
- 指标覆盖队列深度、最老 job age、merge dirty、merge 时长、活跃下载、FloodWait 次数/秒数和当前 RPC rate。

## 结果与权衡

- 正式运行环境统一为单机 Docker Compose。
- named volume 在容器重建后保存 session 和数据库，但 `docker compose down -v` 会永久删除数据。
- localhost 绑定避免直接从外网暴露未认证指标端点。
- 当前没有随仓库提供 dashboard、告警规则、备份或恢复脚本。

## 历史演进

项目从 virtualenv/systemd 建议调整为 Dockerfile 和 Compose。镜像构建、非 root 身份、数据目录写入和 Compose 配置在提交前得到验证。

## 证据与可信度

- **A — 仓库事实**：Dockerfile、Compose、README、配置和 metrics 模块直接支持。
- **负责人确认**：正式环境是单机 Docker Compose；暂不需要 dashboard 或告警规则。
- **B — 推测**：宿主 localhost 绑定也承担基础安全隔离作用。

## 待确认

- Session 和 SQLite 的备份、加密及恢复演练策略尚未确定。
