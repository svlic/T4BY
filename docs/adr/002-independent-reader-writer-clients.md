# ADR-002：Reader 与 Writer 使用独立 Telegram 用户会话和凭据

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

服务的读取和写入职责由两个 Telegram 用户账号承担。开发早期两个 session 共用一组 API ID/API Hash，并曾由 Writer 解析全部频道。

## 决策

Reader 与 Writer 分别使用独立的 API ID、API Hash 和 session。

- Reader 解析 SOURCE，并消费 INFO 事件；它不承担业务写职责。
- Writer 解析 INFO、VER、MAN、ONESHOT、REPEAT、UP 和 BLACKLIST；它不需要且长期不应访问 SOURCE。
- Writer 分类原件仅从 INFO/VER 读取；手工迁移读取目标频道副本，合并可复用已有 UP 媒体。Writer 共用网关在 Telegram RPC 前拒绝 SOURCE 的读取、历史搜索、下载、转发、发送、删除及过期媒体回退，Reader 网关不受此限制。
- 两个客户端顺序完成首次登录，避免两个未认证 session 争抢同一终端输入。

## 结果与权衡

- 账号权限、Telegram 限流和业务职责相互隔离。
- 每个账号可以使用不同 Telegram API 应用配置。
- 部署需要管理两套长期 session 和凭据。
- Reader/Writer 权限边界成为运行配置的一部分，错误授权可能直到启动或真实调用时才暴露。

## 历史演进

实现期发生过三项明确调整：

1. 共享 API 凭据改为独立凭据；
2. Writer 解析全部频道改为按账号权限分别解析；
3. 并发启动两个未登录客户端改为顺序登录。

这些调整在最终提交前完成，没有独立正式 Commit。

## 证据与可信度

- **A — 仓库事实**：配置、应用启动代码、环境变量示例和测试均使用独立凭据与 session。
- **负责人确认**：双账号用于频道限制、限流和业务隔离；Reader 不需要写；Writer 不访问 SOURCE。

## 待确认

无。
