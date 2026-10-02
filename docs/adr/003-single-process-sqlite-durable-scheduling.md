# ADR-003：采用单进程 SQLite WAL 与持久化调度

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

事件处理包含固定延迟、Telegram 限流后的重试和可合并工作。仅使用内存队列会在进程重启后丢失这些状态。

## 决策

使用 SQLite、WAL 和 `aiosqlite` 作为业务数据与调度状态的共同存储，并将部署限制为单进程。

- `jobs` 保存 reader、classify、manual 和 man 任务；`(kind, dedupe_key)` 唯一。
- classification 的 10 秒等待使用 `not_before`，worker 不原地睡眠。
- `merge_dirty` 保存待合并 Hash、dirty/running 状态和时间。
- 启动时把遗留 running job 恢复为 retry_wait，并把运行中的 merge 重新标记为 dirty。
- 事务认领使用 `BEGIN IMMEDIATE` 和进程内锁协调单连接访问。

## 结果与权衡

- 延迟任务和 merge 状态可在重启后恢复。
- 部署和运维不需要外部数据库或消息队列。
- 不支持多个实例共享同一 SQLite 文件，也没有横向扩展能力。
- Schema 直接嵌入源码，目前没有 schema version 或迁移框架。
- 普通异常会把 job 标记为 failed；当前没有通用重试上限、死信队列或人工重放接口。

## 历史演进

没有证据表明项目曾采用 PostgreSQL、Redis 或独立消息队列。

## 证据与可信度

- **A — 仓库事实**：README 明确限制单进程；Schema 启用 WAL；测试覆盖 delayed job 和 running job 恢复。
- **负责人确认**：暂无数据库迁移或通用失败任务机制的进一步规划。
- **B — 推测**：该选择以扩展能力换取单机部署简单性。

## 待确认

- 预期吞吐量和数据保留周期尚未确定。
