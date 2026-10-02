# ADR-008：使用持久化 debounce merge 与先建后删策略

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

每个新分类立即重建聚合会产生大量重复的发送和删除操作。与此同时，Telegram 与 SQLite 无法形成原子事务，聚合替换必须明确外部副作用的顺序。

## 决策

- Repeat 和 UP 变化只写入持久化 `merge_dirty`，由单一 merge scheduler 执行。
- 相同 bucket 的 seed Hash 合并，静默 5 秒或累计等待 20 秒后可被认领。
- merge 期间到达的新 dirty 状态留给下一轮。
- 替换顺序为：创建新聚合、删除旧消息、提交新的 active group 并 supersede 旧 group。
- UP 删除旧消息前写入 `expected_deletions`，避免程序删除被解释为人工隐藏。
- Merge 失败报告通过持久化 MAN job 执行，而不是在异常路径中即时转发。

## 结果与权衡

- debounce 合并短时间内的重复工作，降低 Telegram write/delete 压力。
- 先成功创建新内容再删除旧内容，避免在发送失败时先失去旧聚合。
- Telegram 写入成功后、数据库提交前发生崩溃时可能留下重复或状态不一致，需要人工处理。
- 当前只有 RetryAfter 会将 failed Hash 重新标 dirty；其他 merge 异常会结束本轮并创建 MAN 报告，不具备通用自动重试机制。

## 历史演进

Merge 失败最初直接尝试发送 MAN，随后改为持久化 MAN job，使报告本身可随任务系统恢复。

## 证据与可信度

- **A — 仓库事实**：README 记录非原子边界；`merge_dirty`、scheduler、MergeEngine 和测试支持 debounce 与先建后删顺序。
- **负责人确认**：quiet/max-wait 参数没有额外依据；非 RetryAfter 的 merge 失败也应有一定重试机制以提高鲁棒性。
- **B — 推测**：debounce 的主要收益是减少发送、删除和媒体处理成本。

## 后续事项

- 设计有界、可观测且不会永久热循环的通用 merge 重试机制。
- 是否需要自动 reconciliation，而不仅是 MAN 报告，尚未确定。
