# 架构决策记录

本目录记录 T4BY 的重要架构决策。现有 ADR 于 2026-10-02 根据完整 Git 历史、提交关联的开发记录、当前实现与项目负责人补充说明进行历史重建。

## 证据标记

- **A — 仓库事实**：可由 Git 历史、项目文档、代码或测试直接确认。
- **B — 推测**：根据实现推测的动机，不作为已确认事实。
- **负责人确认**：历史重建期间由项目负责人补充的决策背景。
- **待确认**：目前没有证据，保留为未知，不根据最终代码倒推。

“已接受（历史重建）”表示该决策能够从现有证据确认，但不表示它已经过生产验证，也不表示已完成文档中的后续事项。

## 决策索引

| ADR | 标题 | 状态 |
| --- | --- | --- |
| [ADR-001](001-python-async-telethon-gateway.md) | 采用 Python 异步单体与 Telethon Gateway 分层 | 已接受（历史重建） |
| [ADR-002](002-independent-reader-writer-clients.md) | Reader 与 Writer 使用独立 Telegram 用户会话和凭据 | 已接受（历史重建） |
| [ADR-003](003-single-process-sqlite-durable-scheduling.md) | 采用单进程 SQLite WAL 与持久化调度 | 已接受（历史重建） |
| [ADR-004](004-logical-message-occurrence-identity.md) | 以 Logical Message 和 occurrence_id 建模业务身份 | 已接受（历史重建） |
| [ADR-005](005-reader-source-matching.md) | 使用转发元数据和双向窗口匹配 SOURCE | 已接受（历史重建） |
| [ADR-006](006-sha256-classification.md) | 使用 INFO+VER 媒体 SHA-256 分类 | 已接受（历史重建） |
| [ADR-007](007-hash-connected-component-merging.md) | 使用 Hash 传递连通分量聚合 Repeat 与 UP | 已接受（历史重建） |
| [ADR-008](008-durable-debounced-merge-consistency.md) | 使用持久化 debounce merge 与先建后删策略 | 已接受（历史重建） |
| [ADR-009](009-adaptive-telegram-backpressure.md) | 由应用接管 Telegram 背压并实施自适应限流 | 已接受（历史重建） |
| [ADR-010](010-telegram-media-reference-reuse.md) | 优先复用 Telegram media reference | 已接受（历史重建） |
| [ADR-011](011-manual-migration-and-deletion-semantics.md) | 以消息身份执行人工迁移并区分隐藏与删除 | 已接受（历史重建） |
| [ADR-012](012-docker-compose-and-observability.md) | 以 Docker Compose 部署并暴露 Prometheus 指标 | 已接受（历史重建） |

## 历史范围

正式可达历史只有两个提交：

- `f62acc584baeb86679e6438094bc91c6fc42accd`：仅创建项目标题。
- `c5d33e59496aee731438a0e1b49f437b5af3c868`：一次性加入应用、测试和部署配置。

对象库中的两个不可达提交是 `git stash` 内部对象，没有保存可用于重建源码演进的中间版本。多数实现期调整只能由最终 diff 和提交关联开发记录确认，不能映射到独立的正式提交。
