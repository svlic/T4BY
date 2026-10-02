# ADR-001：采用 Python 异步单体与 Telethon Gateway 分层

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`f62acc584baeb86679e6438094bc91c6fc42accd`、`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

初始仓库只有 README 标题，没有既有技术栈。服务需要同时处理 Telegram 事件、下载媒体、持久化任务、延迟调度和并发限流。

## 决策

采用 Python 3.11 和 asyncio 构建单体服务，使用 Telethon 访问 Telegram。代码按 models、gateway、reader、writer、manual、merger、storage、metrics 和 app 职责拆分。

Telegram 操作集中在 `TelegramGateway` Protocol 后，由 `TelethonGateway` 实现。业务服务依赖该接口，测试使用 FakeGateway，不连接 Telegram。

项目不调用 LLM 或其他生成式模型。代码中的“模型”仅指 Message、LogicalMessage、Media、Occurrence 等业务数据模型。

## 结果与权衡

- 异步运行时可以让 Telegram I/O、任务 worker 和调度器共存于一个进程。
- Gateway 降低业务逻辑对 Telethon 具体 API 的耦合，并允许离线测试。
- 单体部署较简单，但进程内锁、任务和限流状态不能直接扩展到多实例。
- Python 类型 Protocol 只提供静态约束，不形成独立网络服务边界。

## 历史演进

没有证据表明项目曾正式采用其他语言、Bot API、Pyrogram 或微服务方案。

## 证据与可信度

- **A — 仓库事实**：`pyproject.toml` 指定 Python 3.11、Telethon 和 asyncio 测试栈；`gateway.py` 定义 Protocol；测试使用 FakeGateway。
- **负责人确认**：没有比较过其他技术栈；使用 Telegram User Client 是业务身份要求。
- **B — 推测**：Gateway 分层也有助于未来替换 Telegram 适配器，但这不是已确认路线。

## 待确认

- Gateway 是否应作为长期稳定的内部接口，目前没有明确承诺。
