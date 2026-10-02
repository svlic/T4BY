# ADR-009：由应用接管 Telegram 背压并实施自适应限流

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

Telegram 会通过 FloodWait 和 SlowModeWait 要求客户端降低速率。若 worker 原地等待，会占用有限并发；若当作业务错误送往 MAN，则会产生错误告警和重复人工处理。

## 决策

- Telethon client 使用 `flood_sleep_threshold=0`，由应用接管等待策略。
- 将 FloodWait/SlowModeWait 转换为带方法或 chat cooldown 的 `RetryAfter`，并增加 1 秒加随机抖动。
- worker 收到 `RetryAfter` 后，将 job 持久化为 `retry_wait`，不发送 MAN。
- control、history、全局 write 和 per-chat write 使用独立 gate。
- 基线为 control 5/s、history 1/s、write 2/s、per-chat 1/s；control inflight 4、history inflight 1。
- 下载全局并发 4，small 3，large 2；upload 并发 1。
- FloodWait 后相关 token rate 减半或下载并发减一；30 分钟后逐步恢复到基线。
- 自适应状态只保存在内存，不跨重启持久化。

## 结果与权衡

- Telegram 背压不会占住 worker 原地等待，也不会误入 MAN。
- 各类 RPC 相互隔离，history 或下载不会直接使用 write 配额。
- 重启会恢复基线速率，丢失之前学习到的降速状态。
- 固定基线依赖文档和预设值，没有运行时读取 server limit 的实现。

## 历史演进

- 初版 RetryAfter 会被通用异常捕获并送往 MAN，随后增加显式传播。
- small/large 下载最初是固定 Semaphore，随后改为 AdaptiveConcurrency。

## 证据与可信度

- **A — 仓库事实**：Gateway、job worker、Prometheus 指标和 FloodWait 测试直接支持。
- **负责人确认**：基线参数来自文档；限流状态不需要跨重启保存。

## 待确认

- 是否需要读取 Telegram runtime/server 配置动态调整上限，尚未确定。
