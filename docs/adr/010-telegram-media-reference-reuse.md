# ADR-010：优先复用 Telegram media reference

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

Repeat 和 UP 重建聚合时需要再次发送既有媒体。每次完整下载再上传会增加带宽、耗时和 Telegram 写入压力，但 Telegram media reference 可能过期。

## 决策

- 聚合时优先从来源 Telegram message 重新取得 media reference，并直接传给 `send_file`。
- 只有 Telegram 明确抛出 `FileReferenceExpiredError` 时才进入 fallback。
- fallback 重新下载对应媒体到临时目录，再上传目标频道。
- fallback 下载仍受 small/large/global gate 控制，upload 全局并发为 1。
- 权限错误、消息不存在等其他异常不伪装成引用过期。

## 结果与权衡

- 正常路径避免重复上传，减少带宽和处理时间。
- 引用过期时仍可恢复发送，前提是来源消息和媒体仍可访问。
- 服务不维护独立对象存储；来源消息不可用时无法重建媒体。
- 没有实际带宽成本或 fallback 发生率数据可验证收益。

## 历史演进

初版只复用 media reference，没有引用过期处理。审查发现缺口后增加了精确捕获 `FileReferenceExpiredError` 的 upload fallback。

## 证据与可信度

- **A — 仓库事实**：`send_media` 和 `_upload_fallback` 直接实现该策略。
- **负责人确认**：没有带宽或成本基线；不计划引入对象存储。
- **B — 推测**：复用引用的主要动机是降低上传成本和延迟。

## 待确认

- media reference 的实际过期率尚无数据。
