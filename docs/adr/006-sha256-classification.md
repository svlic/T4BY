# ADR-006：使用 INFO+VER 媒体 SHA-256 分类

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

分类需要判断媒体是否首次出现、是否属于 UP 或 BLACKLIST，并且同一个 occurrence 可以包含多个 INFO/VER 媒体。

## 决策

- 下载 INFO 与 VER 的全部媒体，对原始字节流计算 SHA-256。
- INFO 和 VER 下载可并行；文件 Hash 以 1 MiB 分块读取，并在线程中执行，避免阻塞事件循环。
- occurrence 中任意 Hash 命中即按以下优先级决定整个 occurrence：
  1. BLACKLIST；
  2. UP；
  3. 没有历史 Hash 时为 ONESHOT；
  4. 否则为 REPEAT。
- 使用进程内 classification commit lock 串行执行“读取历史→决定分类→写 Telegram→提交历史”，媒体下载仍保持并发。

## 结果与权衡

- 内容身份不依赖 Telegram message ID 或文件名。
- INFO Hash 可以建立关系，即使最终 Repeat 只展示 VER 媒体。
- 字节级 Hash 无法识别转码、重新压缩或元数据变化后的视觉等价媒体。
- 分类锁避免两个并发输入同时被判断为首次出现，但限定了单进程模型。
- Telegram 写入与 SQLite 提交不能形成原子事务。

## 历史演进

初版在事件循环中同步读取文件；静态检查发现阻塞风险后改为 `asyncio.to_thread`。没有证据表明项目采用过其他 Hash 或相似度算法。

## 证据与可信度

- **A — 仓库事实**：Gateway、Hash Registry、分类函数、进程锁和测试直接支持该规则。
- **负责人确认**：不考虑 Telegram file ID、感知 Hash 等替代方案；任一媒体命中影响整个 occurrence 符合业务；不需要识别转码后的等价媒体。

## 待确认

无。
