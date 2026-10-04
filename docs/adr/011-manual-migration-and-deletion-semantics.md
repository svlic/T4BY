# ADR-011：以消息身份执行人工迁移并区分隐藏与删除

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

用户从 ONESHOT/REPEAT 手工转发到 UP 或 BLACKLIST 时，转发是迁移命令而不只是新增内容。相同 code 可以对应不同 occurrence，因此不能仅按 caption/code 定位来源。UP 中用户删除媒体还需要与程序重建聚合时的删除区分。

## 决策

- 通过转发来源的 chat/message identity 查找当前 active group；ONESHOT/REPEAT 二次转发保留 VER 来源时，通过 VER 对应的 occurrence 查找 active group。
- BLACKLIST 触发消息仍保留 SOURCE 来源且身份查找失败时，只读取 BLACKLIST 中的媒体计算 Hash，与已保存的 VER Hash 匹配；全部触发 Hash 必须唯一匹配一个 active ONESHOT/REPEAT group，否则送往 MAN。此过程不访问 SOURCE，也不按 code 猜测关联。
- SOURCE 来源的触发媒体已全部列入 BLACKLIST 时忽略，避免程序补转的消息再次触发迁移。
- 无来源身份、来源 group 不存在或 bucket 不支持时，将 trigger 送往 MAN。
- 手工迁移到 UP 时，传播全部 group Hash，转发原 INFO/VER，并标记 UP merge dirty。
- 手工迁移到 BLACKLIST 时，保留用户转入的 trigger，直接从 INFO 频道补转原 INFO；先持久化全部 blacklist Hash，再删除旧 group，最后清理 active state。
- 程序删除 UP 旧消息前写入带 TTL 的 `expected_deletions`。
- 非 expected 的 UP 消息删除会将对应 Hash 写入 `up_hidden_hashes`；该 Hash 不再展示，但继续参与 UP 命中和连通。
- 当前不支持 UP→BLACKLIST 或 BLACKLIST→其他 bucket。

## 结果与权衡

- 重复 code 不会导致人工迁移错误定位其他记录。
- 先持久化 BLACKLIST Hash，避免删除后丢失传播状态。
- 隐藏状态是永久业务状态，目前没有恢复接口。
- 依赖 forward metadata；保护内容或隐藏来源时只能进入 MAN。

## 历史演进

人工 BLACKLIST 最初先删除来源消息、后写入 Hash；审查发现不符合一致性要求后调整为先持久化 Hash。

## 证据与可信度

- **A — 仓库事实**：ManualService、bucket Hash、hidden Hash、expected deletion 和测试直接支持。
- **负责人确认**：不需要恢复已隐藏媒体；未定义的跨桶迁移未来需要支持。

## 后续事项

- 未来为 UP→BLACKLIST 和 BLACKLIST→其他 bucket 单独定义迁移语义、数据保留与删除顺序；在该决策完成前不得根据现有逻辑类推实现。
