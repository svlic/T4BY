# ADR-004：以 Logical Message 和 occurrence_id 建模业务身份

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

Telegram 的一个业务输入可能是一条消息，也可能是由相同 `grouped_id` 组成的 album。六位 code 可以重复，因此不能作为一条业务输入的唯一身份。

## 决策

- 将单条消息和 album 统一折叠为 Logical Message。
- Logical Message 保存有序的全部 message IDs，并聚合各成员文本和媒体。
- 每个不同的 INFO Logical Message 生成 UUID `occurrence_id`。
- 相同 INFO chat/message identity 的重试复用原 occurrence。
- 相同 code 的不同 INFO、以及相同内容被重新发布的消息，均产生新的 occurrence。
- INFO、SOURCE 和 VER 的映射保存 chat ID、全部 message IDs 和 grouped ID。

## 结果与权衡

- code 可以安全重复，caption 对 code 去重不会破坏业务身份。
- album 不会被拆成多个相互独立的业务记录。
- 幂等边界依赖 Telegram chat/message identity，而不是内容 Hash。
- 当前不专门处理消息编辑；编辑仍沿用原 Telegram identity。

## 历史演进

开发审查发现重试同一 INFO 可能重复创建 occurrence，随后增加 `occurrence_info_messages` 映射和复用测试。

## 证据与可信度

- **A — 仓库事实**：模型、表结构和测试覆盖 album 折叠、重复 code、不同 INFO identity 和同 INFO 重试。
- **负责人确认**：暂不考虑消息编辑的特殊语义；删除后重新发布相同内容应产生新 occurrence；没有额外审计保留要求。

## 待确认

无。
