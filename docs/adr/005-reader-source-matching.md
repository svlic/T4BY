# ADR-005：使用转发元数据和双向窗口匹配 SOURCE

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

Reader 需要从 INFO 的转发来源定位 SOURCE 附近的验证消息，并按六位 code 找到对应 Logical Message。

## 决策

1. 从完整文本中提取第一个独立六位数字，边界规则为 `(?<!\d)\d{6}(?!\d)`。
2. 使用 Telegram forward metadata 定位 SOURCE 原消息或完整 album。
3. 先检查原消息之后最近 5 个 Logical Message；全部未命中后，再检查之前最近 5 个。
4. history 每页批量读取最多 60 条 raw message，再按 `grouped_id` 在本地折叠，不以 `message_id ± 5` 代替 Logical Message 窗口。
5. 找到第一个包含独立 code 的候选后停止。
6. 无 code、来源不可见、来源频道不符或窗口内未命中时，将 INFO 送往 MAN。

## 结果与权衡

- 搜索顺序确定且 API 调用数量受控。
- 本地折叠可以正确计算 album 数量，但分页和 album 边界仍依赖 Telegram history 行为。
- 窗口外的真实匹配会被判为未命中。
- 保护内容或隐藏转发来源时不使用替代匹配方式。

## 历史演进

没有证据表明项目曾正式采用全文索引、全频道扫描或基于内容 Hash 的 SOURCE 匹配。

## 证据与可信度

- **A — 仓库事实**：Reader 算法及测试直接验证先新后旧和 Logical Message 窗口。
- **负责人确认**：窗口和 page size 是经验值；方向顺序来自业务特性；来源不可见时不考虑替代方案。
- **B — 推测**：批量读取后本地折叠同时减少 history RPC 成本。

## 待确认

无。
