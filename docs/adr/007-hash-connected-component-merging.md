# ADR-007：使用 Hash 传递连通分量聚合 Repeat 与 UP

- **状态**：已接受（历史重建）
- **决策日期**：2026-10-02
- **相关提交**：`c5d33e59496aee731438a0e1b49f437b5af3c868`

## 背景

不同 occurrence 可能通过一个或多个共享媒体形成关系，而且关系具有传递性。Repeat 与 UP 使用相同的关系基础，但展示内容不同。

## 决策

- 将 occurrence 或既有 UP group 作为节点，将其关联的媒体 SHA-256 作为节点属性。
- 两个节点只要共享任意 Hash 即连通；从 dirty seed 展开完整传递连通分量。
- 多个互不相关的 dirty seed 分别形成独立分量，不能被一次调度误合并。
- Repeat 的关系使用 INFO+VER Hash，但新聚合只展示 VER 媒体。
- UP 的关系与展示均可包含 INFO+VER；旧 group 和 `up_pending` occurrence 一同参与。
- 媒体按 Hash 稳定去重，最多 10 个一批；每批 caption 使用相同的完整 code 并集。

## 结果与权衡

- `A↔B↔C` 会形成一个整体，即使 A 与 C 没有直接共享 Hash。
- 新关系可吸收旧 group，并用新的 active group 取代它们。
- Repeat 与 UP 复用连通算法，但保留不同的展示规则。
- 当前实现从数据库读取候选节点并在 Python 中遍历；大型连通分量的时间和内存成本没有生产数据验证。

## 历史演进

没有证据表明项目曾采用只合并直接邻居、按 code 分组或增量维护并查集的正式方案。

## 证据与可信度

- **A — 仓库事实**：`connected_component`、`partition_components`、MergeEngine 和测试直接支持传递关系和 bucket 差异。
- **负责人确认**：传递连通是明确业务语义；Repeat 只展示 VER 是业务设计。

## 待确认

- 超大连通分量是否需要容量、执行时间或重算成本上限，尚未确定。
