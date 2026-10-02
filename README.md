# T4BY — Telegram Reader / Writer

基于两个 Telegram 用户账号，把 INFO 与 SOURCE 中的验证消息匹配，并按媒体 SHA-256 自动归类到 `oneshot`、`repeat`、`up`、`blacklist`。

## 运行要求

- Docker Engine + Docker Compose
- 两个 Telegram API 用户会话（不是 Bot Token）
- 单进程部署；SQLite 使用 WAL，可在重启后恢复延迟任务和 merge dirty 状态

## Docker 部署

```bash
cp .env.example .env
docker compose build
```

编辑 `.env`，分别填写 reader 和 writer 的 `API_ID`、`API_HASH` 及频道 ID。两个账号可以使用完全不同的 Telegram API 应用凭据。

首次启动必须在交互终端中依次登录 reader 和 writer：

```bash
docker compose run --rm t4by
```

两个账号都登录成功并看到 `T4BY started` 后按 `Ctrl+C`，再转为后台长期运行：

```bash
docker compose up -d
docker compose logs -f t4by
```

Session 与 SQLite 存放在 Docker named volume `t4by-data`，重建容器不会丢失。Prometheus 指标位于 `http://127.0.0.1:9464/metrics`。

停止或升级：

```bash
docker compose down           # 保留数据
docker compose build --pull
docker compose up -d
```

除非确定要永久删除 session 和数据库，否则不要执行 `docker compose down -v`。

## 配置

所有频道值支持数字 chat id（推荐 `-100...`）或公开用户名。reader 与 writer 必须使用不同 session，并分别配置：

```text
T4BY_READER_API_ID / T4BY_READER_API_HASH
T4BY_WRITER_API_ID / T4BY_WRITER_API_HASH
```

详见 [`.env.example`](.env.example)。账号权限及频道用途必须符合业务规格。

## 开发验证

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[test]'
pytest
ruff check .
```

测试不连接 Telegram，使用临时 SQLite 和 fake gateway 覆盖 code 解析、Logical Message 折叠、双向搜索、分类优先级、Hash 传递连通、merge 调度和 expected deletion。

项目的重要架构决策记录在 [`docs/adr/`](docs/adr/README.md)。现有 ADR 根据 Git 历史、实现和项目负责人补充说明进行历史重建。

## 运行边界

- 这是单进程实现；不要同时启动两个实例共享同一个 SQLite 文件。
- Telegram 无法与数据库组成原子事务。实现遵循“先生成新内容，再删除旧内容”，持久化 job 会在崩溃后重试；极端情况下可能留下可人工清理的重复目标消息，但不会先删除唯一的旧聚合。
- Telegram 来源开启“保护内容”或隐藏转发来源时，无法取得 SOURCE / 手工迁移定位信息，会按规格转入 `man`。
