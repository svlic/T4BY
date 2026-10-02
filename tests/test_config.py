from t4by.config import Settings


def test_reader_and_writer_use_independent_api_credentials(monkeypatch, tmp_path) -> None:
    values = {
        "T4BY_READER_API_ID": "111",
        "T4BY_READER_API_HASH": "reader-hash",
        "T4BY_WRITER_API_ID": "222",
        "T4BY_WRITER_API_HASH": "writer-hash",
        "T4BY_DATABASE": str(tmp_path / "db.sqlite3"),
        "T4BY_READER_SESSION": str(tmp_path / "reader"),
        "T4BY_WRITER_SESSION": str(tmp_path / "writer"),
        "T4BY_SOURCE_CHAT": "-1001",
        "T4BY_INFO_CHAT": "-1002",
        "T4BY_VER_CHAT": "-1003",
        "T4BY_MAN_CHAT": "-1004",
        "T4BY_ONESHOT_CHAT": "-1005",
        "T4BY_REPEAT_CHAT": "-1006",
        "T4BY_UP_CHAT": "-1007",
        "T4BY_BLACKLIST_CHAT": "-1008",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    settings = Settings.from_env()

    assert settings.reader_api_id == 111
    assert settings.reader_api_hash == "reader-hash"
    assert settings.writer_api_id == 222
    assert settings.writer_api_hash == "writer-hash"
