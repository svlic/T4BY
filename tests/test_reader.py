from conftest import FakeGateway, logical

from t4by.models import Message
from t4by.reader import find_source_match


async def test_searches_newer_first_and_stops_before_older() -> None:
    gateway = FakeGateway()
    source = logical(1, 100, 101, grouped_id=77, text="123456")
    gateway.history[(True, 101)] = [
        Message(1, 102, text="none"),
        Message(1, 103, grouped_id=80),
        Message(1, 104, grouped_id=80, text="match 123456"),
    ]
    gateway.history[(False, 100)] = [Message(1, 99, text="also 123456")]

    result = await find_source_match(gateway, source, "123456")

    assert result is not None
    assert result.message_ids == (103, 104)


async def test_searches_older_only_after_five_newer_logical_messages_miss() -> None:
    gateway = FakeGateway()
    source = logical(1, 100, text="123456")
    gateway.history[(True, 100)] = [Message(1, value, text="miss") for value in range(101, 106)]
    gateway.history[(False, 100)] = [
        Message(1, 99, text="miss"),
        Message(1, 98, text="miss"),
        Message(1, 97, text="123456"),
    ]

    result = await find_source_match(gateway, source, "123456")

    assert result is not None
    assert result.message_ids == (97,)
