from t4by.models import (
    Media,
    Message,
    Origin,
    caption_for_codes,
    collapse_messages,
    connected_component,
    contains_code,
    extract_code,
    stable_unique_media,
)


def test_extracts_first_independent_six_digit_code() -> None:
    assert extract_code("编号： 566494 （260928）") == "566494"
    assert extract_code("x1234567 y") is None
    assert contains_code("a 123456 b", "123456")
    assert not contains_code("a 0123456 b", "123456")


def test_collapses_album_as_one_logical_message() -> None:
    raw = [
        Message(1, 12, grouped_id=9, text="caption"),
        Message(1, 11, grouped_id=9),
        Message(1, 13, text="single"),
    ]
    logical = collapse_messages(raw)
    assert [item.message_ids for item in logical] == [(11, 12), (13,)]
    assert logical[0].effective_text == "caption"


def test_transitive_component_and_stable_hash_deduplication() -> None:
    nodes = {"A": {"H1", "H2"}, "B": {"H2", "H3"}, "C": {"H3", "H4"}, "D": {"X"}}
    assert connected_component({"H1"}, nodes) == {"A", "B", "C"}
    first = Media("H1", Origin.INFO, 1, 1, 0, "photo")
    duplicate = Media("H1", Origin.VER, 2, 2, 0, "video")
    second = Media("H2", Origin.VER, 2, 3, 1, "video")
    assert stable_unique_media([[first], [duplicate, second]]) == [first, second]
    assert caption_for_codes(["222222", "111111", "222222"]) == "111111\n222222"
