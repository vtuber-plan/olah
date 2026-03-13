import pytest

from olah.utils import url_utils


def test_get_url_tail_preserves_path_params_query_and_fragment():
    url = "https://example.com/a/b;v=1?x=1&y=2#frag"

    assert url_utils.get_url_tail(url) == "/a/b;v=1?x=1&y=2#frag"


def test_parse_content_range_supports_normal_and_wildcard_ranges():
    assert url_utils.parse_content_range("bytes 0-9/100") == ("bytes", 0, 9, 100)
    assert url_utils.parse_content_range("bytes */100") == ("bytes", None, None, 100)
    assert url_utils.parse_content_range("bytes 5-9") == ("bytes", 5, 9, None)


def test_parse_content_range_rejects_invalid_unit():
    with pytest.raises(Exception, match="Invalid range unit"):
        url_utils.parse_content_range("items 0-9/100")


def test_parse_range_params_supports_multiple_open_and_suffix_ranges():
    assert url_utils.parse_range_params("bytes=0-4, 10-") == (
        "bytes",
        [(0, 4), (10, None)],
        None,
    )
    assert url_utils.parse_range_params("bytes=-500") == ("bytes", [], 500)
    assert url_utils.parse_range_params("bytes=0-4, -20") == (
        "bytes",
        [(0, 4), (None, 20)],
        None,
    )


@pytest.mark.parametrize(
    "header",
    ["", "bytes", "bytes=abc", "bytes=1", "bytes=-"],
)
def test_parse_range_params_rejects_invalid_formats(header):
    with pytest.raises(ValueError):
        url_utils.parse_range_params(header)


def test_get_all_ranges_normalizes_bounds_and_skips_invalid_ranges():
    ranges = [(0, 4), (5, None), (99, 120), (8, 3), (None, 2)]

    assert url_utils.get_all_ranges(10, "bytes", ranges, None) == [
        (0, 5),
        (5, 10),
        (0, 3),
    ]
    assert url_utils.get_all_ranges(10, "bytes", [], 3) == [(7, 10)]


def test_query_param_helpers_add_read_and_remove_values():
    base_url = "https://example.com/path?foo=1"
    updated = url_utils.add_query_param(base_url, "bar", "2")

    assert url_utils.check_url_has_param_name(updated, "foo") is True
    assert url_utils.check_url_has_param_name(updated, "bar") is True
    assert url_utils.get_url_param_name(updated, "bar") == "2"
    assert url_utils.remove_query_param(updated, "foo") == "https://example.com/path?bar=2"


def test_clean_path_removes_parent_segments_and_duplicate_separators():
    assert url_utils.clean_path(r"..\\unsafe//nested/../file.txt") == "/unsafe/nested/file.txt"
