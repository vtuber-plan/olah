import pytest

from olah.utils.http_utils import (
    is_transient_upstream_error,
    is_transient_upstream_status,
)


def test_transient_upstream_status_codes():
    assert is_transient_upstream_status(429) is True
    assert is_transient_upstream_status(502) is True
    assert is_transient_upstream_status(404) is False


def test_transient_upstream_error_message_heuristics():
    assert is_transient_upstream_error(Exception("HTTP 503 from upstream")) is True
    assert is_transient_upstream_error(Exception("connection reset by peer")) is True
    assert is_transient_upstream_error(Exception("invalid rev id")) is False


def test_configs_performance_section_reads_block_size(tmp_path):
    cfg_path = tmp_path / "configs.toml"
    cfg_path.write_text(
        """
[performance]
cache-block-size = "64MB"
cache-gzip-level = 2
stream-read-timeout = 120
remote-retry-max = 3
http2 = true
""",
        encoding="utf-8",
    )
    from olah.configs import OlahConfig

    cfg = OlahConfig(str(cfg_path))
    assert cfg.cache_block_size == 64 * 1024 * 1024
    assert cfg.cache_gzip_level == 2
    assert cfg.stream_read_timeout == 120
    assert cfg.remote_retry_max == 3
    assert cfg.http2 is True
