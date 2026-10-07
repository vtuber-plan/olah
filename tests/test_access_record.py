import os
import time

from olah.utils.access_record import access_age, record_access
from olah.utils.auth_utils import token_hash


def test_token_hash_is_a_full_sha256_digest_and_never_the_token():
    key = token_hash("Bearer hf_secret")
    assert len(key) == 64
    assert "hf_secret" not in key
    assert token_hash(None) == token_hash("") == "anon"


def test_records_are_per_caller(tmp_path):
    record_access(str(tmp_path), "models", "team", "demo", token_hash("Bearer alice"))

    assert 0 <= access_age(str(tmp_path), "models", "team", "demo", token_hash("Bearer alice")) < 5
    assert access_age(str(tmp_path), "models", "team", "demo", token_hash("Bearer mallory")) is None
    assert access_age(str(tmp_path), "models", "team", "demo", token_hash(None)) is None


def test_age_comes_from_the_record_not_the_file_mtime(tmp_path):
    caller = token_hash("Bearer alice")
    record_access(str(tmp_path), "models", "team", "demo", caller)
    path = os.path.join(tmp_path, "access", "models", "team", "demo", caller)
    with open(path, "w", encoding="utf-8") as f:
        f.write(repr(time.time() - 3600))
    os.utime(path)

    assert access_age(str(tmp_path), "models", "team", "demo", caller) >= 3600


def test_unreadable_or_future_records_count_as_unconfirmed(tmp_path):
    caller = token_hash("Bearer alice")
    path = os.path.join(tmp_path, "access", "models", "team", "demo", caller)
    os.makedirs(os.path.dirname(path))

    for content in ("", "not a time", repr(time.time() + 3600)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        assert access_age(str(tmp_path), "models", "team", "demo", caller) is None
