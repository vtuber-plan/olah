import types

import pytest

pytest.importorskip("portalocker")

import olah.utils.lfs_object_index as idx
from olah.utils.lfs_object_index import (
    authorize_lfs_object,
    authorize_xet_object,
    cache_allowed_for_lfs_object,
    get_lfs_object_repos,
    get_xet_metadata,
    register_lfs_object,
    register_xet_object,
)


def _app(tmp_path, offline=False):
    cfg = types.SimpleNamespace(repos_path=str(tmp_path), offline=offline)
    return types.SimpleNamespace(state=types.SimpleNamespace(app_settings=types.SimpleNamespace(config=cfg)))


@pytest.mark.asyncio
async def test_register_and_candidates(tmp_path):
    app = _app(tmp_path)
    h = "a" * 64
    await register_lfs_object(app, "models", "orgA", "repoA", h)
    await register_lfs_object(app, "models", "orgB", "repoB", h)
    await register_lfs_object(app, "models", "orgA", "repoA", h)  # idempotent
    repos = await get_lfs_object_repos(app, h)
    assert {r[2] for r in repos} == {"repoA", "repoB"}
    assert await get_lfs_object_repos(app, "unknown") == []


@pytest.mark.asyncio
async def test_authorize_fail_closed(monkeypatch, tmp_path):
    app = _app(tmp_path)
    h = "a" * 64
    await register_lfs_object(app, "models", "orgA", "repoA", h)

    # Unknown object (no candidates) defers to upstream -> None, and is never
    # cached (HF enforces access on the live proxy; we can't re-validate later).
    assert await authorize_lfs_object(app, "b" * 64, "tok") is None
    assert await cache_allowed_for_lfs_object(app, "b" * 64) is False

    # No candidate accessible -> 403.
    async def denied(app, rt, org, repo, auth):
        return False
    monkeypatch.setattr(idx, "_visibility_cached", denied)
    assert await authorize_lfs_object(app, h, "tok") == 403

    # At least one candidate accessible -> authorized (None).
    async def allowed(app, rt, org, repo, auth):
        return True
    monkeypatch.setattr(idx, "_visibility_cached", allowed)
    assert await authorize_lfs_object(app, h, "tok") is None


@pytest.mark.asyncio
async def test_authorize_offline_trusts_local(monkeypatch, tmp_path):
    app = _app(tmp_path, offline=True)
    # Offline never probes and never blocks (cache is trusted); even an unknown
    # hash is "authorized" (a miss then simply cannot be fetched).
    assert await authorize_lfs_object(app, "never-registered", None) is None


@pytest.mark.asyncio
async def test_cache_allowed_follows_rules(monkeypatch, tmp_path):
    app = _app(tmp_path)
    h = "a" * 64
    await register_lfs_object(app, "models", "orgA", "repoA", h)

    import olah.utils.rule_utils as rule_utils

    async def rules_allow(app, rt, org, repo):
        return repo == "repoA"

    async def rules_deny(app, rt, org, repo):
        return False

    monkeypatch.setattr(rule_utils, "check_cache_rules_hf", rules_allow)
    assert await cache_allowed_for_lfs_object(app, h) is True

    monkeypatch.setattr(rule_utils, "check_cache_rules_hf", rules_deny)
    assert await cache_allowed_for_lfs_object(app, h) is False

    # Unknown object -> not cached.
    assert await cache_allowed_for_lfs_object(app, "unknown") is False


@pytest.mark.asyncio
async def test_xet_register_metadata_and_authorize(monkeypatch, tmp_path):
    app = _app(tmp_path)
    xet = "b" * 64
    lfs_oid = "a" * 64
    await register_xet_object(app, "models", "Qwen", "Qwen3-4B", "tokenizer.json", "deadbeef", lfs_oid, xet, 11422654)

    refs, size, oid = await get_xet_metadata(app, xet)
    assert size == 11422654
    assert oid == lfs_oid
    assert refs and refs[0][:3] == ("models", "Qwen", "Qwen3-4B")
    assert refs[0][3] == "tokenizer.json"

    # Unknown xet hash -> empty refs.
    assert await get_xet_metadata(app, "c" * 64) == ([], None, None)

    # Authorized when a candidate repo is visible.
    async def visible(app, rt, org, repo, auth):
        return repo == "Qwen3-4B"
    monkeypatch.setattr(idx, "_visibility_cached", visible)
    assert await authorize_xet_object(app, xet, "tok") is None

    # Denied when no candidate is visible.
    async def denied(app, rt, org, repo, auth):
        return False
    monkeypatch.setattr(idx, "_visibility_cached", denied)
    assert await authorize_xet_object(app, xet, "tok") == 403

    # Offline always proceeds.
    app.state.app_settings.config.offline = True
    assert await authorize_xet_object(app, xet, None) is None
