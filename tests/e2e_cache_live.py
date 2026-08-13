# coding=utf-8
# End-to-end cache test against the REAL Hugging Face backend.
#
# Opt-in only: it hits the network and downloads small files from huggingface.co.
#   OLAH_E2E_LIVE=1 python -m pytest tests/e2e_cache_live.py -q -s
# Default `pytest tests/` skips it so the suite stays network-free and offline.
#
# What it proves, in-process (olah's FastAPI app driven via httpx ASGI transport,
# with `_get_file_range_from_remote` wrapped to count upstream byte-fetches):
#   1. First pull  (cache MISS): upstream byte-fetch happens, bytes correct, and
#      the cache directory is populated (meta.bin + chunks.crc + blocks).
#   2. Second pull (cache HIT) : ZERO upstream byte-fetches, identical bytes.
#   3. Partial pull (HALF cache): a range request caches only the covered blocks;
#      a subsequent full pull fetches the rest and then a further pull is a hit.

import os

import httpx
import pytest

if not os.environ.get("OLAH_E2E_LIVE"):
    pytest.skip("live network e2e; set OLAH_E2E_LIVE=1 to run", allow_module_level=True)

pytest.importorskip("fastapi")

from olah.configs import OlahConfig
from olah.proxy import files as proxy_files
from olah.server import AppSettings, app
from olah.utils.repo_utils import get_org_repo

BLOCK = 64 * 1024
CHUNK = 16 * 1024

# (label, repo_type, org, repo, file). org=None => user-level repo.
FILES = [
    ("model-single", "models", None, "bert-base-uncased", "config.json"),
    ("dataset-single", "datasets", "Open-Orca", "SlimOrca", ".gitattributes"),
    ("model-multi", "models", None, "bert-base-uncased", "vocab.txt"),
]


def _hf_url(repo_type, org, repo, f):
    org_repo = get_org_repo(org, repo)
    # HF omits the "models/" prefix (only datasets/spaces are prefixed); olah's
    # mirror path keeps it for routing, but the upstream URL must match HF.
    prefix = "" if repo_type == "models" else f"{repo_type}/"
    return f"https://huggingface.co/{prefix}{org_repo}/resolve/main/{f}"


def _mirror_path(repo_type, org, repo, f):
    org_repo = get_org_repo(org, repo)
    # Real HF clients omit the "models/" prefix (datasets/spaces are prefixed).
    prefix = "" if repo_type == "models" else f"{repo_type}/"
    return f"/{prefix}{org_repo}/resolve/main/{f}"


def _find_cache_dir(repos_path, repo_type, org, repo, f):
    # olah resolves the branch (e.g. "main") to a concrete commit SHA before
    # building the cache path, so match any commit under resolve/. Returns the
    # cache dir or None (the dir only exists after the first MISS writes it).
    import glob
    org_repo = get_org_repo(org, repo)
    pattern = os.path.join(repos_path, "files", repo_type, org_repo, "resolve", "*", f, "meta.bin")
    matches = glob.glob(pattern)
    return os.path.dirname(matches[0]) if matches else None


async def _ground_truth(url):
    """Fetch the true bytes from HF directly, with retries + a browser UA
    (HF occasionally 401s a bare GET on public files)."""
    headers = {"User-Agent": "olah-e2e-test/1.0"}
    last = None
    for _ in range(4):
        r = await httpx.AsyncClient().get(url, headers=headers, follow_redirects=True, timeout=120)
        if r.status_code == 200:
            return r.content
        last = r
    raise AssertionError(f"ground-truth fetch failed: {url} -> {last.status_code if last else '?'}: {last.text[:120] if last else ''}")


def _configure_app(repos_path):
    config = OlahConfig()
    config.repos_path = repos_path
    config.offline = False
    config.cache_block_size = BLOCK
    config.cache_chunk_size = CHUNK
    config.hf_scheme = "https"
    config.hf_netloc = "huggingface.co"
    config.hf_lfs_netloc = "cdn-lfs.huggingface.co"
    config.mirror_netloc = "localhost:8090"
    config.mirror_lfs_netloc = "localhost:8090"
    app.state.app_settings = AppSettings(config=config)


@pytest.fixture
def fetch_counter():
    """Wrap the upstream byte-fetch path and return a mutable counter dict."""
    calls = {"n": 0}
    original = proxy_files._get_file_range_from_remote

    async def counting(*args, **kwargs):
        calls["n"] += 1
        async for chunk in original(*args, **kwargs):
            yield chunk

    proxy_files._get_file_range_from_remote = counting
    try:
        yield calls
    finally:
        proxy_files._get_file_range_from_remote = original


@pytest.mark.asyncio
async def test_e2e_cache_miss_hit_and_partial(tmp_path, fetch_counter):
    repos_path = str(tmp_path / "repos")
    _configure_app(repos_path)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8090", timeout=120) as client:
        for label, repo_type, org, repo, f in FILES:
            hf_url = _hf_url(repo_type, org, repo, f)
            mirror_path = _mirror_path(repo_type, org, repo, f)

            # Ground truth directly from HF.
            gt = await _ground_truth(hf_url)

            # --- 1. MISS: first pull fetches from upstream and populates cache ---
            fetch_counter["n"] = 0
            r1 = await client.get(mirror_path)
            assert r1.status_code == 200, (label, r1.status_code, r1.text[:200])
            assert r1.content == gt, f"{label}: MISS bytes != ground truth"
            assert fetch_counter["n"] > 0, f"{label}: MISS should fetch from upstream"

            cache_dir = _find_cache_dir(repos_path, repo_type, org, repo, f)
            assert cache_dir, f"{label}: cache dir not created after MISS"
            assert os.path.exists(os.path.join(cache_dir, "meta.bin")), f"{label}: meta.bin missing"
            assert os.path.exists(os.path.join(cache_dir, "chunks.crc")), f"{label}: chunks.crc missing"
            blocks = [x for x in os.listdir(os.path.join(cache_dir, "blocks")) if not x.startswith(".")]
            assert blocks, f"{label}: no blocks cached after MISS"

            # --- 2. HIT: second pull serves from cache (no upstream byte-fetch) ---
            fetch_counter["n"] = 0
            r2 = await client.get(mirror_path)
            assert r2.status_code == 200, (label, r2.status_code)
            assert r2.content == gt, f"{label}: HIT bytes != ground truth"
            assert fetch_counter["n"] == 0, f"{label}: HIT must not fetch from upstream (got {fetch_counter['n']})"

            if label == "model-multi":
                nblocks_total = len(blocks)

        # --- 3. PARTIAL (half cache): only present for the multi-block file ---
        _, repo_type, org, repo, f = FILES[2]  # vocab.txt
        hf_url = _hf_url(repo_type, org, repo, f)
        mirror_path = _mirror_path(repo_type, org, repo, f)
        cache_dir = _find_cache_dir(repos_path, repo_type, org, repo, f)
        assert cache_dir, "vocab.txt cache dir should exist from the MISS/HIT loop"
        gt = await _ground_truth(hf_url)
        assert len(gt) > 2 * BLOCK, "multi-block file should span >2 blocks"
        assert nblocks_total >= 3, "vocab.txt should span at least 3 blocks"

        # Wipe vocab.txt's cache so we start from empty for the partial scenario.
        import shutil
        shutil.rmtree(cache_dir, ignore_errors=True)

        def _n_cached_blocks():
            bd = os.path.join(cache_dir, "blocks")
            return len([x for x in os.listdir(bd) if not x.startswith(".")]) if os.path.isdir(bd) else 0

        # Range pull of the first ~half -> caches only the covered blocks.
        fetch_counter["n"] = 0
        half = 2 * BLOCK  # first two blocks
        rr = await client.get(mirror_path, headers={"range": f"bytes=0-{half - 1}"})
        assert rr.status_code == 206, (rr.status_code, rr.text[:200])
        assert rr.content == gt[0:half], "partial range bytes != ground truth slice"
        assert fetch_counter["n"] > 0, "partial pull should fetch from upstream"
        cached_after_partial = _n_cached_blocks()
        assert 0 < cached_after_partial < nblocks_total, (
            f"partial pull should cache SOME but not all blocks (got {cached_after_partial}/{nblocks_total})"
        )

        # Full pull now: remaining blocks fetched, full content correct.
        fetch_counter["n"] = 0
        rf = await client.get(mirror_path)
        assert rf.status_code == 200
        assert rf.content == gt, "full pull after partial != ground truth"
        assert fetch_counter["n"] > 0, "full pull after partial should still fetch the missing blocks"
        assert _n_cached_blocks() == nblocks_total, "all blocks should be cached after the full pull"

        # Another full pull: now a complete HIT.
        fetch_counter["n"] = 0
        rf2 = await client.get(mirror_path)
        assert rf2.content == gt
        assert fetch_counter["n"] == 0, "second full pull must be a complete cache hit"


# Content hash (lfs.oid) of Qwen/Qwen3-4B tokenizer.json -- a real 11 MB
# LFS/Xet file (see test_e2e_real_lfs_file_qwen3).
QWEN3_TOKENIZER_OID = "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4"


@pytest.mark.asyncio
async def test_e2e_real_lfs_file_qwen3(tmp_path, fetch_counter):
    """Real 11 MB LFS/Xet file (Qwen3-4B tokenizer.json).

    Modern HF serves weights via Xet, so olah serves these bytes through the
    resolve path (it follows the Xet redirect internally) -- the path where the
    etag revalidation, per-chunk CRC, and compressed sub-chunk caching live. The
    resolve flow also populates the LFS content-hash registry that the dedicated
    /repos/... LFS route relies on, which we assert here. (The classic cdn-lfs
    route itself can't be exercised against modern Xet HF; its authorization
    logic is covered by tests/test_lfs_object_index.py.)
    """
    repos_path = str(tmp_path / "repos")
    config = OlahConfig()
    config.repos_path = repos_path
    config.offline = False
    config.cache_block_size = 1 * 1024 * 1024   # 1 MB -> ~11 blocks for an 11 MB file
    config.cache_chunk_size = 256 * 1024         # 256 KB sub-chunks
    config.cache_compression = "gzip"            # JSON compresses well -> exercises sub-chunks
    config.hf_netloc = "huggingface.co"
    config.hf_lfs_netloc = "cdn-lfs.huggingface.co"
    config.mirror_netloc = "localhost:8090"
    config.mirror_lfs_netloc = "localhost:8090"
    app.state.app_settings = AppSettings(config=config)

    hf_url = "https://huggingface.co/Qwen/Qwen3-4B/resolve/main/tokenizer.json"
    mirror_path = "/Qwen/Qwen3-4B/resolve/main/tokenizer.json"

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost:8090", timeout=300) as client:
        gt = await _ground_truth(hf_url)
        assert len(gt) > 2 * 1024 * 1024, "tokenizer.json should be >2MB"
        end = 2 * 1024 * 1024  # first two blocks

        # --- MISS: range pull of the first 2 MB -> caches 2 compressed blocks ---
        fetch_counter["n"] = 0
        r1 = await client.get(mirror_path, headers={"range": f"bytes=0-{end - 1}"})
        assert r1.status_code == 206, (r1.status_code, r1.text[:200])
        assert r1.content == gt[0:end], "range MISS bytes != ground truth slice"
        assert fetch_counter["n"] > 0, "range MISS should fetch from upstream"

        # --- Registry populated from the resolve flow (pathsinfo lfs.oid) ---
        import glob as _glob
        import json as _json
        idx_matches = _glob.glob(os.path.join(repos_path, "lfs", "object_index", "*", "*.json"))
        assert idx_matches, "LFS object index not created by the resolve flow"
        found = False
        for p in idx_matches:
            entry = _json.load(open(p))
            for r in entry.get("repos", []):
                if len(r) == 3 and f"{r[1]}/{r[2]}" == "Qwen/Qwen3-4B":
                    found = True
        assert found, f"Qwen/Qwen3-4B not registered in LFS object index: {idx_matches}"

        # --- HIT: same range -> zero upstream byte-fetch ---
        fetch_counter["n"] = 0
        r2 = await client.get(mirror_path, headers={"range": f"bytes=0-{end - 1}"})
        assert r2.status_code == 206
        assert r2.content == gt[0:end], "range HIT bytes != ground truth slice"
        assert fetch_counter["n"] == 0, "range HIT must not fetch from upstream"

        # --- Full pull: remaining blocks fetched, full content correct ---
        fetch_counter["n"] = 0
        rf = await client.get(mirror_path)
        assert rf.status_code == 200, (rf.status_code, rf.text[:200])
        assert rf.content == gt, "full pull != ground truth"
        assert fetch_counter["n"] > 0, "full pull should fetch the remaining blocks"

        # --- Full HIT ---
        fetch_counter["n"] = 0
        rf2 = await client.get(mirror_path)
        assert rf2.content == gt
        assert fetch_counter["n"] == 0, "second full pull must be a complete cache hit"

        # --- The registered (public) object authorizes successfully against real HF ---
        from olah.utils.lfs_object_index import authorize_lfs_object

        assert await authorize_lfs_object(app, QWEN3_TOKENIZER_OID, None) is None, (
            "registered public Qwen3-4B object should authorize (visibility probe -> 200)"
        )

