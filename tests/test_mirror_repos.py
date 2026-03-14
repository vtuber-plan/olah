import os
import subprocess

from olah.mirror.repos import LocalMirrorRepo


def _git(repo_dir, *args, env=None):
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        env=merged_env,
        check=True,
        capture_output=True,
        text=True,
    )


def _commit(repo_dir, filename, content, message, commit_time):
    file_path = repo_dir / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content, encoding="utf-8")
    _git(repo_dir, "add", filename)
    env = {
        "GIT_AUTHOR_NAME": "Test User",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test User",
        "GIT_COMMITTER_EMAIL": "test@example.com",
        "GIT_AUTHOR_DATE": commit_time,
        "GIT_COMMITTER_DATE": commit_time,
    }
    _git(repo_dir, "commit", "-m", message, env=env)
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _build_repo(tmp_path):
    repo_dir = tmp_path / "mirror-repo"
    repo_dir.mkdir()
    _git(repo_dir, "init")
    first_sha = _commit(
        repo_dir,
        "README.md",
        "---\nlicense: mit\n---\nfirst\n",
        "first commit",
        "2024-01-01T00:00:00+00:00",
    )
    second_sha = _commit(
        repo_dir,
        "README.md",
        "---\nlicense: mit\n---\nsecond\n",
        "second commit",
        "2024-02-01T00:00:00+00:00",
    )
    third_sha = _commit(
        repo_dir,
        "weights.bin",
        "payload\n",
        "third commit",
        "2024-03-01T00:00:00+00:00",
    )
    return repo_dir, first_sha, second_sha, third_sha


def test_get_meta_uses_requested_revision_timestamp_and_root_created_at(tmp_path):
    repo_dir, first_sha, second_sha, _ = _build_repo(tmp_path)
    local_repo = LocalMirrorRepo(str(repo_dir), "models", "team", "demo")

    meta = local_repo.get_meta(second_sha)

    assert meta["sha"] == second_sha
    assert meta["lastModified"] == "2024-02-01T00:00:00.000000Z"
    assert meta["createdAt"] == "2024-01-01T00:00:00.000000Z"
    assert meta["cardData"] == {"license": "mit"}
    assert meta["siblings"] == [{"rfilename": "README.md"}]
    assert first_sha != second_sha


def test_get_meta_caches_created_at_lookup(tmp_path, monkeypatch):
    repo_dir, _, _, third_sha = _build_repo(tmp_path)
    local_repo = LocalMirrorRepo(str(repo_dir), "models", "team", "demo")

    original_iter_commits = local_repo._git_repo.iter_commits
    root_scan_calls = 0

    def counted_iter_commits(*args, **kwargs):
        nonlocal root_scan_calls
        if kwargs.get("rev") == "--all" and kwargs.get("max_parents") == 0:
            root_scan_calls += 1
        return original_iter_commits(*args, **kwargs)

    monkeypatch.setattr(local_repo._git_repo, "iter_commits", counted_iter_commits)

    local_repo.get_meta(third_sha)
    local_repo.get_meta(third_sha)

    assert root_scan_calls == 1


def test_get_commits_uses_bounded_history_walk(tmp_path, monkeypatch):
    repo_dir, _, _, third_sha = _build_repo(tmp_path)
    local_repo = LocalMirrorRepo(str(repo_dir), "models", "team", "demo")

    original_iter_commits = local_repo._git_repo.iter_commits
    iter_kwargs = {}

    def counted_iter_commits(*args, **kwargs):
        if kwargs.get("rev") == third_sha:
            iter_kwargs.update(kwargs)
        return original_iter_commits(*args, **kwargs)

    monkeypatch.setattr(local_repo._git_repo, "iter_commits", counted_iter_commits)

    commits = local_repo.get_commits(third_sha)

    assert [item["id"] for item in commits] == [
        third_sha,
        commits[1]["id"],
        commits[2]["id"],
    ]
    assert len(commits) == 3
    assert iter_kwargs["max_count"] == LocalMirrorRepo.MAX_COMMITS
