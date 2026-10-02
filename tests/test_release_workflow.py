"""Offline checks for release validation and the publication dependency graph."""

import hashlib
import importlib.util
from pathlib import Path
from urllib.error import HTTPError

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "release_checks", ROOT / ".github/scripts/release.py"
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


@pytest.mark.parametrize("version", ["0.0.0", "0.5.2", "1.20.300"])
def test_valid_stable_version(version):
    assert release.version_tuple(version) == tuple(map(int, version.split(".")))


@pytest.mark.parametrize(
    "version",
    [
        "v0.5.2",
        "0.5",
        "0.5.2rc1",
        "0.5.2-dev",
        "01.2.3",
        "1.02.3",
        "1.2.03",
        "1.2.3\n",
        "1.2.3;echo x",
    ],
)
def test_invalid_stable_version(version):
    with pytest.raises(ValueError, match="stable X.Y.Z"):
        release.version_tuple(version)


def test_tag_must_match_package_version(tmp_path):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nversion = "0.5.2"\n')
    assert release.validate_tag("v0.5.2", project) == "0.5.2"
    with pytest.raises(ValueError, match="does not match"):
        release.validate_tag("v0.5.3", project)
    with pytest.raises(ValueError, match="vX.Y.Z"):
        release.validate_tag("0.5.2", project)


@pytest.fixture
def distributions(tmp_path):
    paths = [tmp_path / "olah-0.5.2-py3-none-any.whl", tmp_path / "olah-0.5.2.tar.gz"]
    for path in paths:
        path.write_bytes(path.name.encode())
    return tmp_path


def published_files(dist):
    return {
        "urls": [
            {
                "filename": p.name,
                "digests": {"sha256": hashlib.sha256(p.read_bytes()).hexdigest()},
            }
            for p in sorted(dist.iterdir())
        ]
    }


def test_matching_or_partial_existing_pypi_uploads_are_safe(distributions):
    published = published_files(distributions)
    release.check_existing_files("0.5.2", distributions, {"urls": []})
    release.check_existing_files("0.5.2", distributions, published)
    release.check_existing_files(
        "0.5.2", distributions, {"urls": published["urls"][:1]}
    )


def test_changed_pypi_bytes_are_rejected(distributions):
    published = published_files(distributions)
    published["urls"][0]["digests"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="different bytes"):
        release.check_existing_files("0.5.2", distributions, published)


def test_unexpected_published_file_is_rejected(distributions):
    published = published_files(distributions)
    published["urls"][0]["filename"] = "unexpected.whl"
    with pytest.raises(ValueError, match="unexpected file"):
        release.check_existing_files("0.5.2", distributions, published)


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_distribution_set_must_be_exact(distributions, change):
    if change == "missing":
        (distributions / "olah-0.5.2.tar.gz").unlink()
    else:
        (distributions / "unrelated.whl").write_bytes(b"extra")
    with pytest.raises(ValueError, match="Expected exactly"):
        release.check_existing_files("0.5.2", distributions, {"urls": []})


@pytest.mark.parametrize("status", [404, 403, 500])
def test_only_pypi_404_means_new_version(monkeypatch, distributions, status):
    def failing_open(url, timeout):
        assert url == "https://pypi.org/pypi/olah/0.5.2/json"
        assert timeout == 30
        raise HTTPError(url, status, "failure", {}, None)

    monkeypatch.setattr(release, "urlopen", failing_open)
    if status == 404:
        release.check_pypi("0.5.2", distributions)
    else:
        with pytest.raises(HTTPError):
            release.check_pypi("0.5.2", distributions)


@pytest.mark.parametrize(
    "version,tags,expected",
    [
        ("0.5.2", [], True),
        ("0.5.2", ["v0.5.1", "v0.5.2"], True),
        ("0.5.2", ["v0.5.10"], False),
        ("0.5.10", ["v0.5.2"], True),
        ("0.5.2", ["v1.0.0"], False),
        ("0.5.2", ["v1.0.0rc1", "other-tag"], True),
    ],
)
def test_latest_never_moves_backwards(version, tags, expected):
    assert release.should_promote(version, tags) is expected


def read_workflow(name):
    # BaseLoader follows YAML strings, avoiding YAML 1.1's special boolean "on".
    return yaml.load(
        (ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader
    )


def test_release_has_one_explicit_tag_trigger_and_ordered_publication():
    workflow = read_workflow("release.yml")
    assert workflow["on"] == {"push": {"tags": ["v*"]}}
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"] == {
        "group": "olah-release",
        "cancel-in-progress": "false",
        "queue": "max",
    }
    jobs = workflow["jobs"]
    assert jobs["publish-pypi"]["needs"] == "build"
    assert jobs["publish-github"]["needs"] == ["build", "publish-pypi"]
    assert jobs["publish-images"]["needs"] == ["build", "publish-github"]
    assert jobs["publish-github"]["permissions"] == {"contents": "write"}
    assert (
        jobs["publish-images"]["uses"]
        == "./.github/workflows/docker-image-tag-version.yml"
    )
    assert jobs["publish-images"]["with"]["git_ref"] == "${{ github.sha }}"
    assert set(jobs["publish-images"]["secrets"]) == {
        "DOCKERHUB_USERNAME",
        "DOCKERHUB_TOKEN",
    }
    assert (
        jobs["build"]["outputs"]["artifact-name"]
        == "release-dist-${{ github.run_attempt }}"
    )
    build_steps = jobs["build"]["steps"]
    assert any(
        step.get("run") == 'python -m pytest tests -m "not live"'
        for step in build_steps
    )
    for job in ["publish-pypi", "publish-github"]:
        downloads = [
            s
            for s in jobs[job]["steps"]
            if s.get("uses") == "actions/download-artifact@v4"
        ]
        assert len(downloads) == 1
        assert downloads[0]["with"] == {
            "name": "${{ needs.build.outputs.artifact-name }}",
            "path": "dist",
        }


def test_release_images_are_called_directly_and_manual_repairs_cannot_promote():
    workflow = read_workflow("docker-image-tag-version.yml")
    assert set(workflow["on"]) == {"workflow_call", "workflow_dispatch"}
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]) == {"olah_version"}
    assert (
        workflow["on"]["workflow_call"]["inputs"]["push_latest"]["default"] == "false"
    )
    steps = workflow["jobs"]["build-and-push-docker-image"]["steps"]
    checkout = next(s for s in steps if s.get("uses") == "actions/checkout@v4")
    assert (
        checkout["with"]["ref"]
        == "${{ inputs.git_ref || format('refs/tags/v{0}', inputs.olah_version) }}"
    )
    build = next(s for s in steps if s.get("uses") == "docker/build-push-action@v5")
    assert build["with"]["platforms"] == "linux/amd64,linux/arm64"


def test_post_upload_verification_requires_all_pypi_files(distributions):
    published = published_files(distributions)
    release.check_existing_files("0.5.2", distributions, published, require_all=True)
    with pytest.raises(ValueError, match="complete distribution set"):
        release.check_existing_files(
            "0.5.2", distributions, {"urls": published["urls"][:1]}, require_all=True
        )


def test_image_retry_refreshes_latest_instead_of_trusting_old_job_output():
    workflow = read_workflow("docker-image-tag-version.yml")
    steps = workflow["jobs"]["build-and-push-docker-image"]["steps"]
    latest = next(s for s in steps if s.get("id") == "latest")
    assert (
        'gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100"'
        in latest["run"]
    )
    assert 'release.py latest "$VERSION"' in latest["run"]
    tags = next(
        s for s in steps if s.get("name") == "Determine package source and image tags"
    )
    assert (
        tags["env"]["PUSH_LATEST"]
        == "${{ inputs.push_latest && steps.latest.outputs.promote == 'true' }}"
    )
