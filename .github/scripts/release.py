"""Small, read-only safety checks used by the tag release workflow (Python 3.12)."""

import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import urlopen

STABLE_VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def version_tuple(version):
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError(f"Expected a stable X.Y.Z version, got {version!r}")
    return tuple(int(part) for part in version.split("."))


def validate_tag(tag, project_path=Path("pyproject.toml")):
    if not tag.startswith("v"):
        raise ValueError("Release tags must have the form vX.Y.Z")
    version = tag[1:]
    version_tuple(version)
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10 development test matrix
        import toml as tomllib
    package_version = tomllib.loads(project_path.read_text())["project"]["version"]
    if version != package_version:
        raise ValueError(
            f"Tag version {version} does not match package version {package_version}"
        )
    return version


def check_existing_files(version, dist, published, require_all=False):
    version_tuple(version)
    files = {path.name: path for path in dist.iterdir() if path.is_file()}
    expected = {f"olah-{version}-py3-none-any.whl", f"olah-{version}.tar.gz"}
    if set(files) != expected:
        raise ValueError(f"Expected exactly {sorted(expected)}, found {sorted(files)}")
    entries = published.get("urls", [])
    if require_all and {entry["filename"] for entry in entries} != expected:
        raise ValueError("PyPI does not yet contain the complete distribution set")
    for entry in entries:
        name = entry["filename"]
        if name not in files:
            raise ValueError(f"PyPI already contains an unexpected file: {name}")
        digest = hashlib.sha256(files[name].read_bytes()).hexdigest()
        if digest != entry["digests"]["sha256"]:
            raise ValueError(
                f"PyPI has different bytes for {name}; use the original run's artifacts or a new version"
            )
        print(f"Verified existing PyPI file: {name}")


def check_pypi(version, dist, require_all=False):
    version_tuple(version)
    try:
        with urlopen(
            f"https://pypi.org/pypi/olah/{version}/json", timeout=30
        ) as response:
            published = json.load(response)
    except HTTPError as error:
        if error.code != 404:
            raise
        published = {"urls": []}
    check_existing_files(version, dist, published, require_all)


def should_promote(version, tags):
    current = version_tuple(version)
    stable_versions = [
        version_tuple(tag[1:])
        for tag in tags
        if tag.startswith("v") and STABLE_VERSION.fullmatch(tag[1:])
    ]
    return not stable_versions or current >= max(stable_versions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("tag")
    pypi = commands.add_parser("check-pypi")
    pypi.add_argument("version")
    pypi.add_argument("dist", type=Path)
    pypi.add_argument("--require-all", action="store_true")
    latest = commands.add_parser("latest")
    latest.add_argument("version")
    latest.add_argument("tags", type=Path)
    args = parser.parse_args()
    if args.command == "validate":
        print(f"version={validate_tag(args.tag)}")
    elif args.command == "check-pypi":
        check_pypi(args.version, args.dist, args.require_all)
    else:
        promote = should_promote(args.version, args.tags.read_text().splitlines())
        print(f"promote={str(promote).lower()}")


if __name__ == "__main__":
    main()
