# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
import hashlib
import io
import os
import re
from typing import Any, Dict, List, Union
import gitdb
from git import Commit, Optional, Repo, Tree
from git.objects.base import IndexObjUnion
from gitdb.base import OStream
import yaml

from olah.mirror.meta import RepoMeta


class LocalMirrorRepo(object):
    def __init__(self, path: str, repo_type: str, org: str, repo: str) -> None:
        self._path = path
        self._repo_type = repo_type
        self._org = org
        self._repo = repo

        self._git_repo = Repo(self._path)

    def _sha256(self, text: Union[str, bytes]) -> str:
        if isinstance(text, bytes) or isinstance(text, bytearray):
            bin = text
        elif isinstance(text, str):
            bin = text.encode("utf-8")
        else:
            raise Exception("Invalid sha256 param type.")
        sha256_hash = hashlib.sha256()
        sha256_hash.update(bin)
        hashed_string = sha256_hash.hexdigest()
        return hashed_string

    def _match_card(self, readme: str) -> str:
        pattern = r"\s*---(.*?)---"

        match = re.match(pattern, readme, flags=re.S)

        if match:
            card_string = match.group(1)
            return card_string
        else:
            return ""

    def _remove_card(self, readme: str) -> str:
        pattern = r"\s*---(.*?)---"
        out = re.sub(pattern, "", readme, flags=re.S)
        return out

    def _get_readme(self, commit: Commit) -> str:
        if "README.md" not in commit.tree:
            return ""
        else:
            out: bytes = commit.tree["README.md"].data_stream.read()
            return out.decode()

    def _get_description(self, commit: Commit) -> str:
        readme = self._get_readme(commit)
        return self._remove_card(readme)

    def _get_tree_filepaths_recursive(self, tree: Tree, include_dir: bool = False) -> List[str]:
        out_paths = []
        for entry in tree:
            if entry.type == "tree":
                out_paths.extend(self._get_tree_filepaths_recursive(entry))
                if include_dir:
                    out_paths.append(entry.path)
            else:
                out_paths.append(entry.path)
        return out_paths

    def _get_commit_filepaths_recursive(self, commit: Commit) -> List[str]:
        return self._get_tree_filepaths_recursive(commit.tree)

    def _get_path_info(self, entry: IndexObjUnion, expand: bool = False) -> Dict[str, Union[int, str]]:
        lfs = False
        if entry.type != "tree":
            t = "file"
            repr_size = entry.size
            if repr_size > 120 and repr_size < 150:
                # check lfs
                lfs_data = entry.data_stream.read().decode("utf-8")
                match_groups = re.match(
                    r"version https://git-lfs\.github\.com/spec/v[0-9]\noid sha256:([0-9a-z]{64})\nsize ([0-9]+?)\n",
                    lfs_data,
                )
                if match_groups is not None:
                    lfs = True
                    sha256 = match_groups.group(1)
                    repr_size = int(match_groups.group(2))
                    lfs_data = {
                        "oid": sha256,
                        "size": repr_size,
                        "pointerSize": entry.size,
                    }
        else:
            t = "directory"
            repr_size = entry.size

        if not lfs:
            item = {
                "type": t,
                "oid": entry.hexsha,
                "size": repr_size,
                "path": entry.path,
                "name": entry.name,
            }
        else:
            item = {
                "type": t,
                "oid": entry.hexsha,
                "size": repr_size,
                "path": entry.path,
                "name": entry.name,
                "lfs": lfs_data,
            }
        if expand:
            last_commit = next(self._git_repo.iter_commits(paths=entry.path, max_count=1))
            item["lastCommit"] = {
                "id": last_commit.hexsha,
                "title": last_commit.message,
                "date": last_commit.committed_datetime.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            }
            item["security"] = {
                "blobId": entry.hexsha,
                "name": entry.name,
                "safe": True,
                "indexed": False,
                "avScan": {
                    "virusFound": False,
                    "virusNames": None
                },
                "pickleImportScan": None
            }
        return item

    def _get_tree_files(
        self, tree: Tree, recursive: bool = False, expand: bool = False
    ) -> List[Dict[str, Union[int, str]]]:
        entries = []
        for entry in tree:
            entries.append(self._get_path_info(entry=entry, expand=expand))

        if recursive:
            for entry in tree:
                if entry.type == "tree":
                    entries.extend(self._get_tree_files(entry, recursive=recursive, expand=expand))
        return entries

    def _get_commit_files(self, commit: Commit) -> List[Dict[str, Union[int, str]]]:
        return self._get_tree_files(commit.tree)

    def _get_earliest_commit(self) -> Commit:
        earliest_commit = None
        earliest_commit_date = None

        for commit in self._git_repo.iter_commits():
            commit_date = commit.committed_datetime

            if earliest_commit_date is None or commit_date < earliest_commit_date:
                earliest_commit = commit
                earliest_commit_date = commit_date

        return earliest_commit

    def get_index_object_by_path(
        self, commit_hash: str, path: str
    ) -> Optional[IndexObjUnion]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None
        path_part = path.split("/")
        path_part = [part for part in path_part if len(part.strip()) != 0]
        tree = commit.tree
        items = self._get_tree_files(tree=tree)
        if len(path_part) == 0:
            return None
        for i, part in enumerate(path_part):
            if i != len(path_part) - 1:
                if part not in [
                    item["name"] for item in items if item["type"] == "directory"
                ]:
                    return None
            else:
                if part not in [
                    item["name"] for item in items
                ]:
                    return None
            tree = tree[part]
            if tree.type == "tree":
                items = self._get_tree_files(tree=tree, recursive=False)
        return tree

    def get_pathinfos(
        self, commit_hash: str, paths: List[str]
    ) -> Optional[List[Dict[str, Any]]]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None

        results = []
        for path in paths:
            index_obj = self.get_index_object_by_path(
                commit_hash=commit_hash, path=path
            )
            if index_obj is not None:
                results.append(self._get_path_info(index_obj))
        
        for r in results:
            if "name" in r:
                r.pop("name")
        return results

    def get_tree(
        self, commit_hash: str, path: str, recursive: bool = False, expand: bool = False
    ) -> Optional[Dict[str, Any]]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None

        index_obj = self.get_index_object_by_path(commit_hash=commit_hash, path=path)
        items = self._get_tree_files(tree=index_obj, recursive=recursive, expand=expand)
        for r in items:
            r.pop("name")
        return items
    
    def get_commits(self, commit_hash: str) -> Optional[Dict[str, Any]]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None

        parent_commits = [commit] + [each_commit for each_commit in commit.iter_parents()]
        items = []
        for each_commit in parent_commits:
            item = {
                "id": each_commit.hexsha,
                "title": each_commit.message,
                "message": "",
                "authors": [],
                "date": each_commit.committed_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
            item["authors"].append({
                "name": each_commit.author.name,
                "avatar": None
            })
            items.append(item)
        return items

    def get_meta(self, commit_hash: str) -> Optional[Dict[str, Any]]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None
        meta = RepoMeta()

        meta._id = self._sha256(f"{self._org}/{self._repo}/{commit.hexsha}")
        meta.id = f"{self._org}/{self._repo}"
        meta.author = self._org
        meta.sha = commit.hexsha
        meta.lastModified = self._git_repo.head.commit.committed_datetime.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        meta.private = False
        meta.gated = False
        meta.disabled = False
        meta.tags = []
        meta.description = self._get_description(commit)
        meta.paperswithcode_id = None
        meta.downloads = 0
        meta.likes = 0
        meta.cardData = yaml.load(
            self._match_card(self._get_readme(commit)), Loader=yaml.CLoader
        )
        meta.siblings = [
            {"rfilename": p} for p in self._get_commit_filepaths_recursive(commit)
        ]
        meta.createdAt = self._get_earliest_commit().committed_datetime.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        return meta.to_dict()

    def _contain_path(self, path: str, tree: Tree) -> bool:
        norm_p = os.path.normpath(path).replace("\\", "/")
        parts = norm_p.split("/")
        for part in parts:
            if all([t.name != part for t in tree]):
                return False
            else:
                entry = tree[part]
                if entry.type == "tree":
                    tree = entry
                else:
                    tree = {}
        return True

    def get_file_head(self, commit_hash: str, path: str) -> Optional[Dict[str, Any]]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None

        if not self._contain_path(path, commit.tree):
            return None
        else:
            header = {}
            header["content-length"] = str(commit.tree[path].data_stream.size)
            header["x-repo-commit"] = commit.hexsha
            header["etag"] = self._sha256(commit.tree[path].data_stream.read())
            return header

    def get_file(self, commit_hash: str, path: str) -> Optional[OStream]:
        try:
            commit = self._git_repo.commit(commit_hash)
        except gitdb.exc.BadName:
            return None

        def stream_wrapper(file_bytes: bytes):
            file_stream = io.BytesIO(file_bytes)
            while True:
                chunk = file_stream.read(4096)
                if len(chunk) == 0:
                    break
                else:
                    yield chunk

        if not self._contain_path(path, commit.tree):
            return None
        else:
            return stream_wrapper(commit.tree[path].data_stream.read())
