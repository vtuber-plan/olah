# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from typing import Any, Dict, List, Literal, Optional, Union
import toml
import re
import fnmatch

from olah.utils.disk_utils import convert_to_bytes

DEFAULT_PROXY_RULES = [
    {"repo": "*", "allow": True, "use_re": False},
    {"repo": "*/*", "allow": True, "use_re": False},
]

DEFAULT_CACHE_RULES = [
    {"repo": "*", "allow": True, "use_re": False},
    {"repo": "*/*", "allow": True, "use_re": False},
]


class OlahRule(object):
    def __init__(self, repo: str = "", type: str = "*", allow: bool = False, use_re: bool = False) -> None:
        self.repo = repo
        self.type = type
        self.allow = allow
        self.use_re = use_re

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "OlahRule":
        out = OlahRule()
        if "repo" in data:
            out.repo = data["repo"]
        if "allow" in data:
            out.allow = data["allow"]
        if "use_re" in data:
            out.use_re = data["use_re"]
        return out

    def match(self, repo_name: str) -> bool:
        if self.use_re:
            return self.match_re(repo_name)
        else:
            return self.match_fn(repo_name)

    def match_fn(self, repo_name: str) -> bool:
        return fnmatch.fnmatch(repo_name, self.repo)

    def match_re(self, repo_name: str) -> bool:
        return re.match(self.repo, repo_name) is not None


class OlahRuleList(object):
    def __init__(self) -> None:
        self.rules: List[OlahRule] = []

    @staticmethod
    def from_list(data: List[Dict[str, Any]]) -> "OlahRuleList":
        out = OlahRuleList()
        for item in data:
            out.rules.append(OlahRule.from_dict(item))
        return out

    def clear(self):
        self.rules.clear()

    def allow(self, repo_name: str) -> bool:
        allow = False
        for rule in self.rules:
            if rule.match(repo_name):
                allow = rule.allow
        return allow


class OlahConfig(object):
    def __init__(self, path: Optional[str] = None) -> None:

        # basic
        self.host: Union[List[str], str] = "localhost"
        self.port = 8090
        self.ssl_key = None
        self.ssl_cert = None
        self.repos_path = "./repos"
        self.cache_size_limit: Optional[int] = None
        self.cache_clean_strategy: Literal["LRU", "FIFO", "LARGE_FIRST"] = "LRU"

        self.hf_scheme: str = "https"
        self.hf_netloc: str = "huggingface.co"
        self.hf_lfs_netloc: str = "cdn-lfs.huggingface.co"

        self.mirror_scheme: str = "http" if self.ssl_key is None else "https"
        self.mirror_netloc: str = (
            f"{self.host if self._is_specific_addr(self.host) else 'localhost'}:{self.port}"
        )
        self.mirror_lfs_netloc: str = (
            f"{self.host if self._is_specific_addr(self.host) else 'localhost'}:{self.port}"
        )

        self.mirrors_path: List[str] = []

        # accessibility
        self.offline = False
        self.proxy = OlahRuleList.from_list(DEFAULT_PROXY_RULES)
        self.cache = OlahRuleList.from_list(DEFAULT_CACHE_RULES)

        if path is not None:
            self.read_toml(path)
    
    def _is_specific_addr(self, host: Union[List[str], str]) -> bool:
        if isinstance(host, str):
            return host not in ['0.0.0.0', '::']
        else:
            return False

    def hf_url_base(self) -> str:
        return f"{self.hf_scheme}://{self.hf_netloc}"

    def hf_lfs_url_base(self) -> str:
        return f"{self.hf_scheme}://{self.hf_lfs_netloc}"

    def mirror_url_base(self) -> str:
        return f"{self.mirror_scheme}://{self.mirror_netloc}"

    def mirror_lfs_url_base(self) -> str:
        return f"{self.mirror_scheme}://{self.mirror_lfs_netloc}"

    def empty_str(self, s: str) -> Optional[str]:
        if s == "":
            return None
        else:
            return s

    def read_toml(self, path: str) -> None:
        config = toml.load(path)

        if "basic" in config:
            basic = config["basic"]
            self.host = basic.get("host", self.host)
            self.port = basic.get("port", self.port)
            self.ssl_key = self.empty_str(basic.get("ssl-key", self.ssl_key))
            self.ssl_cert = self.empty_str(basic.get("ssl-cert", self.ssl_cert))
            self.repos_path = basic.get("repos-path", self.repos_path)
            self.cache_size_limit = convert_to_bytes(basic.get("cache-size-limit", self.cache_size_limit))
            self.cache_clean_strategy = basic.get("cache-clean-strategy", self.cache_clean_strategy)

            self.hf_scheme = basic.get("hf-scheme", self.hf_scheme)
            self.hf_netloc = basic.get("hf-netloc", self.hf_netloc)
            self.hf_lfs_netloc = basic.get("hf-lfs-netloc", self.hf_lfs_netloc)

            self.mirror_scheme = basic.get("mirror-scheme", self.mirror_scheme)
            self.mirror_netloc = basic.get("mirror-netloc", self.mirror_netloc)
            self.mirror_lfs_netloc = basic.get(
                "mirror-lfs-netloc", self.mirror_lfs_netloc
            )

            self.mirrors_path = basic.get("mirrors-path", self.mirrors_path)

        if "accessibility" in config:
            accessibility = config["accessibility"]
            self.offline = accessibility.get("offline", self.offline)
            self.proxy = OlahRuleList.from_list(accessibility.get("proxy", DEFAULT_PROXY_RULES))
            self.cache = OlahRuleList.from_list(accessibility.get("cache", DEFAULT_CACHE_RULES))
