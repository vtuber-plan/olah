from dataclasses import dataclass
from typing import Literal, Optional

from fastapi.responses import Response

from olah.constants import REPO_TYPES_MAPPING
from olah.errors import error_page_not_found, error_repo_not_found
from olah.utils.repo_utils import get_org_repo, parse_org_repo
from olah.utils.rule_utils import check_proxy_rules_hf

RepoType = Literal["models", "datasets", "spaces"]


@dataclass(frozen=True)
class RepoRef:
    repo_type: str
    org: Optional[str]
    repo: str

    @property
    def org_repo(self) -> str:
        return get_org_repo(self.org, self.repo)


def build_repo_ref(repo_type: str, org: Optional[str], repo: str) -> RepoRef:
    return RepoRef(repo_type=repo_type, org=org, repo=repo)


def parse_repo_ref(repo_type: str, org_repo: str) -> Optional[RepoRef]:
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return None
    return build_repo_ref(repo_type, org, repo)


def parse_resolve_repo_ref(org_or_repo_type: str, repo_name: str) -> Optional[RepoRef]:
    if org_or_repo_type in REPO_TYPES_MAPPING:
        return parse_repo_ref(org_or_repo_type, repo_name)
    return build_repo_ref("models", org_or_repo_type, repo_name)


async def ensure_repo_access(app, repo: RepoRef) -> Optional[Response]:
    if repo.repo_type not in REPO_TYPES_MAPPING:
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo.repo_type, repo.org, repo.repo):
        return error_repo_not_found()
    return None
