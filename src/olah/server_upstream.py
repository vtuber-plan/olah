from dataclasses import dataclass
from typing import Callable, Literal, Optional, Tuple

from fastapi.responses import Response

from olah.errors import error_repo_not_found, error_revision_not_found
from olah.proxy.result import ProxyResult
from olah.server_access import RepoRef
from olah.utils.repo_utils import check_commit_hf, get_commit_hf, get_newest_commit_hf


@dataclass(frozen=True)
class ResolvedCommit:
    requested: str
    resolved: str

    @property
    def refresh_cache(self) -> bool:
        return self.requested != self.resolved


async def get_latest_commit(app, repo: RepoRef, authorization: Optional[str]) -> Optional[str]:
    return await get_newest_commit_hf(
        app,
        repo.repo_type,
        repo.org,
        repo.repo,
        authorization=authorization,
    )


async def resolve_requested_commit(
    app,
    repo: RepoRef,
    requested_commit: str,
    authorization: Optional[str],
    repo_visible: bool = False,
    missing_commit_response: Literal["repo_not_found", "revision_not_found"] = "revision_not_found",
) -> Tuple[Optional[ResolvedCommit], Optional[Response]]:
    if not app.state.app_settings.config.offline:
        if not repo_visible:
            if not await check_commit_hf(
                app,
                repo.repo_type,
                repo.org,
                repo.repo,
                commit=None,
                authorization=authorization,
            ):
                return None, error_repo_not_found()
        if not await check_commit_hf(
            app,
            repo.repo_type,
            repo.org,
            repo.repo,
            commit=requested_commit,
            authorization=authorization,
        ):
            if missing_commit_response == "repo_not_found":
                return None, error_repo_not_found()
            return None, error_revision_not_found(revision=requested_commit)

    resolved_commit = await get_commit_hf(
        app,
        repo.repo_type,
        repo.org,
        repo.repo,
        commit=requested_commit,
        authorization=authorization,
    )
    if resolved_commit is None:
        return None, error_repo_not_found()
    return ResolvedCommit(requested=requested_commit, resolved=resolved_commit), None


async def prepare_revision_generator(app, resolved_commit: ResolvedCommit, generator_factory: Callable[[str, bool], object]) -> ProxyResult:
    if not app.state.app_settings.config.offline and resolved_commit.refresh_cache:
        refresh_result = await generator_factory(resolved_commit.requested, True)
        async for _ in refresh_result.body:
            pass
        return await generator_factory(resolved_commit.resolved, True)
    return await generator_factory(resolved_commit.resolved, False)
