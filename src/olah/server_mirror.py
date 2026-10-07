import os
from typing import Callable, List, Optional, TypeVar

import git

from olah.mirror.repos import LocalMirrorRepo
from olah.server_access import RepoRef

T = TypeVar("T")


def _mirror_git_paths(app, repo: RepoRef) -> List[str]:
    return [
        os.path.join(mirror_path, repo.repo_type, repo.org or "", repo.repo)
        for mirror_path in app.state.app_settings.config.mirrors_path
    ]


def has_local_mirror(app, repo: RepoRef) -> bool:
    return any(os.path.exists(git_path) for git_path in _mirror_git_paths(app, repo))


def load_local_mirror_payload(app, repo: RepoRef, loader: Callable[[LocalMirrorRepo], Optional[T]], logger) -> Optional[T]:
    for git_path in _mirror_git_paths(app, repo):
        try:
            if not os.path.exists(git_path):
                continue
            local_repo = LocalMirrorRepo(git_path, repo.repo_type, repo.org, repo.repo)
            payload = loader(local_repo)
            if payload is not None:
                return payload
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue
    return None
