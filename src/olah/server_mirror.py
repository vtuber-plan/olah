import os
from typing import Callable, Optional, TypeVar

import git

from olah.mirror.repos import LocalMirrorRepo
from olah.server_access import RepoRef

T = TypeVar("T")


def load_local_mirror_payload(app, repo: RepoRef, loader: Callable[[LocalMirrorRepo], Optional[T]], logger) -> Optional[T]:
    for mirror_path in app.state.app_settings.config.mirrors_path:
        git_path = os.path.join(mirror_path, repo.repo_type, repo.org or "", repo.repo)
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
