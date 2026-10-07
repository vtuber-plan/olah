# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Serve a request from cache, as offline mode does, when the Hub can't answer its access check."""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

from fastapi import Response

from olah.utils.access_record import access_age
from olah.utils.auth_utils import token_hash


@dataclass
class _RequestState:
    upstream_error: Optional[Response] = None


_request_state: ContextVar[Optional[_RequestState]] = ContextVar("upstream_fallback", default=None)


def is_offline(app) -> bool:
    """Whether this request must not contact the Hub: offline mode, or serving through an upstream error."""
    if app.state.app_settings.config.offline:
        return True
    state = _request_state.get()
    return state is not None and state.upstream_error is not None


def serve_from_cache_if_granted(
    app,
    repo_type: str,
    org: Optional[str],
    repo: str,
    authorization: Optional[str],
    upstream_error: Response,
) -> bool:
    """Switch this request to cache-only mode if the Hub confirmed the caller's access within metadata-stale-if-error."""
    config = app.state.app_settings.config
    max_stale = getattr(config, "metadata_stale_if_error", 0)
    if max_stale <= 0:
        return False
    age = access_age(config.repos_path, repo_type, org, repo, token_hash(authorization))
    if age is None or age >= max_stale:
        return False
    state = _request_state.get()
    if state is None:
        state = _RequestState()
        _request_state.set(state)
    state.upstream_error = upstream_error
    return True


class UpstreamFallbackMiddleware:
    """Answer with the upstream error when a cache-only request could not be served from cache."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        state = _RequestState()
        token = _request_state.set(state)
        replaced = False

        async def guarded_send(message):
            nonlocal replaced
            if replaced:
                return
            if (
                message["type"] == "http.response.start"
                and message["status"] >= 400
                and state.upstream_error is not None
            ):
                replaced = True
                await state.upstream_error(scope, receive, send)
                return
            await send(message)

        try:
            await self.app(scope, receive, guarded_send)
        finally:
            _request_state.reset(token)
