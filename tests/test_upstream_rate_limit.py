"""Upstream 429s reach the client as 429 with the Hub's reset headers.

huggingface_hub sleeps until the advertised reset on a 429 but gives up
quickly on a 504, so the status and headers must survive the proxy.
"""
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from olah.configs import OlahConfig
from olah.server import app as olah_app
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
RATE_LIMIT_HEADERS = {
    "retry-after": "42",
    "ratelimit": '"api";r=0;t=42',
    "ratelimit-policy": '"fixed window";"api";q=500;w=300',
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    app.exception_handlers.update(olah_app.exception_handlers)

    async def upstream(request):
        return httpx.Response(429, headers={**RATE_LIMIT_HEADERS, "set-cookie": "token=secret"})

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/models/team/tiny"),
        ("GET", "/api/models/team/tiny/revision/main"),
        ("GET", "/api/models/team/tiny/tree/main"),
        ("HEAD", "/team/tiny/resolve/main/tiny.gguf"),
        ("GET", "/team/tiny/resolve/main/tiny.gguf"),
    ],
)
def test_upstream_rate_limit_is_relayed(client, method, path):
    response = client.request(method, path)

    assert response.status_code == 429
    for name, value in RATE_LIMIT_HEADERS.items():
        assert response.headers[name] == value
    assert "set-cookie" not in response.headers
