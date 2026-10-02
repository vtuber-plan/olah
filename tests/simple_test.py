"""Opt-in live Hugging Face downloads; excluded from deterministic release CI."""

import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from huggingface_hub import snapshot_download
import pytest

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("OLAH_E2E_LIVE") != "1",
        reason="live Hugging Face downloads; set OLAH_E2E_LIVE=1 to run",
    ),
]


@pytest.fixture
def mirror_endpoint(tmp_path):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    netloc = f"127.0.0.1:{port}"
    command = [
        sys.executable,
        "-m",
        "olah.server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--mirror-netloc",
        netloc,
        "--mirror-lfs-netloc",
        netloc,
        "--repos-path",
        str(tmp_path / "repos"),
        "--log-path",
        str(tmp_path / "logs"),
    ]
    # The server uses the same source tree even without an editable installation.
    env = os.environ.copy()
    src = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    with (tmp_path / "server.log").open("w+") as log:
        process = subprocess.Popen(
            command, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(f"Olah exited before startup: {log.read()}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail("Olah did not start within 30 seconds")
            yield f"http://{netloc}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def test_dataset(mirror_endpoint, tmp_path):
    snapshot_download(
        repo_id="Nerfgun3/bad_prompt",
        repo_type="dataset",
        endpoint=mirror_endpoint,
        local_dir=tmp_path / "dataset",
        max_workers=8,
    )


def test_model(mirror_endpoint, tmp_path):
    snapshot_download(
        repo_id="prajjwal1/bert-tiny",
        repo_type="model",
        endpoint=mirror_endpoint,
        local_dir=tmp_path / "model",
        max_workers=8,
    )
