import os
import subprocess
import time

from huggingface_hub import snapshot_download

def test_simple():
    process = subprocess.Popen(['python', '-m', 'olah.server'])

    os.environ['HF_ENDPOINT'] = 'http://localhost:8090'
    snapshot_download(repo_id='Nerfgun3/bad_prompt', repo_type='dataset',
                    local_dir='./dataset_dir', max_workers=8)

    # 终止子进程
    process.terminate()