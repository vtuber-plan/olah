import os
import subprocess
import time

from huggingface_hub import snapshot_download

def test_dataset():
    process = subprocess.Popen(['python', '-m', 'olah.server'])

    os.environ['HF_ENDPOINT'] = 'http://localhost:8090'
    snapshot_download(repo_id='Nerfgun3/bad_prompt', repo_type='dataset',
                    local_dir='./dataset_dir', max_workers=8)

    # 终止子进程
    process.terminate()

def test_model():
    process = subprocess.Popen(['python', '-m', 'olah.server'])

    os.environ['HF_ENDPOINT'] = 'http://localhost:8090'
    snapshot_download(repo_id='prajjwal1/bert-tiny', repo_type='model',
                    local_dir='./model_dir', max_workers=8)

    # 终止子进程
    process.terminate()