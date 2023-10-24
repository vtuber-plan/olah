from huggingface_hub import snapshot_download

snapshot_download(repo_id='Qwen/Qwen-7B',
                  repo_type='model',
                  local_dir='./model_dir',
                  resume_download=True,
                  max_workers=4)
