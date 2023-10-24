from huggingface_hub import snapshot_download

snapshot_download(repo_id='THUDM/AgentInstruct',
                  repo_type='dataset',
                  local_dir='./dataset_dir',
                  resume_download=True,
                  max_workers=1)
