
## Quick Start
Run the command in the console: 
```bash
python -m olah.server
```

Then set the Environment Variable `HF_ENDPOINT` to the mirror site (Here is http://localhost:8090).

Linux: 
```bash
export HF_ENDPOINT=http://localhost:8090
```

Windows Powershell:
```bash
$env:HF_ENDPOINT = "http://localhost:8090"
```

Starting from now on, all download operations in the HuggingFace library will be proxied through this mirror site.
```bash
pip install -U huggingface_hub
```

```python
from huggingface_hub import snapshot_download

snapshot_download(repo_id='Qwen/Qwen-7B', repo_type='model',
                  local_dir='./model_dir', resume_download=True,
                  max_workers=8)
```

Or you can download models and datasets by using huggingface cli.

Download GPT2:
```bash
huggingface-cli download --resume-download openai-community/gpt2 --local-dir gpt2
```

Download WikiText:
```bash
huggingface-cli download --repo-type dataset --resume-download Salesforce/wikitext --local-dir wikitext
```

You can check the path `./repos`, in which olah stores all cached datasets and models.

## Start the server
Run the command in the console: 
```bash
python -m olah.server
```

Or you can specify the host address and listening port:
```bash
python -m olah.server --host localhost --port 8090
```
**Note: Please change --mirror-netloc and --mirror-lfs-netloc to the actual URLs of the mirror sites when modifying the host and port.**
```bash
python -m olah.server --host 192.168.1.100 --port 8090 --mirror-netloc 192.168.1.100:8090
```

The default mirror cache path is `./repos`, you can change it by `--repos-path` parameter:
```bash
python -m olah.server --host localhost --port 8090 --repos-path ./hf_mirrors
```

**Note that the cached data between different versions cannot be migrated. Please delete the cache folder before upgrading to the latest version of Olah.**
