# olah
Olah is self-hosted lightweight huggingface mirror service. `Olah` means `hello` in Hilichurlian.

Other languages: [中文](README_zh.md)
## Features
* Huggingface Data Cache
* Models mirror
* Datasets mirror
* Spaces mirror

## Install

### Method 1: With pip

```bash
pip install olah
```

or:

```bash
pip install git+https://github.com/vtuber-plan/olah.git 
```

### Method 2: From source

1. Clone this repository
```bash
git clone https://github.com/vtuber-plan/olah.git
cd olah
```

2. Install the Package
```bash
pip install --upgrade pip
pip install -e .
```

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
Please remember to change the `--mirror-url` and `--mirror-lfs-url` to the actual URLs of the mirror site while modifying the host and port.

The default mirror cache path is `./repos`, you can change it by `--repos-path` parameter:
```bash
python -m olah.server --host localhost --port 8090 --repos-path ./hf_mirrors
```

**Note that the cached data between different versions cannot be migrated. Please delete the cache folder before upgrading to the latest version of Olah.**

## Future Work

* Authentication
* Administrator and user system
* OOS backend support
* Mirror Update Schedule Task

## License

olah is released under the MIT License.


## See also

- [olah-docs](https://github.com/vtuber-plan/olah/tree/main/docs)
- [olah-source](https://github.com/vtuber-plan/olah)


## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=vtuber-plan/olah&type=Date)](https://star-history.com/#vtuber-plan/olah&Date)

