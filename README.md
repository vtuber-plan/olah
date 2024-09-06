<h1 align="center">Olah</h1>

<p align="center">
<b>Self-hosted Lightweight Huggingface Mirror Service</b>

Olah is a self-hosted lightweight huggingface mirror service. `Olah` means `hello` in Hilichurlian.
Olah implemented the `mirroring` feature for huggingface resources, rather than just a simple `reverse proxy`.
Olah does not immediately mirror the entire huggingface website but mirrors the resources at the file block level when users download them (or we can say cache them).

Other languages: [中文](README_zh.md)

## Advantages of Olah
Olah has the capability to cache files in chunks while users download them. Upon subsequent downloads, the files can be directly retrieved from the cache, greatly enhancing download speeds and saving bandwidth.
Additionally, Olah offers a range of cache control policies. Administrators can configure which repositories are accessible and which ones can be cached through a configuration file.

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
olah-cli
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
olah-cli
```

Or you can specify the host address and listening port:
```bash
olah-cli --host localhost --port 8090
```
**Note: Please change --mirror-netloc and --mirror-lfs-netloc to the actual URLs of the mirror sites when modifying the host and port.**
```bash
olah-cli --host 192.168.1.100 --port 8090 --mirror-netloc 192.168.1.100:8090
```

The default mirror cache path is `./repos`, you can change it by `--repos-path` parameter:
```bash
olah-cli --host localhost --port 8090 --repos-path ./hf_mirrors
```

**Note that the cached data between different versions cannot be migrated. Please delete the cache folder before upgrading to the latest version of Olah.**

## More Configurations

Additional configurations can be controlled through a configuration file by passing the `configs.toml` file as a command parameter:
```bash
olah-cli -c configs.toml
```

The complete content of the configuration file can be found at [assets/full_configs.toml](https://github.com/vtuber-plan/olah/blob/main/assets/full_configs.toml).

### Configuration Details
The first section, `basic`, is used to set up basic configurations for the mirror site:
```toml
[basic]
host = "localhost"
port = 8090
ssl-key = ""
ssl-cert = ""
repos-path = "./repos"
cache-size-limit = ""
cache-clean-strategy = "LRU"
hf-scheme = "https"
hf-netloc = "huggingface.co"
hf-lfs-netloc = "cdn-lfs.huggingface.co"
mirror-scheme = "http"
mirror-netloc = "localhost:8090"
mirror-lfs-netloc = "localhost:8090"
mirrors-path = ["./mirrors_dir"]
```
- `host`: Sets the host address that Olah listens to.
- `port`: Sets the port that Olah listens to.
- `ssl-key` and `ssl-cert`: When enabling HTTPS, specify the file paths for the key and certificate.
- `repos-path`: Specifies the directory for storing cached data.
- `cache-size-limit`: Specifies cache size limit (For example, 100G, 500GB, 2TB). Olah will scan the size of the cache folder every hour. If it exceeds the limit, olah will delete some cache files.
- `cache-clean-strategy`: Specifies cache cleaning strategy (Available strategies: LRU, FIFO, LARGE_FIRST).
- `hf-scheme`: Network protocol for the Hugging Face official site (usually no need to modify).
- `hf-netloc`: Network location of the Hugging Face official site (usually no need to modify).
- `hf-lfs-netloc`: Network location for Hugging Face official site's LFS files (usually no need to modify).
- `mirror-scheme`: Network protocol for the Olah mirror site (should match the above settings; change to HTTPS if providing `ssl-key` and `ssl-cert`).
- `mirror-netloc`: Network location of the Olah mirror site (should match `host` and `port` settings).
- `mirror-lfs-netloc`: Network location for Olah mirror site's LFS (should match `host` and `port` settings).
- `mirrors-path`: Additional mirror file directories. If you have already cloned some Git repositories, you can place them in this directory for downloading. In this example, the directory is `./mirrors_dir`. To add a dataset like `Salesforce/wikitext`, you can place the Git repository in the directory `./mirrors_dir/datasets/Salesforce/wikitext`. Similarly, models can be placed under `./mirrors_dir/models/organization/repository`.

The second section allows for accessibility restrictions:
```toml
[accessibility]
offline = false

[[accessibility.proxy]]
repo = "cais/mmlu"
allow = true

[[accessibility.proxy]]
repo = "adept/fuyu-8b"
allow = false

[[accessibility.proxy]]
repo = "mistralai/*"
allow = true

[[accessibility.proxy]]
repo = "mistralai/Mistral.*"
allow = false
use_re = true

[[accessibility.cache]]
repo = "cais/mmlu"
allow = true

[[accessibility.cache]]
repo = "adept/fuyu-8b"
allow = false
```
- `offline`: Sets whether the Olah mirror site enters offline mode, no longer making requests to the Hugging Face official site for data updates. However, cached repositories can still be downloaded.
- `proxy`: Determines if the repository can be accessed through a proxy. By default, all repositories are allowed. The `repo` field is used to match the repository name. Regular expressions and wildcards can be used by setting `use_re` to control whether to use regular expressions (default is to use wildcards). The `allow` field controls whether the repository is allowed to be proxied.
- `cache`: Determines if the repository will be cached. By default, all repositories are allowed. The `repo` field is used to match the repository name. Regular expressions and wildcards can be used by setting `use_re` to control whether to use regular expressions (default is to use wildcards). The `allow` field controls whether the repository is allowed to be cached.

## Future Work

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

