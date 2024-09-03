## 快速开始
在控制台运行以下命令：
```bash
python -m olah.server
```

然后将环境变量`HF_ENDPOINT`设置为镜像站点(这里是http://localhost:8090/)。

Linux: 
```bash
export HF_ENDPOINT=http://localhost:8090
```

Windows Powershell:
```bash
$env:HF_ENDPOINT = "http://localhost:8090"
```

从现在开始，HuggingFace库中的所有下载操作都将通过此镜像站点代理进行。
```bash
pip install -U huggingface_hub
```

```python
from huggingface_hub import snapshot_download

snapshot_download(repo_id='Qwen/Qwen-7B', repo_type='model',
                  local_dir='./model_dir', resume_download=True,
                  max_workers=8)

```

或者你也可以使用huggingface cli直接下载模型和数据集.

下载GPT2:
```bash
huggingface-cli download --resume-download openai-community/gpt2 --local-dir gpt2
```

下载WikiText:
```bash
huggingface-cli download --repo-type dataset --resume-download Salesforce/wikitext --local-dir wikitext
```

您可以查看路径`./repos`，其中存储了所有数据集和模型的缓存。
