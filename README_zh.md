Olah是一种自托管的轻量级HuggingFace镜像服务。`Olah`在丘丘人语中意味着`你好`。

## Olah的优势
Olah能够在用户下载的同时分块缓存文件。当第二次下载时，直接从缓存中读取，极大地提升了下载速度并节约了流量。
同时Olah提供了丰富的缓存控制策略，管理员可以通过配置文件设置哪些仓库可以访问，哪些仓库可以缓存。

## 特性
* 数据缓存，减少下载流量
* 模型镜像
* 数据集镜像
* 空间镜像

## 安装

### 方法1：使用pip

```bash
pip install olah
```

或者：

```bash
pip install git+https://github.com/vtuber-plan/olah.git
```

### 方法2：从源代码安装

1. 克隆这个仓库
```bash
git clone https://github.com/vtuber-plan/olah.git
cd olah
```

2. 安装包
```bash
pip install --upgrade pip
pip install -e .
```

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

## 启动服务器
在控制台运行以下命令：
```bash
python -m olah.server
```

或者您可以指定主机地址和监听端口：
```bash
python -m olah.server --host localhost --port 8090
```
请记得在修改主机和端口时将`--mirror-url`和`--mirror-lfs-url`更改为镜像站点的实际URL。

默认的镜像缓存路径是`./repos`，您可以通过`--repos-path`参数进行更改：
```bash
python -m olah.server --host localhost --port 8090 --repos-path ./hf_mirrors
```

**注意，不同版本之间的缓存数据不能迁移，请删除缓存文件夹后再进行olah的升级**

## 许可证

olah采用MIT许可证发布。

## 另请参阅

- [olah-docs](https://github.com/vtuber-plan/olah/tree/main/docs)
- [olah-source](https://github.com/vtuber-plan/olah)

## Star历史

[![Star历史图表]()](https://star-history.com/#vtuber-plan/olah&Date)