<h1 align="center">Olah 文档</h1>


<p align="center">
<b>自托管的轻量级HuggingFace镜像服务</b>

Olah是开源的自托管轻量级HuggingFace镜像服务。`Olah`来源于丘丘人语，在丘丘人语中意味着`你好`。
Olah真正地实现了huggingface资源的`镜像`功能，而不仅仅是一个简单的`反向代理`。
Olah并不会立刻对huggingface全站进行镜像，而是在用户下载的同时在文件块级别对资源进行镜像（或者我们可以说是缓存）。
