<h1 align="center">Olah Document</h1>

<p align="center">
<b>Self-hosted Lightweight Huggingface Mirror Service</b>

Olah is a self-hosted lightweight huggingface mirror service. `Olah` means `hello` in Hilichurlian.
Olah implemented the `mirroring` feature for huggingface resources, rather than just a simple `reverse proxy`.
Olah does not immediately mirror the entire huggingface website but mirrors the resources at the file block level when users download them (or we can say cache them).
