# 轻量运行镜像：Python 3.11 自带 sqlite，无需额外系统包
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    LINEAGE_DATA_DIR=/data

WORKDIR /app

# 先装依赖，利用 Docker 层缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 持久化 SQLite 数据库（含 WAL 辅助文件）的目录
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 容器级健康检查：命中应用 /health，失败由编排层可见
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
    CMD python -c "import json,urllib.request,sys; \
r=urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health',timeout=4); \
sys.exit(0 if json.load(r)['status']=='ok' else 1)"

# 单 worker：SQLite 只有一个写者，上传准入与 OCC 重试状态在进程内，
# 查询/上传/提交的执行资源按进程显式有界（见 app/executors.py）。
# 多进程会让准入上限与重试语义分散到各进程，故容器内固定单进程。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--proxy-headers"]
