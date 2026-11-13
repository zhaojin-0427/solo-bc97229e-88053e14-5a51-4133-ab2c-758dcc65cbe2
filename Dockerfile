# 轻量运行镜像：系统自带 sqlite，无需额外系统包
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
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
    CMD python -c "import json,urllib.request,sys; \
r=urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health',timeout=4); \
sys.exit(0 if json.load(r)['status']=='ok' else 1)"

# uvicorn 多 worker 安全：SQLite 以 WAL + 短事务支持多进程并发，
# 写串行化由库内锁与应用层乐观版本重试共同保证
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
