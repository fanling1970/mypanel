# 多架构构建：支持 amd64 / arm64 / armv7（京东云雅典娜 RE-CS-02 为 aarch64/arm64）
# 通过 buildx 的自动平台参数 TARGETARCH 选择对应架构的 docker 静态二进制
FROM python:3.12-slim

LABEL maintainer="mypanel"
LABEL org.opencontainers.image.title="MyPanel"

ARG TARGETARCH=arm64
ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.29.7
ARG GIT_COMMIT=unknown

# 安装系统依赖
# 说明：不再使用 apt 的 docker-cli/docker-compose（在 arm 上包名/可用性不稳定），
# 改为直接下载对应架构的官方静态二进制（docker CLI + compose 插件），
# 这样 amd64 / arm64 / armv7 都能得到一致的 docker/compose 版本。
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tar; \
    rm -rf /var/lib/apt/lists/*; \
    case "${TARGETARCH}" in \
      amd64) DOCKER_ARCH="x86_64"; COMPOSE_ARCH="x86_64" ;; \
      arm64) DOCKER_ARCH="aarch64"; COMPOSE_ARCH="aarch64" ;; \
      arm)   DOCKER_ARCH="armv7l";  COMPOSE_ARCH="armv7" ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    # docker CLI 静态二进制
    curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz" -o /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    mv /tmp/docker/docker /usr/local/bin/docker; \
    chmod +x /usr/local/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    # docker compose v2 插件（同时提供 docker-compose 软链，兼容脚本里的老写法）
    mkdir -p /usr/local/lib/docker/cli-plugins; \
    curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${COMPOSE_ARCH}" -o /usr/local/lib/docker/cli-plugins/docker-compose; \
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose; \
    ln -sf /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose; \
    docker --version; docker compose version

WORKDIR /app

# 安装Python依赖
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制后端代码
COPY backend/ .

# 复制前端静态文件
COPY frontend/ /app/frontend/

# 数据目录
RUN mkdir -p /data

ENV DB_PATH=/data/mypanel.db
ENV APP_PORT=8383
# 构建提交号：/api/debug/perf 与 /api/app-info 会返回，用于确认镜像是否为最新
ENV GIT_COMMIT=${GIT_COMMIT}

EXPOSE 8383

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${APP_PORT:-8383} --log-level info"]
