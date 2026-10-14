# MyPanel 🐳

> 私有化定制的 Docker 容器调度管理面板：**定时启停容器、一键更新镜像、Compose 项目管理**，全在一个网页里搞定。
> 本仓库基于开源项目 [docker-butler](https://github.com/vindy-lu/docker-butler) 源码改造，专门适配 **ARM64 路由器**（已在京东云雅典娜 RE-CS-02 上使用），仅供个人私用。
> 手机端也能用。

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## 🆚 相比上游 docker-butler 做了什么改动

上游发布的镜像只有 `linux/amd64`，在 ARM 路由器上无法直接拉取运行；且默认挂载路径是飞牛/群晖等 NAS 的目录。本仓库据自用需求做了如下改造：

1. **支持多架构构建（核心）**
   - `Dockerfile` 改为按构建架构（`TARGETARCH`）自动下载对应平台的 docker CLI + compose 静态二进制（amd64 / arm64 / armv7 全支持），不再依赖 apt 里的 `docker-cli`/`docker-compose`。
2. **修复架构写死为 amd64 的 Bug**
   - `backend/modules/update_checker.py` 里 3 处判断多架构镜像 manifest 时写死了 `architecture == "amd64"`，导致在 ARM 上“检查更新”永远匹配不到 digest。改为运行时按本机架构（`platform.machine()`）动态选择，可用环境变量 `ARCH_OVERRIDE` 强制覆盖。
3. **挂载路径适配路由器**
   - `docker-compose.yml` / `docker-compose.build.yml` 默认挂载路径改为京东云雅典娜 RE-CS-02 的实际路径（`/mnt/mmcblk0p27/docker`、`/mnt/mmcblk0p27`），仍可用 `COMPOSE_ROOT` / `COMPOSE_VOLUME_ROOT` 环境变量覆盖。
4. **数据/容器命名改为 mypanel**：`/data/mypanel.db`、容器名 `mypanel`、自更新临时容器 `mypanel-self-updater`，并保留对旧名的识别兼容。
5. **品牌改造**：页面标题/页头/2FA 发行方/应用标题统一为 `MyPanel`。
6. **新增多架构 CI**：`.github/workflows/build.yml` 使用 buildx 同时构建 `linux/amd64` 和 `linux/arm64` 并推送到 `ghcr.io/fanling1970/mypanel`。

> 说明：`docs/配图/` 中的界面截图仍为上游原图（仅供参考布局），实际文案会显示为 MyPanel。

## ✨ 功能特性

| 能力 | 说明 |
| --- | --- |
| 容器管理 | 容器列表、CPU/内存/网络I/O/磁盘I/O 实时展示，端口直达链接，一键启停/重启/删除，支持网页新建容器 |
| 定时调度 | 基于 Cron 表达式定时启停/重启/更新容器，支持 Compose 项目级调度，支持自然语言描述（不用背 Cron 语法） |
| 镜像更新 | 自动/手动检测远程新版本，一键更新（实时进度条），可逐容器开关 |
| Compose 项目管理 | 自动识别本机所有 Compose 项目，项目级启停/重启/更新，**内存占用排序**，支持网页新建项目、目录浏览/新建、查看 YAML 配置 |
| 镜像管理 | 镜像列表、批量清理未使用/悬空镜像、**镜像加速器设置**（国内拉取提速）、**本地 tar 导入** |
| 本服务自更新 | 面板内一键更新自己（检测新版本 → 拉取镜像 → 自动重建），支持检查更新、重启本服务 |
| 端口占用扫描 | 一键查看宿主机全部占用端口（系统监听 + 容器映射），创建 Compose 前避开冲突端口 |
| 账号安全 | 双重认证 2FA（TOTP 验证码）、信任设备（30 天免验证码）、修改密码 |
| 视觉 | 4 种配色 + 5 种风格，暗色主题，手机端适配 |

## 🧬 技术架构

- 后端：Python FastAPI + Docker SDK + APScheduler
- 前端：Vue 3 + Element Plus（单页应用，无构建依赖）
- 存储：SQLite（轻量，无需额外数据库）
- 网络：bridge 端口映射（只暴露面板端口），`pid: host` 支持端口占用扫描读取宿主监听

```
路由器宿主机 (ARM64)
└── mypanel 容器 (privileged + pid:host)
    ├── FastAPI (容器内 8383，映射宿主机 8383)
    ├── SQLite (/data/mypanel.db)
    ├── Docker SDK → /var/run/docker.sock → 管理本机其他容器
    └── 挂载：/mnt/mmcblk0p27/docker → /host/compose；/mnt/mmcblk0p27 → /host-vol1；/etc/docker/daemon.json
```

## 🚀 快速开始（京东云雅典娜 RE-CS-02 / 任意 ARM64 路由器）

### 前提
- 路由器已安装 Docker（含 Docker Compose v2），且可 SSH 登录
- 已知你的 Docker 根目录（RE-CS-02 默认为 `/mnt/mmcblk0p27/docker`），可用 `docker info | grep "Docker Root Dir"` 确认

### 方式一：GitHub Actions 多架构镜像（推荐，免本机构建）

本仓库已配置 CI，推送到 `main` 后会自动构建 `linux/amd64` + `linux/arm64` 镜像并推送到 GHCR：

```bash
# 1. 在路由器上克隆你自己的仓库
cd /mnt/mmcblk0p27/docker      # 换成你的 Docker 目录
git clone https://github.com/fanling1970/mypanel.git
cd mypanel

# 2. 启动（首次会自动拉取 arm64 镜像）
docker compose up -d
```

> 若 GHCR 包为私有，需先在路由器 `docker login ghcr.io`（用 GitHub 用户名 + 一个带 `read:packages` 权限的 PAT）。

### 方式二：路由器本机源码构建（无外网镜像可用时）

```bash
cd /mnt/mmcblk0p27/docker
git clone https://github.com/fanling1970/mypanel.git
cd mypanel
docker compose -f docker-compose.build.yml up -d --build
```

构建过程会在路由器上直接编译 `arm64` 镜像（已按架构自动选择 docker CLI / compose 二进制）。

### 方式三：docker run 一条命令

```bash
docker run -d \
  --name mypanel \
  --restart unless-stopped \
  --privileged \
  --pid host \
  -p 8383:8383 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd)/mypanel-data:/data \
  -v /etc/docker/daemon.json:/etc/docker/daemon.json \
  -v /mnt/mmcblk0p27/docker:/host/compose \
  -v /mnt/mmcblk0p27:/host-vol1 \
  -e TZ=Asia/Shanghai \
  -e DB_PATH=/data/mypanel.db \
  -e APP_PORT=8383 \
  ghcr.io/fanling1970/mypanel:latest
```

---

启动完成后浏览器访问：`http://你的路由器IP:8383`

**默认账号：`admin` / `admin`**（⚠️ 首次登录后请立即修改密码）

> 💡 **端口说明**：容器内监听 8383，映射到宿主机 8383（RE-CS-02 上 8383 一般未被占用；若冲突，改 `ports` 左边数字即可）。想换容器内端口，改 `APP_PORT` 和映射右边数字（保持一致）。

## 📚 文档

- [部署手册（完整版）](docs/部署手册.md) — 详细部署步骤、功能使用、FAQ

## ❓ 常见问题

| 现象 | 解决办法 |
| --- | --- |
| `no matching manifest for linux/arm64` | 说明拉的镜像不含 arm64。请用本仓库自己的镜像 `ghcr.io/fanling1970/mypanel`（CI 已构建 arm64），或用方式二本机构建 |
| 检查更新一直显示“已是最新” | 确认已用本仓库改造后的源码（修复了写死 amd64 的问题）；必要时设 `ARCH_OVERRIDE=arm64` |
| 拉镜像失败 | 国内网络可在「镜像管理 → 加速器」配置国内镜像加速器，保存后重启 Docker 服务 |
| 面板打不开 | `docker ps` 看状态，`docker compose logs -f` 看日志；端口 8383 是否被占用 |
| 提示 privileged 警告 | 管理其他容器需要特权模式，不能去掉 |
| 项目详情读不到 YAML | 项目目录不在默认挂载范围内。compose 的 `volumes` 加挂载 + `environment` 加 `EXTRA_MOUNTS=/宿主机目录:/host/xxx`，重建即可 |

## 📄 License

[MIT](LICENSE)。原项目版权归 榴柑橙（docker-butler）所有；本仓库为个人私用改造版。
