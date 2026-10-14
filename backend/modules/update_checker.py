"""镜像更新检测模块 - 通过Docker Daemon检测远程镜像是否有新版本

检测策略（优先级从高到低）：
1. Docker SDK inspect_distribution API（推荐）
   - 优势：直接走Docker Daemon的代理、认证、镜像加速器配置
   - 优势：与docker pull行为完全一致
   - 要求：Docker API v1.30+（Docker 17.06+）

2. Docker CLI manifest inspect（备选）
   - 优势：同样走Docker Daemon的配置
   - 要求：容器内有docker命令且实验模式可用

3. Registry V2 API HTTP请求（最后兜底）
   - 优势：不依赖Docker
   - 劣势：需要自己配置代理和认证

核心原理：
1. 获取本地镜像的 RepoDigests（即拉取时的 manifest digest）
2. 查询远程 Registry 当前 tag 的 digest
3. 精确比较两个 digest 是否一致
"""

import json
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger("update_checker")

# 超时设置（秒）
REQUEST_TIMEOUT = 20
CLI_TIMEOUT = 30

# Docker 配置路径
DOCKER_CONFIG_PATH = Path(os.path.expanduser("~/.docker/config.json"))


def _target_arch() -> str:
    """返回本机 CPU 架构（用于多架构镜像选择对应 manifest）。

    优先读取环境变量 ARCH_OVERRIDE（可强制指定），否则用 platform.machine()。
    常见取值：x86_64 -> amd64，aarch64/arm64 -> arm64，
    armv7l -> arm（京东云雅典娜 RE-CS-02 等路由器为 aarch64/arm64）。
    """
    import platform as _platform

    override = os.environ.get("ARCH_OVERRIDE", "").strip().lower()
    if override:
        return override
    machine = (_platform.machine() or "").lower()
    if machine in ("x86_64", "amd64"):
        return "amd64"
    if machine in ("aarch64", "arm64"):
        return "arm64"
    if machine.startswith("armv7") or machine.startswith("armv6"):
        return "arm"
    if machine.startswith("arm"):
        return "arm64"
    return machine or "amd64"


TARGET_ARCH = _target_arch()

# Docker Hub 认证信息
DOCKER_HUB_AUTH = {
    "auth_url": "https://auth.docker.io/token",
    "service": "registry.docker.io",
    "registry_url": "https://registry-1.docker.io",
}

# 已知注册表认证映射
REGISTRY_AUTH = {
    "ghcr.io": {
        "auth_url": "https://ghcr.io/token",
        "service": "ghcr.io",
        "registry_url": "https://ghcr.io",
    },
    "quay.io": {
        "auth_url": "https://quay.io/v2/auth",
        "service": "quay.io",
        "registry_url": "https://quay.io",
    },
    "gcr.io": {
        "auth_url": "https://gcr.io/v2/token",
        "service": "gcr.io",
        "registry_url": "https://gcr.io",
    },
}

# 已知 redirect 注册表
REDIRECT_REGISTRIES = {
    "lscr.io": "ghcr.io",
}

# 缓存：{image_name: {"has_update": bool|None, "error": str|None, "checked_at": str}}
_update_cache = {}

# Auth token 缓存
_token_cache = {}

# 中止标志（用于SSE流式检查时中止）
_abort_check = False

# 持久化DB路径（由main.py在启动时设置）
_db_path: Optional[str] = None


def init_db_path(db_path: str):
    """初始化数据库路径，用于持久化检查结果"""
    global _db_path
    _db_path = db_path


def _persist_result_to_db(container_name: str, image_name: str, result: dict):
    """将单条检查结果持久化到数据库（同步，非阻塞场景调用）"""
    if not _db_path:
        return
    try:
        import sqlite3
        hu = None
        if result.get("has_update") is True:
            hu = 1
        elif result.get("has_update") is False:
            hu = 0
        conn = sqlite3.connect(_db_path)
        conn.execute(
            """INSERT INTO update_check_results (container_name, image, has_update, error, checked_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(container_name) DO UPDATE SET
                 image=excluded.image, has_update=excluded.has_update,
                 error=excluded.error, checked_at=excluded.checked_at""",
            (container_name, image_name, hu, result.get("error"), result.get("checked_at", ""))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"持久化检查结果失败 {container_name}: {e}")


def request_abort():
    """请求中止当前正在进行的检查"""
    global _abort_check
    _abort_check = True


def recheck_container_update(client, container_name: str) -> dict:
    """更新完成后重新检查单个容器的镜像更新状态（清除缓存，强制刷新）"""
    try:
        c = client.containers.get(container_name)
    except Exception:
        return {}

    image_name = c.attrs.get("Config", {}).get("Image", "")
    if not image_name:
        return {}

    # 清除该镜像的缓存，确保重新检查
    if image_name in _update_cache:
        del _update_cache[image_name]

    result = check_image_update(client, image_name)
    result["checked_at"] = datetime.now().isoformat()
    _update_cache[image_name] = result
    _persist_result_to_db(container_name, image_name, result)
    return {container_name: result}


def recheck_compose_update(client, project_name: str) -> dict:
    """更新完成后重新检查 Compose 项目所有容器的镜像更新状态"""
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    results = {}
    for c in containers:
        image_name = c.attrs.get("Config", {}).get("Image", "")
        if not image_name:
            continue
        if image_name in _update_cache:
            del _update_cache[image_name]
        result = check_image_update(client, image_name)
        result["checked_at"] = datetime.now().isoformat()
        _update_cache[image_name] = result
        _persist_result_to_db(c.name, image_name, result)
        results[c.name] = result
    return results


def is_aborted():
    """检查是否已被请求中止"""
    return _abort_check


def reset_abort():
    """重置中止标志"""
    global _abort_check
    _abort_check = False


# ============ 镜像名称解析 ============

def parse_image_name(image_name: str) -> dict:
    """解析镜像名称为 registry/namespace/repo/tag"""
    registry = "registry-1.docker.io"
    tag = "latest"

    name = image_name.strip()

    if "@" in name:
        name, tag = name.split("@", 1)
    elif ":" in name:
        parts = name.split("/")
        last = parts[-1]
        if ":" in last:
            tag = last.split(":")[-1]
            name = name[: name.rfind(":")]

    if "/" in name:
        first = name.split("/")[0]
        if "." in first or ":" in first:
            registry = first
            name = name[len(first) + 1:]

    if "/" not in name:
        full_repo = f"library/{name}"
    else:
        full_repo = name

    return {
        "registry": registry,
        "full_repo": full_repo,
        "tag": tag,
    }


# ============ 策略1: Docker SDK inspect_distribution（推荐）============

def _get_remote_digest_via_sdk(client, image_name: str) -> tuple:
    """通过Docker SDK的inspect_distribution获取远程digest

    这是最佳方案：直接走Docker Daemon，自动继承代理/认证/加速器配置。
    与 docker pull 行为完全一致。

    返回: (digest: str|None, error: str|None)
    """
    try:
        # 尝试使用 get_registry_data（Docker API v1.30+）
        try:
            registry_data = client.images.get_registry_data(image_name)
            digest = registry_data.id  # 返回 Descriptor.digest
            if digest:
                logger.info(f"SDK获取远程digest成功: {image_name} -> {digest[:32]}...")
                return digest, None
        except Exception as e:
            error_msg = str(e)
            # 检查是否因为Docker API版本不支持
            if "minimum API version" in error_msg or "404" in error_msg:
                return None, f"SDK: API版本不支持({error_msg[:60]})"
            # 其他错误继续尝试下一种方式
            logger.debug(f"SDK get_registry_data失败: {e}")

        # 备选：直接调用底层API
        try:
            result = client.api.inspect_distribution(image_name)
            descriptor = result.get("Descriptor", {})
            digest = descriptor.get("digest")
            if digest:
                logger.info(f"SDK(inspect_distribution)获取远程digest成功: {image_name} -> {digest[:32]}...")
                return digest, None

            # 尝试从 Manifests 获取（multi-arch镜像）
            manifests = result.get("Manifests", [])
            if manifests:
                # 优先找本机架构（TARGET_ARCH，路由器上为 arm64）
                for m in manifests:
                    platform = m.get("platform", {})
                    if platform.get("architecture") == TARGET_ARCH and platform.get("os") == "linux":
                        digest = m.get("digest")
                        if digest:
                            return digest, None
                # 否则取第一个
                digest = manifests[0].get("digest")
                if digest:
                    return digest, None

            return None, "SDK: 无法从distribution数据提取digest"

        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg:
                return None, f"SDK: 镜像不存在(404)"
            if "401" in error_msg or "403" in error_msg:
                return None, f"SDK: 认证失败(需要登录)"
            return None, f"SDK: {error_msg[:80]}"

    except Exception as e:
        return None, f"SDK: {str(e)[:80]}"


# ============ 策略2: Docker CLI ============

def _get_remote_digest_via_cli(image_name: str) -> tuple:
    """使用Docker CLI的manifest inspect获取远程digest"""
    try:
        env = os.environ.copy()
        env["DOCKER_CLI_EXPERIMENTAL"] = "enabled"

        result = subprocess.run(
            ["docker", "manifest", "inspect", "--verbose", image_name],
            capture_output=True, text=True, timeout=CLI_TIMEOUT,
            env=env,
        )

        if result.returncode != 0:
            result = subprocess.run(
                ["docker", "manifest", "inspect", image_name],
                capture_output=True, text=True, timeout=CLI_TIMEOUT,
                env=env,
            )
            if result.returncode != 0:
                return None, f"CLI: {result.stderr.strip()[:60]}"

        output = result.stdout.strip()
        if not output:
            return None, "CLI: 空输出"

        manifest = json.loads(output)

        if "Descriptor" in manifest:
            descriptor = manifest["Descriptor"]
            digest = descriptor.get("digest")
            if digest:
                return digest, None

        manifest_data = manifest.get("Manifest", manifest)
        media_type = manifest_data.get("mediaType", "")

        if "manifest.list" in media_type or "image.index" in media_type:
            manifests = manifest_data.get("manifests", [])
            for m in manifests:
                platform = m.get("platform", {})
                if platform.get("architecture") == TARGET_ARCH and platform.get("os") == "linux":
                    digest = m.get("digest")
                    if digest:
                        return digest, None
            if manifests:
                digest = manifests[0].get("digest")
                if digest:
                    return digest, None

        return None, "CLI: 单manifest无法提取digest"

    except subprocess.TimeoutExpired:
        return None, "CLI: 命令超时"
    except json.JSONDecodeError:
        return None, "CLI: 输出解析失败"
    except FileNotFoundError:
        return None, "CLI: docker命令不可用"
    except Exception as e:
        return None, f"CLI: {str(e)[:60]}"


# ============ 策略3: Registry V2 API ============

def _load_docker_config() -> dict:
    """读取Docker客户端配置"""
    try:
        if DOCKER_CONFIG_PATH.exists():
            with open(DOCKER_CONFIG_PATH, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"无法读取Docker配置: {e}")
    return {}


def _get_proxies() -> dict:
    """从Docker配置和环境变量获取代理设置"""
    proxies = {}
    for key, env_key in [("http", "HTTP_PROXY"), ("https", "HTTPS_PROXY"), ("no", "NO_PROXY")]:
        val = os.environ.get(env_key) or os.environ.get(env_key.lower())
        if val:
            proxies[key] = val

    config = _load_docker_config()
    default_proxy = config.get("proxies", {}).get("default", {})
    if not proxies.get("http") and default_proxy.get("httpProxy"):
        proxies["http"] = default_proxy["httpProxy"]
    if not proxies.get("https") and default_proxy.get("httpsProxy"):
        proxies["https"] = default_proxy["httpsProxy"]
    if not proxies.get("no") and default_proxy.get("noProxy"):
        proxies["no"] = default_proxy["noProxy"]

    return proxies


def _get_requests_session() -> requests.Session:
    """创建配置了代理的requests Session"""
    session = requests.Session()
    proxies = _get_proxies()
    proxy_dict = {}
    if proxies.get("http"):
        proxy_dict["http"] = proxies["http"]
    if proxies.get("https"):
        proxy_dict["https"] = proxies["https"]
    if proxy_dict:
        session.proxies.update(proxy_dict)
    return session


def _get_auth_token(registry: str, full_repo: str) -> Optional[str]:
    """获取registry认证token"""
    if registry == "registry-1.docker.io":
        auth_url = DOCKER_HUB_AUTH["auth_url"]
        service = DOCKER_HUB_AUTH["service"]
    elif registry in REGISTRY_AUTH:
        auth_url = REGISTRY_AUTH[registry]["auth_url"]
        service = REGISTRY_AUTH[registry]["service"]
    else:
        return None

    scope = f"repository:{full_repo}:pull"
    cache_key = (registry, full_repo)

    if cache_key in _token_cache:
        cached_token, cached_time = _token_cache[cache_key]
        if (datetime.now() - cached_time).total_seconds() < 300:
            return cached_token

    session = _get_requests_session()
    try:
        resp = session.get(auth_url, params={"service": service, "scope": scope}, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            token = resp.json().get("token") or resp.json().get("access_token")
            if token:
                _token_cache[cache_key] = (token, datetime.now())
                return token
    except Exception as e:
        logger.warning(f"获取 {registry} token 异常: {e}")

    return None


def _resolve_registry_url(registry: str) -> tuple:
    """解析注册表实际URL"""
    if registry in REDIRECT_REGISTRIES:
        actual = REDIRECT_REGISTRIES[registry]
        if actual in REGISTRY_AUTH:
            return actual, REGISTRY_AUTH[actual]["registry_url"]
        return actual, f"https://{actual}"

    if registry == "registry-1.docker.io":
        return registry, DOCKER_HUB_AUTH["registry_url"]
    if registry in REGISTRY_AUTH:
        return registry, REGISTRY_AUTH[registry]["registry_url"]

    return registry, f"https://{registry}"


def _get_remote_digest_via_api(registry: str, full_repo: str, tag: str) -> tuple:
    """通过Registry V2 API获取远程digest（最后兜底）"""
    actual_registry, registry_url = _resolve_registry_url(registry)
    token = _get_auth_token(actual_registry, full_repo)

    headers = {
        "Accept": (
            "application/vnd.docker.distribution.manifest.v2+json, "
            "application/vnd.docker.distribution.manifest.list.v2+json, "
            "application/vnd.oci.image.manifest.v1+json, "
            "application/vnd.oci.image.index.v1+json"
        ),
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    config = _load_docker_config()
    auths = config.get("auths", {})
    for reg_key, auth_data in auths.items():
        if actual_registry in reg_key and isinstance(auth_data, dict) and auth_data.get("auth"):
            headers["Authorization"] = f"Basic {auth_data['auth']}"
            break

    url = f"{registry_url}/v2/{full_repo}/manifests/{tag}"
    session = _get_requests_session()

    # HEAD
    try:
        resp = session.head(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            digest = resp.headers.get("Docker-Content-Digest")
            if digest:
                return digest, None
    except Exception:
        pass

    # GET
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            digest = resp.headers.get("Docker-Content-Digest")
            if digest:
                return digest, None

            body = resp.json()
            if body.get("mediaType") in (
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.oci.image.index.v1+json",
            ):
                manifests = body.get("manifests", [])
                for m in manifests:
                    platform = m.get("platform", {})
                    if platform.get("architecture") == TARGET_ARCH and platform.get("os") == "linux":
                        return m.get("digest"), None
                if manifests:
                    return manifests[0].get("digest"), None

            config_desc = body.get("config", {})
            if config_desc and config_desc.get("digest"):
                return config_desc["digest"], None
        else:
            return None, f"API: HTTP {resp.status_code}"
    except requests.exceptions.SSLError:
        return None, "API: SSL证书验证失败"
    except requests.exceptions.ProxyError:
        return None, "API: 代理连接失败"
    except requests.exceptions.ConnectTimeout:
        return None, "API: 连接超时"
    except requests.exceptions.ConnectionError as e:
        return None, "API: 网络连接失败"
    except Exception as e:
        return None, f"API: {str(e)[:60]}"

    return None, "API: 无法获取远程digest"


# ============ 本地镜像信息 ============

def _get_local_digest(client, image_name: str) -> tuple:
    """获取本地镜像的RepoDigest

    返回: (digest: str|None, error: str|None)
    """
    image = None

    # 尝试多种方式查找本地镜像
    lookup_names = [image_name]
    if ":" in image_name:
        lookup_names.append(image_name.split(":")[0])
    if "@" in image_name:
        lookup_names.append(image_name.split("@")[0])

    for lookup_name in lookup_names:
        try:
            image = client.images.get(lookup_name)
            break
        except Exception:
            continue

    if image is None:
        # 尝试通过列表搜索
        try:
            all_images = client.images.list()
            for img in all_images:
                repo_tags = img.attrs.get("RepoTags", []) or []
                repo_digests = img.attrs.get("RepoDigests", []) or []
                if image_name in repo_tags or any(image_name.split(":")[0] in d for d in repo_digests):
                    image = img
                    break
        except Exception:
            pass

    if image is None:
        return None, "找不到本地镜像"

    # 优先使用 RepoDigests
    repo_digests = image.attrs.get("RepoDigests", [])
    if repo_digests:
        for rd in repo_digests:
            if "@" in rd:
                digest = rd.split("@", 1)[1]
                logger.debug(f"本地 RepoDigest: {image_name} -> {digest[:32]}...")
                return digest, None

    return None, "本地镜像无RepoDigest(可能为本地构建)"


# ============ 公共接口 ============

def check_image_update(client, image_name: str) -> dict:
    """检查单个镜像是否有远程更新

    策略: Docker SDK优先 -> Docker CLI备选 -> Registry V2 API兜底

    返回: {"image": str, "has_update": bool|None, "error": str|None}
    """
    result = {"image": image_name, "has_update": None, "error": None}

    try:
        parsed = parse_image_name(image_name)

        # 获取本地 digest
        local_digest, local_error = _get_local_digest(client, image_name)
        if not local_digest:
            result["error"] = local_error or "本地镜像无RepoDigest"
            return result

        remote_digest = None
        errors = []

        # 策略1: Docker SDK inspect_distribution（最可靠，走Daemon配置）
        sdk_digest, sdk_error = _get_remote_digest_via_sdk(client, image_name)
        if sdk_digest:
            remote_digest = sdk_digest
            logger.debug(f"使用Docker SDK获取远程digest: {image_name}")
        else:
            errors.append(sdk_error or "SDK失败")
            logger.debug(f"Docker SDK失败: {sdk_error}")

            # 策略2: Docker CLI
            cli_digest, cli_error = _get_remote_digest_via_cli(image_name)
            if cli_digest:
                remote_digest = cli_digest
                logger.debug(f"使用Docker CLI获取远程digest: {image_name}")
            else:
                errors.append(cli_error or "CLI失败")
                logger.debug(f"Docker CLI失败: {cli_error}")

                # 策略3: Registry V2 API
                api_digest, api_error = _get_remote_digest_via_api(
                    parsed["registry"], parsed["full_repo"], parsed["tag"]
                )
                if api_digest:
                    remote_digest = api_digest
                    logger.debug(f"使用Registry API获取远程digest: {image_name}")
                else:
                    errors.append(api_error or "API失败")
                    logger.debug(f"Registry API失败: {api_error}")

        if remote_digest is None:
            result["error"] = " | ".join(errors) if errors else "无法获取远程镜像信息"
            return result

        # 精确比较 digest
        if local_digest == remote_digest:
            result["has_update"] = False
            logger.info(f"[最新] {image_name} (digest一致)")
        else:
            result["has_update"] = True
            logger.info(f"[有更新] {image_name} (本地: {local_digest[:16]}... 远程: {remote_digest[:16]}...)")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"检查镜像更新异常 {image_name}: {e}")

    return result


def check_all_updates(client, settings: dict = None) -> dict:
    """检查所有容器的镜像更新

    Args:
        client: Docker客户端
        settings: 容器更新检查偏好 {container_name: check_enabled}，为None则全部检查

    返回: {container_name: {"image": str, "has_update": bool|None, "error": str|None}}
    """
    global _abort_check
    _abort_check = False

    results = {}

    try:
        containers = client.containers.list(all=True)
    except Exception as e:
        logger.error(f"获取容器列表失败: {e}")
        return results

    for c in containers:
        if _abort_check:
            logger.info("检查已被中止")
            break

        # 跳过关闭更新检查的容器（默认启用）
        if settings is not None and settings.get(c.name, 1) == 0:
            logger.debug(f"跳过已关闭检查的容器: {c.name}")
            continue

        image_name = c.attrs.get("Config", {}).get("Image", "")
        if not image_name:
            continue

        # 检查缓存（30分钟内不重复检查同一镜像）
        cache_key = image_name
        if cache_key in _update_cache:
            cached = _update_cache[cache_key]
            checked_at = cached.get("checked_at")
            if checked_at:
                try:
                    elapsed = (datetime.now() - datetime.fromisoformat(checked_at)).total_seconds()
                    if elapsed < 1800:
                        results[c.name] = cached
                        continue
                except Exception:
                    pass

        if _abort_check:
            break

        logger.info(f"检查镜像更新: {image_name}")
        result = check_image_update(client, image_name)
        results[c.name] = result

        # 写入缓存
        result["checked_at"] = datetime.now().isoformat()
        _update_cache[cache_key] = result

        # 持久化到数据库
        _persist_result_to_db(c.name, image_name, result)

    return results
