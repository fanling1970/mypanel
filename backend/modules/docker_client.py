"""Docker客户端模块 - 封装Docker API操作"""

import docker
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from modules import compose_project

logger = logging.getLogger("docker_client")

# 自身容器名（兼容历史部署的旧容器名 + 本项目私有名，识别自己用）
SELF_CONTAINER_NAMES = {"docker-scheduler", "dockshift", "docker-butler", "mypanel"}


def is_self_name(name: Optional[str]) -> bool:
    """判断容器名是否为自身"""
    return bool(name) and name in SELF_CONTAINER_NAMES


def get_self_container(client=None):
    """获取自身容器对象（兼容历史容器名），找不到返回 None"""
    client = client or get_client()
    for c in client.containers.list(all=True):
        if c.name in SELF_CONTAINER_NAMES:
            return c
    return None


def get_self_image() -> str:
    """获取自身镜像名（当前部署镜像完整名称）"""
    c = get_self_container()
    if not c:
        return ""
    return c.attrs.get("Config", {}).get("Image", "")


def get_self_compose_info() -> dict:
    """获取自身 compose 部署信息（项目名/文件/目录，来自容器 label）"""
    c = get_self_container()
    if not c:
        return {}
    labels = c.attrs.get("Config", {}).get("Labels") or {}
    config_files = labels.get("com.docker.compose.project.config_files", "")
    files = [f.strip() for f in config_files.split(",") if f.strip()] if config_files else []
    compose_file = files[0] if files else ""
    return {
        "project": labels.get("com.docker.compose.project", ""),
        "config_files": config_files,
        "compose_file": compose_file,
        "compose_dir": os.path.dirname(compose_file) if compose_file else "",
    }


def get_self_info() -> Optional[dict]:
    """自身容器完整信息（面板展示用）"""
    c = get_self_container()
    if not c:
        return None
    try:
        image_tags = c.image.tags if c.image and c.image.tags else []
        image_name = image_tags[0] if image_tags else c.attrs.get("Config", {}).get("Image", "")
    except Exception:
        image_name = c.attrs.get("Config", {}).get("Image", "")
    compose = get_self_compose_info()
    info = {
        "name": c.name,
        "id": c.short_id,
        "full_id": c.id,
        "image": image_name,
        "status": c.status,
        "created": c.attrs.get("Created", ""),
        "compose": compose,
    }
    started_at = c.attrs.get("State", {}).get("StartedAt", "")
    if started_at and started_at != "0001-01-01T00:00:00Z":
        info["started_at"] = started_at
    return info

# 全局Docker客户端
_client: Optional[docker.DockerClient] = None

# 全局更新进度字典: {target_key: progress_dict}
_update_progress: dict = {}


def get_update_progress(target_key: str):
    """获取更新进度"""
    return _update_progress.get(target_key)


def clear_update_progress(target_key: str):
    """清除更新进度"""
    _update_progress.pop(target_key, None)


def is_updating(target_key: str) -> bool:
    """检查指定目标是否正在更新中"""
    p = _update_progress.get(target_key)
    return p is not None and not p.get("done")


def _init_progress(target_key: str, total_steps: int, tag: str):
    """初始化进度跟踪"""
    _update_progress[target_key] = {
        "phase": "init",
        "message": "准备更新...",
        "tag": tag,
        "current_image": "",
        "layers": {},
        "current_container": "",
        "current_step": 0,
        "total_steps": total_steps,
        "image_total_size": 0,       # 当前镜像总大小（字节）
        "image_downloaded": 0,       # 当前镜像已下载（字节）
        "image_speed": 0,            # 当前下载速率（字节/秒）
        "image_speed_samples": [],   # 速率采样 [(timestamp, downloaded)]
        "completed_images": [],      # 已完成的镜像列表 [{name, size, downloaded}]
        "done": False,
        "success": False,
        "error": None,
        "timestamp": datetime.now().isoformat(),
    }


def _set_progress(target_key: str, **kwargs):
    """更新进度数据"""
    p = _update_progress.get(target_key)
    if p:
        p.update(kwargs)
        p["timestamp"] = datetime.now().isoformat()


def _compute_download_stats(target_key: str):
    """根据各层进度计算汇总下载统计（总大小/已下载/速率）"""
    p = _update_progress.get(target_key)
    if not p:
        return
    layers = p.get("layers", {})
    total_size = 0
    downloaded = 0
    for layer in layers.values():
        layer_current = layer.get("current", 0)
        layer_total = layer.get("total", 0)
        # 如果某层有 current 但没有 total，用 current 作为 total 估值
        if layer_total == 0 and layer_current > 0:
            layer_total = layer_current
        total_size += layer_total
        downloaded += layer_current

    # 已下载不应超过总大小
    if total_size > 0 and downloaded > total_size:
        downloaded = total_size

    p["image_total_size"] = total_size
    p["image_downloaded"] = downloaded

    # 速率计算：用滑动窗口（保留最近5个采样点）
    now = time.time()
    samples = p.get("image_speed_samples", [])
    samples.append((now, downloaded))
    if len(samples) > 6:
        samples = samples[-6:]
    p["image_speed_samples"] = samples

    if len(samples) >= 2:
        oldest_time, oldest_size = samples[0]
        dt = now - oldest_time
        if dt > 0:
            p["image_speed"] = int((downloaded - oldest_size) / dt)
        else:
            p["image_speed"] = 0
    else:
        p["image_speed"] = 0


def _reset_image_stats(target_key: str, image_name: str):
    """切换到新镜像时，保存旧镜像统计并重置"""
    p = _update_progress.get(target_key)
    if not p:
        return
    old_image = p.get("current_image", "")
    if old_image and old_image != image_name:
        completed = p.get("completed_images", [])
        completed.append({
            "name": old_image,
            "size": p.get("image_total_size", 0),
            "downloaded": p.get("image_downloaded", 0),
        })
        p["completed_images"] = completed
    # 重置当前镜像统计
    p["current_image"] = image_name
    p["layers"] = {}
    p["image_total_size"] = 0
    p["image_downloaded"] = 0
    p["image_speed"] = 0
    p["image_speed_samples"] = []


def _parse_pull_chunk(target_key: str, chunk):
    """解析 Docker 镜像拉取进度块"""
    if not isinstance(chunk, dict):
        return
    p = _update_progress.get(target_key)
    if not p:
        return

    layers = p.get("layers", {})
    layer_id = chunk.get("id", "")
    status = chunk.get("status", "")
    progress_detail = chunk.get("progressDetail", {})

    if layer_id:
        layer_info = layers.get(layer_id, {})
        layer_info["status"] = status
        if "current" in progress_detail:
            layer_info["current"] = progress_detail["current"]
        if "total" in progress_detail:
            layer_info["total"] = progress_detail["total"]
        layers[layer_id] = layer_info
        _set_progress(target_key, layers=layers, message=f"{layer_id}: {status}")
        _compute_download_stats(target_key)
    elif status:
        # "Already exists" 或 "Pull complete" 等无层ID的状态
        if status in ("Already exists", "Pull complete", "Download complete"):
            _compute_download_stats(target_key)
        _set_progress(target_key, message=status)


def get_client() -> docker.DockerClient:
    """获取Docker客户端单例"""
    global _client
    if _client is None:
        _client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    return _client


def _parse_container_ports(c) -> list[str]:
    """解析容器端口映射为 ['0.0.0.0:8080->80/tcp', ...]（仅 IPv4，过滤 IPv6 双栈重复项）"""
    ports = []
    try:
        # 第一来源：NetworkSettings.Ports（运行时实际生效的绑定）
        net_ports = c.attrs.get("NetworkSettings", {}).get("Ports") or {}
        if net_ports:
            for port_key, bindings in net_ports.items():
                if bindings:
                    for b in bindings:
                        host_ip = b.get("HostIp", "0.0.0.0") or "0.0.0.0"
                        # 跳过 IPv6 绑定（:::8080），只留 IPv4
                        if host_ip == "::" or host_ip.startswith("::"):
                            continue
                        ports.append(f"{host_ip}:{b.get('HostPort', '')}->{port_key}")
        # 第二来源（兜底）：HostConfig.PortBindings（配置层，NetworkSettings 为空时用）
        # 结构 {"25600/tcp": [{"HostIp":"","HostPort":"64323"}]}
        if not ports:
            port_bindings = c.attrs.get("HostConfig", {}).get("PortBindings") or {}
            for port_key, bindings in port_bindings.items():
                if bindings:
                    for b in bindings:
                        host_ip = b.get("HostIp", "") or "0.0.0.0"
                        if host_ip == "::" or host_ip.startswith("::"):
                            continue
                        host_port = b.get("HostPort", "")
                        if host_port:
                            ports.append(f"{host_ip}:{host_port}->{port_key}")
    except Exception:
        pass
    return ports


def list_containers(all: bool = True) -> list[dict]:
    """获取容器列表

    Args:
        all: 是否包含已停止的容器
    """
    client = get_client()
    containers = client.containers.list(all=all)

    result = []
    for c in containers:
        try:
            is_self = c.name in SELF_CONTAINER_NAMES
            # State 统一取字典形态，避免字符串/字典差异导致后续 .get 报错
            _st_dict = c.attrs.get("State") if isinstance(c.attrs.get("State"), dict) else {}

            # 解析端口映射（仅 IPv4，过滤 IPv6 双栈重复项）
            ports = _parse_container_ports(c)

            # 获取镜像名：直接读列表已返回的 attrs
            # 原先 c.image.tags 会为每个容器额外发起一次镜像 inspect，
            # 17 个容器就是 17 次 API 往返，是首屏变慢的主要来源之一
            image_name = ""
            try:
                image_name = (c.attrs.get("Config") or {}).get("Image") or ""
                if not image_name:
                    _iid = c.attrs.get("Image") or ""
                    image_name = _iid[7:19] if _iid.startswith("sha256:") else (_iid[:12] or "unknown")
            except Exception:
                image_name = "unknown"

            info = {
                "id": c.short_id,
                "full_id": c.id,
                "name": c.name,
                "is_self": is_self,
                "image": image_name,
                "status": c.status,
                "state": _st_dict.get("Status") or (c.status or "unknown"),
                "ports": ports,
                "created": c.attrs.get("Created", ""),
                "networks": list(c.attrs.get("NetworkSettings", {}).get("Networks", {}).keys()),
            }

            # 运行中的容器：计算运行时长（只读 attrs，替代原先每容器一次 inspect）
            if c.status == "running":
                started_at = _st_dict.get("StartedAt", "") or ""
                if not started_at or started_at == "0001-01-01T00:00:00Z":
                    try:
                        _created = int(c.attrs.get("Created", 0) or 0)
                        if _created:
                            started_at = datetime.fromtimestamp(_created, timezone.utc).isoformat()
                    except Exception:
                        started_at = ""
                if started_at:
                    info["started_at"] = started_at
                    try:
                        _dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                        info["uptime_seconds"] = max(
                            int((datetime.now(timezone.utc) - _dt).total_seconds()), 0
                        )
                    except Exception:
                        pass

            # IP 与健康状态同样只读 attrs
            _ips = []
            for _n, _d in ((c.attrs.get("NetworkSettings") or {}).get("Networks") or {}).items():
                _ip = (_d or {}).get("IPAddress", "")
                if _ip:
                    _ips.append(f"{_n}:{_ip}")
            info["ip_addresses"] = _ips
            _h = _st_dict.get("Health")
            info["health"] = _h.get("Status", "unknown") if _h else None

            result.append(info)
        except Exception as e:
            # 单个容器解析失败不影响整体列表
            logger.warning(f"解析容器 {getattr(c, 'name', 'unknown')} 失败: {e}")
            continue

    # 按状态排序：运行中 > 已停止 > 其他
    status_order = {"running": 0, "exited": 1, "paused": 2, "restarting": 3, "dead": 4, "created": 5}
    result.sort(key=lambda x: status_order.get(x["status"], 6))

    return result


def get_container(container_id: str) -> Optional[dict]:
    """获取单个容器完整信息"""
    client = get_client()
    try:
        c = client.containers.get(container_id)
    except docker.errors.NotFound:
        return None

    # 解析端口映射（仅 IPv4，过滤 IPv6 双栈重复项）
    ports = _parse_container_ports(c)

    # 获取镜像名
    image_name = ""
    image_tags = c.image.tags if c.image and c.image.tags else []
    if image_tags:
        image_name = image_tags[0]
    else:
        image_name = c.image.id[:12] if c.image else ""

    info = {
        "id": c.short_id,
        "full_id": c.id,
        "name": c.name,
        "image": image_name,
        "status": c.status,
        "state": c.attrs.get("State", {}).get("Status", "unknown"),
        "ports": ports,
        "created": c.attrs.get("Created", ""),
        "networks": list(c.attrs.get("NetworkSettings", {}).get("Networks", {}).keys()),
    }

    if c.status == "running":
        started_at = c.attrs.get("State", {}).get("StartedAt", "")
        if started_at and started_at != "0001-01-01T00:00:00Z":
            info["started_at"] = started_at

    return info


def start_container(container_id: str) -> dict:
    """启动容器"""
    client = get_client()
    try:
        c = client.containers.get(container_id)
        if is_self_name(c.name):
            return {"success": False, "message": "不能对自己执行启动操作，请使用面板操作"}
        c.start()
        return {"success": True, "message": f"容器 {c.name} 启动成功"}
    except docker.errors.NotFound:
        return {"success": False, "message": f"容器 {container_id} 不存在"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"启动失败: {str(e)}"}


def stop_container(container_id: str) -> dict:
    """停止容器"""
    client = get_client()
    try:
        c = client.containers.get(container_id)
        if is_self_name(c.name):
            return {"success": False, "message": "不能停止自己，否则面板将无法管理容器（可用'更新自己'重启面板）"}
        c.stop(timeout=10)
        return {"success": True, "message": f"容器 {c.name} 停止成功"}
    except docker.errors.NotFound:
        return {"success": False, "message": f"容器 {container_id} 不存在"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"停止失败: {str(e)}"}


def restart_container(container_id: str) -> dict:
    """重启容器"""
    client = get_client()
    try:
        c = client.containers.get(container_id)
        if is_self_name(c.name):
            # 重启自己：由 daemon 后台执行（重启瞬间本进程会被杀掉）
            return restart_self()
        c.restart(timeout=10)
        return {"success": True, "message": f"容器 {c.name} 重启成功"}
    except docker.errors.NotFound:
        return {"success": False, "message": f"容器 {container_id} 不存在"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"重启失败: {str(e)}"}


def remove_container(container_id: str, force: bool = False) -> dict:
    """删除容器

    Args:
        container_id: 容器ID或名称
        force: 是否强制删除运行中的容器
    """
    client = get_client()
    try:
        c = client.containers.get(container_id)
        if is_self_name(c.name):
            return {"success": False, "message": "不能删除自己，删除后面板将无法恢复"}
        name = c.name
        c.remove(force=force)
        return {"success": True, "message": f"容器 {name} 已删除"}
    except docker.errors.NotFound:
        return {"success": False, "message": f"容器 {container_id} 不存在"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"删除失败: {str(e)}"}


def create_container(config: dict) -> dict:
    """创建并启动容器

    config 字段：
        name: 容器名（可选，Docker自动生成）
        image: 镜像名（必填）
        command: 启动命令（可选）
        ports: 端口映射字符串列表，如 ["8080:80", "127.0.0.1:8080:80", "53:53/udp"]
        env: 环境变量字符串列表，如 ["TZ=Asia/Shanghai"]
        volumes: 卷挂载字符串列表，如 ["/vol1/data:/data", "/vol1/data2:/data2:ro"]
        network_mode: bridge / host / none / 自定义网络名
        restart_policy: no / always / unless-stopped / on-failure
        privileged: 是否特权模式
        auto_start: 创建后是否启动
    """
    client = get_client()
    try:
        image = str(config.get("image", "")).strip()
        if not image:
            return {"success": False, "message": "镜像名不能为空"}

        # 解析端口映射: "8080:80" / "127.0.0.1:8080:80" / "53:53/udp"
        ports = {}
        for item in config.get("ports", []) or []:
            item = str(item).strip()
            if not item:
                continue
            parts = item.split(":")
            if len(parts) == 2:
                host_ip, host_port, container_part = None, parts[0], parts[1]
            elif len(parts) == 3:
                host_ip, host_port, container_part = parts
            else:
                return {"success": False, "message": f"端口映射格式错误: {item}（示例: 8080:80 或 127.0.0.1:8080:80）"}
            if "/" not in container_part:
                container_part += "/tcp"
            try:
                if host_ip:
                    ports[container_part] = (host_ip, int(host_port))
                else:
                    ports[container_part] = int(host_port)
            except ValueError:
                return {"success": False, "message": f"端口映射格式错误: {item}（端口必须是数字）"}

        # 解析环境变量: "KEY=VALUE"
        environment = []
        for item in config.get("env", []) or []:
            item = str(item).strip()
            if item:
                environment.append(item)

        # 解析卷挂载: "/host:/container" 或 "/host:/container:ro"
        volumes = {}
        for item in config.get("volumes", []) or []:
            item = str(item).strip()
            if not item:
                continue
            parts = item.split(":")
            if len(parts) == 2:
                host_path, container_path, mode = parts[0], parts[1], "rw"
            elif len(parts) == 3:
                host_path, container_path, mode = parts
            else:
                return {"success": False, "message": f"卷挂载格式错误: {item}（示例: /vol1/data:/data）"}
            if not host_path or not container_path:
                return {"success": False, "message": f"卷挂载格式错误: {item}（示例: /vol1/data:/data）"}
            volumes[host_path] = {"bind": container_path, "mode": mode}

        network_mode = str(config.get("network_mode", "bridge")).strip() or "bridge"
        restart_policy = str(config.get("restart_policy", "no")).strip() or "no"

        c = client.containers.create(
            image=image,
            name=str(config.get("name", "")).strip() or None,
            command=str(config.get("command", "")).strip() or None,
            ports=ports or None,
            environment=environment or None,
            volumes=volumes or None,
            network_mode=network_mode,
            restart_policy={"Name": restart_policy},
            privileged=bool(config.get("privileged", False)),
            detach=True,
        )
        if config.get("auto_start", True):
            c.start()
            return {"success": True, "message": f"容器 {c.name} 创建并启动成功"}
        return {"success": True, "message": f"容器 {c.name} 创建成功（未启动）"}
    except docker.errors.ImageNotFound:
        return {"success": False, "message": f"镜像不存在: {config.get('image', '')}，请先到镜像管理拉取或确认镜像名正确"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"创建失败: {str(e)}"}


def update_container(container_id: str, progress_key: str = None) -> dict:
    """更新容器：拉取最新镜像，用相同配置重建容器

    Args:
        container_id: 容器ID
        progress_key: 进度跟踪key，传入则启用进度记录（传入容器ID即可）
    """
    client = get_client()
    if progress_key:
        _init_progress(progress_key, total_steps=3, tag="container")
    try:
        c = client.containers.get(container_id)
        name = c.name
        if is_self_name(name):
            if progress_key:
                _set_progress(progress_key, phase="error", done=True, success=False,
                               error="不能对自己执行容器级更新（会中断面板），请使用'更新自己'功能",
                               message="不能对自己执行容器级更新，请使用'更新自己'功能")
            return {"success": False, "message": "不能对自己执行容器级更新（会中断面板），请使用'更新自己'功能"}
        image_name = c.attrs.get("Config", {}).get("Image", "")

        # 保存原始配置
        original_config = {
            "name": name,
            "image": image_name,
            "command": c.attrs.get("Config", {}).get("Cmd"),
            "entrypoint": c.attrs.get("Config", {}).get("Entrypoint"),
            "environment": c.attrs.get("Config", {}).get("Env"),
            "labels": c.attrs.get("Config", {}).get("Labels"),
            "ports": c.attrs.get("HostConfig", {}).get("PortBindings"),
            "volumes": c.attrs.get("HostConfig", {}).get("Binds"),
            "network_mode": c.attrs.get("HostConfig", {}).get("NetworkMode"),
            "restart_policy": c.attrs.get("HostConfig", {}).get("RestartPolicy"),
            "privileged": c.attrs.get("HostConfig", {}).get("Privileged", False),
            "cap_add": c.attrs.get("HostConfig", {}).get("CapAdd"),
            "cap_drop": c.attrs.get("HostConfig", {}).get("CapDrop"),
            "devices": c.attrs.get("HostConfig", {}).get("Devices"),
            "extra_hosts": c.attrs.get("HostConfig", {}).get("ExtraHosts"),
            "log_config": c.attrs.get("HostConfig", {}).get("LogConfig"),
            "healthcheck": c.attrs.get("Config", {}).get("Healthcheck"),
            "hostname": c.attrs.get("Config", {}).get("Hostname"),
            "domainname": c.attrs.get("Config", {}).get("Domainname"),
            "user": c.attrs.get("Config", {}).get("User"),
            "working_dir": c.attrs.get("Config", {}).get("WorkingDir"),
            "stop_signal": c.attrs.get("Config", {}).get("StopSignal"),
        }

        # 拉取最新镜像（带进度）
        if image_name:
            if progress_key:
                _reset_image_stats(progress_key, image_name)
                _set_progress(progress_key, phase="pulling",
                               current_step=1, message=f"正在拉取镜像 {image_name}...")
            try:
                if progress_key:
                    for chunk in client.api.pull(image_name, stream=True, decode=True):
                        _parse_pull_chunk(progress_key, chunk)
                else:
                    client.images.pull(image_name)
            except Exception as e:
                logger.warning(f"拉取镜像 {image_name} 失败: {e}，使用现有镜像继续更新")
                if progress_key:
                    _set_progress(progress_key, message=f"镜像拉取失败: {e}，使用现有镜像继续")

        # 停止并移除旧容器
        if progress_key:
            _set_progress(progress_key, phase="stopping", current_step=2,
                           message=f"正在停止容器 {name}...")
        c.stop(timeout=10)
        if progress_key:
            _set_progress(progress_key, phase="removing", message="正在移除旧容器...")
        c.remove()

        # 用保存的配置重建容器
        run_kwargs = {
            "name": original_config["name"],
            "image": original_config["image"],
            "detach": True,
        }

        # 只添加有值的配置项
        if original_config["command"]:
            run_kwargs["command"] = original_config["command"]
        if original_config["entrypoint"]:
            run_kwargs["entrypoint"] = original_config["entrypoint"]
        if original_config["environment"]:
            run_kwargs["environment"] = original_config["environment"]
        if original_config["labels"]:
            run_kwargs["labels"] = original_config["labels"]
        if original_config["ports"]:
            run_kwargs["ports"] = original_config["ports"]
        if original_config["volumes"]:
            run_kwargs["volumes"] = original_config["volumes"]
        if original_config["network_mode"]:
            run_kwargs["network_mode"] = original_config["network_mode"]
        if original_config["restart_policy"]:
            run_kwargs["restart_policy"] = original_config["restart_policy"]
        if original_config["privileged"]:
            run_kwargs["privileged"] = original_config["privileged"]
        if original_config["cap_add"]:
            run_kwargs["cap_add"] = original_config["cap_add"]
        if original_config["cap_drop"]:
            run_kwargs["cap_drop"] = original_config["cap_drop"]
        if original_config["devices"]:
            run_kwargs["devices"] = original_config["devices"]
        if original_config["extra_hosts"]:
            run_kwargs["extra_hosts"] = original_config["extra_hosts"]
        if original_config["hostname"]:
            run_kwargs["hostname"] = original_config["hostname"]
        if original_config["user"]:
            run_kwargs["user"] = original_config["user"]
        if original_config["working_dir"]:
            run_kwargs["working_dir"] = original_config["working_dir"]
        if original_config["stop_signal"]:
            run_kwargs["stop_signal"] = original_config["stop_signal"]

        if progress_key:
            _set_progress(progress_key, phase="creating", current_step=3,
                           message=f"正在重建容器 {name}...")
        new_container = client.containers.run(**run_kwargs)

        if progress_key:
            _set_progress(progress_key, phase="done", done=True, success=True,
                           message=f"容器 {name} 更新成功，已使用最新镜像重建")
        return {"success": True, "message": f"容器 {name} 更新成功，已使用最新镜像重建"}

    except docker.errors.NotFound:
        if progress_key:
            _set_progress(progress_key, phase="error", done=True, success=False,
                           error=f"容器 {container_id} 不存在",
                           message=f"容器 {container_id} 不存在")
        return {"success": False, "message": f"容器 {container_id} 不存在"}
    except docker.errors.APIError as e:
        if progress_key:
            _set_progress(progress_key, phase="error", done=True, success=False,
                           error=str(e), message=f"更新失败: {str(e)}")
        return {"success": False, "message": f"更新失败: {str(e)}"}
    except Exception as e:
        if progress_key:
            _set_progress(progress_key, phase="error", done=True, success=False,
                           error=str(e), message=f"更新失败: {str(e)}")
        return {"success": False, "message": f"更新失败: {str(e)}"}


def test_connection() -> dict:
    """测试Docker连接"""
    try:
        client = get_client()
        info = client.info()
        return {
            "success": True,
            "docker_version": info.get("ServerVersion", "unknown"),
            "containers": info.get("Containers", 0),
            "containers_running": info.get("ContainersRunning", 0),
            "containers_stopped": info.get("ContainersStopped", 0),
        }
    except Exception as e:
        return {"success": False, "message": str(e)}


def get_container_stats(container_id: str, container=None) -> Optional[dict]:
    """获取容器实时统计信息（CPU/内存/网络/磁盘）

    可传入已获取的 container 对象，省掉一次 containers.get() 往返。
    """
    if container is None:
        client = get_client()
        try:
            container = client.containers.get(container_id)
        except docker.errors.NotFound:
            return None
    c = container

    if c.status != "running":
        return {
            "cpu_percent": 0, "memory_usage": 0, "memory_limit": 0,
            "memory_percent": 0, "network_rx": 0, "network_tx": 0,
            "block_read": 0, "block_write": 0, "pids": 0,
        }

    try:
        stats = c.stats(stream=False)
    except Exception as e:
        logger.error(f"获取容器统计失败: {e}")
        return None

    # CPU
    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    system_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
    cpu_count = len(stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [])) or stats["cpu_stats"].get("online_cpus", 1)
    cpu_percent = round((cpu_delta / system_delta) * cpu_count * 100, 2) if system_delta > 0 else 0

    # 内存
    memory_usage = stats["memory_stats"].get("usage", 0)
    memory_limit = stats["memory_stats"].get("limit", 0)
    memory_percent = round(memory_usage / memory_limit * 100, 2) if memory_limit > 0 else 0

    # 网络
    network_rx = 0
    network_tx = 0
    networks = stats.get("networks", {})
    for net_name, net_data in networks.items():
        network_rx += net_data.get("rx_bytes", 0)
        network_tx += net_data.get("tx_bytes", 0)

    # 磁盘I/O
    block_read = 0
    block_write = 0
    blkio_stats = stats.get("blkio_stats", {})
    io_service_bytes = blkio_stats.get("io_service_bytes_recursive", [])
    for entry in io_service_bytes or []:
        if entry.get("op") == "read":
            block_read += entry.get("value", 0)
        elif entry.get("op") == "write":
            block_write += entry.get("value", 0)

    # PIDs
    pids = stats.get("pids_stats", {}).get("current", 0)

    return {
        "cpu_percent": cpu_percent,
        "memory_usage": memory_usage,
        "memory_limit": memory_limit,
        "memory_percent": memory_percent,
        "network_rx": network_rx,
        "network_tx": network_tx,
        "block_read": block_read,
        "block_write": block_write,
        "pids": pids,
    }


def enrich_container_info(container_info: dict) -> dict:
    """为容器信息补充运行时长、IP、健康状态等字段"""
    client = get_client()
    # 优先用 full_id 查询，更可靠
    lookup_id = container_info.get("full_id") or container_info.get("id", "")
    try:
        c = client.containers.get(lookup_id)
    except Exception:
        return container_info

    # 运行时长
    started_at = c.attrs.get("State", {}).get("StartedAt", "")
    if started_at and started_at != "0001-01-01T00:00:00Z" and c.status == "running":
        try:
            start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            uptime_seconds = int((datetime.now(timezone.utc) - start_dt).total_seconds())
            container_info["uptime_seconds"] = max(uptime_seconds, 0)
        except Exception:
            pass

    # IP地址
    networks = c.attrs.get("NetworkSettings", {}).get("Networks", {})
    ips = []
    for net_name, net_data in networks.items():
        ip = net_data.get("IPAddress", "")
        if ip:
            ips.append(f"{net_name}:{ip}")
    container_info["ip_addresses"] = ips

    # 健康状态
    state = c.attrs.get("State", {})
    health = state.get("Health")
    if health:
        container_info["health"] = health.get("Status", "unknown")
    else:
        container_info["health"] = None  # 没有配置健康检查

    return container_info


def get_container_detail(container_id: str) -> Optional[dict]:
    """获取容器完整详情（用于详情弹窗）"""
    client = get_client()
    try:
        c = client.containers.get(container_id)
    except docker.errors.NotFound:
        return None

    # 基本信息
    image_name = ""
    image_tags = c.image.tags if c.image and c.image.tags else []
    if image_tags:
        image_name = image_tags[0]
    else:
        image_name = c.image.id[:12] if c.image else ""

    # 端口映射
    ports = []
    port_bindings = c.attrs.get("NetworkSettings", {}).get("Ports", {})
    for port_key, bindings in port_bindings.items():
        if bindings:
            for b in bindings:
                ports.append(f"{b.get('HostIp', '0.0.0.0')}:{b.get('HostPort', '')}->{port_key}")
        else:
            ports.append(port_key)

    # 网络/IP
    networks_info = {}
    networks = c.attrs.get("NetworkSettings", {}).get("Networks", {})
    for net_name, net_data in networks.items():
        networks_info[net_name] = {
            "ip": net_data.get("IPAddress", ""),
            "gateway": net_data.get("Gateway", ""),
            "mac": net_data.get("MacAddress", ""),
        }

    # 运行时长
    uptime_seconds = 0
    started_at = c.attrs.get("State", {}).get("StartedAt", "")
    if started_at and started_at != "0001-01-01T00:00:00Z" and c.status == "running":
        try:
            start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            uptime_seconds = max(int((datetime.now(timezone.utc) - start_dt).total_seconds()), 0)
        except Exception:
            pass

    # 健康状态
    state = c.attrs.get("State", {})
    health_status = None
    health_log = []
    health_data = state.get("Health")
    if health_data:
        health_status = health_data.get("Status", "unknown")
        for h in health_data.get("Log", [])[-5:]:
            health_log.append({
                "start": h.get("Start", ""),
                "exit_code": h.get("ExitCode", -1),
                "output": h.get("Output", "")[:200],
            })

    # 挂载点
    mounts = []
    for m in c.attrs.get("Mounts", []):
        mounts.append({
            "source": m.get("Source", ""),
            "destination": m.get("Destination", ""),
            "type": m.get("Type", ""),
            "rw": m.get("RW", False),
        })

    # 环境变量
    env_list = c.attrs.get("Config", {}).get("Env", [])

    # 重启策略
    restart_policy = c.attrs.get("HostConfig", {}).get("RestartPolicy", {})

    detail = {
        "id": c.short_id,
        "full_id": c.id,
        "name": c.name,
        "image": image_name,
        "status": c.status,
        "created": c.attrs.get("Created", ""),
        "started_at": started_at,
        "uptime_seconds": uptime_seconds,
        "ports": ports,
        "networks": networks_info,
        "health": health_status,
        "health_log": health_log,
        "mounts": mounts,
        "env": env_list,
        "restart_policy": restart_policy,
        "labels": c.attrs.get("Config", {}).get("Labels", {}),
        "hostname": c.attrs.get("Config", {}).get("Hostname", ""),
        "entrypoint": c.attrs.get("Config", {}).get("Entrypoint", []),
        "cmd": c.attrs.get("Config", {}).get("Cmd", []),
    }

    return detail


def list_images() -> list[dict]:
    """获取所有镜像列表，标记使用状态"""
    client = get_client()
    images = client.images.list(all=True)

    # 获取所有运行中和停止中的容器使用的镜像ID
    containers = client.containers.list(all=True)
    used_image_ids = set()
    for c in containers:
        try:
            if c.name in SELF_CONTAINER_NAMES:
                continue
            # 用 attrs 里的镜像 ID，避免 c.image 触发逐容器镜像查询
            _img_id = c.attrs.get("Image") or ""
            if _img_id:
                used_image_ids.add(_img_id)
        except Exception:
            pass

    # 获取自身容器的镜像ID（不显示在镜像列表中）
    self_image_id = None
    try:
        self_container = None
        for cname in SELF_CONTAINER_NAMES:
            try:
                self_container = client.containers.get(cname)
                break
            except Exception:
                continue
        self_image_id = (self_container.attrs.get("Image") or None) or self_container.image.id
    except Exception:
        pass

    result = []
    for img in images:
        try:
            # 跳过自身镜像
            if img.id == self_image_id:
                continue

            tags = img.tags if img.tags else []
            is_used = img.id in used_image_ids
            size_mb = round(img.attrs.get("Size", 0) / (1024 * 1024), 1)

            # 解析仓库和标签
            repo_tags = []
            for t in tags:
                if ":" in t:
                    repo, tag = t.rsplit(":", 1)
                else:
                    repo, tag = t, "latest"
                repo_tags.append({"repo": repo, "tag": tag})

            result.append({
                "id": img.id[:12],
                "full_id": img.id,
                "tags": tags,
                "repo_tags": repo_tags,
                "is_used": is_used,
                "size_mb": size_mb,
                "created": img.attrs.get("Created", ""),
                "has_tag": len(tags) > 0,
            })
        except Exception as e:
            logger.warning(f"解析镜像 {getattr(img, 'id', 'unknown')[:12]} 失败: {e}")
            continue

    return result


def delete_image(image_id: str, force: bool = False) -> dict:
    """删除指定镜像"""
    client = get_client()
    try:
        client.images.remove(image_id, force=force)
        return {"success": True, "message": f"镜像 {image_id[:12]} 已删除"}
    except docker.errors.NotFound:
        return {"success": False, "message": f"镜像 {image_id[:12]} 不存在"}
    except docker.errors.APIError as e:
        return {"success": False, "message": f"删除失败: {str(e)}"}
    except Exception as e:
        return {"success": False, "message": f"删除失败: {str(e)}"}


def cleanup_unused_images() -> dict:
    """清理所有未使用镜像和无Tag镜像"""
    client = get_client()
    images = client.images.list(all=True)

    # 获取使用中的镜像ID
    containers = client.containers.list(all=True)
    used_image_ids = set()
    for c in containers:
        try:
            used_image_ids.add(c.image.id)
        except Exception:
            pass

    # 获取自身镜像ID
    self_image_id = None
    try:
        self_container = None
        for cname in SELF_CONTAINER_NAMES:
            try:
                self_container = client.containers.get(cname)
                break
            except Exception:
                continue
        self_image_id = self_container.image.id
    except Exception:
        pass

    deleted = 0
    failed = 0
    errors = []

    for img in images:
        if img.id == self_image_id:
            continue
        is_used = img.id in used_image_ids
        has_tag = len(img.tags) > 0

        # 删除未使用镜像或无Tag镜像
        if not is_used or not has_tag:
            try:
                client.images.remove(img.id, force=True)
                deleted += 1
            except Exception as e:
                failed += 1
                tag_info = ", ".join(img.tags) if img.tags else "<none>"
                errors.append(f"{tag_info}: {str(e)}")

    message = f"已删除 {deleted} 个镜像"
    if failed > 0:
        message += f"，{failed} 个删除失败"
    return {"success": True, "deleted": deleted, "failed": failed, "errors": errors, "message": message}


# ============ Compose 项目管理 ============

def list_compose_projects() -> list[dict]:
    """列出所有 Docker Compose 项目（按 com.docker.compose.project 标签分组）"""
    client = get_client()
    containers = client.containers.list(all=True)

    projects: dict[str, dict] = {}
    for c in containers:
        try:
            is_self = c.name in SELF_CONTAINER_NAMES
            labels = c.attrs.get("Config", {}).get("Labels") or {}
            project = labels.get("com.docker.compose.project")
            if not project:
                continue

            if project not in projects:
                projects[project] = {
                    "name": project,
                    "is_self": False,
                    "services": set(),
                    "images": set(),
                    "ports": set(),
                    "containers": [],
                    "running": 0,
                    "stopped": 0,
                    "total": 0,
                    # 容器内路径 → 宿主机真实路径（面板新建的项目 label 是容器内 /host/compose/...，要转换展示）
                    "working_dir": compose_project._host_path(labels.get("com.docker.compose.project.working_dir", "")),
                    "config_files": compose_project._host_path(labels.get("com.docker.compose.project.config_files", "")),
                    "compose_version": labels.get("com.docker.compose.version", ""),
                }

            p = projects[project]
            if is_self:
                p["is_self"] = True
            p["total"] += 1
            service = labels.get("com.docker.compose.service", "")
            if service:
                p["services"].add(service)

            try:
                image_tags = c.image.tags if c.image and c.image.tags else []
                image_name = image_tags[0] if image_tags else (c.image.id[:12] if c.image else "")
            except Exception:
                image_name = c.attrs.get("Config", {}).get("Image", "")[:30] or "unknown"
            if image_name:
                p["images"].add(image_name)

            # 聚合端口映射
            for port_str in _parse_container_ports(c):
                p["ports"].add(port_str)

            if c.status == "running":
                p["running"] += 1
            else:
                p["stopped"] += 1

            p["containers"].append({
                "id": c.short_id,
                "full_id": c.id,
                "name": c.name,
                "status": c.status,
                "image": image_name,
                "service": service,
                "ports": _parse_container_ports(c),
                "started_at": c.attrs.get("State", {}).get("StartedAt", ""),
            })
        except Exception as e:
            logger.warning(f"解析 Compose 容器 {getattr(c, 'name', 'unknown')} 失败: {e}")
            continue

    # 转换为列表，计算聚合状态和运行时长
    result = []
    for name, p in projects.items():
        p["services"] = sorted(p["services"])
        p["images"] = sorted(p["images"])
        p["ports"] = sorted(p["ports"])
        if p["running"] == p["total"]:
            p["status"] = "running"
        elif p["running"] == 0:
            p["status"] = "stopped"
        else:
            p["status"] = "partial"

        # 计算运行时长（取最早启动的容器；内存占用由 /api/compose/stats 异步填充，避免拖慢列表）
        uptime_seconds = 0
        for c_info in p["containers"]:
            started_at = c_info.get("started_at", "")
            if started_at and started_at != "0001-01-01T00:00:00Z":
                try:
                    start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                    sec = int((datetime.now(timezone.utc) - start_dt).total_seconds())
                    if uptime_seconds == 0 or sec < uptime_seconds:
                        uptime_seconds = sec
                except Exception:
                    pass
        p["uptime_seconds"] = max(uptime_seconds, 0)
        p["memory_usage"] = 0
        result.append(p)

    status_order = {"running": 0, "partial": 1, "stopped": 2}
    result.sort(key=lambda x: status_order.get(x["status"], 3))
    return result


def get_compose_project_detail(project_name: str) -> Optional[dict]:
    """获取 Compose 项目完整详情"""
    client = get_client()
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return None

    labels = containers[0].attrs.get("Config", {}).get("Labels") or {}
    service_list = []
    running = 0
    total = len(containers)
    all_images = set()

    for c in containers:
        c_labels = c.attrs.get("Config", {}).get("Labels") or {}
        service = c_labels.get("com.docker.compose.service", "")
        image_tags = c.image.tags if c.image and c.image.tags else []
        image_name = image_tags[0] if image_tags else (c.image.id[:12] if c.image else "")
        if image_name:
            all_images.add(image_name)
        if c.status == "running":
            running += 1

        # 端口映射
        ports = []
        if c.attrs.get("NetworkSettings", {}).get("Ports"):
            for port_key, bindings in c.attrs["NetworkSettings"]["Ports"].items():
                if bindings:
                    for b in bindings:
                        ports.append(f"{b.get('HostIp', '0.0.0.0')}:{b.get('HostPort', '')}->{port_key}")

        # 运行时长
        uptime_seconds = 0
        started_at = c.attrs.get("State", {}).get("StartedAt", "")
        if started_at and started_at != "0001-01-01T00:00:00Z" and c.status == "running":
            try:
                start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                uptime_seconds = max(int((datetime.now(timezone.utc) - start_dt).total_seconds()), 0)
            except Exception:
                pass

        service_list.append({
            "id": c.short_id,
            "full_id": c.id,
            "name": c.name,
            "status": c.status,
            "image": image_name,
            "service": service,
            "ports": ports,
            "uptime_seconds": uptime_seconds,
            "created": c.attrs.get("Created", ""),
        })

    if running == total:
        status = "running"
    elif running == 0:
        status = "stopped"
    else:
        status = "partial"

    # 项目创建时间 = 最早创建的容器时间
    created_at = ""
    created_list = [c.attrs.get("Created", "") for c in containers if c.attrs.get("Created")]
    if created_list:
        created_at = min(created_list)

    # 读取 compose YAML 内容（config_files 是宿主机路径，转容器内路径读取）
    yaml_content = ""
    config_files = labels.get("com.docker.compose.project.config_files", "")
    if config_files:
        first_file = [f.strip() for f in config_files.split(",") if f.strip()][0]
        try:
            container_path = compose_project._container_path(first_file)
            with open(container_path, encoding="utf-8") as f:
                yaml_content = f.read()
        except Exception as e:
            yaml_content = f"# 读取 compose 文件失败: {e}"

    return {
        "name": project_name,
        "status": status,
        "running": running,
        "stopped": total - running,
        "total": total,
        "services": service_list,
        "images": sorted(all_images),
        # 容器内路径 → 宿主机真实路径（与列表接口一致，面板新建的项目 label 是容器内 /host/compose/...）
        "working_dir": compose_project._host_path(labels.get("com.docker.compose.project.working_dir", "")),
        "config_files": compose_project._host_path(config_files),
        "compose_version": labels.get("com.docker.compose.version", ""),
        "created_at": created_at,
        "yaml_content": yaml_content,
    }


def _is_self_project(project_name: str) -> bool:
    """判断 compose 项目是否为自身（含自身容器）"""
    client = get_client()
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    return any(is_self_name(c.name) for c in containers)


def start_compose_project(project_name: str) -> dict:
    """启动 Compose 项目的所有容器"""
    client = get_client()
    if _is_self_project(project_name):
        return {"success": False, "message": "不能对自己执行启动操作，请使用'更新自己'功能"}
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return {"success": False, "message": f"Compose 项目 {project_name} 不存在"}

    success_count = 0
    errors = []
    for c in containers:
        if c.status != "running":
            try:
                c.start()
                success_count += 1
            except Exception as e:
                errors.append(f"{c.name}: {str(e)}")

    if errors:
        return {"success": False, "message": f"部分启动失败: {'; '.join(errors)}"}
    return {"success": True, "message": f"Compose 项目 {project_name} 已启动 ({success_count} 个容器)"}


def stop_compose_project(project_name: str) -> dict:
    """停止 Compose 项目的所有容器"""
    client = get_client()
    if _is_self_project(project_name):
        return {"success": False, "message": "不能停止自己，否则面板将无法管理容器（可用'更新自己'重启面板）"}
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return {"success": False, "message": f"Compose 项目 {project_name} 不存在"}

    success_count = 0
    errors = []
    for c in containers:
        if c.status == "running":
            try:
                c.stop(timeout=30)
                success_count += 1
            except Exception as e:
                errors.append(f"{c.name}: {str(e)}")

    if errors:
        return {"success": False, "message": f"部分停止失败: {'; '.join(errors)}"}
    return {"success": True, "message": f"Compose 项目 {project_name} 已停止 ({success_count} 个容器)"}


def restart_compose_project(project_name: str) -> dict:
    """重启 Compose 项目的所有容器"""
    client = get_client()
    if _is_self_project(project_name):
        return {"success": False, "message": "不能对自己执行重启操作，请使用'更新自己'功能"}
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return {"success": False, "message": f"Compose 项目 {project_name} 不存在"}

    success_count = 0
    errors = []
    for c in containers:
        try:
            c.restart(timeout=30)
            success_count += 1
        except Exception as e:
            errors.append(f"{c.name}: {str(e)}")

    if errors:
        return {"success": False, "message": f"部分重启失败: {'; '.join(errors)}"}
    return {"success": True, "message": f"Compose 项目 {project_name} 已重启 ({success_count} 个容器)"}


def update_compose_project(project_name: str, progress_key: str = None) -> dict:
    """更新 Compose 项目：通过 Docker SDK 拉取最新镜像并逐个重建容器

    不依赖宿主机的 docker-compose.yml 文件，直接通过 Docker SDK 操作：
    1. 拉取项目所有容器引用的镜像（去重）
    2. 逐个保存容器完整配置 → 停止删除旧容器 → 用新镜像重建 → 恢复网络别名 → 启动

    Args:
        project_name: Compose 项目名称
        progress_key: 进度跟踪key，传入则启用进度记录（传入项目名即可）
    """
    client = get_client()
    if _is_self_project(project_name):
        return {"success": False, "message": "不能对自己执行 Compose 更新（会中断面板），请使用'更新自己'功能"}
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return {"success": False, "message": f"Compose 项目 {project_name} 不存在"}

    # 按服务名排序，保持重建顺序一致
    def _get_service_name(c):
        labels = c.attrs.get("Config", {}).get("Labels") or {}
        return labels.get("com.docker.compose.service", c.name)
    containers_sorted = sorted(containers, key=_get_service_name)

    # 统计唯一镜像数用于进度计算
    unique_images = set()
    for c in containers_sorted:
        img = c.attrs.get("Config", {}).get("Image", "")
        if img:
            unique_images.add(img)
    num_images = len(unique_images)
    num_containers = len(containers_sorted)
    total_steps = num_images + num_containers

    if progress_key:
        _init_progress(progress_key, total_steps=total_steps, tag="compose")

    # 第一阶段：拉取所有镜像（去重）
    pulled = set()
    img_step = 0
    for c in containers_sorted:
        image_name = c.attrs.get("Config", {}).get("Image", "")
        if image_name and image_name not in pulled:
            pulled.add(image_name)
            img_step += 1
            if progress_key:
                _reset_image_stats(progress_key, image_name)
                _set_progress(progress_key, phase="pulling",
                               current_step=img_step, total_steps=total_steps,
                               message=f"正在拉取镜像 {image_name} ({img_step}/{num_images})...")
            try:
                if progress_key:
                    for chunk in client.api.pull(image_name, stream=True, decode=True):
                        _parse_pull_chunk(progress_key, chunk)
                else:
                    client.images.pull(image_name)
            except docker.errors.ImageNotFound:
                pass
            except Exception as e:
                logger.warning(f"拉取镜像 {image_name} 失败: {e}，使用本地已有镜像继续")
                if progress_key:
                    _set_progress(progress_key, message=f"镜像 {image_name} 拉取失败: {e}，使用本地已有镜像继续")

    # 第二阶段：逐个重建容器
    updated_count = 0
    errors = []

    for i, c in enumerate(containers_sorted):
        container_name = c.name
        step = num_images + i + 1
        if progress_key:
            _set_progress(progress_key, phase="rebuilding", current_container=container_name,
                           current_step=step, total_steps=total_steps,
                           message=f"正在重建容器 {container_name} ({i+1}/{num_containers})...")
        try:
            attrs = c.attrs
            config_labels = attrs.get("Config", {}).get("Labels") or {}
            service_name = config_labels.get("com.docker.compose.service", "")

            # 保存完整配置
            saved = {
                "name": container_name,
                "image": attrs.get("Config", {}).get("Image", ""),
                "command": attrs.get("Config", {}).get("Cmd"),
                "entrypoint": attrs.get("Config", {}).get("Entrypoint"),
                "environment": attrs.get("Config", {}).get("Env"),
                "labels": config_labels,
                "ports": attrs.get("HostConfig", {}).get("PortBindings"),
                "volumes": attrs.get("HostConfig", {}).get("Binds"),
                "network_mode": attrs.get("HostConfig", {}).get("NetworkMode"),
                "restart_policy": attrs.get("HostConfig", {}).get("RestartPolicy"),
                "privileged": attrs.get("HostConfig", {}).get("Privileged", False),
                "cap_add": attrs.get("HostConfig", {}).get("CapAdd"),
                "cap_drop": attrs.get("HostConfig", {}).get("CapDrop"),
                "devices": attrs.get("HostConfig", {}).get("Devices"),
                "extra_hosts": attrs.get("HostConfig", {}).get("ExtraHosts"),
                "hostname": attrs.get("Config", {}).get("Hostname"),
                "user": attrs.get("Config", {}).get("User"),
                "working_dir": attrs.get("Config", {}).get("WorkingDir"),
                "stop_signal": attrs.get("Config", {}).get("StopSignal"),
            }

            # 保存网络信息（含别名，用于恢复 Compose 服务发现）
            network_settings = attrs.get("NetworkSettings", {}).get("Networks", {})
            saved_networks = []
            for net_name, net_data in network_settings.items():
                saved_networks.append({
                    "name": net_name,
                    "aliases": net_data.get("Aliases") or [],
                })

            # 停止并移除旧容器
            try:
                c.stop(timeout=30)
            except Exception:
                pass
            c.remove()

            # 构建创建参数
            create_kwargs = {
                "name": saved["name"],
                "image": saved["image"],
                "detach": True,
            }
            # 设置 network_mode（连接到原有网络）
            if saved["network_mode"] and saved["network_mode"] != "none":
                create_kwargs["network_mode"] = saved["network_mode"]
            for k in ["command", "entrypoint", "environment", "labels", "ports",
                       "volumes", "restart_policy", "privileged", "cap_add",
                       "cap_drop", "devices", "extra_hosts", "hostname",
                       "user", "working_dir", "stop_signal"]:
                if saved[k]:
                    create_kwargs[k] = saved[k]

            # 创建新容器（尚未启动）
            new_container = client.containers.create(**create_kwargs)

            # 恢复网络别名（断开自动连接的网络 → 重新连接附带 service 别名）
            for net_info in saved_networks:
                try:
                    network = client.networks.get(net_info["name"])
                    # 先断开（创建时通过 network_mode 自动连接的，没有别名）
                    try:
                        network.disconnect(new_container, force=True)
                    except Exception:
                        pass
                    # 重新连接，附加别名（过滤掉容器名本身，Docker 会自动添加）
                    aliases = [a for a in net_info["aliases"] if a and a != container_name]
                    if service_name and service_name not in aliases:
                        aliases.append(service_name)
                    network.connect(new_container, aliases=aliases if aliases else None)
                except Exception as e:
                    logger.warning(f"容器 {container_name} 连接网络 {net_info['name']} 失败: {e}")

            # 启动新容器
            new_container.start()
            updated_count += 1
            logger.info(f"容器 {container_name} 重建成功")
            if progress_key:
                _set_progress(progress_key, message=f"容器 {container_name} 重建成功")

        except Exception as e:
            errors.append(f"{container_name}: {str(e)}")
            logger.error(f"重建容器 {container_name} 失败: {e}")
            if progress_key:
                _set_progress(progress_key, message=f"容器 {container_name} 重建失败: {e}")

    if errors:
        if updated_count > 0:
            msg = f"Compose 项目 {project_name} 部分更新成功: {updated_count} 个成功, {len(errors)} 个失败: {'; '.join(errors)}"
        else:
            msg = f"更新失败: {'; '.join(errors)}"
    else:
        msg = f"Compose 项目 {project_name} 更新成功，已拉取最新镜像并重建 {updated_count} 个容器"

    if progress_key:
        _set_progress(progress_key, phase="done", done=True,
                       success=(updated_count > 0 if errors else True),
                       message=msg, error="; ".join(errors) if errors else None)

    if errors and updated_count == 0:
        return {"success": False, "message": msg}
    return {"success": True, "message": msg}


def get_compose_project_stats(project_name: str) -> Optional[dict]:
    """获取 Compose 项目的聚合统计信息"""
    client = get_client()
    containers = client.containers.list(
        all=True, filters={"label": [f"com.docker.compose.project={project_name}"]}
    )
    if not containers:
        return None

    running_containers = [c for c in containers if c.status == "running"]
    if not running_containers:
        return {
            "cpu_percent": 0, "memory_usage": 0, "memory_limit": 0,
            "memory_percent": 0, "network_rx": 0, "network_tx": 0,
            "block_read": 0, "block_write": 0, "pids": 0,
            "service_count": len(containers),
            "running_count": 0,
        }

    total_cpu = 0
    total_mem_usage = 0
    total_mem_limit = 0
    total_net_rx = 0
    total_net_tx = 0
    total_block_read = 0
    total_block_write = 0
    total_pids = 0

    for c in running_containers:
        try:
            stats = c.stats(stream=False)
        except Exception:
            continue

        # CPU
        cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
        system_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
        cpu_count = len(stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [])) or stats["cpu_stats"].get("online_cpus", 1)
        cpu_percent = round((cpu_delta / system_delta) * cpu_count * 100, 2) if system_delta > 0 else 0
        total_cpu += cpu_percent

        # 内存
        total_mem_usage += stats["memory_stats"].get("usage", 0)
        total_mem_limit += stats["memory_stats"].get("limit", 0)

        # 网络
        for net_data in stats.get("networks", {}).values():
            total_net_rx += net_data.get("rx_bytes", 0)
            total_net_tx += net_data.get("tx_bytes", 0)

        # 磁盘I/O
        for entry in stats.get("blkio_stats", {}).get("io_service_bytes_recursive", []) or []:
            if entry.get("op") == "read":
                total_block_read += entry.get("value", 0)
            elif entry.get("op") == "write":
                total_block_write += entry.get("value", 0)

        # PIDs
        total_pids += stats.get("pids_stats", {}).get("current", 0)

    memory_percent = round(total_mem_usage / total_mem_limit * 100, 2) if total_mem_limit > 0 else 0

    return {
        "cpu_percent": round(total_cpu, 2),
        "memory_usage": total_mem_usage,
        "memory_limit": total_mem_limit,
        "memory_percent": memory_percent,
        "network_rx": total_net_rx,
        "network_tx": total_net_tx,
        "block_read": total_block_read,
        "block_write": total_block_write,
        "pids": total_pids,
        "service_count": len(containers),
        "running_count": len(running_containers),
    }


# ============ 自身管理（展示自己 / 更新自己） ============

SELF_UPDATER_NAME = "mypanel-self-updater"


def get_all_compose_memory() -> dict:
    """批量获取所有 Compose 项目的内存占用（并发取 stats，用于列表页内存列异步填充）"""
    client = get_client()
    try:
        containers = client.containers.list(all=True)
    except Exception as e:
        logger.warning(f"批量取 Compose 内存失败: {e}")
        return {}

    # 运行中的 compose 容器: (项目名, 容器对象)
    running = []
    for c in containers:
        try:
            labels = c.attrs.get("Config", {}).get("Labels") or {}
            project = labels.get("com.docker.compose.project")
            if project and c.status == "running":
                running.append((project, c))
        except Exception:
            continue

    if not running:
        return {}

    def _fetch_mem(item) -> tuple:
        project, c = item
        try:
            s = c.stats(stream=False)
            if isinstance(s, dict):
                mem = s.get("memory_stats", {}).get("usage", 0) or 0
            else:
                mem = next(iter(s), {}).get("memory_stats", {}).get("usage", 0) or 0
            return project, mem
        except Exception:
            return project, 0

    memory_map: dict[str, int] = {}
    # 并发取内存（每容器 1-2s 的 stats 调用并行执行，总耗时≈单个耗时）
    with ThreadPoolExecutor(max_workers=min(10, len(running))) as pool:
        for project, mem in pool.map(_fetch_mem, running):
            memory_map[project] = memory_map.get(project, 0) + mem
    return memory_map


def restart_self() -> dict:
    """重启自己：由 daemon 后台执行 restart，本进程重启瞬间会中断，接口先返回成功"""
    client = get_client()
    c = get_self_container(client)
    if not c:
        return {"success": False, "message": "未找到自身容器"}
    container_id = c.id

    def _do_restart():
        try:
            # 用底层 API 触发，避免 SDK 高层封装在连接断开时抛异常
            client.api.restart(container_id, timeout=10)
        except Exception as e:
            logger.warning(f"自身重启指令异常（daemon 仍会继续执行）: {e}")

    import threading
    t = threading.Thread(target=_do_restart, daemon=True)
    t.start()
    return {"success": True, "message": "重启指令已发送，面板将短暂离线后自动恢复"}


def start_self_update() -> dict:
    """更新自己（updater 容器模式）：
    1. 预拉取新镜像（daemon 执行，不重启面板）
    2. 清理旧 updater 容器
    3. 启动 updater 容器：挂 docker.sock + compose 目录，执行
       `docker compose -p <project> pull && docker compose -p <project> up -d --force-recreate`
       updater 跑在宿主机 daemon 上，与面板容器相互独立——面板被 recreate 时 updater 不受影响
    """
    client = get_client()
    c = get_self_container(client)
    if not c:
        return {"success": False, "message": "未找到自身容器"}

    image = c.attrs.get("Config", {}).get("Image", "")
    if not image:
        return {"success": False, "message": "无法获取自身镜像名"}

    # 已是最新版本则直接返回，不重建面板（避免无意义离线）
    try:
        from modules import update_checker
        check = update_checker.check_image_update(client, image)
        if check.get("has_update") is False:
            return {"success": True, "started": False, "no_update": True,
                    "message": "已是最新版本，无需更新"}
        logger.info(f"检测到新版本，开始更新: {check.get('error') or ''}")
    except Exception as e:
        logger.warning(f"更新前版本检查失败，继续执行更新: {e}")

    labels = c.attrs.get("Config", {}).get("Labels") or {}
    project = labels.get("com.docker.compose.project", "")
    config_files = labels.get("com.docker.compose.project.config_files", "")
    files = [f.strip() for f in config_files.split(",") if f.strip()] if config_files else []
    compose_file = files[0] if files else "docker-compose.yml"
    compose_dir = os.path.dirname(compose_file)
    if not compose_dir:
        return {"success": False, "message": f"无法从 config_files 解析 compose 目录: {config_files}"}

    # 1. 预拉新镜像（失败不阻塞，updater 内会再次 pull）
    try:
        logger.info(f"预拉取自身镜像 {image} ...")
        client.images.pull(image)
        logger.info("自身镜像预拉取完成")
    except Exception as e:
        logger.warning(f"自身镜像预拉取失败（updater 内会重试）: {e}")

    # 2. 清理旧 updater 容器
    try:
        old = client.containers.get(SELF_UPDATER_NAME)
        old.remove(force=True)
        logger.info("已清理旧 updater 容器")
    except docker.errors.NotFound:
        pass
    except Exception as e:
        logger.warning(f"清理旧 updater 失败: {e}")

    # 3. 启动 updater 容器
    #    注意：updater 镜像用自身镜像（内含 docker CLI + compose），挂载 docker.sock + compose 目录
    #    关键：compose 目录必须挂载到容器内【与宿主机相同的路径】（如 /mnt/mmcblk0p27/docker/mypanel），
    #    这样 compose 写出的 label(working_dir/config_files) 就是宿主机真实路径；
    #    如果挂到 /work 之类的别名路径，重建后 label 漂移成 /work，宿主机路径全错。
    #    project 名用 -p 固定，避免 updater 目录名成为新 project 名。
    updater_cmd = (
        f"cd '{compose_dir}' && "
        f"docker compose -p {project} -f docker-compose.yml pull && "
        f"docker compose -p {project} -f docker-compose.yml up -d --force-recreate && "
        f"echo SELF_UPDATE_OK && docker image prune -f --filter dangling=true && docker rm -f {SELF_UPDATER_NAME} || echo SELF_UPDATE_FAIL"
    )
    host_env = c.attrs.get("Config", {}).get("Env") or []
    env = {}
    for e in host_env:
        if e.startswith("COMPOSE_ROOT") or e.startswith("COMPOSE_VOLUME_ROOT"):
            k, _, v = e.partition("=")
            env[k] = v
    # compose 文件里用的是 ${COMPOSE_ROOT:-...}（无 _HOST 后缀），
    # 必须从 COMPOSE_ROOT_HOST 补一份同名变量，否则重建时挂载路径会退回默认值
    if "COMPOSE_ROOT_HOST" in env and "COMPOSE_ROOT" not in env:
        env["COMPOSE_ROOT"] = env["COMPOSE_ROOT_HOST"]
    if "COMPOSE_VOLUME_ROOT_HOST" in env and "COMPOSE_VOLUME_ROOT" not in env:
        env["COMPOSE_VOLUME_ROOT"] = env["COMPOSE_VOLUME_ROOT_HOST"]

    try:
        updater = client.containers.run(
            image=image,
            name=SELF_UPDATER_NAME,
            detach=True,
            entrypoint="sh",
            command=["-c", updater_cmd],
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                # bind 目标 = 源路径：容器内自动创建同名目录树，compose label 保持宿主机路径
                compose_dir: {"bind": compose_dir, "mode": "rw"},
            },
            environment=env or None,
            network_mode="host",
            restart_policy={"Name": "no"},
            # 不要打 com.docker.compose.project 标签，否则 updater 会混进自身项目
            labels={"dock.shift.self-updater": "1"},
        )
        logger.info(f"updater 容器已启动: {updater.short_id}")
    except Exception as e:
        logger.error(f"启动 updater 容器失败: {e}")
        return {"success": False, "message": f"启动更新执行器失败: {e}"}

    return {
        "success": True,
        "started": True,
        "message": "更新已启动：正在拉取镜像并重建面板容器，约 30 秒后自动恢复，请稍候刷新",
    }


def get_self_update_status() -> dict:
    """查询 updater 容器状态与日志（面板重启恢复后用于确认更新结果）"""
    client = get_client()
    try:
        c = client.containers.get(SELF_UPDATER_NAME)
    except docker.errors.NotFound:
        return {"exists": False, "running": False, "success": None, "message": "无更新任务", "logs": ""}

    logs = ""
    try:
        logs = c.logs(tail=100).decode("utf-8", errors="replace")
    except Exception:
        pass

    if c.status == "running":
        return {"exists": True, "running": True, "success": None, "message": "更新执行中", "logs": logs}

    success = "SELF_UPDATE_OK" in logs
    if success:
        # 成功后后端自动清理 updater 容器（面板重启 token 失效时前端清理可能不执行，
        # 失败时保留容器便于查日志）
        try:
            c.remove(force=True)
            logger.info("更新成功，updater 容器已自动清理")
        except Exception as e:
            logger.warning(f"自动清理 updater 失败: {e}")
        return {"exists": False, "running": False, "success": True, "message": "更新完成", "logs": logs}
    return {"exists": True, "running": False, "success": False, "message": "更新失败（详见日志）", "logs": logs}


def cleanup_self_update() -> dict:
    """删除残留的 updater 容器，并顺带清理悬空镜像（每次更新留下的无Tag旧镜像）"""
    client = get_client()
    msg = ""
    try:
        c = client.containers.get(SELF_UPDATER_NAME)
        c.remove(force=True)
        msg = "已清理更新执行器"
    except docker.errors.NotFound:
        msg = "无残留更新执行器"
    except Exception as e:
        return {"success": False, "message": f"清理失败: {e}"}

    # 清理悬空镜像（无 tag、无容器引用，docker 标准安全操作）
    try:
        pruned = client.images.prune(filters={"dangling": True})
        freed = pruned.get("SpaceReclaimed", 0)
        if freed > 0:
            msg += f"，清理悬空镜像释放 {freed / 1024 / 1024:.1f}MB"
    except Exception as e:
        logger.warning(f"清理悬空镜像失败: {e}")

    return {"success": True, "message": msg}


# ============ 镜像加速器（registry-mirrors） ============

DAEMON_JSON_PATH = "/etc/docker/daemon.json"


def get_registry_mirrors() -> dict:
    """读取 daemon.json 中的 registry-mirrors 配置"""
    exists = os.path.exists(DAEMON_JSON_PATH)
    if not exists:
        return {"mirrors": [], "path": DAEMON_JSON_PATH, "exists": False}
    try:
        with open(DAEMON_JSON_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return {"mirrors": data.get("registry-mirrors", []), "path": DAEMON_JSON_PATH, "exists": True}
    except Exception as e:
        return {"mirrors": [], "path": DAEMON_JSON_PATH, "exists": True, "error": str(e)}


def set_registry_mirrors(mirrors: list) -> dict:
    """写 daemon.json 的 registry-mirrors（先备份原文件，写后校验 JSON）"""
    try:
        clean = [str(m).strip() for m in (mirrors or []) if str(m).strip()]
        for m in clean:
            if not m.startswith(("http://", "https://")):
                return {"success": False, "message": f"加速器地址必须以 http(s):// 开头: {m}"}
        data = {}
        if os.path.exists(DAEMON_JSON_PATH):
            with open(DAEMON_JSON_PATH, encoding="utf-8") as f:
                data = json.load(f)
            import shutil
            shutil.copy2(DAEMON_JSON_PATH, DAEMON_JSON_PATH + ".bak")
        data["registry-mirrors"] = clean
        with open(DAEMON_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        with open(DAEMON_JSON_PATH, encoding="utf-8") as f:
            json.load(f)
        return {"success": True, "message": "已写入 daemon.json（原文件已备份 daemon.json.bak），重启 Docker 服务后生效"}
    except json.JSONDecodeError:
        return {"success": False, "message": "写入失败：daemon.json 内容不是合法 JSON（已停止，请手动检查）"}
    except Exception as e:
        return {"success": False, "message": f"写入失败: {e}"}


def load_image_from_file(tar_path: str) -> dict:
    """从本地 tar/gz 文件导入镜像（docker load）"""
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "load", "-i", tar_path],
            capture_output=True, text=True, timeout=600,
        )
        output = (result.stdout + result.stderr).strip()
        if result.returncode == 0:
            return {"success": True, "message": output or "导入成功"}
        return {"success": False, "message": f"导入失败: {output[-300:]}"}
    except subprocess.TimeoutExpired:
        return {"success": False, "message": "导入超时（10分钟）"}
    except Exception as e:
        return {"success": False, "message": f"导入失败: {e}"}


def get_port_usage() -> dict:
    """扫描宿主端口占用：
    - /proc/net/tcp + tcp6 的 LISTEN 端口（host 网络模式下=宿主网络栈，包含反代等所有系统监听）
    - 所有容器（含停止）的端口映射配置
    合并去重，标注来源，方便创建 Compose 时避开已占用端口。
    """
    client = get_client()

    # 1. 系统监听端口（LISTEN state = 0A）
    #    优先读 /proc/1/net/tcp：pid=host 模式下 PID1 是宿主 init，其网络栈=宿主网络栈
    #    （host 网络下容器 PID1 与宿主共享网络栈，同样有效）
    listen_ports = set()
    if os.path.exists("/proc/1/net/tcp"):
        proc_files = ("/proc/1/net/tcp", "/proc/1/net/tcp6")
    else:
        proc_files = ("/proc/net/tcp", "/proc/net/tcp6")
    for proc_file in proc_files:
        try:
            with open(proc_file, encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[1:]
            for line in lines:
                parts = line.split()
                if len(parts) < 4 or parts[3] != "0A":
                    continue
                local = parts[1]  # 形如 0100007F:1F90
                if ":" in local:
                    port_hex = local.rsplit(":", 1)[1]
                    try:
                        listen_ports.add(int(port_hex, 16))
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"读取 {proc_file} 失败: {e}")

    # 2. 所有容器的端口映射（含已停止容器，避免创建后冲突）
    container_ports: dict[int, set] = {}
    try:
        containers = client.containers.list(all=True)
    except Exception:
        containers = []
    for c in containers:
        try:
            bindings = c.attrs.get("HostConfig", {}).get("PortBindings") or {}
            for port_key, binds in bindings.items():
                cont_port = port_key.split("/")[0]
                if not cont_port.isdigit():
                    continue
                for b in binds or []:
                    host_port = (b.get("HostPort") or "").strip()
                    if host_port.isdigit():
                        container_ports.setdefault(int(host_port), set()).add(f"{c.name}:{cont_port}")
        except Exception:
            continue

    all_ports = sorted(listen_ports | set(container_ports.keys()))
    result = []
    for p in all_ports:
        sources = []
        if p in listen_ports:
            sources.append("系统监听")
        if p in container_ports:
            sources.extend(sorted(container_ports[p]))
        result.append({"port": p, "sources": sources})

    note = ""
    if not listen_ports:
        note = "当前容器非 host 网络，无法读取宿主系统监听端口，以下仅含容器映射端口"

    return {"ports": result, "total": len(result), "container_mapped": len(container_ports),
            "host_listen": len(listen_ports), "note": note}
