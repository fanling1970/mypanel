"""Compose 项目创建/删除管理（基于宿主机挂载目录 + docker compose CLI）

原理：
- 宿主机 compose 根目录（默认 /vol1/1000/Docker）挂载到容器 /host/compose
- 宿主机数据盘根（默认 /vol1）挂载到容器 /host-vol1，支持在任意 /vol1 下目录创建项目
- 创建项目 = 写 docker-compose.yml → 容器内执行 `docker compose up -d`（通过 docker.sock 操作宿主机 daemon）
- 删除项目 = `docker compose down`（停止并移除容器/网络，保留文件）

注意：容器内执行 docker compose 时，bind 卷的相对路径（./data）会被 daemon 按宿主机
文件系统解释成容器内路径，导致挂载失败。创建时自动把相对路径转成宿主机绝对路径。
"""
import logging
import os
import re
import subprocess
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# 容器内挂载路径（compose 里通过 ${COMPOSE_ROOT:-/vol1/1000/Docker}:/host/compose 挂载）
COMPOSE_ROOT = os.environ.get("COMPOSE_ROOT", "/host/compose")
# 宿主机真实路径（卷路径转换用；daemon 按宿主机文件系统解释 bind mount）
COMPOSE_ROOT_HOST = os.environ.get("COMPOSE_ROOT_HOST", COMPOSE_ROOT)

# 宿主机数据盘根挂载（${COMPOSE_VOLUME_ROOT:-/vol1}:/host-vol1），支持任意目录
COMPOSE_VOLUME_ROOT = os.environ.get("COMPOSE_VOLUME_ROOT", "/host-vol1")
COMPOSE_VOLUME_ROOT_HOST = os.environ.get("COMPOSE_VOLUME_ROOT_HOST", "/vol1")

# 额外挂载映射（解决 1Panel 等宿主目录不在默认挂载范围内的问题）
# 格式："宿主机路径:容器内路径;宿主机路径:容器内路径"（分号分隔多组）
# 例：EXTRA_MOUNTS="/opt/1panel/docker/compose:/host/1panel-compose"
# snap 版 Docker 需先在宿主机 mount --bind 到 $HOME 下再挂载（/opt 对 snap 只读）
_EXTRA_MOUNTS_RAW = os.environ.get("EXTRA_MOUNTS", "").strip()
EXTRA_MOUNTS: list[tuple[str, str]] = []
if _EXTRA_MOUNTS_RAW:
    for pair in _EXTRA_MOUNTS_RAW.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        if ":" in pair:
            host_p, cont_p = pair.split(":", 1)
            host_p = host_p.strip().rstrip("/")
            cont_p = cont_p.strip().rstrip("/")
            if host_p and cont_p:
                EXTRA_MOUNTS.append((host_p, cont_p))


def _container_path(host_path: str) -> str:
    """宿主机绝对路径 → 容器内路径（挂载映射），支持根路径自身（如 /vol1）"""
    host_path = str(host_path or "").strip().rstrip("/") or "/"
    if host_path == COMPOSE_ROOT_HOST.rstrip("/"):
        return COMPOSE_ROOT
    if host_path == COMPOSE_VOLUME_ROOT_HOST.rstrip("/"):
        return COMPOSE_VOLUME_ROOT
    if host_path.startswith(COMPOSE_ROOT_HOST + os.sep) and COMPOSE_ROOT_HOST != COMPOSE_ROOT:
        return COMPOSE_ROOT + host_path[len(COMPOSE_ROOT_HOST):]
    if host_path.startswith(COMPOSE_VOLUME_ROOT_HOST + os.sep):
        return COMPOSE_VOLUME_ROOT + host_path[len(COMPOSE_VOLUME_ROOT_HOST):]
    # 额外挂载映射（EXTRA_MOUNTS，按最长前缀优先匹配）
    for host_p, cont_p in sorted(EXTRA_MOUNTS, key=lambda x: len(x[0]), reverse=True):
        if host_path == host_p:
            return cont_p
        if host_path.startswith(host_p + os.sep):
            return cont_p + host_path[len(host_p):]
    # 无法映射（容器内路径原样返回，兼容直接传容器内路径）
    return host_path


def _host_path(container_path: str) -> str:
    """容器内路径 → 宿主机绝对路径（挂载映射反方向），支持根路径自身（如 /host-vol1）"""
    container_path = str(container_path or "").strip().rstrip("/") or "/"
    if container_path == COMPOSE_ROOT.rstrip("/"):
        return COMPOSE_ROOT_HOST
    if container_path == COMPOSE_VOLUME_ROOT.rstrip("/"):
        return COMPOSE_VOLUME_ROOT_HOST
    if container_path.startswith(COMPOSE_ROOT + os.sep) and COMPOSE_ROOT_HOST != COMPOSE_ROOT:
        return COMPOSE_ROOT_HOST + container_path[len(COMPOSE_ROOT):]
    if container_path.startswith(COMPOSE_VOLUME_ROOT + os.sep):
        return COMPOSE_VOLUME_ROOT_HOST + container_path[len(COMPOSE_VOLUME_ROOT):]
    # 额外挂载映射反方向（EXTRA_MOUNTS，按最长前缀优先匹配）
    for host_p, cont_p in sorted(EXTRA_MOUNTS, key=lambda x: len(x[1]), reverse=True):
        if container_path == cont_p:
            return host_p
        if container_path.startswith(cont_p + os.sep):
            return host_p + container_path[len(cont_p):]
    # 无法映射（已是宿主机路径则原样返回）
    return container_path


def _resolve_target(target: str) -> tuple:
    """解析用户输入的项目目录（相对名或宿主机绝对路径）

    Returns:
        (project_dir 容器内, host_project_dir 宿主机, label 展示名, error)
        error 非空表示解析失败
    """
    target = str(target or "").strip()
    if not target:
        return "", "", "", "项目目录不能为空"
    if "\\" in target or ".." in target:
        return "", "", "", f"目录不合法: {target}"

    # 兼容容器内路径输入（如 compose 标签里的 /host-vol1/...），统一转宿主机路径
    target = _host_path(target)

    if target.startswith("/"):
        # 宿主机绝对路径：允许 COMPOSE_ROOT_HOST（浏览树根）和 COMPOSE_VOLUME_ROOT_HOST（存储根）两个挂载范围
        host_dir = target
        root_host = COMPOSE_ROOT_HOST.rstrip("/")
        vol_host = COMPOSE_VOLUME_ROOT_HOST.rstrip("/")
        in_root = host_dir == root_host or host_dir.startswith(root_host + os.sep)
        in_vol = host_dir == vol_host or host_dir.startswith(vol_host + os.sep)
        if not (in_root or in_vol):
            return "", "", "", f"只支持 {root_host} 或 {vol_host} 下的目录（当前: {host_dir}）"
        project_dir = _container_path(host_dir)
        return project_dir, host_dir, host_dir, ""
    else:
        # 相对名 → 默认根目录下
        if "/" in target:
            return "", "", "", f"目录名不合法（不支持子路径，用绝对路径可指向任意目录）: {target}"
        project_dir = os.path.join(COMPOSE_ROOT, target)
        host_project_dir = os.path.join(COMPOSE_ROOT_HOST, target)
        return project_dir, host_project_dir, target, ""


def _compose_base_cmd() -> list[str]:
    """返回可用的 compose 命令前缀（docker compose 或 docker-compose），找不到抛异常"""
    for base in (["docker", "compose"], ["docker-compose"]):
        try:
            r = subprocess.run([*base, "version"], capture_output=True, text=True, timeout=15)
            if r.returncode == 0:
                return base
        except (subprocess.TimeoutExpired, FileNotFoundError):
            continue
    raise RuntimeError("未找到可用的 compose 命令")


def _run_compose(args: list[str], workdir: str) -> tuple[bool, str]:
    """在容器内执行 compose 命令（同步，返回输出）"""
    try:
        result = subprocess.run(
            [*_compose_base_cmd(), *args],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = (result.stdout + result.stderr).strip()
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "执行超时（5分钟）"
    except RuntimeError as e:
        return False, str(e)


# ============ 异步任务（构建日志实时展示） ============
_tasks: dict = {}
_task_lock = threading.Lock()
_task_seq = 0


def _new_task(cmd: list[str], workdir: str) -> str:
    global _task_seq
    with _task_lock:
        _task_seq += 1
        task_id = f"t{_task_seq}"
        _tasks[task_id] = {"buffer": [], "done": False, "exit_code": None, "cmd": cmd, "workdir": workdir}
    return task_id


def _task_worker(task_id: str) -> None:
    task = _tasks.get(task_id)
    if not task:
        return
    try:
        proc = subprocess.Popen(
            [*task["cmd"], "up", "-d"],
            cwd=task["workdir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if proc.stdout:
            for line in proc.stdout:
                with _task_lock:
                    task["buffer"].append(line)
        proc.wait()
        with _task_lock:
            task["done"] = True
            task["exit_code"] = proc.returncode
    except Exception as e:
        with _task_lock:
            task["buffer"].append(f"任务异常: {e}\n")
            task["done"] = True
            task["exit_code"] = -1


def get_task_log(task_id: str, offset: int = 0) -> dict:
    """获取任务增量日志"""
    with _task_lock:
        task = _tasks.get(task_id)
        if not task:
            return {"done": True, "exit_code": -1, "logs": "", "offset": 0, "error": "任务不存在或已过期"}
        logs = "".join(task["buffer"][offset:])
        new_offset = len(task["buffer"])
        return {"done": task["done"], "exit_code": task["exit_code"], "logs": logs, "offset": new_offset}


def list_compose_dirs() -> list[dict]:
    """列出默认根目录下的一级子目录（可作为 compose 项目目录）"""
    root = Path(COMPOSE_ROOT)
    if not root.is_dir():
        return []
    result = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and not d.name.startswith("."):
            has_compose = (d / "docker-compose.yml").is_file() or (d / "docker-compose.yaml").is_file()
            result.append({"name": d.name, "path": os.path.join(COMPOSE_ROOT_HOST, d.name), "has_compose": has_compose})
    return result


def browse_compose_dirs(path: str = "") -> dict:
    """浏览宿主机目录：返回给定路径（宿主机视角）下的一级子目录

    Args:
        path: 宿主机绝对路径，空则从默认根目录开始

    Returns:
        {"items": [...], "current": 当前宿主机路径, "parent": 上级宿主机路径(空=已到顶)}
    """
    if path:
        container_base = _container_path(path)
        host_current = path.rstrip("/") or "/"
    else:
        container_base = COMPOSE_ROOT
        host_current = os.environ.get("COMPOSE_ROOT_HOST", COMPOSE_ROOT)
    base = Path(container_base)
    result = []
    if base.is_dir():
        for d in sorted(base.iterdir()):
            if d.is_dir() and not d.name.startswith("."):
                # 映射回宿主机路径展示
                host_path = _host_path(str(d))
                result.append({"name": d.name, "path": host_path, "has_compose": (d / "docker-compose.yml").is_file() or (d / "docker-compose.yaml").is_file()})
    # 上级目录（宿主机视角）：仅允许在挂载范围内上跳
    parent = ""
    if host_current and host_current != "/":
        parent_host = os.path.dirname(host_current)
        # 默认挂载范围 + 额外挂载（EXTRA_MOUNTS）都在允许上跳的范围内
        in_extra = any(parent_host.startswith(host_p) for host_p, _ in EXTRA_MOUNTS)
        in_range = (parent_host.startswith(COMPOSE_ROOT_HOST) or parent_host.startswith(COMPOSE_VOLUME_ROOT_HOST) or in_extra) if parent_host != "/" else False
        if in_range:
            parent_container = _container_path(parent_host)
            if Path(parent_container).is_dir():
                parent = parent_host
    return {"items": result, "current": host_current, "parent": parent}


def mkdir_dir(host_path: str, name: str) -> dict:
    """在指定宿主机目录下创建子目录（仅限挂载范围内：COMPOSE_ROOT / COMPOSE_VOLUME_ROOT）"""
    if not host_path or not name:
        return {"success": False, "message": "路径与目录名不能为空"}
    name = os.path.basename(name.strip().strip("/"))
    if not name or name in (".", "..") or "/" in name:
        return {"success": False, "message": "目录名不合法"}
    base_container = _container_path(host_path.rstrip("/"))
    root_host = os.environ.get("COMPOSE_ROOT_HOST", "")
    vol_host = os.environ.get("COMPOSE_VOLUME_ROOT_HOST", "")
    allowed = False
    if root_host and host_path.startswith(root_host):
        allowed = True
    if vol_host and host_path.startswith(vol_host):
        allowed = True
    if not allowed:
        return {"success": False, "message": "只能在挂载目录范围内创建（COMPOSE_ROOT / COMPOSE_VOLUME_ROOT）"}
    target = Path(base_container) / name
    try:
        target.mkdir(parents=True, exist_ok=True)
        return {"success": True, "message": f"已创建 {os.path.join(host_path.rstrip('/'), name)}", "path": os.path.join(host_path.rstrip("/"), name)}
    except Exception as e:
        return {"success": False, "message": f"创建失败: {e}"}


def _fix_volume_paths(yaml_text: str, project_dir: str, host_project_dir: str, host_dirs: list | None = None) -> str:
    """把 YAML 中相对路径的 bind 卷（- ./data:/data）转成宿主机绝对路径

    Args:
        yaml_text: compose 内容
        project_dir: 容器内项目目录（用于在挂载内创建目录/改权限）
        host_project_dir: 宿主机真实项目目录（写进 compose 文件的路径前缀）
        host_dirs: 收集容器内目录路径（供创建目录/权限处理）
    """
    pattern = re.compile(r"^(\s*-\s+)(\.\.?/[^\s:]+)(:.*)?$", re.MULTILINE)

    def repl(m: re.Match) -> str:
        indent, first, rest = m.group(1), m.group(2), m.group(3) or ""
        host = os.path.normpath(os.path.join(host_project_dir, first))
        if host_dirs is not None:
            # 容器内对应路径（挂载目录内创建目录/改权限用）
            container_host = os.path.normpath(os.path.join(project_dir, first))
            host_dirs.append(container_host)
        return f"{indent}{host}{rest}"

    return pattern.sub(repl, yaml_text)


def start_create_project(target: str, yaml_text: str, project_name: str = "") -> dict:
    """异步创建 compose 项目：写 docker-compose.yml 后立即返回 task_id，后台执行 compose up -d

    Args:
        target: 宿主机目录（绝对路径如 /vol1/1000/Docker/myicon）或相对目录名（默认根下）
        yaml_text: docker-compose.yml 内容
        project_name: 可选，compose 项目名（-p 参数），默认与目录名一致

    Returns:
        {"success": True, "task_id": "...", "message": "..."} 或错误
    """
    project_dir, host_project_dir, label, err = _resolve_target(target)
    if err:
        return {"success": False, "message": err}

    yaml_text = str(yaml_text or "").strip()
    if not yaml_text:
        return {"success": False, "message": "compose 内容不能为空"}

    compose_file = os.path.join(project_dir, "docker-compose.yml")
    try:
        os.makedirs(project_dir, exist_ok=True)
        # 相对卷路径 → 宿主机绝对路径（容器内跑 compose 的必需处理）
        host_dirs: list = []
        fixed_yaml = _fix_volume_paths(yaml_text, project_dir, host_project_dir, host_dirs)
        # 创建相对卷目录并放开写权限（容器内服务可能以非 root 运行）
        for d in host_dirs:
            try:
                os.makedirs(d, exist_ok=True)
                os.chmod(d, 0o777)
            except Exception as e:
                logger.warning(f"创建卷目录失败 {d}: {e}")
        with open(compose_file, "w", encoding="utf-8") as f:
            f.write(fixed_yaml)
        os.chmod(compose_file, 0o666)  # 宿主用户可读可改

        # 构造命令（-p 项目名 + up -d 由任务线程补上）
        cmd = _compose_base_cmd()
        project_name = str(project_name or "").strip()
        if project_name:
            cmd = [*cmd, "-p", project_name]

        task_id = _new_task(cmd, project_dir)
        t = threading.Thread(target=_task_worker, args=(task_id,), daemon=True)
        t.start()
        return {"success": True, "task_id": task_id, "message": f"Compose 项目 {project_name or label} 创建任务已提交"}
    except RuntimeError as e:
        return {"success": False, "message": str(e)}
    except Exception as e:
        logger.exception("创建 compose 项目失败")
        return {"success": False, "message": f"创建失败: {str(e)}"}


def delete_compose_project(target: str) -> dict:
    """删除 compose 项目（docker compose down，停止并移除容器/网络，保留 compose 文件）

    Args:
        target: 宿主机目录（绝对路径）或相对目录名（默认根下）
    """
    if not target:
        return {"success": False, "message": "项目目录不能为空"}
    project_dir, host_project_dir, label, err = _resolve_target(target)
    if err:
        return {"success": False, "message": err}

    compose_file = os.path.join(project_dir, "docker-compose.yml")
    if not os.path.isfile(compose_file):
        return {"success": False, "message": f"目录 {label} 下没有 docker-compose.yml，无法用 compose 方式删除"}

    # 从容器 label 取真实 project 名并 -p 固定：
    # 否则 compose 会读文件顶层 name: 字段（可能与创建时的 -p 不一致），导致 "No resource found"
    real_project = _detect_project_name(project_dir, host_project_dir)
    args = []
    if real_project:
        args = ["-p", real_project]
    args.append("down")

    ok, output = _run_compose(args, workdir=project_dir)
    if ok:
        return {"success": True, "message": f"Compose 项目 {label} 已停止并移除容器（文件保留）", "output": output}
    return {"success": False, "message": f"删除失败: {output[-500:]}", "output": output}


def _detect_project_name(project_dir: str, host_project_dir: str) -> str:
    """从该目录下容器的 compose label 探测真实 project 名（优先容器内路径匹配，兼容宿主机路径 label）"""
    try:
        import docker
        client = docker.from_env()
        for c in client.containers.list(all=True):
            labels = c.attrs.get("Config", {}).get("Labels") or {}
            wd = labels.get("com.docker.compose.project.working_dir", "")
            if wd and (wd == project_dir or wd == host_project_dir):
                p = labels.get("com.docker.compose.project")
                if p:
                    return p
    except Exception as e:
        logger.warning(f"探测 project 名失败: {e}")
    return ""
