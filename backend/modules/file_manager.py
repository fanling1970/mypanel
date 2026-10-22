"""路由器宿主机文件管理（浏览的是宿主机文件，不是容器内文件）

原理：
- docker-compose.yml 里把宿主机根目录挂进容器：${HOST_ROOT:-/}:/host-root
- 面板所有文件操作都先把"宿主机绝对路径"翻译成"容器内挂载路径"，再落盘
- 因此列表/上传/下载/删除看到的都是路由器宿主机上的真实文件

降级策略（老版本 compose 没挂 /host-root 时）：
- 自动退回已有的 /host/compose、/host-vol1、EXTRA_MOUNTS 这几个挂载点，
  仍然可用，只是只能看到这些目录，不会崩。
"""
import logging
import os
import posixpath
import shutil
import stat
import time

logger = logging.getLogger(__name__)

# 宿主机根目录在容器内的挂载点（compose: ${HOST_ROOT:-/}:/host-root）
HOST_ROOT_CONTAINER = os.environ.get("HOST_ROOT_MOUNT", "/host-root").rstrip("/") or "/host-root"
# 该挂载点对应的宿主机真实路径，默认就是根目录
HOST_ROOT_HOST = os.environ.get("HOST_ROOT_HOST", "/").rstrip("/")

# 一级系统目录：允许浏览，但拒绝删除，避免一条命令把路由器搞坏
PROTECTED_FIRST_LEVEL = {
    "bin", "sbin", "lib", "lib64", "usr", "etc", "boot", "dev", "proc", "sys",
    "var", "run", "tmp", "root", "overlay", "rom", "opt",
}

MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024  # 单文件 2GB 上限


class FileManagerError(Exception):
    """可直接展示给用户的文件操作错误"""


# ============ 路径映射 ============

def _roots() -> list[dict]:
    """返回可浏览的根列表，按优先级排序。

    正常情况只有一个"根目录 /"；没有根挂载时退回 Compose 相关挂载点。
    """
    result: list[dict] = []
    if os.path.isdir(HOST_ROOT_CONTAINER):
        result.append({
            "id": "host-root",
            "name": "路由器根目录",
            "host": "/",
            "container": HOST_ROOT_CONTAINER,
        })

    # 降级：把已有的 compose 挂载点也暴露出来，方便老部署直接用
    try:
        from modules import compose_project as cp
        candidates = [
            ("compose", "Compose 根目录", cp.COMPOSE_ROOT_HOST, cp.COMPOSE_ROOT),
            ("volume", "数据盘", cp.COMPOSE_VOLUME_ROOT_HOST, cp.COMPOSE_VOLUME_ROOT),
        ]
        for pair in getattr(cp, "EXTRA_MOUNTS", []) or []:
            try:
                cand_host, cand_cont = pair
            except Exception:
                continue
            candidates.append(("extra:" + cand_host, cand_host, cand_host, cand_cont))

        for rid, name, host, cont in candidates:
            if not host or not cont or not os.path.isdir(cont):
                continue
            if host == "/":
                continue
            if result and host.startswith(result[0]["host"]) and result[0]["host"] == "/":
                # 根挂载已经能覆盖它，不再重复列
                continue
            if any(r["host"] == host for r in result):
                continue
            result.append({"id": rid, "name": name, "host": host, "container": cont})
    except Exception as e:  # pragma: no cover - 依赖模块缺失时不影响主流程
        logger.warning(f"收集降级挂载点失败: {e}")

    if not result:
        raise FileManagerError(
            "未检测到宿主机目录挂载。请在 docker-compose.yml 的 volumes 中增加 "
            "${HOST_ROOT:-/}:/host-root 并重建容器后使用。"
        )
    return result


def get_roots() -> dict:
    """根列表 + 磁盘占用，供前端面包屑初始化和磁盘信息显示"""
    roots = []
    for r in _roots():
        item = dict(r)
        try:
            usage = shutil.disk_usage(r["container"])
            item["total"] = usage.total
            item["used"] = usage.used
            item["free"] = usage.free
        except Exception:
            item["total"] = item["used"] = item["free"] = None
        roots.append(item)
    return {"roots": roots}


def normalize_host_path(path: str) -> str:
    """规范化宿主机绝对路径，挡住 ../ 逃逸"""
    if path is None:
        path = ""
    path = str(path).strip()
    if not path or path == "/":
        return "/"
    if not path.startswith("/"):
        raise FileManagerError("路径必须以 / 开头")
    norm = posixpath.normpath(path)
    if not norm.startswith("/"):
        raise FileManagerError("非法路径")
    return norm


def _match_root(host_path: str) -> dict:
    roots = _roots()
    # 最长前缀优先，保证 /vol1 这类子挂载点先命中
    best = None
    for r in roots:
        rh = r["host"].rstrip("/") or "/"
        if rh == "/":
            hit = True
        else:
            hit = host_path == rh or host_path.startswith(rh + "/")
        if hit:
            if best is None or len(rh) > len(best["host"].rstrip("/") or "/"):
                best = r
    if not best:
        raise FileManagerError("路径不在面板可访问的挂载范围内，请检查 docker-compose.yml 的挂载配置")
    return best


def to_container_path(host_path: str) -> tuple[str, str]:
    """宿主机路径 → (容器内路径, 规范化宿主机路径)"""
    host_path = normalize_host_path(host_path)
    root = _match_root(host_path)
    root_host = root["host"].rstrip("/") or "/"
    root_cont = root["container"].rstrip("/")
    if root_host == "/":
        rel = "" if host_path == "/" else host_path
    else:
        rel = host_path[len(root_host):] if host_path != root_host else ""
    cont = root_cont + rel
    if not cont:
        cont = root_cont
    # 二次校验：真实路径必须仍在挂载点内，避免符号链接/规范化差异导致越界
    real_cont = os.path.realpath(cont)
    real_root = os.path.realpath(root_cont)
    if not (real_cont == real_root or real_cont.startswith(real_root + os.sep)):
        raise FileManagerError("目标路径超出可访问范围")
    return cont, host_path


def _mode_str(st) -> str:
    """八进制权限位，如 755"""
    return format(stat.S_IMODE(st.st_mode), "03o")


def _is_protected(host_path: str) -> bool:
    parts = [p for p in host_path.split("/") if p]
    return len(parts) == 1 and parts[0] in PROTECTED_FIRST_LEVEL


# ============ 列表 ============

def list_dir(host_path: str, show_hidden: bool = True) -> dict:
    cont, host_norm = to_container_path(host_path)
    if not os.path.exists(cont):
        raise FileManagerError("目录不存在")
    if not os.path.isdir(cont):
        raise FileManagerError("该路径不是目录")

    entries = []
    try:
        with os.scandir(cont) as it:
            for e in it:
                name = e.name
                if not show_hidden and name.startswith("."):
                    continue
                try:
                    st = e.stat(follow_symlinks=False)
                except OSError:
                    continue
                is_link = e.is_symlink()
                is_dir = e.is_dir(follow_symlinks=True)
                size = None
                if not is_dir:
                    size = st.st_size
                entries.append({
                    "name": name,
                    "path": (host_norm.rstrip("/") + "/" + name) if host_norm != "/" else "/" + name,
                    "is_dir": bool(is_dir),
                    "is_link": bool(is_link),
                    "size": size,
                    "mtime": int(st.st_mtime),
                    "mode": _mode_str(st),
                    "owner": getattr(st, "st_uid", None),
                })
    except PermissionError:
        raise FileManagerError("没有权限读取该目录")

    # 目录在前，然后按名称排序（自然顺）
    entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))

    # 父目录（根挂载点处不再向上）
    parent = None
    roots = _roots()
    root_hosts = {(r["host"].rstrip("/") or "/") for r in roots}
    if host_norm != "/" and host_norm not in root_hosts:
        parent = posixpath.dirname(host_norm) or "/"

    disk = None
    try:
        usage = shutil.disk_usage(cont)
        disk = {"total": usage.total, "used": usage.used, "free": usage.free}
    except Exception:
        pass

    return {
        "path": host_norm,
        "parent": parent,
        "entries": entries,
        "disk": disk,
        "roots": roots,
        "readonly": False,
    }


# ============ 写操作 ============

def _check_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise FileManagerError("名称不能为空")
    if name in (".", ".."):
        raise FileManagerError("名称非法")
    if "/" in name or "\x00" in name:
        raise FileManagerError("名称不能包含 / 字符")
    return name


def make_dir(host_path: str, name: str) -> dict:
    parent_cont, host_norm = to_container_path(host_path)
    name = _check_name(name)
    target = os.path.join(parent_cont, name)
    host_target = (host_norm.rstrip("/") + "/" + name) if host_norm != "/" else "/" + name
    if os.path.exists(target):
        raise FileManagerError("同名文件或文件夹已存在")
    try:
        os.makedirs(target, exist_ok=False)
    except PermissionError:
        raise FileManagerError("没有权限在该目录创建文件夹")
    except OSError as e:
        raise FileManagerError(f"创建失败: {e}")
    return {"success": True, "path": host_target}


def make_file(host_path: str, name: str, content: str = "") -> dict:
    parent_cont, host_norm = to_container_path(host_path)
    name = _check_name(name)
    target = os.path.join(parent_cont, name)
    host_target = (host_norm.rstrip("/") + "/" + name) if host_norm != "/" else "/" + name
    if os.path.exists(target):
        raise FileManagerError("同名文件或文件夹已存在")
    try:
        with open(target, "x", encoding="utf-8") as f:
            f.write(content or "")
        os.chmod(target, 0o644)
    except PermissionError:
        raise FileManagerError("没有权限在该目录创建文件")
    except OSError as e:
        raise FileManagerError(f"创建失败: {e}")
    return {"success": True, "path": host_target}


def rename(host_path: str, new_name: str) -> dict:
    cont, host_norm = to_container_path(host_path)
    new_name = _check_name(new_name)
    if host_norm == "/" or _is_protected(host_norm):
        raise FileManagerError("该目录受保护，不允许重命名")
    if not os.path.exists(cont):
        raise FileManagerError("文件不存在")
    parent = os.path.dirname(cont)
    target = os.path.join(parent, new_name)
    if os.path.exists(target):
        raise FileManagerError("同名文件或文件夹已存在")
    try:
        os.rename(cont, target)
    except PermissionError:
        raise FileManagerError("没有权限重命名")
    except OSError as e:
        raise FileManagerError(f"重命名失败: {e}")
    host_target = posixpath.join(posixpath.dirname(host_norm), new_name)
    return {"success": True, "path": host_target}


def set_permission(host_path: str, mode: str) -> dict:
    cont, host_norm = to_container_path(host_path)
    mode = (mode or "").strip()
    try:
        mode_int = int(mode, 8)
    except ValueError:
        raise FileManagerError("权限格式错误，请使用八进制如 755")
    if mode_int < 0 or mode_int > 0o7777:
        raise FileManagerError("权限超出范围")
    if not os.path.exists(cont):
        raise FileManagerError("文件不存在")
    try:
        os.chmod(cont, mode_int)
    except PermissionError:
        raise FileManagerError("没有权限修改权限")
    except OSError as e:
        raise FileManagerError(f"修改权限失败: {e}")
    return {"success": True, "mode": mode}


def _iter_targets(paths):
    for p in paths or []:
        p = normalize_host_path(p)
        if p == "/":
            raise FileManagerError("不允许操作根目录")
        if _is_protected(p):
            raise FileManagerError(f"{p} 是系统目录，已阻止操作以免破坏路由器")
        yield p


def delete(paths: list[str]) -> dict:
    deleted, failed = [], []
    for host_norm in _iter_targets(paths):
        try:
            cont, _ = to_container_path(host_norm)
            if not os.path.exists(cont) and not os.path.islink(cont):
                failed.append({"path": host_norm, "error": "不存在"})
                continue
            if os.path.isdir(cont) and not os.path.islink(cont):
                shutil.rmtree(cont)
            else:
                os.remove(cont)
            deleted.append(host_norm)
        except FileManagerError as e:
            failed.append({"path": host_norm, "error": str(e)})
        except PermissionError:
            failed.append({"path": host_norm, "error": "没有权限删除"})
        except OSError as e:
            failed.append({"path": host_norm, "error": str(e)})
    if not deleted and failed:
        raise FileManagerError("；".join(f"{f['path']}: {f['error']}" for f in failed))
    return {"success": True, "deleted": deleted, "failed": failed}


def _transfer(paths: list[str], dest: str, move: bool) -> dict:
    dest_cont, dest_host = to_container_path(dest)
    if not os.path.isdir(dest_cont):
        raise FileManagerError("目标目录不存在")
    done, failed = [], []
    for host_norm in _iter_targets(paths):
        try:
            src_cont, _ = to_container_path(host_norm)
            if not os.path.exists(src_cont):
                failed.append({"path": host_norm, "error": "不存在"})
                continue
            name = os.path.basename(src_cont.rstrip("/"))
            target = os.path.join(dest_cont, name)
            if os.path.abspath(src_cont) == os.path.abspath(target):
                failed.append({"path": host_norm, "error": "源与目标相同"})
                continue
            if os.path.exists(target):
                failed.append({"path": host_norm, "error": "目标已存在同名项"})
                continue
            if move:
                try:
                    shutil.move(src_cont, target)
                except OSError:
                    # 跨分区时 shutil.move 会先复制再删除
                    raise
            else:
                if os.path.isdir(src_cont) and not os.path.islink(src_cont):
                    shutil.copytree(src_cont, target, symlinks=True)
                else:
                    shutil.copy2(src_cont, target, follow_symlinks=False)
            done.append(host_norm)
        except FileManagerError as e:
            failed.append({"path": host_norm, "error": str(e)})
        except PermissionError:
            failed.append({"path": host_norm, "error": "没有权限"})
        except OSError as e:
            failed.append({"path": host_norm, "error": str(e)})
    if not done and failed:
        raise FileManagerError("；".join(f"{f['path']}: {f['error']}" for f in failed))
    return {"success": True, "done": done, "failed": failed}


def move(paths: list[str], dest: str) -> dict:
    return _transfer(paths, dest, move=True)


def copy(paths: list[str], dest: str) -> dict:
    return _transfer(paths, dest, move=False)


def save_upload(host_dir: str, filename: str, fileobj, overwrite: bool = False) -> dict:
    """保存上传文件到宿主机目录"""
    dir_cont, host_norm = to_container_path(host_dir)
    if not os.path.isdir(dir_cont):
        raise FileManagerError("目标目录不存在")
    safe_name = os.path.basename((filename or "").replace("\\", "/")).strip()
    safe_name = _check_name(safe_name)
    target = os.path.join(dir_cont, safe_name)
    if os.path.exists(target) and not overwrite:
        raise FileManagerError(f"{safe_name} 已存在")
    written = 0
    try:
        with open(target, "wb") as f:
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    f.close()
                    try:
                        os.remove(target)
                    except Exception:
                        pass
                    raise FileManagerError("文件超过 2GB 上限，请通过 SSH/SMB 上传")
                f.write(chunk)
        os.chmod(target, 0o644)
    except PermissionError:
        raise FileManagerError("没有权限写入该目录")
    except OSError as e:
        raise FileManagerError(f"上传失败: {e}")
    host_target = (host_norm.rstrip("/") + "/" + safe_name) if host_norm != "/" else "/" + safe_name
    return {"success": True, "path": host_target, "size": written}


def resolve_download(host_path: str) -> tuple[str, str]:
    """返回 (容器内文件路径, 下载文件名)"""
    cont, host_norm = to_container_path(host_path)
    if not os.path.isfile(cont):
        raise FileManagerError("文件不存在")
    return cont, os.path.basename(cont)
