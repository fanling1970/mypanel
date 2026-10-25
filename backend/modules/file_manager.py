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

# ============ 在线编辑 / 预览 ============
# 超过这个大小一律拒绝在浏览器里打开（10MB 的文本足以卡死页面）
MAX_PREVIEW_BYTES = 10 * 1024 * 1024

# 允许在线查看/编辑的扩展名。只放纯文本类，
# 二进制（可执行文件、压缩包、镜像）不进去，避免读成乱码再写坏文件。
TEXT_EXTENSIONS = {
    # 配置 / 数据
    "conf", "cfg", "ini", "config", "properties", "env", "toml", "yaml", "yml",
    "json", "json5", "xml", "plist", "csv", "tsv", "lock", "list", "repo",
    # 脚本
    "sh", "bash", "zsh", "fish", "py", "pyw", "pl", "lua", "rb", "php",
    "js", "mjs", "cjs", "ts", "jsx", "tsx", "vue", "svelte", "css", "scss", "less",
    # 文档 / 文本
    "txt", "md", "markdown", "rst", "log", "text", "nfo", "diff", "patch",
    "html", "htm", "sql", "go", "rs", "java", "c", "h", "cpp", "hpp", "cs",
    "swift", "kt", "gradle", "dockerfile", "makefile", "gitignore", "dockerignore",
}

# 没有扩展名但通常是文本的文件名
TEXT_FILENAMES = {
    "dockerfile", "makefile", "readme", "license", "changelog", "authors",
    "gitignore", "dockerignore", "env", ".env", "hosts", "passwd", "group",
    "fstab", "crontab", "nginx.conf",
}


def is_editable_name(name: str) -> bool:
    """按文件名判断是否属于可在线编辑的纯文本类型"""
    if not name:
        return False
    low = name.lower()
    if low in TEXT_FILENAMES:
        return True
    if low.startswith(".env"):
        return True
    ext = low.rsplit(".", 1)[-1] if "." in low else ""
    return bool(ext) and ext in TEXT_EXTENSIONS


def _decode_text(raw: bytes) -> tuple[str, str]:
    """尝试解码为文本，返回 (内容, 编码)。全部失败则抛错"""
    for enc in ("utf-8-sig", "utf-8", "gb18030", "latin-1"):
        try:
            return raw.decode(enc), enc
        except (UnicodeDecodeError, LookupError):
            continue
    raise FileManagerError("此文件不支持预览")


def read_text(host_path: str) -> dict:
    """读取宿主机文本文件内容，供在线编辑器使用。

    三道校验，顺序刻意：先看类型（便宜），再看大小，最后才读盘。
    """
    cont, host_norm = to_container_path(host_path)
    if not os.path.exists(cont):
        raise FileManagerError("文件不存在")
    if os.path.isdir(cont):
        raise FileManagerError("该路径是目录")
    if not is_editable_name(os.path.basename(host_norm)):
        raise FileManagerError("此文件不支持预览")

    try:
        size = os.path.getsize(cont)
    except OSError:
        raise FileManagerError("无法读取文件信息")
    if size > MAX_PREVIEW_BYTES:
        raise FileManagerError("文件超过10兆，无法打开")

    try:
        with open(cont, "rb") as f:
            raw = f.read()
    except PermissionError:
        raise FileManagerError("没有权限读取该文件")
    except OSError as e:
        raise FileManagerError(f"读取失败：{e}")

    # NUL 字节基本只出现在二进制里，按“不支持预览”处理，避免把乱码写回去
    if b"\x00" in raw[:8192]:
        raise FileManagerError("此文件不支持预览")

    content, encoding = _decode_text(raw)
    crlf = content.count("\r\n")
    lf_only = content.count("\n") - crlf
    eol = "CRLF" if crlf > lf_only else "LF"

    st = os.stat(cont)
    return {
        "success": True,
        "path": host_norm,
        "name": os.path.basename(host_norm),
        "content": content.replace("\r\n", "\n"),
        "size": size,
        "encoding": encoding,
        "eol": eol,
        "mode": _mode_str(st),
        "mtime": int(st.st_mtime),
        "readonly": not os.access(cont, os.W_OK),
    }


def write_text(host_path: str, content: str, eol: str = "LF", encoding: str = "utf-8") -> dict:
    """写回宿主机文本文件。写前留一份 .bak，避免改坏配置后无法回退。"""
    cont, host_norm = to_container_path(host_path)
    if not os.path.exists(cont):
        raise FileManagerError("文件不存在")
    if os.path.isdir(cont):
        raise FileManagerError("该路径是目录")
    if not is_editable_name(os.path.basename(host_norm)):
        raise FileManagerError("此文件不支持编辑")
    if not os.access(cont, os.W_OK):
        raise FileManagerError("没有写入权限")

    data = content.replace("\r\n", "\n")
    if str(eol).upper() == "CRLF":
        data = data.replace("\n", "\r\n")

    enc = encoding if encoding in ("utf-8", "utf-8-sig", "gb18030", "latin-1") else "utf-8"
    payload = data.encode(enc, errors="surrogateescape") if enc != "utf-8-sig" else data.encode("utf-8-sig", errors="surrogateescape")

    # 保留原文件权限位：直接 open 会沿用已有 inode 的权限，这里仅做兜底校验
    try:
        st = os.stat(cont)
        backup_path = cont + ".mypanel.bak"
        try:
            with open(cont, "rb") as src, open(backup_path, "wb") as dst:
                dst.write(src.read())
            os.chmod(backup_path, stat.S_IMODE(st.st_mode))
        except OSError:
            pass

        with open(cont, "wb") as f:
            f.write(payload)
        try:
            os.chmod(cont, stat.S_IMODE(st.st_mode))
        except OSError:
            pass
    except PermissionError:
        raise FileManagerError("没有写入权限")
    except OSError as e:
        raise FileManagerError(f"保存失败：{e}")

    return {
        "success": True,
        "path": host_norm,
        "size": len(payload),
        "encoding": enc,
        "message": "已保存",
    }


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
                    "editable": (not is_dir) and is_editable_name(name),
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
