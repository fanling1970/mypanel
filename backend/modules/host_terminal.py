"""路由器宿主机终端（不是容器内终端）

原理：
- docker-compose.yml 里已配置 privileged: true + pid: host
- 因此容器内可以用 nsenter 进入宿主机 PID 1 的各个命名空间：
    nsenter -t 1 -m -u -i -n -p -- /bin/sh -c '...'
  这样执行的就是宿主机上的命令，看到的是路由器的真实文件系统和网络。

为什么不做真正的交互式 PTY 会话：
- 交互式 PTY 需要常驻 reader 线程 + 缓冲队列，长时间空闲容易泄漏、
  异常退出后 session 变成僵尸，面板上表现为"终端卡住再也不返回"。
- 这里采用"每条命令一次执行 + 服务端记住 cwd"的方式：
  每个请求是独立子进程，带超时，结束后一定回收，不会留下任何残留状态。
  对路由器运维（看日志、查端口、改配置、重启服务）足够用；
  vim/top 这类全屏交互程序不支持，会在输出里看到相应报错。
"""
import logging
import os
import shlex
import shutil
import subprocess
import threading
import time

logger = logging.getLogger(__name__)

# 单条命令最长执行时间（秒）。太长会占住工作线程，且面板上像卡死。
CMD_TIMEOUT = 30
# 单次返回的最大输出长度，防止 `cat /dev/zero` 之类把内存吃满
MAX_OUTPUT = 200 * 1024
# 默认起始目录
DEFAULT_CWD = "/"

# 每个会话的当前目录：{session_id: cwd}
_sessions: dict = {}
_sessions_lock = threading.Lock()

# 用来把 cwd 从输出里分离出来的标记，带随机串避免与真实输出碰撞
_CWD_MARK = "__MYPANEL_CWD_7f3a__"


class TerminalError(Exception):
    pass


def _nsenter_path() -> str:
    p = shutil.which("nsenter")
    if not p:
        raise TerminalError("容器内缺少 nsenter，无法进入宿主机命名空间")
    return p


def _enter_prefix() -> list:
    """构造进入宿主机命名空间的前缀命令"""
    return [_nsenter_path(), "-t", "1", "-m", "-u", "-i", "-n", "-p", "--"]


def _resolve_shell() -> str:
    for sh in ("/bin/sh", "/bin/ash", "/bin/bash"):
        # 注意：这里判断的是宿主机里的路径，容器内不一定有，
        # 所以只做字符串兜底，实际由宿主机决定能否执行。
        return sh
    return "/bin/sh"


def available() -> dict:
    """终端环境自检：面板上给出明确提示，而不是让用户面对空白等不到输出"""
    ok = True
    reason = ""
    if not shutil.which("nsenter"):
        ok = False
        reason = "容器内缺少 nsenter（需要 privileged + pid: host）"
    else:
        try:
            r = subprocess.run(_enter_prefix() + ["/bin/sh", "-c", "echo ok"],
                               capture_output=True, timeout=5)
            if r.returncode != 0:
                ok = False
                reason = (r.stderr or b"").decode("utf-8", "replace").strip()[:200] or "无法进入宿主机命名空间"
        except subprocess.TimeoutExpired:
            ok = False
            reason = "进入宿主机命名空间超时"
        except OSError as e:
            ok = False
            reason = str(e)
    return {"available": ok, "reason": reason}


def open_session(session_id: str) -> dict:
    info = available()
    if not info["available"]:
        raise TerminalError(info["reason"])
    with _sessions_lock:
        _sessions[session_id] = DEFAULT_CWD
    host = ""
    try:
        r = subprocess.run(_enter_prefix() + ["/bin/sh", "-c", "uname -a"],
                           capture_output=True, timeout=5)
        host = (r.stdout or b"").decode("utf-8", "replace").strip()
    except Exception:
        pass
    return {"success": True, "session_id": session_id, "cwd": DEFAULT_CWD, "host": host}


def close_session(session_id: str) -> dict:
    with _sessions_lock:
        _sessions.pop(session_id, None)
    return {"success": True}


def exec_command(session_id: str, command: str) -> dict:
    """在宿主机上执行一条命令，返回输出与执行后的 cwd"""
    if not command or not command.strip():
        with _sessions_lock:
            cwd = _sessions.get(session_id, DEFAULT_CWD)
        return {"success": True, "output": "", "cwd": cwd, "exit_code": 0}

    with _sessions_lock:
        cwd = _sessions.get(session_id, DEFAULT_CWD)

    # 命令整体交给宿主机 sh 执行；先 cd 到会话目录，再回显最终 pwd。
    # 用单引号包裹并转义，避免 cwd 里有空格/特殊字符时把命令拆坏。
    safe_cwd = cwd.replace("'", "'\\''")
    script = (
        "cd '%s' 2>/dev/null || cd /; "
        "{ %s\n } 2>&1; "
        "__rc=$?; printf '\\n%s%%s\\n' \"$(pwd)\"; exit $__rc"
    ) % (safe_cwd, command, _CWD_MARK)

    started = time.time()
    try:
        proc = subprocess.run(
            _enter_prefix() + [_resolve_shell(), "-c", script],
            capture_output=True, timeout=CMD_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise TerminalError("命令执行超时（%d 秒），已终止" % CMD_TIMEOUT)
    except OSError as e:
        raise TerminalError("命令执行失败：%s" % e)

    raw = (proc.stdout or b"") + (proc.stderr or b"")
    out = raw.decode("utf-8", "replace")

    # 拆出尾部的 cwd
    new_cwd = cwd
    idx = out.rfind(_CWD_MARK)
    if idx >= 0:
        tail = out[idx + len(_CWD_MARK):].strip().splitlines()
        if tail:
            cand = tail[0].strip()
            if cand.startswith("/"):
                new_cwd = cand
        out = out[:idx]
    out = out.rstrip("\n")

    if len(out) > MAX_OUTPUT:
        out = out[:MAX_OUTPUT] + "\n… 输出过长已截断"

    with _sessions_lock:
        _sessions[session_id] = new_cwd

    return {
        "success": True,
        "output": out,
        "cwd": new_cwd,
        "exit_code": proc.returncode,
        "elapsed_ms": int((time.time() - started) * 1000),
    }


def set_cwd(session_id: str, cwd: str) -> dict:
    """显式切换会话目录（前端点快捷路径时用）"""
    with _sessions_lock:
        _sessions[session_id] = cwd or DEFAULT_CWD
    return {"success": True, "cwd": cwd or DEFAULT_CWD}
