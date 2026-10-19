"""MyPanel - Docker容器调度管理 FastAPI主应用（基于 docker-butler 源码私有化改造，适配 ARM 路由器）"""

APP_VERSION = "2.7.3-mypanel"

import asyncio
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import pyotp

from fastapi import FastAPI, HTTPException, Query, Request, Response, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel

from modules import database as db
from modules import docker_client
from modules import compose_project
from modules import scheduler as sched
from modules import update_checker

# ============ JWT Token 管理 ============
# 简单的 token 存储：{token: {"username": str, "expires": float}}
_token_store: dict[str, dict] = {}
TOKEN_EXPIRE_SECONDS = 86400 * 7  # 7天过期


def generate_token(username: str) -> str:
    """生成随机 token"""
    token = secrets.token_hex(32)
    _token_store[token] = {
        "username": username,
        "expires": time.time() + TOKEN_EXPIRE_SECONDS,
    }
    return token


def verify_token(token: str) -> Optional[str]:
    """验证 token，返回用户名或 None"""
    info = _token_store.get(token)
    if not info:
        return None
    if time.time() > info["expires"]:
        _token_store.pop(token, None)
        return None
    return info["username"]


def revoke_token(token: str):
    """注销 token"""
    _token_store.pop(token, None)

# ============ 日志配置 ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ============ 后台自动检查镜像更新 ============
_auto_check_task: Optional[asyncio.Task] = None


def _get_auto_check_interval() -> int:
    """从数据库读取自动检查间隔（同步封装，用于async场景）"""
    import sqlite3
    try:
        conn = sqlite3.connect(db.DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT value FROM app_settings WHERE key='auto_check_interval'")
        row = cursor.fetchone()
        conn.close()
        if row:
            return int(row["value"]) * 3600
    except Exception:
        pass
    return 3600  # 默认1小时


def _get_check_delay_seconds() -> int:
    """从数据库读取启动延迟秒数"""
    import sqlite3
    try:
        conn = sqlite3.connect(db.DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT value FROM app_settings WHERE key='check_delay_seconds'")
        row = cursor.fetchone()
        conn.close()
        if row:
            return int(row["value"])
    except Exception:
        pass
    return 60  # 默认60秒


def _is_auto_check_enabled() -> bool:
    """从数据库读取自动检查是否开启"""
    import sqlite3
    try:
        conn = sqlite3.connect(db.DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT value FROM app_settings WHERE key='auto_check_enabled'")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row["value"] == "1"
    except Exception:
        pass
    return False  # 默认关闭（本私有版移除启动自动检查）


async def _auto_check_updates_loop():
    """后台循环：定期自动检查所有容器镜像更新"""
    # 从设置中读取启动延迟
    delay = _get_check_delay_seconds()
    logger.info(f"[自动检查] 将在 {delay} 秒后开始首次检查")
    await asyncio.sleep(delay)
    while True:
        try:
            # 读取配置：是否开启自动检查
            if not _is_auto_check_enabled():
                logger.info("[自动检查] 已关闭，跳过本次检查")
                await db.add_schedule_log(
                    schedule_id=0, schedule_name="自动检查",
                    container_name="全部", action="check-update",
                    status="success", message="自动检查已关闭，跳过本次检查"
                )
            else:
                logger.info("[自动检查] 开始检查镜像更新...")
                await db.add_schedule_log(
                    schedule_id=0, schedule_name="自动检查",
                    container_name="全部", action="check-update",
                    status="success", message="自动检查开始..."
                )
                client = await asyncio.to_thread(docker_client.get_client)
                # 获取容器更新检查偏好，跳过未开启的容器
                settings = await db.get_container_update_settings()
                results = await asyncio.to_thread(update_checker.check_all_updates, client, settings)
                logger.info("[自动检查] 镜像更新检查完成")
                # 写入执行日志
                await _log_check_results(results, "自动检查")
        except Exception as e:
            logger.error(f"[自动检查] 镜像更新检查失败: {e}")
            await db.add_schedule_log(
                schedule_id=0, schedule_name="自动检查",
                container_name="全部", action="check-update",
                status="failed", message=f"检查失败: {e}"
            )
        # 读取配置：检查间隔
        interval = _get_auto_check_interval()
        await asyncio.sleep(interval)


async def _log_check_results(results: dict, source: str = "手动检查"):
    """将检查结果写入执行日志"""
    if not results:
        await db.add_schedule_log(
            schedule_id=0, schedule_name=source,
            container_name="全部", action="check-update",
            status="success", message="没有需要检查的容器"
        )
        return
    has_update = sum(1 for r in results.values() if r.get("has_update") is True)
    up_to_date = sum(1 for r in results.values() if r.get("has_update") is False)
    unknown = sum(1 for r in results.values() if r.get("has_update") is None)
    total = len(results)
    message = f"共检查{total}个容器: {has_update}个有更新, {up_to_date}个最新, {unknown}个未知"
    await db.add_schedule_log(
        schedule_id=0, schedule_name=source,
        container_name="全部", action="check-update",
        status="success", message=message
    )


# ============ 应用生命周期 ============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动与关闭"""
    global _auto_check_task
    logger.info("初始化数据库...")
    await db.init_db()

    logger.info("加载定时任务...")
    await sched.load_all_schedules()

    logger.info("启动调度器...")
    sched.start_scheduler()

    # 初始化更新检查模块的DB路径（用于同步持久化）
    update_checker.init_db_path(db.DB_PATH)

    # 从数据库加载上次检查结果到内存缓存
    try:
        saved_results = await db.get_all_update_check_results()
        for container_name, r in saved_results.items():
            image = r.get("image", "")
            if image:
                update_checker._update_cache[image] = {
                    "has_update": r.get("has_update"),
                    "error": r.get("error"),
                    "checked_at": r.get("checked_at", ""),
                }
        logger.info(f"从数据库加载了 {len(saved_results)} 条更新检查结果缓存")
    except Exception as e:
        logger.warning(f"加载更新检查缓存失败: {e}")

    # 本私有版：已移除启动后的后台自动检查更新任务
    logger.info("后台自动检查镜像更新：已禁用（启动不再自动检查）")

    logger.info("MyPanel 应用已启动")
    yield

    logger.info("停止后台自动检查...")
    if _auto_check_task:
        _auto_check_task.cancel()
    logger.info("停止调度器...")
    sched.stop_scheduler()
    logger.info("应用已关闭")


app = FastAPI(title="MyPanel", version=APP_VERSION, lifespan=lifespan)

# 静态文件
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# ============ 认证中间件 ============
# 不需要认证的路径前缀
_AUTH_WHITELIST = {"/", "/api/auth/login", "/api/auth/verify", "/api/app-info"}
_AUTH_WHITELIST_PREFIXES = ("/static",)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """HTTP 认证中间件：拦截所有 /api/ 请求（白名单除外）"""
    path = request.url.path

    # 白名单路径放行
    if path in _AUTH_WHITELIST:
        return await call_next(request)
    for prefix in _AUTH_WHITELIST_PREFIXES:
        if path.startswith(prefix):
            return await call_next(request)

    # 只对 /api/ 路径做认证
    if path.startswith("/api/"):
        auth_header = request.headers.get("Authorization", "")
        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        # SSE 流式请求不支持自定义 header，从查询参数获取 token
        if not token:
            token = request.query_params.get("token")
        if not token:
            return JSONResponse(status_code=401, content={"detail": "未登录"})
        username = verify_token(token)
        if not username:
            return JSONResponse(status_code=401, content={"detail": "登录已过期，请重新登录"})
        # 将用户名注入 request state
        request.state.username = username

    return await call_next(request)


# ============ 认证 API ============

class LoginRequest(BaseModel):
    username: str
    password: str
    code: str = ""          # 2FA 动态验证码
    trust_device: bool = False   # 登录成功后信任此设备（30天内免验证码）
    device_token: str = ""      # 已信任设备的 token


class TwoFactorCodeRequest(BaseModel):
    code: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    """用户登录（2FA 开启时需验证码或设备 token）"""
    user = await db.authenticate_user(req.username, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    twofa_enabled = await db.get_setting("2fa_enabled") == "1"
    if not twofa_enabled:
        token = generate_token(user["username"])
        return {
            "success": True,
            "data": {
                "token": token,
                "username": user["username"],
            }
        }

    # 2FA 已开启：先验设备 token（信任设备免验证码），再验动态码
    if req.device_token and await db.verify_device_token(req.device_token):
        token = generate_token(user["username"])
        return {
            "success": True,
            "data": {
                "token": token,
                "username": user["username"],
            }
        }

    if req.code:
        secret = await db.get_setting("2fa_secret")
        if secret and pyotp.TOTP(secret).verify(req.code.strip()):
            token = generate_token(user["username"])
            resp = {
                "success": True,
                "data": {
                    "token": token,
                    "username": user["username"],
                }
            }
            if req.trust_device:
                resp["data"]["device_token"] = await db.create_device_token()
            return resp

    # 密码正确但缺少/错误验证码 → 要求输入验证码
    return {
        "success": False,
        "data": {"2fa_required": True},
        "message": "请输入验证码",
    }


@app.get("/api/auth/2fa/status")
async def get_2fa_status(request: Request):
    """查询 2FA 状态（登录后）"""
    if not getattr(request.state, "username", None):
        raise HTTPException(status_code=401, detail="未登录")
    enabled = await db.get_setting("2fa_enabled") == "1"
    has_secret = bool(await db.get_setting("2fa_secret"))
    return {"success": True, "data": {"enabled": enabled, "has_secret": has_secret}}


@app.post("/api/auth/2fa/setup")
async def setup_2fa(request: Request):
    """生成 2FA 配置（secret + otpauth + 二维码），未确认前不生效"""
    if not getattr(request.state, "username", None):
        raise HTTPException(status_code=401, detail="未登录")
    secret = pyotp.random_base32()
    otpauth = pyotp.totp.TOTP(secret).provisioning_uri(name="MyPanel", issuer_name="mypanel")
    # 存 secret（enabled 仍为 0，确认后才开启）
    await db.set_setting("2fa_secret", secret)
    # 生成二维码（base64 PNG）
    import base64
    import io
    import qrcode
    img = qrcode.make(otpauth)
    buf = io.BytesIO()
    img.save(buf, "PNG")
    qr_b64 = base64.b64encode(buf.getvalue()).decode()
    return {"success": True, "data": {"secret": secret, "otpauth": otpauth, "qr_base64": qr_b64}}


@app.post("/api/auth/2fa/confirm")
async def confirm_2fa(req: TwoFactorCodeRequest, request: Request):
    """输入验证码确认开启 2FA"""
    if not getattr(request.state, "username", None):
        raise HTTPException(status_code=401, detail="未登录")
    secret = await db.get_setting("2fa_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="请先获取 2FA 配置")
    if not pyotp.TOTP(secret).verify(req.code.strip()):
        raise HTTPException(status_code=400, detail="验证码错误")
    await db.set_setting("2fa_enabled", "1")
    return {"success": True, "message": "2FA 已开启"}


@app.post("/api/auth/2fa/disable")
async def disable_2fa(req: TwoFactorCodeRequest, request: Request):
    """输入当前验证码关闭 2FA"""
    if not getattr(request.state, "username", None):
        raise HTTPException(status_code=401, detail="未登录")
    secret = await db.get_setting("2fa_secret")
    if not secret or not pyotp.TOTP(secret).verify(req.code.strip()):
        raise HTTPException(status_code=400, detail="验证码错误")
    await db.set_setting("2fa_enabled", "0")
    await db.set_setting("2fa_secret", "")
    await db.clear_trusted_devices()
    return {"success": True, "message": "2FA 已关闭"}


@app.get("/api/auth/verify")
async def verify_auth(request: Request):
    """验证 token 是否有效"""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else None
    if token:
        username = verify_token(token)
        if username:
            user = await db.get_user(username)
            return {"success": True, "data": {"username": username}}
    return {"success": False, "data": None}


@app.put("/api/auth/password")
async def change_password(req: ChangePasswordRequest, request: Request):
    """修改密码"""
    username = getattr(request.state, "username", None)
    if not username:
        raise HTTPException(status_code=401, detail="未登录")
    result = await db.change_password(username, req.old_password, req.new_password)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return {"success": True, "message": result["message"]}


@app.post("/api/auth/logout")
async def logout(request: Request):
    """退出登录"""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else None
    if token:
        revoke_token(token)
    return {"success": True, "message": "已退出"}


# ============ 页面路由 ============

@app.get("/")
async def index():
    """首页"""
    # 不缓存 HTML，保证面板升级后刷新即新页面（浏览器强缓存会导致旧界面/旧功能）
    resp = FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


# ============ Docker信息 ============

@app.get("/api/docker/info")
async def get_docker_info():
    """获取Docker信息"""
    result = await asyncio.to_thread(docker_client.test_connection)
    return result


@app.get("/api/app-info")
async def get_app_info():
    """获取应用信息（版本号等）"""
    version = APP_VERSION
    # 尝试从 manifest 动态读取（若路径正确则覆盖默认值）
    try:
        manifest_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "manifest")
        if not os.path.exists(manifest_path):
            manifest_path = "/app/manifest"
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f:
                for line in f:
                    if line.strip().startswith("version="):
                        v = line.strip().split("=", 1)[1].strip()
                        if v:
                            version = v
                        break
    except Exception:
        pass
    return {"success": True, "data": {"version": version}}


# ============ 容器管理 API ============

@app.get("/api/containers")
async def list_containers():
    """获取容器列表（快速返回基本信息，不含stats）"""
    try:
        containers = await asyncio.to_thread(docker_client.list_containers, True)
        # 附加更新检查偏好 + 基本信息（运行时长/IP/健康状态）
        settings = await db.get_container_update_settings()
        for c in containers:
            c["check_update_enabled"] = settings.get(c["name"], 1)
            try:
                docker_client.enrich_container_info(c)
            except Exception:
                pass
            # stats 不在此处获取，由前端异步请求 /api/containers/stats
            c["stats"] = None
        return {"success": True, "data": containers}
    except Exception as e:
        logger.error(f"获取容器列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/containers/stats")
async def get_all_containers_stats():
    """批量获取所有运行中容器的实时统计（并行获取）"""
    try:
        containers = await asyncio.to_thread(docker_client.list_containers, False)
        running = [c for c in containers if c["status"] == "running"]
        # 并行获取所有运行中容器的stats
        tasks = []
        for c in running:
            tasks.append(asyncio.to_thread(docker_client.get_container_stats, c["id"]))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        stats_map = {}
        for c, result in zip(running, results):
            if isinstance(result, Exception) or result is None:
                stats_map[c["id"]] = None
            else:
                stats_map[c["id"]] = result
        return {"success": True, "data": stats_map}
    except Exception as e:
        logger.error(f"批量获取容器统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/compose/stats")
async def get_all_compose_stats():
    """批量获取所有 Compose 项目的内存占用（并行获取，用于列表页内存列异步填充）"""
    try:
        memory_map = await asyncio.to_thread(docker_client.get_all_compose_memory)
        return {"success": True, "data": memory_map}
    except Exception as e:
        logger.error(f"批量获取 Compose 内存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/containers/{container_id}/info")
async def get_container_info(container_id: str):
    """获取单个容器信息（附带更新检查偏好）"""
    result = await asyncio.to_thread(docker_client.get_container, container_id)
    if not result:
        raise HTTPException(status_code=404, detail="容器不存在")
    settings = await db.get_container_update_settings()
    result["check_update_enabled"] = settings.get(result.get("name", ""), 1)
    return {"success": True, "data": result}


@app.get("/api/containers/update-settings")
async def get_update_settings():
    """获取所有容器的更新检查偏好"""
    settings = await db.get_container_update_settings()
    return {"success": True, "data": settings}


@app.put("/api/containers/{container_name}/update-setting")
async def set_update_setting(container_name: str, check_enabled: int = Query(..., ge=0, le=1)):
    """设置单个容器的更新检查偏好"""
    result = await db.set_container_update_setting(container_name, check_enabled)
    return {"success": True, "data": result}


@app.put("/api/containers/update-settings/batch")
async def set_all_update_settings(check_enabled: int = Query(..., ge=0, le=1)):
    """批量设置所有容器的更新检查偏好"""
    containers = await asyncio.to_thread(docker_client.list_containers, True)
    names = [c["name"] for c in containers]
    result = await db.set_all_container_update_settings(names, check_enabled)
    # 更新内存中的容器列表
    for c in containers:
        c["check_update_enabled"] = check_enabled
    return {"success": True, "data": result}


@app.post("/api/containers/{container_id}/start")
async def start_container(container_id: str):
    """启动容器"""
    result = await asyncio.to_thread(docker_client.start_container, container_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/containers/{container_id}/stop")
async def stop_container(container_id: str):
    """停止容器"""
    result = await asyncio.to_thread(docker_client.stop_container, container_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/containers/{container_id}/restart")
async def restart_container(container_id: str):
    """重启容器"""
    result = await asyncio.to_thread(docker_client.restart_container, container_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


class ContainerCreateRequest(BaseModel):
    """创建容器请求"""
    name: str = ""
    image: str = ""
    command: str = ""
    ports: list[str] = []
    env: list[str] = []
    volumes: list[str] = []
    network_mode: str = "bridge"
    restart_policy: str = "no"
    privileged: bool = False
    auto_start: bool = True


@app.post("/api/containers")
async def create_container(req: ContainerCreateRequest):
    """创建并启动容器"""
    result = await asyncio.to_thread(docker_client.create_container, req.model_dump())
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.delete("/api/containers/{container_id}")
async def remove_container(container_id: str, force: int = Query(0, ge=0, le=1)):
    """删除容器（force=1 强制删除运行中的容器）"""
    result = await asyncio.to_thread(docker_client.remove_container, container_id, force=bool(force))
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/containers/{container_id}/update")
async def update_container(container_id: str):
    """更新容器（拉取最新镜像并重建）- 后台异步执行，返回进度key"""
    if docker_client.is_updating(container_id):
        raise HTTPException(status_code=409, detail="该容器正在更新中，请等待完成")

    # 路由层直接拦截对自己执行容器级更新（后台线程拦截会先返回"已开始"，体验差）
    info = await asyncio.to_thread(docker_client.get_container, container_id)
    if info and docker_client.is_self_name(info.get("name")):
        raise HTTPException(status_code=400, detail="不能对自己执行容器级更新（会中断面板），请使用'更新自己'功能")

    # 获取容器名用于日志
    container_name = info["name"] if info else container_id

    async def _bg_update():
        try:
            result = await asyncio.to_thread(
                docker_client.update_container, container_id, container_id
            )
            status = "success" if result["success"] else "failed"
            await db.add_schedule_log(
                schedule_id=0, schedule_name="手动操作",
                container_name=container_name, action="update",
                status=status, message=result.get("message", "")
            )
        except Exception as e:
            logger.error(f"容器更新后台任务异常: {e}")
            try:
                await db.add_schedule_log(
                    schedule_id=0, schedule_name="手动操作",
                    container_name=container_name, action="update",
                    status="failed", message=str(e)
                )
            except Exception:
                pass

    asyncio.create_task(_bg_update())
    return {"success": True, "started": True, "progress_key": container_id, "message": "更新已开始"}


@app.get("/api/update-progress/{target_key}")
async def get_update_progress(target_key: str):
    """查询更新进度"""
    progress = docker_client.get_update_progress(target_key)
    if progress is None:
        raise HTTPException(status_code=404, detail="无进度信息")
    return {"success": True, "data": progress}


@app.delete("/api/update-progress/{target_key}")
async def clear_update_progress(target_key: str):
    """清除更新进度"""
    docker_client.clear_update_progress(target_key)
    return {"success": True}


# ============ 定时任务 API ============

class ScheduleCreate(BaseModel):
    name: str
    container_id: str
    container_name: str
    action: str  # start / stop / restart / update
    cron_expression: str
    enabled: int = 1
    target_type: str = "container"  # container / compose


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    container_id: Optional[str] = None
    container_name: Optional[str] = None
    action: Optional[str] = None
    cron_expression: Optional[str] = None
    enabled: Optional[int] = None
    target_type: Optional[str] = None


@app.get("/api/schedules")
async def list_schedules():
    """获取所有定时任务"""
    schedules = await db.list_schedules()
    # 附加下次执行时间
    scheduler = sched.get_scheduler()
    for s in schedules:
        job_id = f"schedule_{s['id']}"
        job = scheduler.get_job(job_id)
        s["next_run"] = str(job.next_run_time) if job and job.next_run_time else None
    return {"success": True, "data": schedules}


@app.post("/api/schedules")
async def create_schedule(req: ScheduleCreate):
    """创建定时任务"""
    # 验证cron表达式
    try:
        sched.parse_cron(req.cron_expression)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 验证action
    if req.action not in ("start", "stop", "restart", "update"):
        raise HTTPException(status_code=400, detail="action必须是start/stop/restart/update")

    try:
        schedule = await db.create_schedule(req.model_dump())
    except Exception as e:
        logger.error(f"创建定时任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建失败: {str(e)}")

    # 如果启用则加入调度器
    if schedule["enabled"]:
        await sched.add_job(
            schedule_id=schedule["id"],
            container_id=schedule["container_id"],
            container_name=schedule["container_name"],
            action=schedule["action"],
            cron_expression=schedule["cron_expression"],
            target_type=schedule.get("target_type", "container"),
        )

    return {"success": True, "data": schedule}


@app.put("/api/schedules/{schedule_id}")
async def update_schedule(schedule_id: int, req: ScheduleUpdate):
    """更新定时任务"""
    existing = await db.get_schedule(schedule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="定时任务不存在")

    # 验证cron表达式
    if req.cron_expression:
        try:
            sched.parse_cron(req.cron_expression)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    data = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        schedule = await db.update_schedule(schedule_id, data)
    except Exception as e:
        logger.error(f"更新定时任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")

    # 重新加载调度任务
    await sched.remove_job(schedule_id)
    if schedule["enabled"]:
        await sched.add_job(
            schedule_id=schedule["id"],
            container_id=schedule["container_id"],
            container_name=schedule["container_name"],
            action=schedule["action"],
            cron_expression=schedule["cron_expression"],
            target_type=schedule.get("target_type", "container"),
        )

    return {"success": True, "data": schedule}


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: int):
    """删除定时任务"""
    existing = await db.get_schedule(schedule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="定时任务不存在")

    await sched.remove_job(schedule_id)
    await db.delete_schedule(schedule_id)
    return {"success": True, "message": "已删除"}


@app.post("/api/schedules/{schedule_id}/toggle")
async def toggle_schedule(schedule_id: int):
    """切换定时任务启用/禁用状态"""
    existing = await db.get_schedule(schedule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="定时任务不存在")

    new_enabled = 0 if existing["enabled"] else 1
    schedule = await db.update_schedule(schedule_id, {"enabled": new_enabled})

    # 更新调度器
    await sched.remove_job(schedule_id)
    if schedule["enabled"]:
        await sched.add_job(
            schedule_id=schedule["id"],
            container_id=schedule["container_id"],
            container_name=schedule["container_name"],
            action=schedule["action"],
            cron_expression=schedule["cron_expression"],
            target_type=schedule.get("target_type", "container"),
        )

    return {"success": True, "data": schedule}


# ============ 执行日志 API ============

@app.get("/api/schedule-logs")
async def list_schedule_logs(
    schedule_id: Optional[int] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
):
    """获取执行日志"""
    logs = await db.list_schedule_logs(schedule_id=schedule_id, limit=limit, offset=offset)
    total = await db.count_schedule_logs(schedule_id=schedule_id)
    return {"success": True, "data": logs, "total": total}


@app.delete("/api/schedule-logs")
async def clear_logs(before_days: int = Query(default=30)):
    """清理执行日志"""
    count = await db.clear_schedule_logs(before_days=before_days)
    return {"success": True, "message": f"已清理 {count} 条日志"}


# ============ 镜像更新检测 API ============

@app.get("/api/containers/update-status")
async def get_update_status():
    """获取镜像更新状态（优先从数据库持久化数据读取）"""
    try:
        results = await db.get_all_update_check_results()
        return {"success": True, "data": results}
    except Exception as e:
        logger.error(f"获取更新状态失败: {e}")
        return {"success": True, "data": {}}


@app.post("/api/containers/check-updates")
async def check_container_updates():
    """立即检查所有容器镜像是否有远程更新（手动触发，同步返回）"""
    try:
        client = await asyncio.to_thread(docker_client.get_client)
        if not client:
            raise HTTPException(status_code=500, detail="Docker 连接失败")
        results = await asyncio.to_thread(update_checker.check_all_updates, client)
        return {"success": True, "data": results}
    except Exception as e:
        logger.error(f"检查镜像更新失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/containers/check-updates/abort")
async def abort_check_updates():
    """中止正在进行的镜像更新检查"""
    update_checker.request_abort()
    return {"success": True, "message": "已发送中止请求"}


@app.post("/api/containers/{container_name}/recheck-update")
async def recheck_container_update(container_name: str):
    """更新完成后重新检查单个容器的镜像更新状态"""
    client = await asyncio.to_thread(docker_client.get_client)
    results = await asyncio.to_thread(update_checker.recheck_container_update, client, container_name)
    return {"success": True, "data": results}


@app.post("/api/compose/projects/{project_name}/recheck-update")
async def recheck_compose_update(project_name: str):
    """更新完成后重新检查 Compose 项目所有容器的镜像更新状态"""
    client = await asyncio.to_thread(docker_client.get_client)
    results = await asyncio.to_thread(update_checker.recheck_compose_update, client, project_name)
    return {"success": True, "data": results}


@app.get("/api/containers/check-updates/stream")
async def check_container_updates_stream():
    """SSE流式检查所有容器镜像更新，逐个推送进度和结果"""

    # 重置中止标志
    update_checker.reset_abort()

    async def event_generator():
        try:
            client = await asyncio.to_thread(docker_client.get_client)
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Docker连接失败: {e}'}, ensure_ascii=False)}\n\n"
            return

        # 获取容器列表
        try:
            containers_raw = await asyncio.to_thread(lambda: client.containers.list(all=True))
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'获取容器列表失败: {e}'}, ensure_ascii=False)}\n\n"
            return

        # 获取容器更新检查偏好，跳过未开启的容器
        settings = await db.get_container_update_settings()

        # 过滤掉无镜像的容器，以及未开启检查的容器（自身也参与检查）
        container_list = []
        for c in containers_raw:
            image_name = c.attrs.get("Config", {}).get("Image", "")
            if not image_name:
                continue
            # 跳过关闭更新检查的容器（默认启用）
            if settings.get(c.name, 1) == 0:
                continue
            container_list.append({"name": c.name, "image": image_name})

        total = len(container_list)
        yield f"data: {json.dumps({'type': 'start', 'total': total}, ensure_ascii=False)}\n\n"

        checked = 0
        all_results = {}
        aborted = False

        for c_info in container_list:
            # 检查是否被中止
            if update_checker.is_aborted():
                aborted = True
                break

            container_name = c_info["name"]
            image_name = c_info["image"]

            yield f"data: {json.dumps({'type': 'checking', 'container': container_name, 'image': image_name, 'checked': checked, 'total': total}, ensure_ascii=False)}\n\n"

            result = await asyncio.to_thread(update_checker.check_image_update, client, image_name)

            # 再次检查中止
            if update_checker.is_aborted():
                aborted = True
                # 仍然保存这个结果
                checked += 1
                all_results[container_name] = result
                from datetime import datetime as dt
                result["checked_at"] = dt.now().isoformat()
                update_checker._update_cache[image_name] = result
                await db.save_update_check_result(container_name, image_name, result.get("has_update"), result.get("error"), result["checked_at"])
                yield f"data: {json.dumps({'type': 'result', 'container': container_name, 'image': image_name, 'has_update': result.get('has_update'), 'error': result.get('error'), 'checked': checked, 'total': total}, ensure_ascii=False)}\n\n"
                break

            checked += 1
            all_results[container_name] = result

            from datetime import datetime as dt
            result["checked_at"] = dt.now().isoformat()
            update_checker._update_cache[image_name] = result
            await db.save_update_check_result(container_name, image_name, result.get("has_update"), result.get("error"), result["checked_at"])

            yield f"data: {json.dumps({'type': 'result', 'container': container_name, 'image': image_name, 'has_update': result.get('has_update'), 'error': result.get('error'), 'checked': checked, 'total': total}, ensure_ascii=False)}\n\n"

            await asyncio.sleep(0)

        # 发送完成/中止事件
        has_update_count = sum(1 for r in all_results.values() if r.get("has_update") is True)
        up_to_date_count = sum(1 for r in all_results.values() if r.get("has_update") is False)
        unknown_count = sum(1 for r in all_results.values() if r.get("has_update") is None)

        if aborted:
            # 写入执行日志
            await _log_check_results(all_results, "手动检查(已中止)")
            yield f"data: {json.dumps({'type': 'aborted', 'total': total, 'checked': checked, 'has_update': has_update_count, 'up_to_date': up_to_date_count, 'unknown': unknown_count}, ensure_ascii=False)}\n\n"
        else:
            # 写入执行日志
            await _log_check_results(all_results, "手动检查")
            yield f"data: {json.dumps({'type': 'done', 'total': total, 'checked': checked, 'has_update': has_update_count, 'up_to_date': up_to_date_count, 'unknown': unknown_count}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no-no",
        }
    )


# ============ 应用设置 API ============

@app.get("/api/settings")
async def get_settings():
    """获取所有应用设置"""
    settings = await db.get_all_settings()
    return {"success": True, "data": settings}


@app.put("/api/settings")
async def update_settings(req: dict):
    """批量更新应用设置"""
    try:
        result = await db.set_settings_bulk(req)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"更新设置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ 容器统计 & 详情 API ============

@app.get("/api/containers/{container_id}/stats")
async def get_container_stats_api(container_id: str):
    """获取容器实时统计（CPU/内存/网络/磁盘）"""
    result = await asyncio.to_thread(docker_client.get_container_stats, container_id)
    if result is None:
        raise HTTPException(status_code=404, detail="容器不存在或未运行")
    return {"success": True, "data": result}


@app.get("/api/containers/{container_id}/detail")
async def get_container_detail_api(container_id: str):
    """获取容器完整详情"""
    result = await asyncio.to_thread(docker_client.get_container_detail, container_id)
    if result is None:
        raise HTTPException(status_code=404, detail="容器不存在")
    # 附加实时统计
    if result.get("status") == "running":
        stats = await asyncio.to_thread(docker_client.get_container_stats, container_id)
        if stats:
            result["stats"] = stats
    return {"success": True, "data": result}


@app.get("/api/containers/{container_id}/logs")
async def get_container_logs_api(container_id: str, tail: int = Query(default=200, le=2000)):
    """获取容器日志"""
    try:
        client = await asyncio.to_thread(docker_client.get_client)
        c = client.containers.get(container_id)
        logs = await asyncio.to_thread(lambda: c.logs(tail=tail, timestamps=True).decode("utf-8", errors="replace"))
        return {"success": True, "data": logs}
    except Exception as e:
        logger.error(f"获取容器日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============ 镜像管理 API ============

@app.get("/api/images")
async def list_images():
    """获取所有镜像列表"""
    try:
        images = await asyncio.to_thread(docker_client.list_images)
        return {"success": True, "data": images}
    except Exception as e:
        logger.error(f"获取镜像列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/images/{image_id}/delete")
async def delete_image(image_id: str, force: bool = Query(default=False)):
    """删除指定镜像"""
    result = await asyncio.to_thread(docker_client.delete_image, image_id, force)
    # 记录执行日志
    await db.add_schedule_log(
        schedule_id=0, schedule_name="手动操作",
        container_name=image_id[:12],
        action="delete-image",
        status="success" if result["success"] else "failed",
        message=result.get("message", "")
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/images/cleanup")
async def cleanup_unused_images():
    """清理所有未使用镜像和无Tag镜像"""
    result = await asyncio.to_thread(docker_client.cleanup_unused_images)
    # 记录执行日志
    status = "success" if result.get("success") else "failed"
    message = result.get("message", "")
    if result.get("errors") and len(result["errors"]) > 0:
        message += f" | 部分失败: {'; '.join(result['errors'][:3])}"
    await db.add_schedule_log(
        schedule_id=0, schedule_name="手动操作",
        container_name="全部镜像",
        action="cleanup-images",
        status=status, message=message
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["message"])
    return result


@app.get("/api/images/mirrors")
async def get_mirrors():
    """读取镜像加速器配置"""
    return {"success": True, "data": await asyncio.to_thread(docker_client.get_registry_mirrors)}


@app.put("/api/images/mirrors")
async def set_mirrors(req: dict):
    """保存镜像加速器配置"""
    mirrors = (req or {}).get("mirrors", [])
    result = await asyncio.to_thread(docker_client.set_registry_mirrors, mirrors)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.post("/api/images/import")
async def import_image(file: UploadFile = File(...)):
    """从本地 tar 文件导入镜像（docker load）"""
    tmp_path = f"/tmp/import_{int(time.time())}.tar"
    try:
        size = 0
        with open(tmp_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                size += len(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="文件为空")
        result = await asyncio.to_thread(docker_client.load_image_from_file, tmp_path)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"导入失败: {e}")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ============ Compose 项目管理 API ============

@app.get("/api/compose/dirs")
async def list_compose_dirs():
    """列出宿主机 compose 根目录下的子目录（用于新建项目时选择目录）"""
    return {"success": True, "data": await asyncio.to_thread(compose_project.list_compose_dirs)}


@app.get("/api/compose/dirs/browse")
async def browse_compose_dirs(path: str = Query("", description="宿主机绝对路径，空则从默认根开始")):
    """浏览宿主机任意目录（返回一级子目录，用于新建项目时选择目录）"""
    data = await asyncio.to_thread(compose_project.browse_compose_dirs, path)
    return {"success": True, "data": data}


@app.post("/api/compose/dirs/mkdir")
async def mkdir_compose_dir(req: dict):
    """在指定宿主机目录下创建子目录（仅限挂载范围内）"""
    path = (req or {}).get("path", "")
    name = (req or {}).get("name", "")
    result = await asyncio.to_thread(compose_project.mkdir_dir, path, name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


class ComposeCreateRequest(BaseModel):
    """创建 Compose 项目请求"""
    dir_name: str = ""
    project_name: str = ""
    yaml: str = ""


@app.post("/api/compose/projects")
async def create_compose_project(req: ComposeCreateRequest):
    """创建 Compose 项目（异步）：写 compose 文件后返回 task_id，后台执行 up -d"""
    result = await asyncio.to_thread(compose_project.start_create_project, req.dir_name, req.yaml, req.project_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.get("/api/compose/tasks/{task_id}/logs")
async def get_compose_task_log(task_id: str, offset: int = Query(0, ge=0)):
    """获取异步创建任务的增量日志（轮询用）"""
    return {"success": True, "data": await asyncio.to_thread(compose_project.get_task_log, task_id, offset)}


@app.delete("/api/compose/projects")
async def delete_compose_project(target: str = Query(..., description="项目目录（宿主机绝对路径或目录名）")):
    """删除 Compose 项目（docker compose down，保留文件）"""
    if target in docker_client.SELF_CONTAINER_NAMES or target.rstrip("/").rsplit("/", 1)[-1] in docker_client.SELF_CONTAINER_NAMES:
        raise HTTPException(status_code=400, detail="不能删除自己，删除后面板将无法恢复")
    result = await asyncio.to_thread(compose_project.delete_compose_project, target)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.get("/api/compose/projects")
async def list_compose_projects():
    """列出所有 Compose 项目"""
    try:
        projects = await asyncio.to_thread(docker_client.list_compose_projects)
        return {"success": True, "data": projects}
    except Exception as e:
        logger.error(f"获取 Compose 项目列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/compose/projects/{project_name}")
async def get_compose_project(project_name: str):
    """获取 Compose 项目详情"""
    result = await asyncio.to_thread(docker_client.get_compose_project_detail, project_name)
    if not result:
        raise HTTPException(status_code=404, detail="Compose 项目不存在")
    return {"success": True, "data": result}


@app.get("/api/compose/projects/{project_name}/stats")
async def get_compose_project_stats(project_name: str):
    """获取 Compose 项目聚合统计"""
    result = await asyncio.to_thread(docker_client.get_compose_project_stats, project_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Compose 项目不存在")
    return {"success": True, "data": result}


@app.post("/api/compose/projects/{project_name}/start")
async def start_compose_project(project_name: str):
    """启动 Compose 项目"""
    result = await asyncio.to_thread(docker_client.start_compose_project, project_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    # 记录执行日志
    await db.add_schedule_log(
        schedule_id=0, schedule_name="手动操作",
        container_name=project_name, action="start",
        status="success", message=result["message"]
    )
    return result


@app.post("/api/compose/projects/{project_name}/stop")
async def stop_compose_project(project_name: str):
    """停止 Compose 项目"""
    result = await asyncio.to_thread(docker_client.stop_compose_project, project_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    await db.add_schedule_log(
        schedule_id=0, schedule_name="手动操作",
        container_name=project_name, action="stop",
        status="success", message=result["message"]
    )
    return result


@app.post("/api/compose/projects/{project_name}/restart")
async def restart_compose_project(project_name: str):
    """重启 Compose 项目"""
    result = await asyncio.to_thread(docker_client.restart_compose_project, project_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    await db.add_schedule_log(
        schedule_id=0, schedule_name="手动操作",
        container_name=project_name, action="restart",
        status="success", message=result["message"]
    )
    return result


@app.post("/api/compose/projects/{project_name}/update")
async def update_compose_project(project_name: str):
    """更新 Compose 项目（拉取最新镜像并重建）- 后台异步执行，返回进度key"""
    if docker_client.is_updating(project_name):
        raise HTTPException(status_code=409, detail="该项目正在更新中，请等待完成")

    async def _bg_compose_update():
        try:
            result = await asyncio.to_thread(
                docker_client.update_compose_project, project_name, project_name
            )
            status = "success" if result["success"] else "failed"
            await db.add_schedule_log(
                schedule_id=0, schedule_name="手动操作",
                container_name=project_name, action="update",
                status=status, message=result.get("message", "")
            )
        except Exception as e:
            logger.error(f"Compose 更新后台任务异常: {e}")
            try:
                await db.add_schedule_log(
                    schedule_id=0, schedule_name="手动操作",
                    container_name=project_name, action="update",
                    status="failed", message=str(e)
                )
            except Exception:
                pass

    asyncio.create_task(_bg_compose_update())
    return {"success": True, "started": True, "progress_key": project_name, "message": "更新已开始"}


# ============ 自身管理 API（展示自己 / 更新自己） ============

@app.get("/api/self/info")
async def get_self_info():
    """自身容器信息（面板'本机'行展示用）"""
    info = await asyncio.to_thread(docker_client.get_self_info)
    if not info:
        raise HTTPException(status_code=404, detail="未找到自身容器")
    return {"success": True, "data": info}


@app.post("/api/self/check-update")
async def self_check_update():
    """检查自身镜像是否有更新（复用 update_checker 的 digest 对比）"""
    client = await asyncio.to_thread(docker_client.get_client)
    image = await asyncio.to_thread(docker_client.get_self_image)
    if not image:
        raise HTTPException(status_code=404, detail="未找到自身镜像")
    result = await asyncio.to_thread(update_checker.check_image_update, client, image)
    # 持久化到数据库：否则页面刷新后 loadUpdateStatus 会读到旧的检查结果（显示"有更新"）
    if result.get("has_update") is not None:
        from datetime import datetime as dt
        result["checked_at"] = dt.now().isoformat()
        self_cont = await asyncio.to_thread(docker_client.get_self_container, await asyncio.to_thread(docker_client.get_client))
        self_name = (self_cont.name if self_cont else None) or "mypanel"
        try:
            await db.save_update_check_result(self_name, image, result.get("has_update"), result.get("error"), result["checked_at"])
        except Exception as e:
            logger.warning(f"持久化自身检查结果失败: {e}")
    return {"success": True, "data": result}


@app.post("/api/self/update")
async def self_update():
    """更新自己（updater 容器模式：预拉新镜像 + 独立执行器容器重建面板）"""
    result = await asyncio.to_thread(docker_client.start_self_update)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    # no_update 也正常返回（success=True, started=False），前端友好提示"已是最新"
    return result


@app.post("/api/self/restart")
async def self_restart():
    """重启自己（daemon 后台执行，立即返回）"""
    result = await asyncio.to_thread(docker_client.restart_self)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.get("/api/self/update-status")
async def self_update_status():
    """查询自身更新执行器状态（面板重启恢复后确认结果）"""
    status = await asyncio.to_thread(docker_client.get_self_update_status)
    return {"success": True, "data": status}


@app.delete("/api/self/update")
async def self_update_cleanup():
    """清理残留的更新执行器容器"""
    result = await asyncio.to_thread(docker_client.cleanup_self_update)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@app.get("/api/ports/usage")
async def get_ports_usage():
    """扫描宿主端口占用（系统监听 + 所有容器映射），方便创建 Compose 选未占用端口"""
    data = await asyncio.to_thread(docker_client.get_port_usage)
    return {"success": True, "data": data}
