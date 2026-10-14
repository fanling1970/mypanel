"""SQLite数据库模块 - 管理定时任务和执行日志"""

import aiosqlite
import hashlib
import json
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("database")

DB_PATH = os.environ.get("DB_PATH", "/data/mypanel.db")


def hash_password(password: str) -> str:
    """SHA256加密密码"""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


async def get_db() -> aiosqlite.Connection:
    """获取数据库连接"""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db():
    """初始化数据库表"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")

        # 定时任务表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                container_id TEXT NOT NULL,
                container_name TEXT NOT NULL,
                action TEXT NOT NULL CHECK(action IN ('start', 'stop', 'restart', 'update')),
                cron_expression TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # 执行日志表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                schedule_name TEXT,
                container_name TEXT,
                action TEXT,
                status TEXT NOT NULL CHECK(status IN ('success', 'failed')),
                message TEXT,
                executed_at TEXT NOT NULL
            )
        """)

        # 执行日志索引
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_logs_schedule_id
            ON schedule_logs(schedule_id)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_logs_executed_at
            ON schedule_logs(executed_at)
        """)

        # 迁移：检查 schedules 表的 CHECK 约束是否包含 'update'
        cursor = await db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='schedules'")
        row = await cursor.fetchone()
        if row and row[0] and "'update'" not in row[0]:
            logger.info("检测到旧版 schedules 表，开始迁移 CHECK 约束以支持 update 操作...")
            await db.execute("ALTER TABLE schedules RENAME TO _schedules_old")
            await db.execute("""
                CREATE TABLE schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    container_id TEXT NOT NULL,
                    container_name TEXT NOT NULL,
                    action TEXT NOT NULL CHECK(action IN ('start', 'stop', 'restart', 'update')),
                    cron_expression TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_run TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            await db.execute("""
                INSERT INTO schedules (id, name, container_id, container_name, action, cron_expression, enabled, last_run, created_at, updated_at)
                SELECT id, name, container_id, container_name, action, cron_expression, enabled, last_run, created_at, updated_at FROM _schedules_old
            """)
            await db.execute("DROP TABLE _schedules_old")
            logger.info("schedules 表迁移完成，已支持 update 操作")

        # 迁移：为 schedules 表添加 target_type 字段（支持 compose 项目定时任务）
        cursor = await db.execute("PRAGMA table_info(schedules)")
        columns = [r[1] for r in await cursor.fetchall()]
        if "target_type" not in columns:
            await db.execute("ALTER TABLE schedules ADD COLUMN target_type TEXT NOT NULL DEFAULT 'container'")
            logger.info("schedules 表已添加 target_type 字段")

        # 容器更新检查偏好表（默认全部启用，用户可关闭单个容器）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS container_update_settings (
                container_name TEXT PRIMARY KEY,
                check_enabled INTEGER NOT NULL DEFAULT 1
            )
        """)

        # 应用设置表（key-value）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # 2FA 信任设备表（设备 token 长期有效，登录时跳过验证码）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trusted_devices (
                token TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)

        # 镜像更新检查结果表（持久化，应用重启后保留）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS update_check_results (
                container_name TEXT PRIMARY KEY,
                image TEXT NOT NULL,
                has_update INTEGER,
                error TEXT,
                checked_at TEXT
            )
        """)

        # 用户表（登录认证）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # 初始化默认管理员账户 admin/admin（如果不存在）
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        row = await cursor.fetchone()
        if row[0] == 0:
            now = datetime.now().isoformat()
            admin_hash = hash_password("admin")
            await db.execute(
                "INSERT INTO users (username, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?)",
                ("admin", admin_hash, now, now)
            )
            logger.info("已创建默认管理员账户 admin/admin")

        await db.commit()


# ============ 容器更新检查偏好 CRUD ============

async def get_container_update_settings() -> dict:
    """获取所有容器的更新检查偏好，返回 {container_name: check_enabled}"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM container_update_settings")
        rows = await cursor.fetchall()
        return {r["container_name"]: r["check_enabled"] for r in rows}


async def set_container_update_setting(container_name: str, check_enabled: int) -> dict:
    """设置单个容器的更新检查偏好（upsert）"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO container_update_settings (container_name, check_enabled)
               VALUES (?, ?)
               ON CONFLICT(container_name) DO UPDATE SET check_enabled=excluded.check_enabled""",
            (container_name, check_enabled)
        )
        await db.commit()
    return {"container_name": container_name, "check_enabled": check_enabled}


async def get_container_update_setting(container_name: str) -> int:
    """获取单个容器的更新检查偏好，默认1(启用)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT check_enabled FROM container_update_settings WHERE container_name=?",
            (container_name,)
        )
        row = await cursor.fetchone()
        return row["check_enabled"] if row else 1


async def set_all_container_update_settings(container_names: list[str], check_enabled: int) -> dict:
    """批量设置多个容器的更新检查偏好（upsert）"""
    async with aiosqlite.connect(DB_PATH) as db:
        for name in container_names:
            await db.execute(
                """INSERT INTO container_update_settings (container_name, check_enabled)
                   VALUES (?, ?)
                   ON CONFLICT(container_name) DO UPDATE SET check_enabled=excluded.check_enabled""",
                (name, check_enabled)
            )
        await db.commit()
    return {"count": len(container_names), "check_enabled": check_enabled}

async def list_schedules(enabled_only: bool = False) -> list[dict]:
    """获取所有定时任务"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = "SELECT * FROM schedules ORDER BY created_at DESC"
        if enabled_only:
            sql = "SELECT * FROM schedules WHERE enabled=1 ORDER BY created_at DESC"
        cursor = await db.execute(sql)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_schedule(schedule_id: int) -> Optional[dict]:
    """获取单个定时任务"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM schedules WHERE id=?", (schedule_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def create_schedule(data: dict) -> dict:
    """创建定时任务"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO schedules (name, container_id, container_name, action, cron_expression, enabled, target_type, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (data["name"], data["container_id"], data["container_name"],
             data["action"], data["cron_expression"], data.get("enabled", 1),
             data.get("target_type", "container"),
             now, now)
        )
        await db.commit()
        schedule_id = cursor.lastrowid
        return await get_schedule(schedule_id)


async def update_schedule(schedule_id: int, data: dict) -> Optional[dict]:
    """更新定时任务"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        sets = []
        vals = []
        for key in ["name", "container_id", "container_name", "action", "cron_expression", "enabled", "target_type"]:
            if key in data:
                sets.append(f"{key}=?")
                vals.append(data[key])
        if not sets:
            return await get_schedule(schedule_id)
        sets.append("updated_at=?")
        vals.append(now)
        vals.append(schedule_id)
        await db.execute(
            f"UPDATE schedules SET {', '.join(sets)} WHERE id=?", vals
        )
        await db.commit()
        return await get_schedule(schedule_id)


async def delete_schedule(schedule_id: int) -> bool:
    """删除定时任务"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        await db.commit()
        return cursor.rowcount > 0


async def update_schedule_last_run(schedule_id: int):
    """更新定时任务上次执行时间"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE schedules SET last_run=?, updated_at=? WHERE id=?",
            (now, now, schedule_id)
        )
        await db.commit()


# ============ 执行日志 CRUD ============

async def add_schedule_log(schedule_id: int, schedule_name: str,
                           container_name: str, action: str,
                           status: str, message: str = ""):
    """添加执行日志"""
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO schedule_logs (schedule_id, schedule_name, container_name, action, status, message, executed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (schedule_id, schedule_name, container_name, action, status, message, now)
        )
        await db.commit()


async def list_schedule_logs(schedule_id: Optional[int] = None,
                              limit: int = 100, offset: int = 0) -> list[dict]:
    """获取执行日志"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if schedule_id:
            cursor = await db.execute(
                "SELECT * FROM schedule_logs WHERE schedule_id=? ORDER BY executed_at DESC LIMIT ? OFFSET ?",
                (schedule_id, limit, offset)
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM schedule_logs ORDER BY executed_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def count_schedule_logs(schedule_id: Optional[int] = None) -> int:
    """统计执行日志条数"""
    async with aiosqlite.connect(DB_PATH) as db:
        if schedule_id:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM schedule_logs WHERE schedule_id=?",
                (schedule_id,)
            )
        else:
            cursor = await db.execute("SELECT COUNT(*) FROM schedule_logs")
        row = await cursor.fetchone()
        return row[0]


async def clear_schedule_logs(before_days: int = 30):
    """清理指定天数之前的执行日志"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=before_days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM schedule_logs WHERE executed_at < ?", (cutoff,)
        )
        await db.commit()
        return cursor.rowcount


# ============ 应用设置 CRUD ============

# 默认设置值
DEFAULT_SETTINGS = {
    "auto_check_enabled": "1",       # 自动检查更新开关（1=开启，0=关闭）
    "auto_check_interval": "1",      # 自动检查间隔（小时）
    "check_delay_seconds": "60",     # 启动后延迟检查秒数
}


async def get_all_settings() -> dict:
    """获取所有应用设置，未设置的返回默认值"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM app_settings")
        rows = await cursor.fetchall()
        saved = {r["key"]: r["value"] for r in rows}
    # 合并默认值
    result = dict(DEFAULT_SETTINGS)
    result.update(saved)
    return result


async def get_setting(key: str) -> str:
    """获取单个设置值，未设置返回默认值"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT value FROM app_settings WHERE key=?", (key,))
        row = await cursor.fetchone()
        if row:
            return row["value"]
    return DEFAULT_SETTINGS.get(key, "")


async def set_setting(key: str, value: str) -> dict:
    """设置单个配置项（upsert）"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO app_settings (key, value)
               VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, value)
        )
        await db.commit()
    return {"key": key, "value": value}


async def set_settings_bulk(settings: dict) -> dict:
    """批量设置配置项"""
    async with aiosqlite.connect(DB_PATH) as db:
        for key, value in settings.items():
            await db.execute(
                """INSERT INTO app_settings (key, value)
                   VALUES (?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (key, str(value))
            )
        await db.commit()
    return settings


# ============ 镜像更新检查结果 CRUD ============

async def save_update_check_result(container_name: str, image: str,
                                    has_update: Optional[bool], error: Optional[str],
                                    checked_at: str):
    """保存单个容器的更新检查结果（upsert）"""
    hu = None
    if has_update is True:
        hu = 1
    elif has_update is False:
        hu = 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO update_check_results (container_name, image, has_update, error, checked_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(container_name) DO UPDATE SET
                 image=excluded.image, has_update=excluded.has_update,
                 error=excluded.error, checked_at=excluded.checked_at""",
            (container_name, image, hu, error, checked_at)
        )
        await db.commit()


async def save_update_check_results_bulk(results: list):
    """批量保存更新检查结果 [{container_name, image, has_update, error, checked_at}]"""
    async with aiosqlite.connect(DB_PATH) as db:
        for r in results:
            hu = None
            if r.get("has_update") is True:
                hu = 1
            elif r.get("has_update") is False:
                hu = 0
            await db.execute(
                """INSERT INTO update_check_results (container_name, image, has_update, error, checked_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(container_name) DO UPDATE SET
                     image=excluded.image, has_update=excluded.has_update,
                     error=excluded.error, checked_at=excluded.checked_at""",
                (r["container_name"], r["image"], hu, r.get("error"), r.get("checked_at", ""))
            )
        await db.commit()


# ============ 用户认证 CRUD ============

async def authenticate_user(username: str, password: str) -> Optional[dict]:
    """验证用户登录，返回用户信息或None"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, username, password_hash FROM users WHERE username=?",
            (username,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        if row["password_hash"] != hash_password(password):
            return None
        return {"id": row["id"], "username": row["username"]}


# ============ 2FA 信任设备 ============

async def create_device_token(days: int = 30) -> str:
    """生成并保存设备信任 token（默认 30 天有效）"""
    token = secrets.token_hex(32)
    now = datetime.now(timezone.utc)
    await _db_execute(
        "INSERT OR REPLACE INTO trusted_devices (token, created_at, expires_at) VALUES (?, ?, ?)",
        (token, now.isoformat(), (now + timedelta(days=days)).isoformat()),
    )
    return token


async def verify_device_token(token: str) -> bool:
    """验证设备 token 是否有效（存在且未过期）"""
    if not token:
        return False
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT expires_at FROM trusted_devices WHERE token=?", (token,))
            row = await cursor.fetchone()
            if not row:
                return False
            expires = datetime.fromisoformat(row["expires_at"])
            return expires > datetime.now(timezone.utc)
    except Exception:
        return False


async def clear_trusted_devices() -> None:
    """清空所有信任设备（关闭 2FA 或重置时调用）"""
    await _db_execute("DELETE FROM trusted_devices")


async def _db_execute(sql: str, params: tuple = ()) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, params)
        await db.commit()


async def get_user(username: str) -> Optional[dict]:
    """获取用户信息"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, username, created_at, updated_at FROM users WHERE username=?",
            (username,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return dict(row)


async def change_password(username: str, old_password: str, new_password: str) -> dict:
    """修改用户密码，需验证旧密码"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, password_hash FROM users WHERE username=?",
            (username,)
        )
        row = await cursor.fetchone()
        if not row:
            return {"success": False, "message": "用户不存在"}
        if row["password_hash"] != hash_password(old_password):
            return {"success": False, "message": "旧密码不正确"}
        now = datetime.now().isoformat()
        await db.execute(
            "UPDATE users SET password_hash=?, updated_at=? WHERE username=?",
            (hash_password(new_password), now, username)
        )
        await db.commit()
        return {"success": True, "message": "密码修改成功"}


async def get_all_update_check_results() -> dict:
    """获取所有容器的更新检查结果，返回 {container_name: {image, has_update, error, checked_at}}"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM update_check_results")
        rows = await cursor.fetchall()
        results = {}
        for r in rows:
            hu = r["has_update"]
            if hu == 1:
                hu = True
            elif hu == 0:
                hu = False
            else:
                hu = None
            results[r["container_name"]] = {
                "image": r["image"],
                "has_update": hu,
                "error": r["error"],
                "checked_at": r["checked_at"],
            }
        return results
