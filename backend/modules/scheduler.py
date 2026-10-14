"""定时任务调度模块 - 基于APScheduler的cron调度引擎"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from modules import database as db
from modules import docker_client

logger = logging.getLogger("scheduler")

# 全局调度器
_scheduler: Optional[AsyncIOScheduler] = None


def get_scheduler() -> AsyncIOScheduler:
    """获取调度器单例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(
            timezone="Asia/Shanghai",
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 60,
            }
        )
    return _scheduler


def parse_cron(expression: str) -> dict:
    """解析cron表达式为APScheduler CronTrigger参数

    支持标准5段式: 分 时 日 月 周
    例如: '0 8 * * *' 表示每天8:00执行
         '30 2 * * 1-5' 表示工作日2:30执行
         '0 */6 * * *' 表示每6小时执行
    """
    parts = expression.strip().split()
    if len(parts) != 5:
        raise ValueError(f"cron表达式必须是5段格式: 分 时 日 月 周, 当前: {expression}")

    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


async def execute_scheduled_action(schedule_id: int, container_id: str,
                                    container_name: str, action: str,
                                    target_type: str = "container"):
    """执行定时任务动作（将阻塞的Docker操作放到线程池，避免卡死事件循环）"""
    logger.info(f"执行定时任务: {container_name} -> {action} (schedule_id={schedule_id}, type={target_type})")

    try:
        loop = asyncio.get_event_loop()
        if target_type == "compose":
            # Compose 项目操作
            if action == "start":
                result = await loop.run_in_executor(None, docker_client.start_compose_project, container_name)
            elif action == "stop":
                result = await loop.run_in_executor(None, docker_client.stop_compose_project, container_name)
            elif action == "restart":
                result = await loop.run_in_executor(None, docker_client.restart_compose_project, container_name)
            elif action == "update":
                result = await loop.run_in_executor(None, docker_client.update_compose_project, container_name)
            else:
                result = {"success": False, "message": f"未知操作: {action}"}
        else:
            # 容器操作
            if action == "start":
                result = await loop.run_in_executor(None, docker_client.start_container, container_id)
            elif action == "stop":
                result = await loop.run_in_executor(None, docker_client.stop_container, container_id)
            elif action == "restart":
                result = await loop.run_in_executor(None, docker_client.restart_container, container_id)
            elif action == "update":
                result = await loop.run_in_executor(None, docker_client.update_container, container_id)
            else:
                result = {"success": False, "message": f"未知操作: {action}"}

        status = "success" if result["success"] else "failed"

        # 记录日志
        schedule = await db.get_schedule(schedule_id)
        schedule_name = schedule["name"] if schedule else "未知任务"
        await db.add_schedule_log(
            schedule_id=schedule_id,
            schedule_name=schedule_name,
            container_name=container_name,
            action=action,
            status=status,
            message=result.get("message", "")
        )

        # 更新上次执行时间
        await db.update_schedule_last_run(schedule_id)

        logger.info(f"定时任务执行完成: {container_name} -> {action}, 状态: {status}")

    except Exception as e:
        logger.error(f"定时任务执行异常: {e}")
        try:
            schedule = await db.get_schedule(schedule_id)
            schedule_name = schedule["name"] if schedule else "未知任务"
            await db.add_schedule_log(
                schedule_id=schedule_id,
                schedule_name=schedule_name,
                container_name=container_name,
                action=action,
                status="failed",
                message=str(e)
            )
        except Exception as e2:
            logger.error(f"记录执行日志失败: {e2}")


def _build_job_id(schedule_id: int) -> str:
    """构建调度任务ID"""
    return f"schedule_{schedule_id}"


async def add_job(schedule_id: int, container_id: str, container_name: str,
                  action: str, cron_expression: str, target_type: str = "container"):
    """添加调度任务"""
    scheduler = get_scheduler()
    job_id = _build_job_id(schedule_id)

    # 如果已存在则先移除
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)

    cron_params = parse_cron(cron_expression)
    trigger = CronTrigger(**cron_params, timezone="Asia/Shanghai")

    scheduler.add_job(
        execute_scheduled_action,
        trigger=trigger,
        id=job_id,
        args=[schedule_id, container_id, container_name, action, target_type],
        replace_existing=True,
    )
    logger.info(f"添加调度任务: {job_id}, cron={cron_expression}, {container_name}->{action} (type={target_type})")


async def remove_job(schedule_id: int):
    """移除调度任务"""
    scheduler = get_scheduler()
    job_id = _build_job_id(schedule_id)
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)
        logger.info(f"移除调度任务: {job_id}")


async def load_all_schedules():
    """启动时加载所有已启用的定时任务到调度器"""
    schedules = await db.list_schedules(enabled_only=True)
    for s in schedules:
        try:
            await add_job(
                schedule_id=s["id"],
                container_id=s["container_id"],
                container_name=s["container_name"],
                action=s["action"],
                cron_expression=s["cron_expression"],
                target_type=s.get("target_type", "container"),
            )
        except Exception as e:
            logger.error(f"加载定时任务 {s['id']} 失败: {e}")
    logger.info(f"已加载 {len(schedules)} 个定时任务到调度器")


def start_scheduler():
    """启动调度器"""
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()
        logger.info("调度器已启动")


def stop_scheduler():
    """停止调度器"""
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("调度器已停止")
