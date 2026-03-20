import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from core.account import load_accounts_from_source
from core.base_task_service import BaseTask, BaseTaskService, TaskCancelledError, TaskStatus
from core.config import config
from core.exa_automation import ExaAutomation
from core.mail_providers import create_temp_mail_client
from core.proxy_utils import parse_proxy_setting

logger = logging.getLogger("exa.register")
EMAIL_LOGIN_RETRY_LIMIT = 3
EMAIL_LOGIN_RETRY_SLEEP_SECONDS = 5


@dataclass
class RegisterTask(BaseTask):
    """注册任务数据类"""
    count: int = 0
    domain: Optional[str] = None
    mail_provider: Optional[str] = None

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        base_dict["count"] = self.count
        base_dict["domain"] = self.domain
        base_dict["mail_provider"] = self.mail_provider
        return base_dict


class RegisterService(BaseTaskService[RegisterTask]):
    """Exa 批量注册服务。"""

    def __init__(
        self,
        multi_account_mgr,
        http_client,
        user_agent: str,
        retry_policy,
        session_cache_ttl_seconds: int,
        global_stats_provider: Callable[[], dict],
        set_multi_account_mgr: Optional[Callable[[Any], None]] = None,
    ) -> None:
        super().__init__(
            multi_account_mgr,
            http_client,
            user_agent,
            retry_policy,
            session_cache_ttl_seconds,
            global_stats_provider,
            set_multi_account_mgr,
            log_prefix="REGISTER",
        )

    @staticmethod
    def _cleanup_mail(client, log_cb) -> None:
        """注册完成后清理临时邮箱（仅 Moemail 支持）"""
        if hasattr(client, "delete_email"):
            try:
                client.delete_email()
            except Exception as e:
                log_cb("warning", f"⚠️ 清理邮箱失败: {e}")

    def _get_running_task(self) -> Optional[RegisterTask]:
        for task in self._tasks.values():
            if isinstance(task, RegisterTask) and task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                return task
        return None

    async def start_register(
        self,
        count: Optional[int] = None,
        domain: Optional[str] = None,
        mail_provider: Optional[str] = None,
    ) -> RegisterTask:
        async with self._lock:
            if os.environ.get("ACCOUNTS_CONFIG"):
                raise ValueError("已设置 ACCOUNTS_CONFIG 环境变量，注册功能已禁用")

            provider = (mail_provider or "").strip().lower() or (config.basic.temp_mail_provider or "duckmail").lower()
            domain_value = (domain or "").strip() or ((config.basic.register_domain or "").strip() if provider == "duckmail" else "")
            domain_value = domain_value or None
            register_count = max(1, int(count or config.basic.register_default_count))

            running_task = self._get_running_task()
            if running_task:
                running_task.count += register_count
                self._append_log(running_task, "info", f"📝 添加 {register_count} 个账户到现有任务 (总计: {running_task.count})")
                return running_task

            task = RegisterTask(id=str(uuid.uuid4()), count=register_count, domain=domain_value, mail_provider=provider)
            self._tasks[task.id] = task
            self._append_log(task, "info", f"📝 创建 Exa 注册任务 (数量: {register_count}, 域名: {domain_value or 'default'}, 提供商: {provider})")
            self._current_task_id = task.id
            asyncio.create_task(self._run_task_directly(task))
            return task

    async def _run_task_directly(self, task: RegisterTask) -> None:
        try:
            await self._run_one_task(task)
        finally:
            async with self._lock:
                if self._current_task_id == task.id:
                    self._current_task_id = None

    def _execute_task(self, task: RegisterTask):
        return self._run_register_async(task, task.domain, task.mail_provider)

    async def _run_register_async(self, task: RegisterTask, domain: Optional[str], mail_provider: Optional[str]) -> None:
        loop = asyncio.get_running_loop()
        self._append_log(task, "info", f"🚀 Exa 注册任务已启动 (共 {task.count} 个账号)")

        for idx in range(task.count):
            if task.cancel_requested:
                self._append_log(task, "warning", f"register task cancelled: {task.cancel_reason or 'cancelled'}")
                task.status = TaskStatus.CANCELLED
                task.finished_at = time.time()
                return

            try:
                self._append_log(task, "info", f"📊 进度: {idx + 1}/{task.count}")
                result = await loop.run_in_executor(self._executor, self._register_one, domain, mail_provider, task)
            except TaskCancelledError:
                task.status = TaskStatus.CANCELLED
                task.finished_at = time.time()
                return
            except Exception as exc:
                result = {"success": False, "error": str(exc)}

            task.progress += 1
            task.results.append(result)

            if result.get("success"):
                task.success_count += 1
                email = result.get("email", "未知")
                self._append_log(task, "info", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                self._append_log(task, "info", f"✅ Exa 注册成功: {email}")
                self._append_log(task, "info", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            else:
                task.fail_count += 1
                error = result.get("error", "未知错误")
                self._append_log(task, "error", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                self._append_log(task, "error", f"❌ Exa 注册失败: {error}")
                self._append_log(task, "error", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

            if idx < task.count - 1 and not task.cancel_requested:
                self._append_log(task, "info", "⏳ 等待 8 秒后处理下一个账号...")
                await asyncio.sleep(8)

        task.status = TaskStatus.CANCELLED if task.cancel_requested else (TaskStatus.SUCCESS if task.fail_count == 0 else TaskStatus.FAILED)
        task.finished_at = time.time()
        self._current_task_id = None
        self._append_log(task, "info", f"🏁 注册任务完成 (成功: {task.success_count}, 失败: {task.fail_count}, 总计: {task.count})")

    def _register_one(self, domain: Optional[str], mail_provider: Optional[str], task: RegisterTask) -> dict:
        log_cb = lambda level, message: self._append_log(task, level, message)
        log_cb("info", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        log_cb("info", "🆕 开始注册 Exa 账号")
        log_cb("info", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        provider = (mail_provider or "").strip().lower() or (config.basic.temp_mail_provider or "duckmail").lower()
        if provider == "freemail" and not config.basic.freemail_jwt_token:
            return {"success": False, "error": "Freemail JWT Token 未配置"}

        log_cb("info", f"📧 步骤 1/4: 创建邮箱 (提供商={provider})...")
        client = create_temp_mail_client(provider, domain=domain, log_cb=log_cb)

        if not client.register_account(domain=domain):
            self._cleanup_mail(client, log_cb)
            return {"success": False, "error": f"{provider} 注册失败"}
        if not getattr(client, "email", None):
            self._cleanup_mail(client, log_cb)
            return {"success": False, "error": f"{provider} 邮箱地址未生成"}
        log_cb("info", f"✅ 邮箱创建成功: {client.email}")

        proxy_for_auth, _ = parse_proxy_setting(config.basic.proxy_for_auth)
        automation = ExaAutomation(
            proxy=proxy_for_auth,
            log_callback=log_cb,
        )
        log_cb("info", f"🌐 步骤 2/4: 启动浏览器 (模式={automation.browser_mode})...")
        self._add_cancel_hook(task.id, lambda: None)

        log_cb("info", "🔐 步骤 3/4: 执行 Exa 登录与初始化...")
        redeem_coupon_enabled = bool(getattr(config.basic, "exa_redeem_coupon_enabled", False))
        configured_coupon_code = str(getattr(config.basic, "exa_coupon_code", "") or "").strip()
        redeem_coupon = redeem_coupon_enabled and bool(configured_coupon_code)
        if redeem_coupon_enabled and not configured_coupon_code:
            log_cb("warning", "⚠️ 已启用兑换码自动兑换，但兑换码为空，本次将跳过兑换")
        elif not redeem_coupon_enabled:
            log_cb("info", "🎟️ 兑换码自动兑换未启用，跳过兑换步骤")

        result = None
        for attempt in range(1, EMAIL_LOGIN_RETRY_LIMIT + 1):
            if attempt > 1:
                log_cb("warning", f"⚠️ Exa 邮箱登录暂不可用，开始第 {attempt}/{EMAIL_LOGIN_RETRY_LIMIT} 次重试...")
            result = automation.register_and_setup(
                email=client.email,
                mail_client=client,
                coupon_code=configured_coupon_code,
                redeem_coupon=redeem_coupon,
            )
            if result.get("success"):
                break
            if result.get("error_code") != "exa_email_login_unavailable" or attempt >= EMAIL_LOGIN_RETRY_LIMIT:
                break
            log_cb("warning", f"⏳ 等待 {EMAIL_LOGIN_RETRY_SLEEP_SECONDS} 秒后重试 Exa 邮箱登录...")
            time.sleep(EMAIL_LOGIN_RETRY_SLEEP_SECONDS)

        if not result or not result.get("success"):
            self._cleanup_mail(client, log_cb)
            return {
                "success": False,
                "error": (result or {}).get("error", "Exa 自动化流程失败"),
                "error_code": (result or {}).get("error_code"),
            }

        config_data = result["config"]
        config_data["mail_provider"] = provider
        config_data["mail_address"] = client.email
        config_data["mail_password"] = getattr(client, "password", "") or ""
        config_data["coupon_code"] = configured_coupon_code if redeem_coupon else ""

        # 保存邮箱供应商参数（与旧项目行为保持一致）
        if provider == "freemail":
            config_data["mail_password"] = ""
            config_data["mail_base_url"] = config.basic.freemail_base_url
            config_data["mail_jwt_token"] = config.basic.freemail_jwt_token
            config_data["mail_verify_ssl"] = config.basic.freemail_verify_ssl
            config_data["mail_domain"] = config.basic.freemail_domain
        elif provider == "gptmail":
            config_data["mail_password"] = ""
            config_data["mail_base_url"] = config.basic.gptmail_base_url
            config_data["mail_api_key"] = config.basic.gptmail_api_key
            config_data["mail_verify_ssl"] = config.basic.gptmail_verify_ssl
            config_data["mail_domain"] = config.basic.gptmail_domain
        elif provider == "cfmail":
            config_data["mail_password"] = getattr(client, "jwt_token", "") or getattr(client, "password", "")
            config_data["mail_base_url"] = config.basic.cfmail_base_url
            config_data["mail_api_key"] = config.basic.cfmail_api_key
            config_data["mail_verify_ssl"] = config.basic.cfmail_verify_ssl
            config_data["mail_domain"] = config.basic.cfmail_domain
        elif provider == "moemail":
            config_data["mail_password"] = getattr(client, "email_id", "") or getattr(client, "password", "")
            config_data["mail_base_url"] = config.basic.moemail_base_url
            config_data["mail_api_key"] = config.basic.moemail_api_key
            config_data["mail_domain"] = config.basic.moemail_domain
        elif provider == "duckmail":
            config_data["mail_base_url"] = config.basic.duckmail_base_url
            config_data["mail_api_key"] = config.basic.duckmail_api_key

        log_cb("info", "💾 步骤 4/4: 保存账号配置...")
        accounts_data = load_accounts_from_source()
        updated = False
        for acc in accounts_data:
            if acc.get("id") == config_data["id"]:
                acc.update(config_data)
                updated = True
                break
        if not updated:
            accounts_data.append(config_data)

        self._apply_accounts_update(accounts_data)
        masked_key = (config_data.get("exa_api_key", "") or "")[:8]
        log_cb("info", f"✅ 已保存 Exa key 前缀: {masked_key}...")
        self._cleanup_mail(client, log_cb)
        return {"success": True, "email": client.email, "config": config_data}
