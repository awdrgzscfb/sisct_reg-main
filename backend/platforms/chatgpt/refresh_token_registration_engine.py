"""
ChatGPT Refresh Token 注册引擎。

主链路采用两段式推进：
1. `ChatGPTClient.register_complete_flow()` 负责把注册状态机推进到 about_you
2. `OAuthClient.login_and_get_tokens()` 承接前序会话继续完成 about_you / workspace / token
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from core.task_runtime import DeferAttemptRequested, TaskInterruption

from .chatgpt_client import ChatGPTClient
from .oauth import OAuthManager
from .oauth_client import OAuthClient
from .utils import (
    generate_random_birthday,
    generate_random_name,
    generate_random_password,
)

logger = logging.getLogger(__name__)


@dataclass
class RegistrationResult:
    """注册结果。"""

    success: bool
    email: str = ""
    password: str = ""
    account_id: str = ""
    workspace_id: str = ""
    access_token: str = ""
    refresh_token: str = ""
    id_token: str = ""
    session_token: str = ""
    error_message: str = ""
    logs: list | None = None
    metadata: dict | None = None
    source: str = "register"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "email": self.email,
            "password": self.password,
            "account_id": self.account_id,
            "workspace_id": self.workspace_id,
            "access_token": self.access_token[:20] + "..." if self.access_token else "",
            "refresh_token": self.refresh_token[:20] + "..." if self.refresh_token else "",
            "id_token": self.id_token[:20] + "..." if self.id_token else "",
            "session_token": self.session_token[:20] + "..." if self.session_token else "",
            "error_message": self.error_message,
            "logs": self.logs or [],
            "metadata": self.metadata or {},
            "source": self.source,
        }


@dataclass
class SignupFormResult:
    """保留旧结构，兼容外部引用。"""

    success: bool
    page_type: str = ""
    is_existing_account: bool = False
    response_data: Dict[str, Any] | None = None
    error_message: str = ""


class EmailServiceAdapter:
    """将现有 email_service 适配给 ChatGPTClient / OAuthClient 状态机。"""

    def __init__(
        self,
        email_service,
        email: str,
        log_fn: Callable[[str], None],
        interrupt_check: Callable[[], None] | None = None,
    ):
        self.email_service = email_service
        self.email = email
        self.log_fn = log_fn
        self.interrupt_check = interrupt_check
        self._used_codes: set[str] = set()
        self._last_code: str = ""
        self._last_code_at: float = 0.0
        self._last_success_code: str = ""
        self._last_success_code_at: float = 0.0

    @property
    def last_code(self) -> str:
        return self._last_success_code or self._last_code

    def _remember_code(self, code: str, *, successful: bool = False) -> None:
        code = str(code or "").strip()
        if not code:
            return
        now = time.time()
        self._last_code = code
        self._last_code_at = now
        self._used_codes.add(code)
        if successful:
            self._last_success_code = code
            self._last_success_code_at = now

    def remember_successful_code(self, code: str) -> None:
        self._remember_code(code, successful=True)

    def _checkpoint(self) -> None:
        if callable(self.interrupt_check):
            self.interrupt_check()

    def get_recent_code(
        self,
        max_age_seconds: int = 180,
        *,
        prefer_successful: bool = True,
    ) -> str:
        now = time.time()
        if (
            prefer_successful
            and self._last_success_code
            and now - self._last_success_code_at <= max_age_seconds
        ):
            return self._last_success_code
        if self._last_code and now - self._last_code_at <= max_age_seconds:
            return self._last_code
        return ""

    def wait_for_verification_code(
        self,
        email: str,
        timeout: int = 90,
        otp_sent_at: float | None = None,
        exclude_codes=None,
    ):
        excluded = {str(item).strip() for item in (exclude_codes or set()) if str(item or "").strip()}
        self._checkpoint()
        self.log_fn(f"正在等待邮箱 {email} 的验证码 ({timeout}s)...")
        code = self.email_service.get_verification_code(
            email=email,
            timeout=timeout,
            otp_sent_at=otp_sent_at,
            exclude_codes=excluded,
            interrupt_check=self.interrupt_check,
        )
        if code:
            code = str(code).strip()
            self._remember_code(code, successful=False)
            self.log_fn(f"成功获取验证码: {code}")
        self._checkpoint()
        return code


class RefreshTokenRegistrationEngine:
    """Refresh token 注册引擎。"""

    def __init__(
        self,
        email_service,
        proxy_url: Optional[str] = None,
        callback_logger: Optional[Callable[[str], None]] = None,
        task_uuid: Optional[str] = None,
        browser_mode: str = "protocol",
        max_retries: int = 3,
        extra_config: Optional[dict] = None,
        interrupt_check: Callable[[], None] | None = None,
    ):
        self.email_service = email_service
        self.proxy_url = proxy_url
        self.callback_logger = callback_logger or (lambda msg: logger.info(msg))
        self.task_uuid = task_uuid
        self.browser_mode = str(browser_mode or "protocol").strip().lower() or "protocol"
        # 已移除整流程重试能力，保留参数仅兼容调用方
        self.max_retries = 1
        self.extra_config = dict(extra_config or {})
        self.interrupt_check = interrupt_check

        self.email: Optional[str] = None
        self.password: Optional[str] = None
        self.email_info: Optional[Dict[str, Any]] = None
        self.logs: list[str] = []
        self._create_email_error: str = ""

    @staticmethod
    def _stage_label(stage: str) -> str:
        mapping = {
            "create_email": "创建邮箱",
            "authorize_continue": "进入授权",
            "otp": "邮箱验证码",
            "about_you": "资料提交",
            "workspace_select": "工作区选择",
            "token_exchange": "令牌获取",
            "oauth_login": "OAuth 登录",
            "register_flow": "注册流程",
        }
        return mapping.get(str(stage or "").strip(), str(stage or "").strip() or "未知阶段")

    def _build_failure_metadata(
        self,
        *,
        stage: str,
        origin: str,
        detail: str,
        resume_supported: bool,
    ) -> dict[str, Any]:
        return {
            "success": False,
            "email_service": self.email_service.service_type.value,
            "proxy_used": self.proxy_url,
            "browser_mode": self.browser_mode,
            "email": self.email or "",
            "password": self.password or "",
            "failure_stage": stage,
            "failure_stage_label": self._stage_label(stage),
            "failure_origin": origin,
            "failure_detail": str(detail or "").strip(),
            "resume_supported": bool(resume_supported),
            "email_binding": (
                dict((self.email_info or {}).get("account") or {})
                if isinstance((self.email_info or {}).get("account"), dict)
                else {}
            ),
        }

    def _log(self, message: str, level: str = "info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.logs.append(log_message)

        if self.callback_logger:
            self.callback_logger(log_message)

        if level == "error":
            logger.error(log_message)
        elif level == "warning":
            logger.warning(log_message)
        else:
            logger.info(log_message)

    def _create_email(self) -> bool:
        self._create_email_error = ""
        try:
            self._log(f"正在创建 {self.email_service.service_type.value} 邮箱...")
            self.email_info = self.email_service.create_email()

            email_value = str(
                self.email
                or (self.email_info or {}).get("email")
                or ""
            ).strip()
            if not email_value:
                self._log(
                    f"创建邮箱失败: {self.email_service.service_type.value} 返回空邮箱地址",
                    "error",
                )
                self._create_email_error = (
                    f"{self.email_service.service_type.value} 返回空邮箱地址"
                )
                return False

            if self.email_info is None:
                self.email_info = {}
            self.email_info["email"] = email_value
            self.email = email_value
            self._log(f"成功创建邮箱: {self.email}")
            return True
        except Exception as e:
            self._create_email_error = str(e or "").strip()
            self._log(f"创建邮箱失败: {e}", "error")
            return False

    def _read_int_config(
        self,
        primary_key: str,
        *,
        fallback_keys: tuple[str, ...] = (),
        default: int,
        minimum: int,
        maximum: int,
    ) -> int:
        keys = (primary_key, *tuple(fallback_keys or ()))
        for key in keys:
            if key not in self.extra_config:
                continue
            value = self.extra_config.get(key)
            try:
                parsed = int(value)
            except Exception:
                continue
            return max(minimum, min(parsed, maximum))
        return max(minimum, min(int(default), maximum))

    @staticmethod
    def _should_switch_to_login_after_register_failure(message: str) -> bool:
        text = str(message or "").lower()
        markers = (
            "user_already_exists",
            "account already exists",
            "please login instead",
            "add_phone",
            "add-phone",
            "redirected_to_login_password",
            "login_password",
        )
        return any(marker in text for marker in markers)

    def _build_chatgpt_client(self) -> ChatGPTClient:
        client = ChatGPTClient(
            proxy=self.proxy_url,
            verbose=False,
            browser_mode=self.browser_mode,
            extra_config=self.extra_config,
        )
        client._log = lambda msg: self._log(f"[注册流程] {msg}")
        return client

    def _build_oauth_client(self) -> OAuthClient:
        client = OAuthClient(
            self.extra_config,
            proxy=self.proxy_url,
            verbose=False,
            browser_mode=self.browser_mode,
        )
        client._log = lambda msg: self._log(f"[令牌流程] {msg}")
        return client

    def _reuse_register_browser_context(
        self,
        register_client: ChatGPTClient,
        oauth_client: OAuthClient,
    ) -> None:
        oauth_client.adopt_browser_context(
            register_client.session,
            device_id=getattr(register_client, "device_id", "") or "",
            user_agent=getattr(register_client, "ua", None),
            sec_ch_ua=getattr(register_client, "sec_ch_ua", None),
            accept_language=(
                getattr(register_client.session, "headers", {}).get("Accept-Language", "")
                if getattr(register_client, "session", None) is not None
                else ""
            ),
        )
        oauth_client.impersonate = str(
            getattr(register_client, "impersonate", "") or ""
        ).strip()
        self._log("已接入前序 session/cookie/fingerprint，继续处理 OAuth 后续步骤")

    def _extract_account_info(self, tokens: dict[str, Any]) -> dict[str, Any]:
        id_token = str((tokens or {}).get("id_token") or "").strip()
        if not id_token:
            return {}
        manager = OAuthManager(proxy_url=self.proxy_url)
        return manager.extract_account_info(id_token)

    @staticmethod
    def _extract_workspace_id(oauth_client: OAuthClient) -> str:
        workspace_id = str(getattr(oauth_client, "last_workspace_id", "") or "").strip()
        if workspace_id:
            return workspace_id

        try:
            session_data = oauth_client._decode_oauth_session_cookie() or {}
        except Exception:
            session_data = {}

        workspaces = session_data.get("workspaces") or []
        if not workspaces:
            return ""
        return str((workspaces[0] or {}).get("id") or "").strip()

    @staticmethod
    def _extract_session_token(oauth_client: OAuthClient) -> str:
        getter = getattr(oauth_client, "_get_cookie_value", None)
        if not callable(getter):
            return ""
        return str(
            getter("__Secure-next-auth.session-token", "chatgpt.com")
            or getter("__Secure-authjs.session-token", "chatgpt.com")
            or ""
        ).strip()

    def _populate_result_from_tokens(
        self,
        result: RegistrationResult,
        tokens: dict[str, Any],
        oauth_client: OAuthClient,
        registration_message: str,
        source: str,
        register_client: Any,
    ) -> None:
        account_info = self._extract_account_info(tokens)
        workspace_id = self._extract_workspace_id(oauth_client)
        session_token = self._extract_session_token(oauth_client)

        result.success = True
        result.email = self.email or ""
        result.password = self.password or ""
        result.access_token = str(tokens.get("access_token") or "").strip()
        result.refresh_token = str(tokens.get("refresh_token") or "").strip()
        result.id_token = str(tokens.get("id_token") or "").strip()
        result.account_id = str(
            tokens.get("account_id")
            or account_info.get("account_id")
            or ""
        ).strip()
        result.workspace_id = workspace_id
        result.session_token = session_token
        result.source = source
        result.metadata = {
            "email_service": self.email_service.service_type.value,
            "proxy_used": self.proxy_url,
            "registered_at": datetime.now().isoformat(),
            "registration_message": registration_message,
            "registration_flow": "chatgpt_client.register_complete_flow",
            "token_flow": "oauth_client.login_and_get_tokens",
            "token_login_mode": "passwordless",
            "browser_mode": self.browser_mode,
            "device_id": getattr(register_client, "device_id", ""),
            "impersonate": getattr(register_client, "impersonate", ""),
            "user_agent": getattr(register_client, "ua", ""),
            "workspace_id": workspace_id,
            "account_claims_email": account_info.get("email", ""),
            "email_binding": (
                dict((self.email_info or {}).get("account") or {})
                if isinstance((self.email_info or {}).get("account"), dict)
                else {}
            ),
        }

    def run(self) -> RegistrationResult:
        result = RegistrationResult(success=False, logs=self.logs)
        last_error = ""
        fixed_email = str(self.email or "").strip()
        retry_resume_stage = str(self.extra_config.get("retry_resume_stage") or "").strip().lower()
        retry_resume_origin = str(self.extra_config.get("retry_resume_origin") or "").strip().lower()
        register_otp_wait_seconds = self._read_int_config(
            "chatgpt_register_otp_wait_seconds",
            fallback_keys=("chatgpt_otp_wait_seconds",),
            default=600,
            minimum=30,
            maximum=3600,
        )
        register_otp_resend_wait_seconds = self._read_int_config(
            "chatgpt_register_otp_resend_wait_seconds",
            fallback_keys=("chatgpt_register_otp_wait_seconds", "chatgpt_otp_wait_seconds"),
            default=300,
            minimum=30,
            maximum=3600,
        )

        try:
            registration_message = ""
            source = "register"
            register_client: ChatGPTClient | None = None
            oauth_client: OAuthClient | None = None

            self._log("=" * 60)
            self._log("ChatGPT RT 全新主链路启动")
            self._log(f"请求模式: {self.browser_mode}")
            self._log("实现策略: 注册状态机 + OAuth 接续流程")
            self._log("=" * 60)

            if not fixed_email:
                self.email = None

            self._log("1. 创建邮箱...")
            if not self._create_email():
                last_error = self._create_email_error or "创建邮箱失败"
                result.error_message = last_error
                result.metadata = self._build_failure_metadata(
                    stage="create_email",
                    origin="create_email",
                    detail=last_error,
                    resume_supported=False,
                )
                detail_text = str(last_error or "").strip()
                if any(
                    marker in detail_text
                    for marker in (
                        "本地微软邮箱池为空",
                        "本地微软邮箱池中不存在账号",
                        "本地令牌池为空",
                        "令牌池为空",
                    )
                ):
                    result.metadata["stop_task_on_failure"] = True
                    result.metadata["stop_task_reason"] = detail_text
                return result

            result.email = self.email or ""
            self.password = self.password or generate_random_password(16)
            result.password = self.password

            first_name, last_name = generate_random_name()
            birthdate = generate_random_birthday()
            self._log(f"邮箱: {result.email}")
            self._log(f"密码: {self.password}")
            self._log(f"注册信息: {first_name} {last_name}, 生日: {birthdate}")
            self._log("流程策略: 注册阶段推进到 about_you 后切换到 OAuth 流程继续完成后续步骤")
            self._log(
                "验证码等待策略: "
                f"register_wait={register_otp_wait_seconds}s, "
                f"register_resend_wait={register_otp_resend_wait_seconds}s, "
                "oauth_wait=读取 OAuthClient 配置（默认600s）"
            )

            email_adapter = EmailServiceAdapter(
                self.email_service,
                result.email,
                self._log,
                interrupt_check=self.interrupt_check,
            )

            direct_oauth_retry = retry_resume_stage in {"about_you", "workspace_select", "token_exchange"} or (
                retry_resume_origin == "oauth" and retry_resume_stage in {"authorize_continue", "otp"}
            )
            if direct_oauth_retry:
                registered = False
                registration_message = f"retry_resume:{retry_resume_stage or 'oauth'}"
                source = "retry"
                self._log(f"2. 重试模式：从 {self._stage_label(retry_resume_stage)} 继续")
                register_client = self._build_chatgpt_client()
            else:
                register_client = self._build_chatgpt_client()
                self._log("2. 执行注册状态机（interrupt 模式：不在注册阶段提交 about_you）...")
                registered, registration_message = register_client.register_complete_flow(
                    result.email,
                    self.password,
                    first_name,
                    last_name,
                    birthdate,
                    email_adapter,
                    stop_before_about_you_submission=True,
                    otp_wait_timeout=register_otp_wait_seconds,
                    otp_resend_wait_timeout=register_otp_resend_wait_seconds,
                )

            if not registered and not direct_oauth_retry:
                if not self._should_switch_to_login_after_register_failure(
                    registration_message
                ):
                    last_error = f"注册状态机失败: {registration_message}"
                    result.error_message = last_error
                    result.metadata = self._build_failure_metadata(
                        stage=str(getattr(register_client, "last_stage", "") or "register_flow"),
                        origin="register",
                        detail=last_error,
                        resume_supported=bool(result.email),
                    )
                    return result

                self._log(
                    "注册阶段命中可继续处理的终态，改走 OAuth 登录流程",
                    "warning",
                )
                self._log(f"切换原因: {registration_message}")
                source = "login"
            else:
                if registration_message == "pending_about_you_submission":
                    self._log("注册状态机已推进至 about_you，符合预期。下一步进入 OAuth 会话补全资料")
                else:
                    self._log(
                        "注册状态机返回成功但未停在 about_you。"
                        "将继续进入 OAuth 会话，按状态机实际返回推进。"
                    )

            oauth_client = self._build_oauth_client()
            oauth_client.config.setdefault(
                "chatgpt_oauth_otp_wait_seconds",
                register_otp_wait_seconds,
            )
            oauth_client.config.setdefault("chatgpt_oauth_otp_resend_wait_seconds", 45)

            use_continued_session = (not direct_oauth_retry) and registered and (
                registration_message == "pending_about_you_submission"
            )

            if use_continued_session:
                self._reuse_register_browser_context(register_client, oauth_client)
                self._log("3. 承接前序 session，继续走 OAuth passwordless 流程")
                self._log("4. 沿用前序阶段的 cookie / device_id / 浏览器指纹")
                self._log("5. 登录成功后提交 about_you，并继续 workspace/token 流程")
                tokens = oauth_client.login_and_get_tokens(
                    result.email,
                    self.password,
                    device_id=getattr(register_client, "device_id", "") or "",
                    user_agent=getattr(register_client, "ua", None),
                    sec_ch_ua=getattr(register_client, "sec_ch_ua", None),
                    impersonate=getattr(register_client, "impersonate", None),
                    skymail_client=email_adapter,
                    prefer_passwordless_login=True,
                    allow_phone_verification=False,
                    force_new_browser=False,
                    force_chatgpt_entry=False,
                    screen_hint="login",
                    force_password_login=False,
                    complete_about_you_if_needed=True,
                    first_name=first_name,
                    last_name=last_name,
                    birthdate=birthdate,
                    login_source="post_register_workspace_continue",
                )
            else:
                self._log("3. 新开 OAuth session，按 screen_hint=login + passwordless OTP 登录...")
                self._log("4. 若命中 about_you，则在 OAuth 会话内提交姓名+生日，再继续 workspace/token")
                tokens = oauth_client.login_and_get_tokens(
                    result.email,
                    self.password,
                    device_id="",
                    user_agent=getattr(register_client, "ua", None),
                    sec_ch_ua=getattr(register_client, "sec_ch_ua", None),
                    impersonate=getattr(register_client, "impersonate", None),
                    skymail_client=email_adapter,
                    prefer_passwordless_login=True,
                    allow_phone_verification=False,
                    force_new_browser=True,
                    force_chatgpt_entry=False,
                    screen_hint="login",
                    force_password_login=False,
                    complete_about_you_if_needed=True,
                    first_name=first_name,
                    last_name=last_name,
                    birthdate=birthdate,
                    login_source=(
                        "existing_account_continue" if source == "login" else "post_register_workspace_continue"
                    ),
                )

            if not tokens:
                last_error = oauth_client.last_error or "OAuth 登录状态机失败"
                result.error_message = last_error
                result.metadata = self._build_failure_metadata(
                    stage=str(getattr(oauth_client, "last_stage", "") or "oauth_login"),
                    origin="oauth",
                    detail=last_error,
                    resume_supported=bool(result.email),
                )
                return result

            self._populate_result_from_tokens(
                result=result,
                tokens=tokens,
                oauth_client=oauth_client,
                registration_message=registration_message,
                source=source,
                register_client=register_client,
            )

            self._log("5. 主链路完成")
            self._log(f"Account ID: {result.account_id}")
            self._log(f"Workspace ID: {result.workspace_id}")
            self._log("=" * 60)
            return result

        except TaskInterruption:
            raise
        except DeferAttemptRequested as exc:
            metadata = dict(getattr(exc, "metadata", None) or {})
            if isinstance((self.email_info or {}).get("account"), dict) and not isinstance(metadata.get("email_binding"), dict):
                metadata["email_binding"] = dict((self.email_info or {}).get("account") or {})
            exc.metadata = metadata
            raise
        except Exception as e:
            self._log(f"RT 注册主链路异常: {e}", "error")
            result.error_message = str(e)
            result.metadata = self._build_failure_metadata(
                stage="register_flow",
                origin="engine",
                detail=str(e),
                resume_supported=bool(result.email),
            )
            return result

    def save_to_database(self, result: RegistrationResult) -> bool:
        """保留旧接口，占位返回。"""
        return bool(result and result.success)
