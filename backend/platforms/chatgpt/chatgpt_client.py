"""
ChatGPT 注册客户端模块
使用 curl_cffi 模拟浏览器行为
"""

import random
import queue
import threading
import uuid
import time
from urllib.parse import urlparse
from core.proxy_utils import build_requests_proxy_config, instrument_session_proxy_requests
from core.task_runtime import DeferAttemptRequested, TaskInterruption

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    print("❌ 需要安装 curl_cffi: pip install curl_cffi")
    import sys

    sys.exit(1)

from .sentinel_token import build_sentinel_token
from .sentinel_browser import get_sentinel_token_via_browser
from .utils import (
    FlowState,
    build_browser_headers,
    decode_jwt_payload,
    describe_flow_state,
    extract_flow_state,
    generate_datadog_trace,
    normalize_flow_url,
    random_delay,
    seed_oai_device_cookie,
)


# Chrome 指纹配置
_CHROME_PROFILES = [
    {
        "major": 131,
        "impersonate": "chrome131",
        "build": 6778,
        "patch_range": (69, 205),
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    },
    {
        "major": 133,
        "impersonate": "chrome133a",
        "build": 6943,
        "patch_range": (33, 153),
        "sec_ch_ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    },
    {
        "major": 136,
        "impersonate": "chrome136",
        "build": 7103,
        "patch_range": (48, 175),
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    },
]


def _random_chrome_version():
    """随机选择一个 Chrome 版本"""
    profile = random.choice(_CHROME_PROFILES)
    major = profile["major"]
    build = profile["build"]
    patch = random.randint(*profile["patch_range"])
    full_ver = f"{major}.0.{build}.{patch}"
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_ver} Safari/537.36"
    return profile["impersonate"], major, full_ver, ua, profile["sec_ch_ua"]


class ChatGPTClient:
    """ChatGPT 注册客户端"""

    BASE = "https://chatgpt.com"
    AUTH = "https://auth.openai.com"

    def __init__(self, proxy=None, verbose=True, browser_mode="protocol", extra_config=None):
        """
        初始化 ChatGPT 客户端

        Args:
            proxy: 代理地址
            verbose: 是否输出详细日志
            browser_mode: protocol | headless | headed
        """
        self.proxy = proxy
        self.verbose = verbose
        self.browser_mode = browser_mode or "protocol"
        self.config = dict(extra_config or {})
        self.device_id = str(uuid.uuid4())
        self.accept_language = random.choice(
            [
                "en-US,en;q=0.9",
                "en-US,en;q=0.9,zh-CN;q=0.8",
                "en,en-US;q=0.9",
                "en-US,en;q=0.8",
            ]
        )

        # 随机 Chrome 版本
        (
            self.impersonate,
            self.chrome_major,
            self.chrome_full,
            self.ua,
            self.sec_ch_ua,
        ) = _random_chrome_version()

        # 创建 session
        self.session = curl_requests.Session(impersonate=self.impersonate)
        instrument_session_proxy_requests(self.session, proxy_url=self.proxy)

        if self.proxy:
            self.session.proxies = build_requests_proxy_config(self.proxy)

        # 设置基础 headers
        self.session.headers.update(
            {
                "User-Agent": self.ua,
                "Accept-Language": self.accept_language,
                "sec-ch-ua": self.sec_ch_ua,
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-ch-ua-arch": '"x86"',
                "sec-ch-ua-bitness": '"64"',
                "sec-ch-ua-full-version": f'"{self.chrome_full}"',
                "sec-ch-ua-platform-version": f'"{random.randint(10, 15)}.0.0"',
            }
        )

        # 设置 oai-did cookie
        seed_oai_device_cookie(self.session, self.device_id)
        self.last_registration_state = FlowState()
        self.last_stage = ""

    def _otp_suspend_settings(self, prefix: str) -> tuple[int, int, int]:
        suspend_after = max(
            90,
            int(self.config.get(f"{prefix}_otp_suspend_after_seconds", 180) or 180),
        )
        suspend_poll = max(
            30,
            int(self.config.get(f"{prefix}_otp_suspend_poll_seconds", 120) or 120),
        )
        total_timeout = max(
            suspend_after,
            int(self.config.get(f"{prefix}_otp_total_timeout_seconds", 1800) or 1800),
        )
        return suspend_after, suspend_poll, total_timeout

    @staticmethod
    def _otp_wait_interval(
        *,
        elapsed_seconds: int,
        remaining_seconds: int,
        suspend_after: int,
        suspended_probe_seconds: int = 10,
    ) -> int:
        if elapsed_seconds >= suspend_after:
            return max(1, min(int(suspended_probe_seconds or 10), remaining_seconds))

        wait_time = 30
        if elapsed_seconds >= 90:
            wait_time = min(90, 30 + (((elapsed_seconds - 90) // 30) + 1) * 15)
        return max(1, min(wait_time, remaining_seconds))

    def _clear_otp_suspend_state(self, prefix: str) -> None:
        for key in (
            f"{prefix}_otp_started_at",
            f"{prefix}_otp_total_deadline_at",
            f"{prefix}_otp_sent_at",
            f"{prefix}_otp_next_resend_at",
            f"{prefix}_otp_send_attempts",
        ):
            self.config.pop(key, None)

    def _build_otp_defer(
        self,
        *,
        prefix: str,
        origin: str,
        stage: str,
        started_at: float,
        deadline_at: float,
        otp_sent_at: float,
        next_resend_at: float,
        otp_send_attempts: int,
        delay_seconds: int,
    ) -> DeferAttemptRequested:
        return DeferAttemptRequested(
            "等待验证码超过 3 分钟，先挂起当前账号",
            delay_seconds=delay_seconds,
            metadata={
                "retry_resume_stage": stage,
                "retry_resume_origin": origin,
                "config_overrides": {
                    f"{prefix}_otp_started_at": float(started_at),
                    f"{prefix}_otp_total_deadline_at": float(deadline_at),
                    f"{prefix}_otp_sent_at": float(otp_sent_at),
                    f"{prefix}_otp_next_resend_at": float(next_resend_at),
                    f"{prefix}_otp_send_attempts": int(otp_send_attempts),
                },
            },
        )

    def _get_sentinel_token(self, flow: str, *, page_url: str | None = None):
        prefer_browser = flow in {"username_password_create", "oauth_create_account"}
        for attempt in range(2):
            if prefer_browser:
                token, source = self._get_sentinel_token_race(flow=flow, page_url=page_url)
                if token:
                    if source == "browser":
                        self._log(f"{flow}: 已通过 Playwright SentinelSDK 获取 token")
                    else:
                        self._log(f"{flow}: 已通过 HTTP PoW 获取 token")
                    return token

            token = None
            if not prefer_browser:
                token = build_sentinel_token(
                    self.session,
                    self.device_id,
                    flow=flow,
                    user_agent=self.ua,
                    sec_ch_ua=self.sec_ch_ua,
                    impersonate=self.impersonate,
                )
                if token:
                    self._log(f"{flow}: 已通过 HTTP PoW 获取 token")
                    return token
            if attempt < 1:
                self._log(f"{flow}: sentinel 获取失败，准备重试")
                time.sleep(1.0)
        return None

    def _get_sentinel_token_race(self, *, flow: str, page_url: str | None = None) -> tuple[str | None, str]:
        result_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        done = threading.Event()

        def browser_runner() -> None:
            try:
                token = get_sentinel_token_via_browser(
                    flow=flow,
                    proxy=self.proxy,
                    page_url=page_url,
                    headless=self.browser_mode != "headed",
                    device_id=self.device_id,
                    log_fn=lambda msg: (not done.is_set()) and self._log(msg),
                )
            except Exception:
                token = None
            if token and not done.is_set():
                result_queue.put(("browser", token))

        def http_runner() -> None:
            try:
                token = build_sentinel_token(
                    self.session,
                    self.device_id,
                    flow=flow,
                    user_agent=self.ua,
                    sec_ch_ua=self.sec_ch_ua,
                    impersonate=self.impersonate,
                )
            except Exception:
                token = None
            if token and not done.is_set():
                result_queue.put(("http", token))

        browser_thread = threading.Thread(target=browser_runner, daemon=True)
        http_thread = threading.Thread(target=http_runner, daemon=True)
        browser_thread.start()
        http_thread.start()

        deadline = time.time() + 16
        while time.time() < deadline:
            try:
                source, token = result_queue.get(timeout=0.25)
            except queue.Empty:
                continue
            done.set()
            return token, source

        done.set()
        return None, ""

    def _log(self, msg):
        """输出日志"""
        if self.verbose:
            print(f"  {msg}")

    def _enter_stage(self, stage: str, detail: str = ""):
        self.last_stage = str(stage or "").strip()
        if self.last_stage:
            message = f"[stage={self.last_stage}]"
            if detail:
                message += f" {detail}"
            self._log(message)

    def _browser_pause(self, low=0.15, high=0.45):
        """在 headed 模式下加入轻微停顿，模拟有头浏览器节奏。"""
        if self.browser_mode == "headed":
            random_delay(low, high)

    def _headers(
        self,
        url,
        *,
        accept,
        referer=None,
        origin=None,
        content_type=None,
        navigation=False,
        fetch_mode=None,
        fetch_dest=None,
        fetch_site=None,
        extra_headers=None,
    ):
        return build_browser_headers(
            url=url,
            user_agent=self.ua,
            sec_ch_ua=self.sec_ch_ua,
            chrome_full_version=self.chrome_full,
            accept=accept,
            accept_language=self.accept_language,
            referer=referer,
            origin=origin,
            content_type=content_type,
            navigation=navigation,
            fetch_mode=fetch_mode,
            fetch_dest=fetch_dest,
            fetch_site=fetch_site,
            headed=self.browser_mode == "headed",
            extra_headers=extra_headers,
        )

    def _reset_session(self):
        """重置浏览器指纹与会话，用于绕过偶发的 Cloudflare/SPA 中间页。"""
        self.device_id = str(uuid.uuid4())
        (
            self.impersonate,
            self.chrome_major,
            self.chrome_full,
            self.ua,
            self.sec_ch_ua,
        ) = _random_chrome_version()
        self.accept_language = random.choice(
            [
                "en-US,en;q=0.9",
                "en-US,en;q=0.9,zh-CN;q=0.8",
                "en,en-US;q=0.9",
                "en-US,en;q=0.8",
            ]
        )

        self.session = curl_requests.Session(impersonate=self.impersonate)
        instrument_session_proxy_requests(self.session, proxy_url=self.proxy)
        if self.proxy:
            self.session.proxies = build_requests_proxy_config(self.proxy)

        self.session.headers.update(
            {
                "User-Agent": self.ua,
                "Accept-Language": self.accept_language,
                "sec-ch-ua": self.sec_ch_ua,
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-ch-ua-arch": '"x86"',
                "sec-ch-ua-bitness": '"64"',
                "sec-ch-ua-full-version": f'"{self.chrome_full}"',
                "sec-ch-ua-platform-version": f'"{random.randint(10, 15)}.0.0"',
            }
        )
        seed_oai_device_cookie(self.session, self.device_id)

    def _state_from_url(self, url, method="GET"):
        state = extract_flow_state(
            current_url=normalize_flow_url(url, auth_base=self.AUTH),
            auth_base=self.AUTH,
            default_method=method,
        )
        if method:
            state.method = str(method).upper()
        return state

    def _state_from_payload(self, data, current_url=""):
        return extract_flow_state(
            data=data,
            current_url=current_url,
            auth_base=self.AUTH,
        )

    def _state_signature(self, state: FlowState):
        return (
            state.page_type or "",
            state.method or "",
            state.continue_url or "",
            state.current_url or "",
        )

    def _is_registration_complete_state(self, state: FlowState):
        current_url = (state.current_url or "").lower()
        continue_url = (state.continue_url or "").lower()
        page_type = state.page_type or ""
        return (
            page_type in {"callback", "chatgpt_home", "oauth_callback"}
            or ("chatgpt.com" in current_url and "redirect_uri" not in current_url)
            or (
                "chatgpt.com" in continue_url
                and "redirect_uri" not in continue_url
                and page_type != "external_url"
            )
        )

    def _state_is_password_registration(self, state: FlowState):
        return state.page_type in {"create_account_password", "password"}

    def _state_is_login_password(self, state: FlowState):
        target = f"{state.continue_url} {state.current_url}".lower()
        return state.page_type == "login_password" or "log-in/password" in target

    def _state_is_email_otp(self, state: FlowState):
        target = f"{state.continue_url} {state.current_url}".lower()
        return (
            state.page_type == "email_otp_verification"
            or "email-verification" in target
            or ("email-otp" in target and "email-otp/send" not in target)
        )

    def _state_is_email_otp_send(self, state: FlowState):
        target = f"{state.continue_url} {state.current_url}".lower()
        return (
            state.page_type == "email_otp_send"
            or "email-otp/send" in target
        )

    def _state_is_about_you(self, state: FlowState):
        target = f"{state.continue_url} {state.current_url}".lower()
        return state.page_type == "about_you" or "about-you" in target

    def _state_requires_navigation(self, state: FlowState):
        if (state.method or "GET").upper() != "GET":
            return False
        if state.page_type == "external_url" and state.continue_url:
            return True
        if state.continue_url and state.continue_url != state.current_url:
            return True
        return False

    def _follow_flow_state(self, state: FlowState, referer=None):
        """跟随服务端返回的 continue_url，推进注册状态机。"""
        target_url = state.continue_url or state.current_url
        if not target_url:
            return False, "缺少可跟随的 continue_url"

        try:
            self._browser_pause()
            r = self.session.get(
                target_url,
                headers=self._headers(
                    target_url,
                    accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    referer=referer,
                    navigation=True,
                ),
                allow_redirects=True,
                timeout=30,
            )
            final_url = str(r.url)
            self._log(f"follow -> {r.status_code} {final_url}")

            content_type = (r.headers.get("content-type", "") or "").lower()
            if "application/json" in content_type:
                try:
                    next_state = self._state_from_payload(
                        r.json(), current_url=final_url
                    )
                except Exception:
                    next_state = self._state_from_url(final_url)
            else:
                next_state = self._state_from_url(final_url)

            self._log(f"follow state -> {describe_flow_state(next_state)}")
            return True, next_state
        except Exception as e:
            self._log(f"跟随 continue_url 失败: {e}")
            return False, str(e)

    def _get_cookie_value(self, name, domain_hint=None):
        """读取当前会话中的 Cookie。"""
        for cookie in self.session.cookies.jar:
            if cookie.name != name:
                continue
            if domain_hint and domain_hint not in (cookie.domain or ""):
                continue
            return cookie.value
        return ""

    def get_next_auth_session_token(self):
        """获取 ChatGPT next-auth 会话 Cookie。"""
        return (
            self._get_cookie_value("__Secure-next-auth.session-token", "chatgpt.com")
            or self._get_cookie_value("__Secure-authjs.session-token", "chatgpt.com")
        )

    def fetch_chatgpt_session(self, max_attempts=5, retry_delay=1.2):
        """请求 ChatGPT Session 接口并返回原始会话数据。"""
        url = f"{self.BASE}/api/auth/session"
        last_error = ""

        for attempt in range(max(1, int(max_attempts or 1))):
            try:
                self._browser_pause()
                response = self.session.get(
                    url,
                    headers=self._headers(
                        url,
                        accept="application/json",
                        referer=f"{self.BASE}/",
                        fetch_site="same-origin",
                    ),
                    timeout=30,
                )
            except Exception as exc:
                last_error = f"/api/auth/session 请求异常: {exc}"
                if attempt < max_attempts - 1:
                    self._log(
                        f"{last_error}，等待 {retry_delay:.1f}s 后重试 "
                        f"({attempt + 1}/{max_attempts})"
                    )
                    time.sleep(retry_delay)
                    continue
                return False, last_error

            if response.status_code != 200:
                last_error = f"/api/auth/session -> HTTP {response.status_code}"
                if attempt < max_attempts - 1:
                    self._log(
                        f"{last_error}，等待 {retry_delay:.1f}s 后重试 "
                        f"({attempt + 1}/{max_attempts})"
                    )
                    time.sleep(retry_delay)
                    continue
                return False, last_error

            try:
                data = response.json()
            except Exception as exc:
                last_error = f"/api/auth/session 返回非 JSON: {exc}"
                if attempt < max_attempts - 1:
                    self._log(
                        f"{last_error}，等待 {retry_delay:.1f}s 后重试 "
                        f"({attempt + 1}/{max_attempts})"
                    )
                    time.sleep(retry_delay)
                    continue
                return False, last_error

            access_token = str(data.get("accessToken") or "").strip()
            if access_token:
                return True, data

            last_error = "/api/auth/session 未返回 accessToken"
            if attempt < max_attempts - 1:
                self._log(
                    f"{last_error}，等待 {retry_delay:.1f}s 后重试 "
                    f"({attempt + 1}/{max_attempts})"
                )
                try:
                    self.session.get(
                        f"{self.BASE}/",
                        headers=self._headers(
                            f"{self.BASE}/",
                            accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            referer=f"{self.BASE}/",
                            navigation=True,
                        ),
                        allow_redirects=True,
                        timeout=30,
                    )
                except Exception:
                    pass
                time.sleep(retry_delay)
                continue

            return False, last_error

        return False, last_error or "/api/auth/session 未返回 accessToken"

    def reuse_session_and_get_tokens(self):
        """
        承接前序阶段已建立的 ChatGPT 会话，直接读取 Session / AccessToken。

        Returns:
            tuple[bool, dict|str]: 成功时返回标准化 token/session 数据；失败时返回错误信息。
        """
        self._enter_stage("token_exchange", "reuse session -> /api/auth/session")
        state = self.last_registration_state or FlowState()
        self._log("步骤 1/4: 跟随注册回调 external_url ...")
        if state.page_type == "external_url" or self._state_requires_navigation(state):
            ok, followed = self._follow_flow_state(
                state,
                referer=state.current_url or f"{self.AUTH}/about-you",
            )
            if not ok:
                return False, f"注册回调落地失败: {followed}"
            self.last_registration_state = followed
        else:
            self._log("注册回调已落地，跳过额外跟随")

        self._log("步骤 2/4: 检查 __Secure-next-auth.session-token ...")
        session_cookie = ""
        for attempt in range(5):
            session_cookie = self.get_next_auth_session_token()
            if session_cookie:
                break
            self._log(
                f"next-auth session cookie 尚未落地，补一次 ChatGPT 首页触达 "
                f"({attempt + 1}/5)"
            )
            try:
                self._browser_pause(0.2, 0.5)
                self.session.get(
                    f"{self.BASE}/",
                    headers=self._headers(
                        f"{self.BASE}/",
                        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        referer=state.current_url or f"{self.AUTH}/about-you",
                        navigation=True,
                    ),
                    allow_redirects=True,
                    timeout=30,
                )
            except Exception as exc:
                self._log(f"补触达 ChatGPT 首页异常: {exc}")
            time.sleep(1.0)
        if not session_cookie:
            return False, "缺少 ChatGPT session-token，注册回调可能未完全落地"

        self._log("步骤 3/4: 请求 ChatGPT /api/auth/session ...")
        ok, session_or_error = self.fetch_chatgpt_session()
        if not ok:
            return False, session_or_error

        session_data = session_or_error
        access_token = str(session_data.get("accessToken") or "").strip()
        session_token = str(
            session_data.get("sessionToken") or session_cookie or ""
        ).strip()
        user = session_data.get("user") or {}
        account = session_data.get("account") or {}
        jwt_payload = decode_jwt_payload(access_token)
        auth_payload = jwt_payload.get("https://api.openai.com/auth") or {}

        account_id = (
            str(account.get("id") or "").strip()
            or str(auth_payload.get("chatgpt_account_id") or "").strip()
        )
        user_id = (
            str(user.get("id") or "").strip()
            or str(auth_payload.get("chatgpt_user_id") or "").strip()
            or str(auth_payload.get("user_id") or "").strip()
        )

        normalized = {
            "access_token": access_token,
            "session_token": session_token,
            "account_id": account_id,
            "user_id": user_id,
            "workspace_id": account_id,
            "expires": session_data.get("expires"),
            "user": user,
            "account": account,
            "auth_provider": session_data.get("authProvider"),
            "raw_session": session_data,
        }

        self._log("步骤 4/4: 已从当前会话中提取 accessToken")
        if account_id:
            self._log(f"Session Account ID: {account_id}")
        if user_id:
            self._log(f"Session User ID: {user_id}")
        return True, normalized

    def visit_homepage(self):
        """访问首页，建立 session"""
        self._log("访问 ChatGPT 首页...")
        url = f"{self.BASE}/"
        try:
            self._browser_pause()
            r = self.session.get(
                url,
                headers=self._headers(
                    url,
                    accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    navigation=True,
                ),
                allow_redirects=True,
                timeout=30,
            )
            return r.status_code == 200
        except Exception as e:
            self._log(f"访问首页失败: {e}")
            return False

    def get_csrf_token(self):
        """获取 CSRF token"""
        self._log("获取 CSRF token...")
        url = f"{self.BASE}/api/auth/csrf"
        try:
            r = self.session.get(
                url,
                headers=self._headers(
                    url,
                    accept="application/json",
                    referer=f"{self.BASE}/",
                    fetch_site="same-origin",
                ),
                timeout=30,
            )

            if r.status_code == 200:
                data = r.json()
                token = data.get("csrfToken", "")
                if token:
                    self._log(f"CSRF token: {token[:20]}...")
                    return token
        except Exception as e:
            self._log(f"获取 CSRF token 失败: {e}")

        return None

    def signin(self, email, csrf_token):
        """
        提交邮箱，获取 authorize URL

        Returns:
            str: authorize URL
        """
        self._log(f"提交邮箱: {email}")
        url = f"{self.BASE}/api/auth/signin/openai"

        params = {
            "prompt": "login",
            "ext-oai-did": self.device_id,
            "auth_session_logging_id": str(uuid.uuid4()),
            "screen_hint": "login_or_signup",
            "login_hint": email,
        }

        form_data = {
            "callbackUrl": f"{self.BASE}/",
            "csrfToken": csrf_token,
            "json": "true",
        }

        try:
            self._browser_pause()
            r = self.session.post(
                url,
                params=params,
                data=form_data,
                headers=self._headers(
                    url,
                    accept="application/json",
                    referer=f"{self.BASE}/",
                    origin=self.BASE,
                    content_type="application/x-www-form-urlencoded",
                    fetch_site="same-origin",
                ),
                timeout=30,
            )

            if r.status_code == 200:
                data = r.json()
                authorize_url = data.get("url", "")
                if authorize_url:
                    self._log(f"获取到 authorize URL")
                    return authorize_url
        except Exception as e:
            self._log(f"提交邮箱失败: {e}")

        return None

    def authorize(self, url, max_retries=3):
        """
        访问 authorize URL，跟随重定向（带重试机制）
        这是关键步骤，建立 auth.openai.com 的 session

        Returns:
            str: 最终重定向的 URL
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self._log(
                        f"访问 authorize URL... (尝试 {attempt + 1}/{max_retries})"
                    )
                    time.sleep(1)  # 重试前等待
                else:
                    self._log("访问 authorize URL...")

                self._browser_pause()
                r = self.session.get(
                    url,
                    headers=self._headers(
                        url,
                        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        referer=f"{self.BASE}/",
                        navigation=True,
                    ),
                    allow_redirects=True,
                    timeout=30,
                )

                final_url = str(r.url)
                self._log(f"重定向到: {final_url}")
                return final_url

            except Exception as e:
                error_msg = str(e)
                is_tls_error = (
                    "TLS" in error_msg
                    or "SSL" in error_msg
                    or "curl: (35)" in error_msg
                )

                if is_tls_error and attempt < max_retries - 1:
                    self._log(
                        f"Authorize TLS 错误 (尝试 {attempt + 1}/{max_retries}): {error_msg[:100]}"
                    )
                    continue
                else:
                    self._log(f"Authorize 失败: {e}")
                    return ""

        return ""

    def callback(self, callback_url=None, referer=None):
        """完成注册回调"""
        self._log("执行回调...")
        url = callback_url or f"{self.AUTH}/api/accounts/authorize/callback"
        ok, _ = self._follow_flow_state(
            self._state_from_url(url),
            referer=referer or f"{self.AUTH}/about-you",
        )
        return ok

    def register_user(self, email, password, return_state=False):
        """
        注册用户（邮箱 + 密码）

        Returns:
            tuple: (success, message)
        """
        self._enter_stage("authorize_continue", f"register_user email={email}")
        self._log(f"注册用户: {email}")
        url = f"{self.AUTH}/api/accounts/user/register"

        headers = self._headers(
            url,
            accept="application/json",
            referer=f"{self.AUTH}/create-account/password",
            origin=self.AUTH,
            content_type="application/json",
            fetch_site="same-origin",
        )
        headers.update(generate_datadog_trace())
        headers["oai-device-id"] = self.device_id

        sentinel_token = self._get_sentinel_token(
            "username_password_create",
            page_url=f"{self.AUTH}/create-account/password",
        )
        if sentinel_token:
            headers["openai-sentinel-token"] = sentinel_token

        payload = {
            "username": email,
            "password": password,
        }

        try:
            self._browser_pause()
            r = self.session.post(url, json=payload, headers=headers, timeout=30)

            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = {}
                next_state = self._state_from_payload(
                    data,
                    current_url=str(r.url) or f"{self.AUTH}/api/accounts/email-otp/send",
                )
                self._log("注册成功")
                self._log(f"authorize_continue/register_user 响应 URL: {str(r.url)[:120]}")
                self._log(f"register_user 响应状态: {describe_flow_state(next_state)}")
                return (True, next_state) if return_state else (True, "注册成功")
            else:
                try:
                    error_data = r.json()
                    error_msg = error_data.get("error", {}).get("message", r.text[:200])
                except:
                    error_msg = r.text[:200]
                self._log(f"注册失败: {r.status_code} - {error_msg}")
                return False, f"HTTP {r.status_code}: {error_msg}"

        except Exception as e:
            self._log(f"注册异常: {e}")
            return False, str(e)

    def send_email_otp(self, referer=None, target_url=None, return_state=False):
        """触发发送邮箱验证码"""
        self._enter_stage("otp", "send email otp")
        self._log("触发发送验证码...")
        url = target_url or f"{self.AUTH}/api/accounts/email-otp/send"

        try:
            self._browser_pause()
            r = self.session.get(
                url,
                headers=self._headers(
                    url,
                    accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    referer=referer or f"{self.AUTH}/create-account/password",
                    navigation=True,
                    fetch_site="same-origin",
                ),
                allow_redirects=False,
                timeout=30,
            )
            self._log(f"验证码发送状态: {r.status_code}")
            if r.status_code in {301, 302, 303, 307, 308}:
                location = normalize_flow_url(
                    r.headers.get("location") or f"{self.AUTH}/email-verification",
                    auth_base=self.AUTH,
                )
                self._log(f"验证码发送跳转: {location[:120]}")
                next_state = self._state_from_url(location)
                return (True, next_state) if return_state else True

            if r.status_code != 200:
                self._log(f"验证码发送失败响应: {r.text[:180]}")
                return (False, FlowState()) if return_state else False

            try:
                payload = r.json()
            except Exception:
                payload = {}

            if isinstance(payload, dict) and payload:
                next_state = self._state_from_payload(payload, current_url=str(r.url) or url)
                self._log(f"验证码发送响应: {describe_flow_state(next_state)}")
                self._log(f"otp/send 当前 URL: {str(r.url)[:120]}")
                return (True, next_state) if return_state else True
            else:
                next_state = self._state_from_url(str(r.url) or f"{self.AUTH}/email-verification")
                self._log("验证码发送响应: 非 JSON（按已触发处理）")
                self._log(f"验证码发送最终状态: {describe_flow_state(next_state)}")
                return (True, next_state) if return_state else True
        except Exception as e:
            self._log(f"发送验证码失败: {e}")
            return (False, FlowState()) if return_state else False

    def verify_email_otp(self, otp_code, return_state=False):
        """
        验证邮箱 OTP 码

        Args:
            otp_code: 6位验证码

        Returns:
            tuple: (success, message)
        """
        self._enter_stage("otp", f"verify email otp code={otp_code}")
        self._log(f"验证 OTP 码: {otp_code}")
        url = f"{self.AUTH}/api/accounts/email-otp/validate"

        headers = self._headers(
            url,
            accept="application/json",
            referer=f"{self.AUTH}/email-verification",
            origin=self.AUTH,
            content_type="application/json",
            fetch_site="same-origin",
        )
        headers.update(generate_datadog_trace())

        payload = {"code": otp_code}

        try:
            self._browser_pause()
            r = self.session.post(url, json=payload, headers=headers, timeout=30)

            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = {}
                next_state = self._state_from_payload(
                    data, current_url=str(r.url) or f"{self.AUTH}/about-you"
                )
                self._log(f"验证成功 {describe_flow_state(next_state)}")
                self._log(f"otp/validate 当前 URL: {str(r.url)[:120]}")
                return (True, next_state) if return_state else (True, "验证成功")
            else:
                error_msg = r.text[:200]
                error_code = ""
                try:
                    payload = r.json()
                    if isinstance(payload, dict):
                        error_code = str(((payload.get("error") or {}).get("code") or "")).strip()
                except Exception:
                    error_code = ""
                self._log(f"验证失败: {r.status_code} - {error_msg}")
                detail = error_code or error_msg
                return False, f"HTTP {r.status_code}: {detail}"

        except Exception as e:
            self._log(f"验证异常: {e}")
            return False, str(e)

    def create_account(self, first_name, last_name, birthdate, return_state=False):
        """
        完成账号创建（提交姓名和生日）

        Args:
            first_name: 名
            last_name: 姓
            birthdate: 生日 (YYYY-MM-DD)

        Returns:
            tuple: (success, message)
        """
        self._enter_stage("about_you", "register create_account")
        name = f"{first_name} {last_name}"
        self._log(f"完成账号创建: {name}")
        url = f"{self.AUTH}/api/accounts/create_account"

        sentinel_token = self._get_sentinel_token(
            "oauth_create_account",
            page_url=f"{self.AUTH}/about-you",
        )
        if sentinel_token:
            self._log("create_account: 已生成 sentinel token")
        else:
            self._log("create_account: 未生成 sentinel token，降级继续请求")

        headers = self._headers(
            url,
            accept="application/json",
            referer=f"{self.AUTH}/about-you",
            origin=self.AUTH,
            content_type="application/json",
            fetch_site="same-origin",
            extra_headers={
                "oai-device-id": self.device_id,
            },
        )
        if sentinel_token:
            headers["openai-sentinel-token"] = sentinel_token
        headers.update(generate_datadog_trace())

        payload = {
            "name": name,
            "birthdate": birthdate,
        }

        try:
            self._browser_pause()
            r = self.session.post(url, json=payload, headers=headers, timeout=30)

            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = {}
                next_state = self._state_from_payload(
                    data, current_url=str(r.url) or self.BASE
                )
                self._log(f"账号创建成功 {describe_flow_state(next_state)}")
                self._log(f"about_you/create_account 当前 URL: {str(r.url)[:120]}")
                return (True, next_state) if return_state else (True, "账号创建成功")
            else:
                error_code = ""
                error_msg = r.text[:200]
                try:
                    error_data = r.json() or {}
                    error_info = error_data.get("error") or {}
                    error_code = str(error_info.get("code") or "").strip()
                    error_msg = str(error_info.get("message") or error_msg).strip()
                except Exception:
                    pass

                detail = f"HTTP {r.status_code}"
                if error_code:
                    detail += f": {error_code}"
                elif error_msg:
                    detail += f": {error_msg}"

                self._log(f"创建失败: {detail} - {error_msg[:200]}")
                return False, detail

        except Exception as e:
            self._log(f"创建异常: {e}")
            return False, str(e)

    def register_complete_flow(
        self,
        email,
        password,
        first_name,
        last_name,
        birthdate,
        skymail_client,
        stop_before_about_you_submission=False,
        otp_wait_timeout=600,
        otp_resend_wait_timeout=300,
    ):
        """
        完整的注册流程（基于原版 run_register 方法）

        Args:
            email: 邮箱
            password: 密码
            first_name: 名
            last_name: 姓
            birthdate: 生日
            skymail_client: Skymail 客户端（用于获取验证码）

        Returns:
            tuple: (success, message)
        """
        from urllib.parse import urlparse

        self._log(
            "注册状态机参数: "
            f"stop_before_about_you_submission={'on' if stop_before_about_you_submission else 'off'}, "
            f"otp_wait_timeout={otp_wait_timeout}s, otp_resend_wait_timeout={otp_resend_wait_timeout}s"
        )

        try:
            otp_wait_timeout = max(30, int(otp_wait_timeout or 600))
        except Exception:
            otp_wait_timeout = 600
        try:
            otp_resend_wait_timeout = max(30, int(otp_resend_wait_timeout or 300))
        except Exception:
            otp_resend_wait_timeout = 300

        max_auth_attempts = 3
        final_url = ""
        final_path = ""

        for auth_attempt in range(max_auth_attempts):
            if auth_attempt > 0:
                self._log(f"预授权阶段重试 {auth_attempt + 1}/{max_auth_attempts}...")
                self._reset_session()

            # 1. 访问首页
            if not self.visit_homepage():
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "访问首页失败"

            # 2. 获取 CSRF token
            csrf_token = self.get_csrf_token()
            if not csrf_token:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "获取 CSRF token 失败"

            # 3. 提交邮箱，获取 authorize URL
            auth_url = self.signin(email, csrf_token)
            if not auth_url:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "提交邮箱失败"

            # 4. 访问 authorize URL（关键步骤！）
            final_url = self.authorize(auth_url)
            if not final_url:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "Authorize 失败"

            final_path = urlparse(final_url).path
            self._log(f"Authorize → {final_path}")

            # /api/accounts/authorize 实际上常对应 Cloudflare 403 中间页，不要继续走 authorize_continue。
            if "api/accounts/authorize" in final_path or final_path == "/error":
                self._log(
                    f"检测到 Cloudflare/SPA 中间页，准备重试预授权: {final_url[:160]}..."
                )
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, f"预授权被拦截: {final_path}"

            break

        state = self._state_from_url(final_url)
        self._log(f"注册状态起点: {describe_flow_state(state)}")

        register_submitted = False
        otp_verified = False
        account_created = False
        seen_states = {}

        otp_send_attempts = 0
        otp_sent_at = 0.0
        tried_otp_codes: set[str] = set()

        for _ in range(12):
            signature = self._state_signature(state)
            seen_states[signature] = seen_states.get(signature, 0) + 1
            self._log(
                f"注册状态推进: step={sum(seen_states.values())} "
                f"state={describe_flow_state(state)} seen={seen_states[signature]}"
            )
            if seen_states[signature] > 2:
                return False, f"注册状态卡住: {describe_flow_state(state)}"

            if self._is_registration_complete_state(state):
                self.last_registration_state = state
                self._log("✅ 注册流程完成")
                return True, "注册成功"

            if self._state_is_password_registration(state):
                self._enter_stage("authorize_continue", describe_flow_state(state))
                self._log("全新注册流程")
                if register_submitted:
                    return False, "注册密码阶段重复进入"
                success, next_state = self.register_user(email, password, return_state=True)
                if not success:
                    return False, f"注册失败: {next_state}"
                register_submitted = True
                state = next_state or self._state_from_url(f"{self.AUTH}/api/accounts/email-otp/send")
                continue

            if self._state_is_email_otp_send(state):
                self._enter_stage("otp", describe_flow_state(state))
                self._log("跟随注册验证码发送状态...")
                otp_send_attempts += 1
                success, next_state = self.send_email_otp(
                    referer=state.current_url or state.continue_url or f"{self.AUTH}/create-account/password",
                    target_url=state.continue_url or state.current_url or f"{self.AUTH}/api/accounts/email-otp/send",
                    return_state=True,
                )
                if not success:
                    return False, "发送注册验证码失败"
                otp_sent_at = time.time() - 15
                self._log(f"发送注册验证码成功: attempt={otp_send_attempts}")
                state = next_state or self._state_from_url(f"{self.AUTH}/email-verification")
                continue

            if self._state_is_email_otp(state):
                self._enter_stage("otp", describe_flow_state(state))
                self._log("等待邮箱验证码...")
                suspend_after, suspend_poll, total_timeout = self._otp_suspend_settings("chatgpt_register")
                now = time.time()
                otp_started_at = float(self.config.get("chatgpt_register_otp_started_at") or now)
                otp_deadline = float(
                    self.config.get("chatgpt_register_otp_total_deadline_at")
                    or (otp_started_at + total_timeout)
                )
                self.config["chatgpt_register_otp_started_at"] = float(otp_started_at)
                self.config["chatgpt_register_otp_total_deadline_at"] = float(otp_deadline)
                if otp_deadline <= now:
                    self._clear_otp_suspend_state("chatgpt_register")
                    return False, "等待邮箱验证码超时 (30分钟)"
                otp_sent_at = float(self.config.get("chatgpt_register_otp_sent_at") or otp_sent_at or (now - 15))
                next_resend_at = float(
                    self.config.get("chatgpt_register_otp_next_resend_at")
                    or min(otp_deadline, now + otp_resend_wait_timeout)
                )
                otp_send_attempts = max(
                    int(self.config.get("chatgpt_register_otp_send_attempts") or 0),
                    otp_send_attempts,
                )
                otp_code = ""

                while time.time() < otp_deadline:
                    now = time.time()
                    elapsed = max(0, int(now - otp_started_at))
                    remaining = max(1, int(otp_deadline - now))
                    is_suspended_cycle = elapsed >= suspend_after
                    wait_time = self._otp_wait_interval(
                        elapsed_seconds=elapsed,
                        remaining_seconds=remaining,
                        suspend_after=suspend_after,
                    )
                    try:
                        otp_code = str(
                            skymail_client.wait_for_verification_code(
                                email,
                                timeout=wait_time,
                                otp_sent_at=otp_sent_at,
                                exclude_codes=tried_otp_codes,
                            )
                            or ""
                        ).strip()
                    except TaskInterruption:
                        raise
                    except Exception as e:
                        self._log(f"等待验证码异常: {e}")
                        otp_code = ""

                    if not otp_code:
                        if time.time() >= next_resend_at:
                            otp_send_attempts += 1
                            self._log(f"未收到验证码，尝试重发: attempt={otp_send_attempts}")
                            resend_ok = self.send_email_otp(
                                referer=state.current_url or state.continue_url or f"{self.AUTH}/email-verification"
                            )
                            if resend_ok:
                                self._log(f"重发验证码成功: attempt={otp_send_attempts}")
                                otp_sent_at = time.time() - 15
                                self.config["chatgpt_register_otp_sent_at"] = float(otp_sent_at)
                            else:
                                self._log(f"重发验证码失败: attempt={otp_send_attempts}")
                            next_resend_at = min(otp_deadline, time.time() + otp_resend_wait_timeout)
                            self.config["chatgpt_register_otp_next_resend_at"] = float(next_resend_at)
                            self.config["chatgpt_register_otp_send_attempts"] = int(otp_send_attempts)
                        if is_suspended_cycle and time.time() < otp_deadline:
                            raise self._build_otp_defer(
                                prefix="chatgpt_register",
                                origin="register",
                                stage="otp",
                                started_at=otp_started_at,
                                deadline_at=otp_deadline,
                                otp_sent_at=otp_sent_at,
                                next_resend_at=next_resend_at,
                                otp_send_attempts=otp_send_attempts,
                                delay_seconds=suspend_poll,
                            )
                        continue

                    tried_otp_codes.add(otp_code)
                    success, next_state = self.verify_email_otp(otp_code, return_state=True)
                    if success:
                        otp_verified = True
                        state = next_state
                        self.last_registration_state = state
                        self._clear_otp_suspend_state("chatgpt_register")
                        break

                    failure_text = str(next_state or "")
                    if any(marker in failure_text for marker in ("wrong_email_otp_code", "Wrong code", "HTTP 401")):
                        self._log("验证码无效，继续等待新的验证码...", "warning")
                        if time.time() >= next_resend_at:
                            otp_send_attempts += 1
                            resend_ok = self.send_email_otp(
                                referer=state.current_url or state.continue_url or f"{self.AUTH}/email-verification"
                            )
                            if resend_ok:
                                self._log(f"重发验证码成功: attempt={otp_send_attempts}")
                                otp_sent_at = time.time() - 15
                            else:
                                self._log(f"重发验证码失败: attempt={otp_send_attempts}")
                            next_resend_at = min(otp_deadline, time.time() + otp_resend_wait_timeout)
                        continue
                    self._clear_otp_suspend_state("chatgpt_register")
                    return False, f"验证码失败: {next_state}"

                if not otp_verified:
                    self._clear_otp_suspend_state("chatgpt_register")
                    return False, "等待邮箱验证码超时 (30分钟)"
                continue

            if (
                self._state_is_login_password(state)
                and (not register_submitted)
                and (not otp_verified)
                and (not account_created)
            ):
                self._enter_stage("authorize_continue", describe_flow_state(state))
                self._log("命中 login_password，当前会话已转入登录链路", "warning")
                return False, "redirected_to_login_password"

            if self._state_is_about_you(state):
                self._enter_stage("about_you", describe_flow_state(state))
                if stop_before_about_you_submission:
                    self.last_registration_state = state
                    self._log(
                        "注册链路已到 about_you，按 interrupt 流程停止。"
                        "下一步交由 OAuth 新会话提交姓名+生日。"
                    )
                    return True, "pending_about_you_submission"
                if account_created:
                    return False, "填写信息阶段重复进入"
                success, next_state = self.create_account(
                    first_name,
                    last_name,
                    birthdate,
                    return_state=True,
                )
                if not success:
                    return False, f"创建账号失败: {next_state}"
                account_created = True
                state = next_state
                self.last_registration_state = state
                continue

            if self._state_requires_navigation(state):
                if "workspace" in f"{state.continue_url} {state.current_url}".lower() or "consent" in f"{state.continue_url} {state.current_url}".lower():
                    self._enter_stage("workspace_select", describe_flow_state(state))
                elif state.page_type == "external_url":
                    self._enter_stage("token_exchange", describe_flow_state(state))
                success, next_state = self._follow_flow_state(
                    state,
                    referer=state.current_url or f"{self.AUTH}/about-you",
                )
                if not success:
                    return False, f"跳转失败: {next_state}"
                state = next_state
                self.last_registration_state = state
                continue

            if (
                (not register_submitted)
                and (not otp_verified)
                and (not account_created)
            ):
                self._log(
                    f"未知起始状态，回退为全新注册流程: {describe_flow_state(state)}"
                )
                state = self._state_from_url(f"{self.AUTH}/create-account/password")
                continue

            return False, f"未支持的注册状态: {describe_flow_state(state)}"

        return False, "注册状态机超出最大步数"
