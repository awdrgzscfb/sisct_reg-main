from __future__ import annotations

import hashlib
import re
import threading
from contextlib import contextmanager
from typing import Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

_PROXY_USAGE_CONTEXT = threading.local()


def normalize_proxy_url(proxy_url: Optional[str]) -> Optional[str]:
    """补全代理协议，并将 socks5:// 规范化为 socks5h://，避免本地 DNS 泄漏。"""
    if proxy_url is None:
        return None

    value = str(proxy_url).strip()
    if not value:
        return None

    if "://" not in value:
        value = f"http://{value}"

    parts = urlsplit(value)
    if (parts.scheme or "").lower() == "socks5":
        parts = parts._replace(scheme="socks5h")
        return urlunsplit(parts)
    return value


def build_requests_proxy_config(proxy_url: Optional[str]) -> Optional[dict[str, str]]:
    normalized = normalize_proxy_url(proxy_url)
    if not normalized:
        return None
    return {"http": normalized, "https": normalized}


def build_playwright_proxy_config(proxy_url: Optional[str]) -> Optional[dict[str, str]]:
    normalized = normalize_proxy_url(proxy_url)
    if not normalized:
        return None

    parts = urlsplit(normalized)
    if not parts.scheme or not parts.hostname or parts.port is None:
        return {"server": normalized}

    config = {"server": f"{parts.scheme}://{parts.hostname}:{parts.port}"}
    if parts.username:
        config["username"] = unquote(parts.username)
    if parts.password:
        config["password"] = unquote(parts.password)
    return config


def isolate_proxy_session(proxy_url: Optional[str], *, scope: str = "") -> Optional[str]:
    normalized = normalize_proxy_url(proxy_url)
    if not normalized:
        return None

    parts = urlsplit(normalized)
    username = unquote(parts.username or "")
    password = unquote(parts.password or "")
    if not username:
        return normalized

    if not re.search(r"(^|-)sid-[^-:@]+", username):
        return normalized

    digest = hashlib.sha1(scope.encode("utf-8", errors="ignore")).hexdigest()[:10] if scope else hashlib.sha1(username.encode("utf-8", errors="ignore")).hexdigest()[:10]
    rotated_username = re.sub(
        r"(^|-)sid-[^-:@]+",
        lambda match: f"{match.group(1)}sid-{digest}",
        username,
        count=1,
    )

    netloc = ""
    if rotated_username:
        netloc += quote(rotated_username, safe="")
        if password:
            netloc += f":{quote(password, safe='')}"
        netloc += "@"
    host = parts.hostname or ""
    netloc += host
    if parts.port is not None:
        netloc += f":{parts.port}"

    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _set_proxy_usage_context(*, proxy_id: int | None, proxy_url: Optional[str]) -> None:
    _PROXY_USAGE_CONTEXT.proxy_id = int(proxy_id) if proxy_id else None
    _PROXY_USAGE_CONTEXT.proxy_url = normalize_proxy_url(proxy_url)


def _get_proxy_usage_context() -> tuple[int | None, str | None]:
    proxy_id = getattr(_PROXY_USAGE_CONTEXT, "proxy_id", None)
    proxy_url = getattr(_PROXY_USAGE_CONTEXT, "proxy_url", None)
    return (int(proxy_id) if proxy_id else None, normalize_proxy_url(proxy_url))


@contextmanager
def proxy_usage_context(*, proxy_id: int | None = None, proxy_url: Optional[str] = None):
    previous_id, previous_url = _get_proxy_usage_context()
    _set_proxy_usage_context(proxy_id=proxy_id, proxy_url=proxy_url)
    try:
        yield
    finally:
        _set_proxy_usage_context(proxy_id=previous_id, proxy_url=previous_url)


def _resolve_proxy_target(
    *,
    proxy_id: int | None = None,
    proxy_url: Optional[str] = None,
    proxies: Optional[dict[str, str]] = None,
) -> tuple[int | None, str | None]:
    normalized_url = normalize_proxy_url(proxy_url)
    if not normalized_url and isinstance(proxies, dict):
        normalized_url = normalize_proxy_url(proxies.get("https") or proxies.get("http"))

    ctx_id, ctx_url = _get_proxy_usage_context()
    resolved_id = int(proxy_id) if proxy_id else (ctx_id or None)
    resolved_url = normalized_url or ctx_url
    return resolved_id, resolved_url


def record_proxy_request_result(
    *,
    success: bool,
    proxy_id: int | None = None,
    proxy_url: Optional[str] = None,
    proxies: Optional[dict[str, str]] = None,
) -> None:
    resolved_id, resolved_url = _resolve_proxy_target(proxy_id=proxy_id, proxy_url=proxy_url, proxies=proxies)
    if not resolved_id and not resolved_url:
        return
    try:
        from app.db import find_proxy_account_id_by_url, update_proxy_usage_result

        final_id = resolved_id or find_proxy_account_id_by_url(resolved_url)
        if final_id:
            update_proxy_usage_result(int(final_id), success=bool(success))
    except Exception:
        return


def tracked_request(
    request_callable,
    method: str,
    url: str,
    *,
    proxy_id: int | None = None,
    proxy_url: Optional[str] = None,
    **kwargs,
):
    resolved_id, resolved_url = _resolve_proxy_target(
        proxy_id=proxy_id,
        proxy_url=proxy_url,
        proxies=kwargs.get("proxies"),
    )
    if not resolved_id and not resolved_url:
        return request_callable(method, url, **kwargs)
    try:
        response = request_callable(method, url, **kwargs)
    except Exception:
        record_proxy_request_result(
            success=False,
            proxy_id=resolved_id,
            proxy_url=resolved_url,
            proxies=kwargs.get("proxies"),
        )
        raise
    record_proxy_request_result(
        success=True,
        proxy_id=resolved_id,
        proxy_url=resolved_url,
        proxies=kwargs.get("proxies"),
    )
    return response


def instrument_session_proxy_requests(session, *, proxy_id: int | None = None, proxy_url: Optional[str] = None):
    if session is None:
        return session

    setattr(session, "_proxy_usage_proxy_id", int(proxy_id) if proxy_id else None)
    setattr(session, "_proxy_usage_proxy_url", normalize_proxy_url(proxy_url))

    if getattr(session, "_proxy_usage_instrumented", False):
        return session

    original_request = session.request

    def wrapped_request(method, url, *args, **kwargs):
        effective_proxy_id = getattr(session, "_proxy_usage_proxy_id", None)
        effective_proxy_url = getattr(session, "_proxy_usage_proxy_url", None)
        resolved_id, resolved_url = _resolve_proxy_target(
            proxy_id=effective_proxy_id,
            proxy_url=effective_proxy_url,
            proxies=kwargs.get("proxies") or getattr(session, "proxies", None),
        )
        if not resolved_id and not resolved_url:
            return original_request(method, url, *args, **kwargs)
        try:
            response = original_request(method, url, *args, **kwargs)
        except Exception:
            record_proxy_request_result(
                success=False,
                proxy_id=resolved_id,
                proxy_url=resolved_url,
                proxies=kwargs.get("proxies") or getattr(session, "proxies", None),
            )
            raise
        record_proxy_request_result(
            success=True,
            proxy_id=resolved_id,
            proxy_url=resolved_url,
            proxies=kwargs.get("proxies") or getattr(session, "proxies", None),
        )
        return response

    session.request = wrapped_request
    setattr(session, "_proxy_usage_instrumented", True)
    return session
