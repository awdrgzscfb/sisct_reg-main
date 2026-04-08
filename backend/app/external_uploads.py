from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from curl_cffi import CurlMime
from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

DEFAULT_OPENAI_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
DEFAULT_SUB2API_GROUP_IDS = [2]


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    try:
        parts = str(token or "").split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _b64url_json(data: dict[str, Any]) -> str:
    raw = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_bytes(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _derive_display_name(email: str) -> str:
    local = (email or "").split("@", 1)[0].replace(".", " ").replace("_", " ").replace("-", " ")
    parts = [part for part in local.split() if part]
    if not parts:
        return "OpenAI User"
    return " ".join(part[:1].upper() + part[1:] for part in parts[:3])


def _extract_auth(payload: dict[str, Any]) -> dict[str, Any]:
    nested = payload.get("https://api.openai.com/auth")
    if isinstance(nested, dict) and nested:
        return nested

    flat: dict[str, Any] = {}
    for key, value in payload.items():
        if key.startswith("https://api.openai.com/auth."):
            flat[key.split(".", 4)[-1]] = value
    return flat


def _build_compat_id_token(*, access_token: str, email: str) -> str:
    payload = _decode_jwt_payload(access_token)
    if not payload:
        return ""

    auth_info = _extract_auth(payload)
    email_from_token = (
        ((payload.get("https://api.openai.com/profile") or {}).get("email"))
        or payload.get("email")
        or email
        or ""
    ).strip()
    email_verified = bool(
        ((payload.get("https://api.openai.com/profile") or {}).get("email_verified"))
        if isinstance(payload.get("https://api.openai.com/profile"), dict)
        else payload.get("email_verified", True)
    )
    account_id = str(auth_info.get("chatgpt_account_id") or auth_info.get("account_id") or "").strip()
    user_id = str(auth_info.get("chatgpt_user_id") or auth_info.get("user_id") or payload.get("sub") or "").strip()
    iat = int(payload.get("iat") or 0)
    exp = int(payload.get("exp") or 0)
    auth_time = int(payload.get("pwd_auth_time") or payload.get("auth_time") or iat or 0)
    session_id = str(payload.get("session_id") or f"compat_session_{(account_id or user_id or 'unknown').replace('-', '')[:24]}").strip()
    organization_id = str(auth_info.get("organization_id") or "").strip() or f"org-{user_id[:24] or 'compat'}"
    project_id = str(auth_info.get("project_id") or "").strip() or f"proj_{organization_id[-12:]}"
    plan_type = str(auth_info.get("chatgpt_plan_type") or "free").strip() or "free"

    compat_payload = {
        "amr": ["pwd", "otp", "mfa", "urn:openai:amr:otp_email"],
        "aud": [DEFAULT_OPENAI_CLIENT_ID],
        "auth_provider": "password",
        "auth_time": auth_time,
        "email": email_from_token,
        "email_verified": email_verified,
        "exp": exp,
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id,
            "chatgpt_plan_type": plan_type,
            "chatgpt_user_id": user_id,
            "completed_platform_onboarding": bool(auth_info.get("completed_platform_onboarding", False)),
            "groups": auth_info.get("groups", []),
            "is_org_owner": bool(auth_info.get("is_org_owner", True)),
            "localhost": bool(auth_info.get("localhost", True)),
            "organization_id": organization_id,
            "organizations": auth_info.get("organizations") or [{"id": organization_id, "is_default": True, "role": "owner", "title": "Personal"}],
            "project_id": project_id,
            "user_id": str(auth_info.get("user_id") or user_id or "").strip(),
        },
        "iat": iat,
        "iss": payload.get("iss") or "https://auth.openai.com",
        "jti": f"compat-{user_id[:32] or 'token'}",
        "name": _derive_display_name(email_from_token),
        "sid": session_id,
        "sub": payload.get("sub") or user_id,
    }
    header = {"alg": "RS256", "typ": "JWT", "kid": "compat"}
    signature = _b64url_bytes(b"compat_signature_for_cpa_parsing_only")
    return f"{_b64url_json(header)}.{_b64url_json(compat_payload)}.{signature}"


def generate_cpa_token_json(result: Any) -> dict[str, Any]:
    email = str(getattr(result, "email", "") or "").strip()
    access_token = str(getattr(result, "access_token", "") or "").strip()
    refresh_token = str(getattr(result, "refresh_token", "") or "").strip()
    id_token = str(getattr(result, "id_token", "") or "").strip()
    if access_token and not id_token:
        id_token = _build_compat_id_token(access_token=access_token, email=email)

    expired_str = ""
    account_id = str(getattr(result, "account_id", "") or "").strip()
    if access_token:
        payload = _decode_jwt_payload(access_token)
        auth_info = _extract_auth(payload)
        account_id = account_id or str(auth_info.get("chatgpt_account_id") or "").strip()
        exp_timestamp = payload.get("exp")
        if isinstance(exp_timestamp, int) and exp_timestamp > 0:
            exp_dt = datetime.fromtimestamp(exp_timestamp, tz=timezone(timedelta(hours=8)))
            expired_str = exp_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")

    now = datetime.now(tz=timezone(timedelta(hours=8)))
    return {
        "type": "codex",
        "email": email,
        "expired": expired_str,
        "id_token": id_token,
        "account_id": account_id,
        "access_token": access_token,
        "last_refresh": now.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "refresh_token": refresh_token,
    }


def _parse_group_ids(raw: Any) -> list[int]:
    if isinstance(raw, str):
        candidates = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set)):
        candidates = list(raw)
    elif raw is None:
        candidates = []
    else:
        candidates = [raw]

    values: list[int] = []
    for item in candidates:
        text = str(item or "").strip()
        if not text:
            continue
        try:
            values.append(int(text))
        except ValueError:
            continue
    return values or list(DEFAULT_SUB2API_GROUP_IDS)


def _extract_organization_id(id_token: str) -> str:
    payload = _decode_jwt_payload(id_token)
    auth_info = _extract_auth(payload)
    organization_id = str(auth_info.get("organization_id") or "").strip()
    if organization_id:
        return organization_id
    organizations = auth_info.get("organizations") or []
    if isinstance(organizations, list):
        for item in organizations:
            if isinstance(item, dict):
                organization_id = str(item.get("id") or "").strip()
                if organization_id:
                    return organization_id
    return ""


def _format_iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def build_sub2api_export_account(result: Any) -> dict[str, Any]:
    token_data = generate_cpa_token_json(result)
    access_token = str(token_data.get("access_token") or "").strip()
    refresh_token = str(token_data.get("refresh_token") or "").strip()
    id_token = str(token_data.get("id_token") or "").strip()
    email = str(token_data.get("email") or getattr(result, "email", "") or "").strip()
    access_payload = _decode_jwt_payload(access_token)
    access_auth = _extract_auth(access_payload)
    expires_at = access_payload.get("exp")
    if isinstance(expires_at, int) and expires_at > 0:
        expires_at_text = _format_iso8601(datetime.fromtimestamp(expires_at, tz=timezone.utc))
    else:
        expires_at_text = _format_iso8601(datetime.now(tz=timezone.utc) + timedelta(days=10))
    token_version = int(access_payload.get("iat") or int(time.time())) * 1000

    return {
        "name": email,
        "platform": "openai",
        "type": "oauth",
        "credentials": {
            "_token_version": token_version,
            "access_token": access_token,
            "chatgpt_account_id": str(access_auth.get("chatgpt_account_id") or token_data.get("account_id") or "").strip(),
            "chatgpt_user_id": str(access_auth.get("chatgpt_user_id") or "").strip(),
            "email": email,
            "expires_at": expires_at_text,
            "expires_in": 864000,
            "id_token": id_token,
            "organization_id": _extract_organization_id(id_token),
            "refresh_token": refresh_token,
        },
        "extra": {"email": email},
        "concurrency": 10,
        "priority": 1,
        "rate_multiplier": 1,
        "auto_pause_on_expired": True,
    }


def build_sub2api_export_payload(results: list[Any]) -> dict[str, Any]:
    return {
        "exported_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "proxies": [],
        "accounts": [build_sub2api_export_account(item) for item in results],
    }


def upload_to_cpa(result: Any, *, api_url: str, api_key: str) -> tuple[bool, str]:
    api_url = str(api_url or "").strip()
    api_key = str(api_key or "").strip()
    if not api_url:
        return False, "CPA API URL 未配置"

    upload_url = f"{api_url.rstrip('/')}/v0/management/auth-files"
    token_data = generate_cpa_token_json(result)
    filename = f"{token_data['email']}.json"
    file_content = json.dumps(token_data, ensure_ascii=False, indent=2).encode("utf-8")
    headers = {"Authorization": f"Bearer {api_key}"}

    mime = None
    try:
        mime = CurlMime()
        mime.addpart(name="file", data=file_content, filename=filename, content_type="application/json")
        response = cffi_requests.post(
            upload_url,
            multipart=mime,
            headers=headers,
            proxies=None,
            verify=False,
            timeout=30,
            impersonate="chrome110",
        )
        if response.status_code in (200, 201):
            return True, "上传成功"
        try:
            detail = response.json()
            if isinstance(detail, dict):
                return False, str(detail.get("message") or detail.get("msg") or f"HTTP {response.status_code}")
        except Exception:
            pass
        return False, f"上传失败: HTTP {response.status_code} - {response.text[:200]}"
    except Exception as exc:
        logger.exception("CPA 上传异常")
        return False, f"上传异常: {exc}"
    finally:
        if mime:
            mime.close()


def upload_to_sub2api(result: Any, *, api_url: str, api_key: str, group_ids: Any = None) -> tuple[bool, str]:
    api_url = str(api_url or "").strip()
    api_key = str(api_key or "").strip()
    if not api_url:
        return False, "Sub2API API URL 未配置"
    if not api_key:
        return False, "Sub2API API Key 未配置"

    token_data = generate_cpa_token_json(result)
    access_token = str(token_data.get("access_token") or "").strip()
    refresh_token = str(token_data.get("refresh_token") or "").strip()
    id_token = str(token_data.get("id_token") or "").strip()
    email = str(token_data.get("email") or getattr(result, "email", "") or "").strip()
    access_payload = _decode_jwt_payload(access_token)
    access_auth = _extract_auth(access_payload)
    expires_at = access_payload.get("exp")
    if not isinstance(expires_at, int) or expires_at <= 0:
        expires_at = int(time.time()) + 863999

    payload = {
        "name": email,
        "notes": "",
        "platform": "openai",
        "type": "oauth",
        "credentials": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": 863999,
            "expires_at": expires_at,
            "chatgpt_account_id": str(access_auth.get("chatgpt_account_id") or token_data.get("account_id") or "").strip(),
            "chatgpt_user_id": str(access_auth.get("chatgpt_user_id") or "").strip(),
            "organization_id": _extract_organization_id(id_token),
            "client_id": DEFAULT_OPENAI_CLIENT_ID,
            "id_token": id_token,
        },
        "extra": {"email": email},
        "group_ids": _parse_group_ids(group_ids),
        "concurrency": 10,
        "priority": 1,
        "auto_pause_on_expired": True,
    }

    url = f"{api_url.rstrip('/')}/api/v1/admin/accounts"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{api_url.rstrip('/')}/admin/accounts",
        "x-api-key": api_key,
    }
    try:
        response = cffi_requests.post(
            url,
            headers=headers,
            json=payload,
            proxies=None,
            verify=False,
            timeout=30,
            impersonate="chrome110",
        )
        if response.status_code in (200, 201):
            return True, "上传成功"
        try:
            detail = response.json()
            if isinstance(detail, dict):
                return False, str(detail.get("message") or detail.get("msg") or detail.get("error") or f"HTTP {response.status_code}")
        except Exception:
            pass
        return False, f"上传失败: HTTP {response.status_code} - {response.text[:200]}"
    except Exception as exc:
        logger.exception("Sub2API 上传异常")
        return False, f"上传异常: {exc}"


def sync_chatgpt_result(result: Any, config: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    cpa_url = str(config.get("cpa_api_url") or "").strip()
    cpa_key = str(config.get("cpa_api_key") or "").strip()
    if cpa_url:
        ok, msg = upload_to_cpa(result, api_url=cpa_url, api_key=cpa_key)
        results.append({"name": "CPA", "ok": ok, "msg": msg})

    sub2api_url = str(config.get("sub2api_api_url") or "").strip()
    sub2api_key = str(config.get("sub2api_api_key") or "").strip()
    sub2api_group_ids = config.get("sub2api_group_ids")
    if sub2api_url and sub2api_key:
        ok, msg = upload_to_sub2api(result, api_url=sub2api_url, api_key=sub2api_key, group_ids=sub2api_group_ids)
        results.append({"name": "Sub2API", "ok": ok, "msg": msg})

    return results
