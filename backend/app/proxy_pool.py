from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .db import (
    batch_import_proxy_pool,
    delete_all_proxy_accounts,
    delete_proxy_account,
    get_proxy_account,
    get_proxy_pool_summary,
    list_enabled_proxy_pool,
    update_proxy_check_result,
)
from .manager import manager

router = APIRouter(prefix="/api/proxies", tags=["proxies"])


class ProxyBatchImportRequest(BaseModel):
    data: str = Field(default="")
    enabled: bool = Field(default=True)


@router.get("/summary")
def read_proxy_summary():
    return get_proxy_pool_summary()


@router.post("/batch-import")
def batch_import_proxies(body: ProxyBatchImportRequest):
    return batch_import_proxy_pool(body.data, enabled=body.enabled)


@router.delete("/accounts/{proxy_id}")
def remove_proxy_account(proxy_id: int):
    if not delete_proxy_account(proxy_id):
        raise HTTPException(404, "代理不存在")
    return {"ok": True, "summary": get_proxy_pool_summary()}


@router.delete("/accounts")
def remove_proxy_accounts(scope: str = "all"):
    scope = str(scope or "all").strip().lower()
    if scope != "all":
        raise HTTPException(400, "scope 仅支持 all")
    deleted = delete_all_proxy_accounts()
    return {"ok": True, "deleted": deleted, "summary": get_proxy_pool_summary()}


@router.post("/test")
def test_proxy_accounts():
    items = list_enabled_proxy_pool()
    tested = 0
    ok_count = 0
    fail_count = 0
    for item in items:
        tested += 1
        try:
            ip, country = manager._query_egress_info(str(item.get("proxy_url") or ""), proxy_id=int(item.get("id") or 0))
            update_proxy_check_result(
                int(item.get("id") or 0),
                ok=True,
                message="ok",
                ip=ip,
                country=country,
            )
            ok_count += 1
        except Exception as exc:
            update_proxy_check_result(
                int(item.get("id") or 0),
                ok=False,
                message=str(exc),
            )
            fail_count += 1
    return {
        "ok": True,
        "tested": tested,
        "success": ok_count,
        "failed": fail_count,
        "summary": get_proxy_pool_summary(),
    }


@router.post("/accounts/{proxy_id}/test")
def test_proxy_account(proxy_id: int):
    item = get_proxy_account(proxy_id)
    if item is None:
        raise HTTPException(404, "代理不存在")
    proxy_url = str(item.get("proxy_url") or "").strip()
    if not proxy_url:
        raise HTTPException(400, "代理为空")
    try:
        ip, country = manager._query_egress_info(proxy_url, proxy_id=int(item.get("id") or 0))
        update_proxy_check_result(
            int(item.get("id") or 0),
            ok=True,
            message="ok",
            ip=ip,
            country=country,
        )
        return {
            "ok": True,
            "ip": ip,
            "country": country,
            "summary": get_proxy_pool_summary(),
        }
    except Exception as exc:
        update_proxy_check_result(
            int(item.get("id") or 0),
            ok=False,
            message=str(exc),
        )
        raise HTTPException(409, str(exc))
