from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .db import (
    batch_import_outlook_accounts,
    delete_all_outlook_accounts,
    delete_outlook_account,
    delete_taken_outlook_accounts,
    get_outlook_pool_summary,
)

router = APIRouter(prefix="/api/outlook", tags=["outlook"])


class OutlookBatchImportRequest(BaseModel):
    data: str = Field(default="")
    enabled: bool = Field(default=True)


@router.get("/summary")
def read_outlook_summary():
    return get_outlook_pool_summary()


@router.post("/batch-import")
def batch_import_outlook(body: OutlookBatchImportRequest):
    return batch_import_outlook_accounts(body.data, enabled=body.enabled)


@router.delete("/accounts/{account_id}")
def remove_outlook_account(account_id: int):
    if not delete_outlook_account(account_id):
        raise HTTPException(404, "邮箱池账号不存在")
    return {"ok": True, "summary": get_outlook_pool_summary()}


@router.delete("/accounts")
def remove_outlook_accounts(scope: str = "all"):
    scope = str(scope or "all").strip().lower()
    if scope == "taken":
        deleted = delete_taken_outlook_accounts()
    elif scope == "all":
        deleted = delete_all_outlook_accounts()
    else:
        raise HTTPException(400, "scope 仅支持 all 或 taken")
    return {"ok": True, "deleted": deleted, "summary": get_outlook_pool_summary()}
