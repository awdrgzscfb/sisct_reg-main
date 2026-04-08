from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .db import (
    batch_import_luckmail_token_accounts,
    delete_all_luckmail_token_accounts,
    delete_luckmail_token_account,
    delete_taken_luckmail_token_accounts,
    get_luckmail_token_pool_summary,
)

router = APIRouter(prefix="/api/luckmail-pool", tags=["luckmail-pool"])


class LuckMailTokenBatchImportRequest(BaseModel):
    data: str = Field(default="")
    enabled: bool = Field(default=True)


@router.get("/summary")
def read_luckmail_token_summary():
    return get_luckmail_token_pool_summary()


@router.post("/batch-import")
def batch_import_luckmail_tokens(body: LuckMailTokenBatchImportRequest):
    return batch_import_luckmail_token_accounts(body.data, enabled=body.enabled)


@router.delete("/accounts/{account_id}")
def remove_luckmail_token_account(account_id: int):
    if not delete_luckmail_token_account(account_id):
        raise HTTPException(404, "令牌池账号不存在")
    return {"ok": True, "summary": get_luckmail_token_pool_summary()}


@router.delete("/accounts")
def remove_luckmail_token_accounts(scope: str = "all"):
    scope = str(scope or "all").strip().lower()
    if scope == "taken":
        deleted = delete_taken_luckmail_token_accounts()
    elif scope == "all":
        deleted = delete_all_luckmail_token_accounts()
    else:
        raise HTTPException(400, "scope 仅支持 all 或 taken")
    return {"ok": True, "deleted": deleted, "summary": get_luckmail_token_pool_summary()}
