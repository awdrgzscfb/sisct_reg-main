from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .db import finalize_orphaned_tasks, get_config, get_task_events, init_db, parse_config_row_values, set_config
from .defaults import DEFAULT_CONFIG
from .luckmail_pool import router as luckmail_pool_router
from .manager import manager
from .outlook_pool import router as outlook_router
from .proxy_pool import router as proxy_router
from .schemas import (
    AppendTaskRequest,
    CreateRegisterTaskRequest,
    DeleteAccountRequest,
    DeleteAccountsBatchRequest,
    ExportAccountsBatchRequest,
    UpdateConfigRequest,
    UploadAccountsBatchRequest,
)

app = FastAPI(title="ChatGPT Register Workbench", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(outlook_router)
app.include_router(luckmail_pool_router)
app.include_router(proxy_router)


@app.on_event("startup")
def _startup():
    init_db()
    finalize_orphaned_tasks()


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/config")
def read_config():
    stored = parse_config_row_values(get_config())
    merged = dict(DEFAULT_CONFIG)
    merged.update(stored)
    return merged


@app.put("/api/config")
def write_config(body: UpdateConfigRequest):
    set_config(body.values)
    stored = parse_config_row_values(get_config())
    merged = dict(DEFAULT_CONFIG)
    merged.update(stored)
    return {"ok": True, "config": merged}


@app.post("/api/register/tasks")
def create_register_task(body: CreateRegisterTaskRequest):
    stored = parse_config_row_values(get_config())
    merged = dict(DEFAULT_CONFIG)
    merged.update(stored)
    task_id = manager.create_task(body, merged)
    return {"task_id": task_id}


@app.post("/api/register/tasks/{task_id}/append")
def append_register_task(task_id: str, body: AppendTaskRequest):
    result = manager.append_to_task(task_id, count=body.count)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason in {"task_not_found"}:
            raise HTTPException(404, "任务不存在")
        if reason in {"task_not_active"}:
            raise HTTPException(409, "任务不在运行中")
        raise HTTPException(400, "追加失败")
    return result


@app.get("/api/register/tasks/{task_id}")
def get_register_task(task_id: str, lite: bool = False):
    task = manager.get_task_snapshot(task_id, lite=bool(lite))
    if not task:
        raise HTTPException(404, "任务不存在")
    return task


@app.post("/api/register/tasks/{task_id}/stop")
def stop_task(task_id: str):
    result = manager.stop_task(task_id)
    if not result.get("ok"):
        raise HTTPException(409, "任务不在运行中")
    return result


@app.post("/api/register/tasks/{task_id}/skip-current")
def skip_current(task_id: str):
    result = manager.skip_current(task_id)
    if not result.get("ok"):
        raise HTTPException(409, "任务不在运行中")
    return result


@app.post("/api/register/tasks/{task_id}/attempts/{attempt_index}/stop")
def stop_register_attempt(task_id: str, attempt_index: int):
    result = manager.stop_attempt(task_id, attempt_index)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason in {"task_not_found", "account_not_found"}:
            raise HTTPException(404, "记录不存在")
        if reason in {"task_not_active", "account_not_running"}:
            raise HTTPException(409, "账号不在注册中")
        raise HTTPException(400, "无法停止该账号")
    return result


@app.post("/api/register/results/{result_id}/retry")
def retry_register_result(result_id: int, target_task_id: str | None = None):
    result = manager.retry_result(result_id, target_task_id=target_task_id)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason in {"result_not_found", "task_not_found"}:
            raise HTTPException(404, "记录不存在")
        if reason == "retry_not_supported":
            raise HTTPException(409, "当前邮箱类型不支持从失败阶段重试")
        if reason == "result_not_failed":
            raise HTTPException(409, "仅失败或已停止账号支持重试")
        raise HTTPException(400, "无法重试该账号")
    return result


@app.post("/api/register/tasks/{task_id}/attempts/{attempt_index}/retry")
def retry_register_attempt(task_id: str, attempt_index: int, target_task_id: str | None = None):
    result = manager.retry_attempt(task_id, attempt_index, target_task_id=target_task_id)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason in {"task_not_found", "account_not_found"}:
            raise HTTPException(404, "记录不存在")
        if reason == "retry_not_supported":
            raise HTTPException(409, "当前邮箱类型不支持从失败阶段重试")
        if reason == "result_not_failed":
            raise HTTPException(409, "仅失败或已停止账号支持重试")
        raise HTTPException(400, "无法重试该账号")
    return result


@app.post("/api/register/accounts/delete")
def delete_register_account(body: DeleteAccountRequest):
    result = manager.delete_account(body.task_id, body.attempt_index, body.task_ids, body.refs)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason == "task_not_found":
            raise HTTPException(404, "任务不存在")
        if reason == "account_not_found":
            raise HTTPException(404, "账号不存在")
        if reason == "account_running":
            raise HTTPException(409, "注册中的账号不可删除")
        raise HTTPException(400, "删除失败")
    return result


@app.post("/api/register/accounts/delete-batch")
def delete_register_accounts_batch(body: DeleteAccountsBatchRequest):
    return manager.delete_accounts_batch(body.items)


@app.post("/api/register/accounts/upload")
def upload_register_accounts(body: UploadAccountsBatchRequest):
    result = manager.upload_accounts(body.target, body.items)
    if not result.get("ok"):
        reason = str(result.get("reason") or "")
        if reason == "invalid_target":
            raise HTTPException(400, "上传目标无效")
        if reason == "target_not_configured":
            raise HTTPException(409, str(result.get("message") or "上传配置未填写"))
        raise HTTPException(400, str(result.get("message") or "上传失败"))
    return result


@app.post("/api/register/accounts/export")
def export_register_accounts(body: ExportAccountsBatchRequest):
    result = manager.export_accounts_bundle(body.items)
    if not result.get("ok"):
        raise HTTPException(400, str(result.get("message") or "导出失败"))
    filename = str(result.get("filename") or "accounts_export.zip")
    return StreamingResponse(
        iter([result.get("content") or b""]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/register/tasks/{task_id}/events")
async def stream_events(task_id: str, since: int = 0):
    if manager.get_task_snapshot(task_id) is None:
        raise HTTPException(404, "任务不存在")

    async def event_generator():
        sent = int(since or 0)
        while True:
            events = get_task_events(task_id, after_seq=sent)
            for event in events:
                sent = int(event.get("seq") or sent)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            snapshot = manager.get_task_snapshot(task_id)
            if snapshot and snapshot.get("status") in {"done", "failed", "stopped"}:
                yield f"data: {json.dumps({'done': True, 'status': snapshot.get('status')}, ensure_ascii=False)}\n\n"
                break
            import asyncio

            await asyncio.sleep(0.75)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/history/tasks")
def list_history(page: int = 1, page_size: int = 20):
    return manager.list_history(page=page, page_size=max(1, min(page_size, 100)))


@app.get("/api/history/tasks/{task_id}")
def history_detail(task_id: str):
    detail = manager.get_history_detail(task_id)
    if not detail:
        raise HTTPException(404, "任务不存在")
    return detail


@app.get("/api/history/tasks/{task_id}/export")
def history_export(task_id: str):
    return JSONResponse(manager.export_results(task_id))


FRONTEND_DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"
if FRONTEND_DIST.exists():
    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str):
        target = FRONTEND_DIST / full_path
        if full_path and target.exists() and target.is_file():
            return FileResponse(target)
        return FileResponse(FRONTEND_DIST / "index.html")
