from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


ExecutorType = Literal["protocol", "headless", "headed"]
MailProvider = Literal["luckmail", "tempmail_lol", "outlook_local"]


class CreateRegisterTaskRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=9999)
    concurrency: int = Field(default=1, ge=1, le=100)
    register_delay_seconds: float = Field(default=0, ge=0, le=600)
    email: Optional[str] = None
    password: Optional[str] = None
    proxy: Optional[str] = None
    use_proxy: bool = True
    executor_type: ExecutorType = "protocol"
    mail_provider: MailProvider = "luckmail"
    provider_config: dict[str, Any] = Field(default_factory=dict)
    phone_config: dict[str, Any] = Field(default_factory=dict)


class UpdateConfigRequest(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class DeleteAccountRef(BaseModel):
    task_id: str
    attempt_index: int = Field(ge=1)


class DeleteAccountRequest(BaseModel):
    task_id: str
    attempt_index: int = Field(ge=1)
    task_ids: list[str] = Field(default_factory=list)
    refs: list[DeleteAccountRef] = Field(default_factory=list)


class DeleteAccountsBatchRequest(BaseModel):
    items: list[DeleteAccountRequest] = Field(default_factory=list)


UploadTarget = Literal["cpa", "sub2api"]


class UploadAccountsBatchRequest(BaseModel):
    target: UploadTarget
    items: list[DeleteAccountRequest] = Field(default_factory=list)


class ExportAccountsBatchRequest(BaseModel):
    items: list[DeleteAccountRequest] = Field(default_factory=list)


class AppendTaskRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=9999)
