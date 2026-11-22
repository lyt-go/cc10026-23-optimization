"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services import export_store

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _publication_headers(record: dict[str, Any]) -> dict[str, str]:
    """把发布凭据放进响应头，下载方无需再猜取的是哪一版。"""
    headers = {
        "X-Export-Publication": str(record["publication_id"]),
        "X-Export-Digest": str(record["digest"]),
        "X-Export-Export-Id": str(record["export_id"]),
        "X-Export-Row-Count": str(record.get("row_count", 0)),
        "X-Export-Source": str(record.get("source", "")),
    }
    if record.get("published_at"):
        headers["X-Export-Published-At"] = str(record["published_at"])
    return headers


@router.get("/export_task/{entry_id}/publications", response_model=dict)
def list_export_publications(entry_id: int) -> dict[str, Any]:
    """列出某个导出任务的全部发布版本，标明当前生效和完整性。"""
    try:
        records = export_store.list_publications(entry_id, verify=True)
    except export_store.PublicationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "export_id": entry_id,
        "items": [
            {
                key: record.get(key)
                for key in (
                    "publication_id", "published_at", "digest", "row_count",
                    "current", "verified", "status", "error", "source",
                )
            }
            for record in records
        ],
        "total": len(records),
    }


@router.get("/export_task/{entry_id}/download")
def download_export(
    entry_id: int,
    publication_id: str | None = Query(default=None),
    format: str = Query(default="csv"),
) -> StreamingResponse:
    """下载某次发布的成果；与归并共用 ``export_store`` 的凭据与校验。"""
    try:
        content, record = export_store.read_publication_artifact(
            entry_id, publication_id, fmt=format
        )
    except export_store.PublicationNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except export_store.PublicationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    filename = (
        f"export-{entry_id:06d}-{record['publication_id']}.{format.lower()}"
    )
    headers = {
        **_publication_headers(record),
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    media_type = "application/json" if format.lower() == "json" else "text/csv; charset=utf-8"
    return StreamingResponse(iter([content]), media_type=media_type, headers=headers)


@router.get("/data_merge/{entry_id}/merge", response_model=dict)
def merge_from_publications(
    entry_id: int,
    publication_id: list[str] | None = Query(default=None),
) -> dict[str, Any]:
    """从导出发布归并取数；返回行与所用发布凭据。

    不传 ``publication_id`` 时取各导出任务当前生效的发布；传入时严格读取
    指定版本。响应里的 ``receipt.sources[*].download`` 可直接下载同一发布，
    因此归并返回行与可下载成果可以逐值核对。
    """
    service = require_service("data_merge")
    try:
        result = service.merge_publications(entry_id, publication_id)
    except export_store.PublicationNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except export_store.PublicationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return result


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
