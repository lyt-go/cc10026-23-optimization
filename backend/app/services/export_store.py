"""导出数据文件组的一致性发布存储。

一次导出会产生一组必须同时生效的文件：``export.json``、``export.csv`` 和
``manifest.json``。同一条导出任务可以被反复执行，因此每次执行都发布为一个
不可变的 *publication*（发布版本），而不是原地覆盖上一版文件。

发布流程：

1. 在 ``data/exports/.staging/`` 下创建独占暂存目录，写入 JSON、CSV 和清单；
2. 重新读取暂存目录，核对 JSON/CSV 的列、行、内容摘要和文件摘要；
3. 用 ``os.replace`` 把整个目录原子改名到 ``publications/<publication_id>/``；
4. 最后原子替换 ``current.json`` 指针。

因此任何时刻读者看到的 ``current.json`` 都指向一个完整、不可变的文件组；
写入一半的 JSON/CSV 只可能存在于暂存目录，永远不会替换可用版本。若进程在
改名之后、更新指针之前中断，旧指针仍然可用；指针丢失或损坏时，读取侧会从
已完整落盘的历史发布中恢复出最新一版，并把恢复结果记录在返回凭据里。

读取方（下载、归并）都通过 ``publication_id`` 或 ``current.json`` 解析出同一份
发布凭据，并在返回数据前同步校验持久化文件组。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from app.services import data_manager

SCHEMA_VERSION = 1
PUBLICATIONS_DIRNAME = "publications"
POINTER_FILENAME = "current.json"
STAGING_DIRNAME = ".staging"
MANIFEST_FILENAME = "manifest.json"
JSON_FILENAME = "export.json"
CSV_FILENAME = "export.csv"

# 同一进程里的并发发布/恢复串行化，避免指针与恢复动作互相覆盖。
_PUBLISH_LOCK = threading.RLock()


class PublicationError(RuntimeError):
    """导出发布相关的统一异常。"""


class PublicationNotFound(PublicationError):
    """指定的导出任务或发布版本不存在。"""


class PublicationCorrupted(PublicationError):
    """发布文件组缺失、摘要不符或 JSON/CSV 内容不一致。"""


class InjectedPublishFailure(PublicationError):
    """测试用：在发布流水线的指定阶段主动中断。"""


# ---------------------------------------------------------------------------
# 路径与基础工具
# ---------------------------------------------------------------------------


def exports_root() -> Path:
    return data_manager.DATA_ROOT / "exports"


def export_dir(export_id: int) -> Path:
    return exports_root() / f"{int(export_id):06d}"


def publications_dir(export_id: int) -> Path:
    return export_dir(export_id) / PUBLICATIONS_DIRNAME


def pointer_path(export_id: int) -> Path:
    return export_dir(export_id) / POINTER_FILENAME


def staging_root() -> Path:
    return exports_root() / STAGING_DIRNAME


def publication_dir(export_id: int, publication_id: str) -> Path:
    return publications_dir(export_id) / publication_id


def _now_iso() -> str:
    return datetime.now(data_manager.TZ).isoformat()


def _json_bytes(data: Any) -> bytes:
    return json.dumps(
        data, ensure_ascii=False, indent=2, default=str
    ).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_bytes(data: Any) -> bytes:
    return json.dumps(
        data, ensure_ascii=False, separators=(",", ":"), default=str
    ).encode("utf-8")


def _data_digest(columns: list[str], rows: list[list[str]]) -> str:
    return _sha256(_canonical_bytes({"columns": columns, "rows": rows}))


def _manifest_digest(manifest: dict[str, Any]) -> str:
    stable = {
        "schema_version": manifest.get("schema_version"),
        "export_id": manifest.get("export_id"),
        "publication_id": manifest.get("publication_id"),
        "published_at": manifest.get("published_at"),
        "columns": manifest.get("columns"),
        "row_count": manifest.get("row_count"),
        "data_digest": manifest.get("data_digest"),
        "files": manifest.get("files"),
    }
    return _sha256(_canonical_bytes(stable))


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise PublicationCorrupted(f"发布文件缺失：{path.name}") from exc


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_dir(path.parent)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _new_publication_id(export_id: int) -> str:
    # 前缀导出任务号，便于从凭据直接看出归属；时间戳保证可排序。
    stamp = datetime.now(data_manager.TZ).strftime("%Y%m%dT%H%M%S%f")
    return f"{int(export_id):06d}-{stamp}-{uuid.uuid4().hex[:8]}"


def _publish_stage(stage_hook: Callable[[str], None] | None, stage: str) -> None:
    if stage_hook is not None:
        stage_hook(stage)


def _check_failpoint(failpoint: str | None, stage: str) -> None:
    if failpoint == stage:
        raise InjectedPublishFailure(f"发布在 {stage} 阶段被中断")


# ---------------------------------------------------------------------------
# 数据规范化与文件渲染
# ---------------------------------------------------------------------------


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _normalize_export_data(
    export_id: int, export_data: dict[str, Any]
) -> tuple[dict[str, Any], list[str], list[list[str]]]:
    """把导出结果规范成 (payload, columns, rows)。

    ``rows`` 既接受二维数组（现有导出任务格式），也接受字典数组，方便归并/
    测试直接发布。最终写入文件组的值统一为字符串，保证 JSON 与 CSV 可逐值比较。
    """
    payload = dict(export_data or {})
    raw_rows = payload.get("rows") or []
    raw_columns = payload.get("columns") or []

    columns = [str(column) for column in raw_columns]
    rows: list[list[str]] = []

    if raw_rows and all(isinstance(row, dict) for row in raw_rows):
        if not columns:
            discovered: list[str] = []
            for row in raw_rows:
                for key in row.keys():
                    key_text = str(key)
                    if key_text not in discovered:
                        discovered.append(key_text)
            columns = discovered
        for row in raw_rows:
            normalized = {str(key): value for key, value in row.items()}
            rows.append([_stringify(normalized.get(column)) for column in columns])
    else:
        for index, row in enumerate(raw_rows):
            if isinstance(row, dict):
                # 混合格式：按已确定的列取值。
                normalized = {str(key): value for key, value in row.items()}
                values = [normalized.get(column) for column in columns]
            else:
                try:
                    values = list(row)
                except TypeError as exc:
                    raise ValueError(f"导出第 {index + 1} 行不是数组或对象") from exc
            if not columns:
                columns = [f"col{position + 1}" for position in range(len(values))]
            if len(values) != len(columns):
                raise ValueError(
                    f"导出第 {index + 1} 行有 {len(values)} 列，"
                    f"与列定义 {len(columns)} 列不一致"
                )
            rows.append([_stringify(value) for value in values])

    if raw_rows and not columns:
        raise ValueError("导出数据缺少列定义")

    payload["columns"] = columns
    payload["rows"] = rows
    # 行数以实际发布内容为准，避免重跑时沿用旧元数据。
    payload["rows_count"] = len(rows)
    payload["export_id"] = int(export_id)
    return payload, columns, rows


def _render_csv(columns: list[str], rows: list[list[str]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _parse_csv(data: bytes) -> list[list[str]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PublicationCorrupted("导出 CSV 不是 UTF-8 编码") from exc
    return [list(row) for row in csv.reader(io.StringIO(text))]


# ---------------------------------------------------------------------------
# 清单与校验
# ---------------------------------------------------------------------------


def _credential_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest.get("schema_version", SCHEMA_VERSION),
        "export_id": int(manifest["export_id"]),
        "publication_id": manifest["publication_id"],
        "digest": manifest["data_digest"],
        "data_digest": manifest["data_digest"],
        "manifest_digest": _manifest_digest(manifest),
        "published_at": manifest.get("published_at"),
        "row_count": int(manifest.get("row_count", 0)),
        "columns": list(manifest.get("columns") or []),
        "files": manifest.get("files") or {},
    }


def _verify_file_group(
    directory: Path,
    export_id: int,
    publication_id: str,
    *,
    check_directory_name: bool = False,
) -> dict[str, Any]:
    """同步核对一个已落盘文件组，返回可安全读取的记录。"""
    if check_directory_name and directory.name != publication_id:
        raise PublicationCorrupted("发布目录名与发布凭据不一致")

    manifest_path = directory / MANIFEST_FILENAME
    manifest = _read_json(manifest_path)
    if not isinstance(manifest, dict) or not manifest:
        raise PublicationCorrupted(
            f"发布 {publication_id} 缺少有效清单"
        )

    if int(manifest.get("export_id", -1)) != int(export_id):
        raise PublicationCorrupted("发布清单的导出任务号不一致")
    if str(manifest.get("publication_id", "")) != publication_id:
        raise PublicationCorrupted("发布清单的发布编号不一致")
    if int(manifest.get("schema_version", 0)) != SCHEMA_VERSION:
        raise PublicationCorrupted("发布清单版本不受支持")

    columns = [str(column) for column in (manifest.get("columns") or [])]
    row_count = int(manifest.get("row_count", -1))

    files = manifest.get("files") or {}
    if set(files) != {"json", "csv"}:
        raise PublicationCorrupted("发布清单必须同时登记 JSON 与 CSV 文件")

    raw_files: dict[str, bytes] = {}
    for kind, filename in (("json", JSON_FILENAME), ("csv", CSV_FILENAME)):
        file_meta = files.get(kind) or {}
        if file_meta.get("name") != filename:
            raise PublicationCorrupted(f"发布清单中的 {kind} 文件名不一致")
        raw = _read_bytes(directory / filename)
        if _sha256(raw) != file_meta.get("sha256"):
            raise PublicationCorrupted(f"发布 {publication_id} 的 {kind} 文件摘要不符")
        if int(file_meta.get("size", -1)) != len(raw):
            raise PublicationCorrupted(f"发布 {publication_id} 的 {kind} 文件大小不符")
        raw_files[kind] = raw

    payload = _read_json(directory / JSON_FILENAME)
    if not isinstance(payload, dict) or not payload:
        raise PublicationCorrupted("导出 JSON 内容不可解析")
    if payload.get("export_id") not in (None, export_id):
        raise PublicationCorrupted("导出 JSON 的任务号与发布清单不一致")
    if payload.get("publication_id") not in (None, publication_id):
        raise PublicationCorrupted("导出 JSON 的发布编号与发布清单不一致")

    json_columns = [str(column) for column in (payload.get("columns") or [])]
    if json_columns != columns:
        raise PublicationCorrupted("导出 JSON 的列与发布清单不一致")

    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, list):
        raise PublicationCorrupted("导出 JSON 的 rows 不是数组")
    rows: list[list[str]] = []
    for index, row in enumerate(raw_rows):
        if not isinstance(row, list) or len(row) != len(columns):
            raise PublicationCorrupted(f"导出 JSON 第 {index + 1} 行列数不一致")
        rows.append([_stringify(value) for value in row])

    if len(rows) != row_count:
        raise PublicationCorrupted("导出 JSON 的行数与发布清单不一致")
    if payload.get("rows_count") not in (None, row_count):
        raise PublicationCorrupted("导出 JSON 的 rows_count 与发布清单不一致")
    digest = _data_digest(columns, rows)
    if digest != manifest.get("data_digest"):
        raise PublicationCorrupted("导出 JSON 的内容摘要与发布清单不一致")
    if payload.get("_publication_digest") not in (None, digest):
        raise PublicationCorrupted("导出 JSON 内嵌的内容摘要与发布清单不一致")

    csv_rows = _parse_csv(raw_files["csv"])
    if not csv_rows:
        raise PublicationCorrupted("导出 CSV 缺少表头")
    if csv_rows[0] != columns:
        raise PublicationCorrupted("导出 CSV 表头与 JSON 列定义不一致")
    csv_body = csv_rows[1:]
    if len(csv_body) != row_count:
        raise PublicationCorrupted("导出 CSV 行数与 JSON/清单不一致")
    if csv_body != rows:
        raise PublicationCorrupted("导出 CSV 的值与 JSON 不一致")

    credential = _credential_from_manifest(manifest)
    return {
        **credential,
        "verified": True,
        "rows": rows,
        "payload": payload,
        "manifest": manifest,
        "credential": credential,
    }


def _write_pointer(export_id: int, manifest: dict[str, Any]) -> None:
    """原子切换 current 指针。

    指针只允许前进：并发发布（含多进程）交错提交时，较旧的发布不会把指针
    覆盖回去。发布目录本身已经不可变，所以这里只影响"当前生效"的选择。
    """
    pointer = _credential_from_manifest(manifest)
    existing = _read_json(pointer_path(export_id))
    if isinstance(existing, dict) and existing.get("publication_id"):
        existing_sort = (
            str(existing.get("published_at") or ""),
            str(existing.get("publication_id") or ""),
        )
        new_sort = (str(pointer.get("published_at") or ""), pointer["publication_id"])
        if new_sort < existing_sort:
            return
    _atomic_write_bytes(pointer_path(export_id), _json_bytes(pointer))


# ---------------------------------------------------------------------------
# 发布
# ---------------------------------------------------------------------------


def publish_export(
    export_id: int,
    export_data: dict[str, Any],
    *,
    stage_hook: Callable[[str], None] | None = None,
    failpoint: str | None = None,
) -> dict[str, Any]:
    """把一次导出结果发布成一个不可变版本，返回发布凭据。

    ``stage_hook``/``failpoint`` 用于故障注入：钩子可以在
    ``json_written``、``csv_written``、``manifest_written``、``verified`` 或
    ``committed`` 阶段抛错，用来验证中断时旧版本仍可读、暂存半成品不会生效。
    """
    export_id = int(export_id)
    payload, columns, rows = _normalize_export_data(export_id, export_data)

    publication_id = _new_publication_id(export_id)
    published_at = _now_iso()
    digest = _data_digest(columns, rows)

    payload["publication_id"] = publication_id
    payload["schema_version"] = SCHEMA_VERSION
    payload["columns"] = columns
    payload["rows"] = rows
    payload["rows_count"] = len(rows)
    payload["published_at"] = published_at
    payload["_published_at"] = published_at
    payload["_publication_id"] = publication_id
    payload["_publication_digest"] = digest
    payload.setdefault("_created_at", published_at)
    payload["_updated_at"] = published_at

    json_bytes = _json_bytes(payload)
    csv_bytes = _render_csv(columns, rows)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "export_id": export_id,
        "publication_id": publication_id,
        "published_at": published_at,
        "columns": columns,
        "row_count": len(rows),
        "data_digest": digest,
        "files": {
            "json": {"name": JSON_FILENAME, "sha256": _sha256(json_bytes), "size": len(json_bytes)},
            "csv": {"name": CSV_FILENAME, "sha256": _sha256(csv_bytes), "size": len(csv_bytes)},
        },
    }
    manifest_bytes = _json_bytes(manifest)

    staging = staging_root() / f"{export_id:06d}-{publication_id}-{uuid.uuid4().hex}"
    try:
        with _PUBLISH_LOCK:
            staging.mkdir(parents=True, exist_ok=False)
            _write_file(staging / JSON_FILENAME, json_bytes)
            _publish_stage(stage_hook, "json_written")
            _check_failpoint(failpoint, "json_written")

            _write_file(staging / CSV_FILENAME, csv_bytes)
            _publish_stage(stage_hook, "csv_written")
            _check_failpoint(failpoint, "csv_written")

            _write_file(staging / MANIFEST_FILENAME, manifest_bytes)
            _publish_stage(stage_hook, "manifest_written")
            _check_failpoint(failpoint, "manifest_written")

            # 落盘后重新读取核对，任何列/值/摘要差异都拒绝发布。
            _verify_file_group(staging, export_id, publication_id)
            _publish_stage(stage_hook, "verified")
            _check_failpoint(failpoint, "verified")

            final_dir = publication_dir(export_id, publication_id)
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            _fsync_dir(staging)
            os.replace(staging, final_dir)
            _fsync_dir(final_dir.parent)
            _publish_stage(stage_hook, "committed")
            _check_failpoint(failpoint, "committed")

            # 只有整个目录原子改名成功后才切换 current 指针。
            _write_pointer(export_id, manifest)
            _publish_stage(stage_hook, "pointer_written")
    except Exception:
        # 暂存目录永远不是可读发布；失败时清掉，旧 current 指针保持不变。
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return _credential_from_manifest(manifest)


def _write_file(path: Path, data: bytes) -> None:
    with open(path, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


# ---------------------------------------------------------------------------
# 读取与恢复
# ---------------------------------------------------------------------------


def _load_published(
    export_id: int,
    publication_id: str,
    *,
    source: str,
    verify: bool = True,
) -> dict[str, Any]:
    directory = publication_dir(export_id, publication_id)
    if not directory.is_dir():
        raise PublicationNotFound(
            f"导出任务 {export_id} 不存在发布版本 {publication_id}"
        )
    if not verify:
        manifest = _read_json(directory / MANIFEST_FILENAME)
        payload = _read_json(directory / JSON_FILENAME)
        if not isinstance(manifest, dict) or not isinstance(payload, dict):
            raise PublicationCorrupted("发布文件组不完整")
        credential = _credential_from_manifest(manifest)
        return {
            **credential,
            "verified": False,
            "rows": payload.get("rows") or [],
            "payload": payload,
            "manifest": manifest,
            "credential": credential,
            "source": source,
        }
    record = _verify_file_group(
        directory, export_id, publication_id, check_directory_name=True
    )
    record["source"] = source
    return record


def _list_publication_ids(export_id: int) -> list[str]:
    directory = publications_dir(export_id)
    if not directory.is_dir():
        return []
    return sorted(
        child.name
        for child in directory.iterdir()
        if child.is_dir() and not child.name.startswith(".")
    )


def recover_current_publication(
    export_id: int,
    *,
    verify: bool = True,
    write_pointer: bool = True,
) -> dict[str, Any] | None:
    """在指针缺失/损坏时，从完整发布中恢复最新一版。

    只考虑已经原子改名到 ``publications/`` 的目录；暂存目录永远不会被提升。
    返回记录中的 ``source`` 为 ``recovered``，并带上 ``recovered_from`` 说明原因。
    """
    export_id = int(export_id)
    candidates: list[dict[str, Any]] = []
    for publication_id in _list_publication_ids(export_id):
        try:
            record = _load_published(
                export_id, publication_id, source="recovered", verify=verify
            )
        except PublicationCorrupted:
            continue
        candidates.append(record)

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            str(item.get("published_at") or ""),
            str(item.get("publication_id") or ""),
        ),
        reverse=True,
    )
    recovered = candidates[0]
    recovered["source"] = "recovered"
    recovered["recovered_from"] = "current 指针缺失或损坏，使用最新完整发布"
    if write_pointer:
        try:
            _write_pointer(export_id, recovered["manifest"])
        except OSError:
            # 指针写不进去也不影响本次读取，下一次读取会再次恢复。
            recovered["pointer_write_failed"] = True
    return recovered


def read_publication(
    export_id: int,
    publication_id: str | None = None,
    *,
    verify: bool = True,
    recover: bool = True,
) -> dict[str, Any] | None:
    """读取一次发布的完整记录；未找到返回 ``None``。

    - 显式给出 ``publication_id`` 时只读该版本，损坏就报错，绝不静默换版本；
    - 未给出时读取 ``current.json``，指针失效则从完整的历史发布中恢复。
    """
    export_id = int(export_id)
    if publication_id:
        return _load_published(
            export_id, str(publication_id), source="explicit", verify=verify
        )

    pointer = _read_json(pointer_path(export_id))
    pointer_id = (
        str(pointer["publication_id"])
        if isinstance(pointer, dict) and pointer.get("publication_id")
        else None
    )
    pointer_failed = False

    if pointer_id:
        if not publication_dir(export_id, pointer_id).is_dir():
            pointer_failed = True
        else:
            try:
                record = _load_published(
                    export_id, pointer_id, source="current", verify=verify
                )
                pointer_digest = pointer.get("digest") or pointer.get("data_digest")
                if pointer_digest and pointer_digest != record.get("digest"):
                    raise PublicationCorrupted("current 指针摘要与发布清单不一致")
                return record
            except PublicationCorrupted:
                if not recover:
                    raise
                pointer_failed = True

    if recover:
        with _PUBLISH_LOCK:
            # 双重检查，避免并发恢复重复工作。
            pointer = _read_json(pointer_path(export_id))
            if isinstance(pointer, dict) and pointer.get("publication_id"):
                try:
                    return _load_published(
                        export_id,
                        str(pointer["publication_id"]),
                        source="current",
                        verify=verify,
                    )
                except PublicationCorrupted:
                    pointer_failed = True
            recovered = recover_current_publication(
                export_id, verify=verify, write_pointer=True
            )
            if recovered is not None:
                return recovered

    legacy = _read_legacy_publication(export_id)
    if legacy is not None:
        return legacy

    if pointer_failed:
        # 指针指向的版本损坏，而且没有其他完整版本可恢复：必须显式报错，
        # 不能把"读不到"伪装成"没有数据"。
        raise PublicationCorrupted(
            f"导出任务 {export_id} 当前发布损坏，且没有可恢复的完整版本"
        )
    return None


def _legacy_paths(export_id: int) -> tuple[Path, Path]:
    base = export_dir(export_id)
    return base.with_suffix(".json"), base.with_suffix(".csv")


def _read_legacy_publication(export_id: int) -> dict[str, Any] | None:
    """兼容旧版平铺的 ``exports/<id>.json`` / ``.csv`` 文件。"""
    json_path, csv_path = _legacy_paths(export_id)
    payload = _read_json(json_path)
    if not isinstance(payload, dict) or not payload:
        return None
    try:
        normalized, columns, rows = _normalize_export_data(export_id, payload)
    except ValueError as exc:
        raise PublicationCorrupted(f"旧版导出文件不可用：{exc}") from exc

    if csv_path.is_file():
        csv_rows = _parse_csv(_read_bytes(csv_path))
        if not csv_rows or csv_rows[0] != columns:
            raise PublicationCorrupted("旧版导出 CSV 表头与 JSON 不一致")
        if csv_rows[1:] != rows:
            raise PublicationCorrupted("旧版导出 CSV 的值与 JSON 不一致")

    digest = _data_digest(columns, rows)
    publication_id = f"legacy-{digest[:12]}"
    published_at = str(
        payload.get("_updated_at") or payload.get("_created_at") or ""
    )
    if not published_at:
        try:
            published_at = datetime.fromtimestamp(
                json_path.stat().st_mtime, data_manager.TZ
            ).isoformat()
        except OSError:
            published_at = _now_iso()

    normalized["_publication_id"] = publication_id
    normalized["_publication_digest"] = digest
    normalized["_published_at"] = published_at
    credential = {
        "schema_version": SCHEMA_VERSION,
        "export_id": export_id,
        "publication_id": publication_id,
        "digest": digest,
        "data_digest": digest,
        "manifest_digest": digest,
        "published_at": published_at,
        "row_count": len(rows),
        "columns": columns,
        "files": {},
    }
    return {
        **credential,
        "verified": True,
        "rows": rows,
        "payload": normalized,
        "manifest": None,
        "credential": credential,
        "source": "legacy",
    }


def list_publications(export_id: int, *, verify: bool = True) -> list[dict[str, Any]]:
    """列出一个导出任务的所有完整发布，损坏的版本会显式标记。"""
    export_id = int(export_id)
    pointer = _read_json(pointer_path(export_id))
    current_id = (
        str(pointer.get("publication_id"))
        if isinstance(pointer, dict) and pointer.get("publication_id")
        else None
    )
    records: list[dict[str, Any]] = []
    for publication_id in _list_publication_ids(export_id):
        try:
            record = _load_published(
                export_id, publication_id, source="listed", verify=verify
            )
        except PublicationCorrupted as exc:
            records.append({
                "export_id": export_id,
                "publication_id": publication_id,
                "current": publication_id == current_id,
                "verified": False,
                "status": "corrupt",
                "error": str(exc),
            })
            continue
        record["current"] = publication_id == current_id
        records.append(record)

    legacy = _read_legacy_publication(export_id)
    if legacy is not None and not records:
        legacy["current"] = True
        records.append(legacy)

    records.sort(
        key=lambda item: (
            str(item.get("published_at") or ""),
            str(item.get("publication_id") or ""),
        ),
        reverse=True,
    )
    return records


def read_publication_artifact(
    export_id: int,
    publication_id: str | None = None,
    *,
    fmt: str = "csv",
) -> tuple[bytes, dict[str, Any]]:
    """读取可下载成果；返回的字节与发布凭据来自同一次校验。"""
    record = read_publication(export_id, publication_id, verify=True)
    if record is None:
        raise PublicationNotFound(f"导出任务 {export_id} 尚无可用发布")

    fmt = fmt.lower()
    if fmt not in {"csv", "json"}:
        raise ValueError(f"不支持的导出格式：{fmt}")

    if record.get("source") == "legacy":
        json_path, csv_path = _legacy_paths(export_id)
        if fmt == "json":
            return _read_bytes(json_path), record
        if csv_path.is_file():
            return _read_bytes(csv_path), record
        return _render_csv(record["columns"], record["rows"]), record

    directory = publication_dir(export_id, str(record["publication_id"]))
    filename = CSV_FILENAME if fmt == "csv" else JSON_FILENAME
    return _read_bytes(directory / filename), record


def read_current_payload(export_id: int) -> dict[str, Any] | None:
    record = read_publication(export_id, verify=True)
    if record is None:
        return None
    payload = dict(record["payload"])
    payload["_publication_id"] = record["publication_id"]
    payload["_publication_digest"] = record["digest"]
    payload["_published_at"] = record.get("published_at")
    payload["_publication_source"] = record.get("source")
    return payload


def list_current_exports(*, verify: bool = True) -> list[dict[str, Any]]:
    """列出每个导出任务当前生效的发布记录（每任务至多一条）。

    某个任务的当前发布损坏时会直接抛错，而不是静默跳过——归并少取一个源
    比读到旧数据更危险。
    """
    root = exports_root()
    if not root.is_dir():
        return []

    records: list[dict[str, Any]] = []
    seen: set[int] = set()
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        if not child.name.isdigit():
            continue
        export_id = int(child.name)
        seen.add(export_id)
        record = read_publication(export_id, verify=verify)
        if record is not None:
            records.append(record)

    # 兼容没有发布目录、只有平铺 JSON 的旧任务。
    for child in sorted(root.glob("[0-9]*.json")):
        if not child.stem.isdigit():
            continue
        export_id = int(child.stem)
        if export_id in seen:
            continue
        record = _read_legacy_publication(export_id)
        if record is not None:
            records.append(record)

    return records


def find_publication(publication_id: str, *, verify: bool = True) -> dict[str, Any] | None:
    """按全局唯一的发布编号查找发布（供只传 publication_id 的调用方使用）。"""
    root = exports_root()
    if not root.is_dir():
        return None
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or not child.name.isdigit():
            continue
        export_id = int(child.name)
        if publication_dir(export_id, publication_id).is_dir():
            return _load_published(
                export_id, publication_id, source="explicit", verify=verify
            )
    return None


def recover_publications(*, clean_staging_older_than: float | None = 3600.0) -> dict[str, Any]:
    """启动/巡检用恢复入口。

    - 清理超过阈值时间的暂存目录（未完成的发布）；
    - 为每个导出任务检查 current 指针，必要时提升最新完整发布；
    - 返回恢复摘要，便于审计"当前取的是哪次发布"。
    """
    cleanup: list[str] = []
    root = staging_root()
    if root.is_dir() and clean_staging_older_than is not None:
        now = datetime.now().timestamp()
        for child in list(root.iterdir()):
            try:
                age = now - child.stat().st_mtime
            except OSError:
                continue
            if age < clean_staging_older_than:
                continue
            shutil.rmtree(child, ignore_errors=True)
            cleanup.append(child.name)

    recovered: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    for child in sorted(exports_root().glob("[0-9]*")) if exports_root().is_dir() else []:
        if not child.is_dir() or not child.name.isdigit():
            continue
        export_id = int(child.name)
        try:
            record = read_publication(export_id)
        except PublicationError as exc:
            errors.append({"export_id": export_id, "error": str(exc)})
            continue
        if record is None:
            continue
        current.append({
            "export_id": export_id,
            "publication_id": record["publication_id"],
            "digest": record["digest"],
            "source": record.get("source"),
        })
        if record.get("source") == "recovered":
            recovered.append(current[-1])

    return {
        "ok": not errors,
        "cleaned_staging": cleanup,
        "recovered": recovered,
        "current": current,
        "errors": errors,
    }


# 便于调用方以更短的名字引用。
publish = publish_export
resolve = read_publication
