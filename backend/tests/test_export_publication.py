"""导出发布一致性测试：同一任务重跑、发布中断与恢复期间两种取数的一致性。

核心场景（对应需求）：
1. 先发布一版三行的文件组；
2. 改掉其中一行，第二次发布在写入 JSON 后中断；
3. 核对中断期间下载与归并仍拿到第一版三行数据，且列/值逐项一致；
4. 恢复后两者同时切到新版本，旧版本仍可显式读取。
"""
import csv
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

TEMP_DATA = tempfile.mkdtemp(prefix="export_publication_test_")
os.environ["DATA_ROOT"] = TEMP_DATA


@pytest.fixture()
def env():
    """每个用例独立的 data 目录与干净的 store。"""
    import shutil

    from app.seed import SEED_ROWS
    from app.store import store

    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    shutil.rmtree(TEMP_DATA, ignore_errors=True)
    os.makedirs(TEMP_DATA, exist_ok=True)

    from app.services import export_store

    yield export_store
    shutil.rmtree(TEMP_DATA, ignore_errors=True)


def _rows(third_name: str, first_name: str = "张三") -> list[dict[str, str]]:
    return [
        {"id": "1", "name": first_name, "amount": "1250.50"},
        {"id": "2", "name": "李四", "amount": "2340.00"},
        {"id": "3", "name": third_name, "amount": "890.75"},
    ]


def _values(third_name: str, first_name: str = "张三") -> list[list[str]]:
    """发布记录里的行按列顺序存的二维数组。"""
    return [[r["id"], r["name"], r["amount"]] for r in _rows(third_name, first_name)]


def _publish(export_store, data, **kwargs):
    return export_store.publish_export(1, {"columns": ["id", "name", "amount"], "rows": data}, **kwargs)


def _csv_rows(export_store, publication_id=None):
    content, record = export_store.read_publication_artifact(1, publication_id, fmt="csv")
    parsed = list(csv.reader(io.StringIO(content.decode("utf-8"))))
    return parsed[0], parsed[1:], record


def _json_rows(export_store, publication_id=None):
    content, record = export_store.read_publication_artifact(1, publication_id, fmt="json")
    payload = json.loads(content)
    return payload["columns"], payload["rows"], record


def _merge(export_store, publication_ids=None):
    from app.services.data_merge import DataMergeService
    from app.store import store

    # 种子里的规则字段是占位文本，这里固定一条按 id 归并的已生效规则。
    rule = store.find("data_merge", 3)
    rule.update({
        "归并键": "id",
        "冲突策略": "从源覆盖",
        "去重策略": "按键去重",
        "结果排序": "id",
        "status": "已生效",
    })
    return DataMergeService().merge_publications(3, publication_ids)


def test_file_group_json_csv_columns_and_values_match(env):
    export_store = env
    credential = _publish(export_store, _rows("王五"))

    json_columns, json_rows, json_record = _json_rows(export_store)
    csv_columns, csv_rows, csv_record = _csv_rows(export_store)

    assert json_columns == csv_columns == ["id", "name", "amount"]
    assert json_rows == csv_rows
    assert json_record["publication_id"] == csv_record["publication_id"] == credential["publication_id"]
    assert json_record["row_count"] == 3


def test_rerun_publishes_new_immutable_version(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))
    second = _publish(export_store, _rows("王五改"))

    assert first["publication_id"] != second["publication_id"]
    assert first["digest"] != second["digest"]

    current = export_store.read_publication(1)
    assert current["publication_id"] == second["publication_id"]
    assert current["rows"][2] == ["3", "王五改", "890.75"]

    old = export_store.read_publication(1, first["publication_id"])
    assert old["rows"][2] == ["3", "王五", "890.75"]

    records = export_store.list_publications(1)
    ids = {record["publication_id"] for record in records}
    assert ids == {first["publication_id"], second["publication_id"]}
    assert sum(1 for record in records if record["current"]) == 1


@pytest.mark.parametrize("failpoint", ["json_written", "csv_written", "manifest_written", "verified"])
def test_interrupted_republish_never_replaces_available_files(env, failpoint):
    export_store = env
    first = _publish(export_store, _rows("王五"))

    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint=failpoint)

    # 指针仍指向第一版，暂存目录没有残留成可读发布。
    current = export_store.read_publication(1)
    assert current["publication_id"] == first["publication_id"]
    assert current["rows"] == _values("王五")
    staging = export_store.staging_root()
    assert not staging.exists() or not list(staging.iterdir())
    assert export_store.list_publications(1)[0]["publication_id"] == first["publication_id"]


def test_download_and_merge_agree_during_interruption(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))

    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="json_written")

    # 中断期间：下载与归并都仍取第一版，并可逐行对上。
    csv_columns, csv_rows, download_record = _csv_rows(export_store)
    merged = _merge(export_store)

    assert download_record["publication_id"] == first["publication_id"]
    assert merged["receipt"]["sources"][0]["publication_id"] == first["publication_id"]
    assert [[row[c] for c in csv_columns] for row in merged["rows"]] == csv_rows
    assert merged["receipt"]["sources"][0]["rows_digest"] == download_record["digest"]
    assert "王五改" not in json.dumps(merged["rows"], ensure_ascii=False)


def test_download_and_merge_agree_after_recovery(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))
    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="json_written")

    second = _publish(export_store, _rows("王五改"))
    assert second["publication_id"] != first["publication_id"]

    csv_columns, csv_rows, download_record = _csv_rows(export_store)
    merged = _merge(export_store)
    json_columns, json_rows, json_record = _json_rows(export_store)

    assert download_record["publication_id"] == second["publication_id"]
    assert json_record["publication_id"] == second["publication_id"]
    assert merged["receipt"]["sources"][0]["publication_id"] == second["publication_id"]
    assert csv_columns == json_columns == merged["receipt"]["sources"][0]["columns"]
    assert json_rows == csv_rows == [[row[c] for c in csv_columns] for row in merged["rows"]]


def test_merge_can_pin_publication_used_for_download(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))
    _publish(export_store, _rows("王五改"))

    # 显式指定第一版：下载与归并都读同一发布凭据。
    _, pinned_rows, download_record = _csv_rows(export_store, first["publication_id"])
    merged = _merge(export_store, [first["publication_id"]])

    assert download_record["publication_id"] == first["publication_id"]
    assert merged["receipt"]["sources"][0]["publication_id"] == first["publication_id"]
    assert [[row["id"], row["name"], row["amount"]] for row in merged["rows"]] == pinned_rows


def test_pointer_loss_recovers_latest_complete_publication(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))
    second = _publish(export_store, _rows("王五改"))

    os.remove(export_store.pointer_path(1))
    recovered = export_store.read_publication(1)
    assert recovered["source"] == "recovered"
    assert recovered["publication_id"] == second["publication_id"]
    assert recovered["rows"][2] == ["3", "王五改", "890.75"]
    assert export_store.pointer_path(1).is_file()

    # 第一版仍然完整可读
    assert export_store.read_publication(1, first["publication_id"])["rows"] == _values("王五")


def test_crash_after_commit_before_pointer_switch(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))

    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="committed")

    # 新版本目录已完整落盘，但指针没切：读者仍拿第一版，恢复时可提升。
    current = export_store.read_publication(1)
    assert current["publication_id"] == first["publication_id"]

    os.remove(export_store.pointer_path(1))
    recovered = export_store.read_publication(1)
    assert recovered["rows"][2] == ["3", "王五改", "890.75"]


def test_corrupted_current_does_not_silently_serve_partial_data(env):
    export_store = env
    _publish(export_store, _rows("王五"))
    directory = export_store.publication_dir(
        1, export_store.read_publication(1)["publication_id"]
    )
    # 篡改 CSV 单个值：JSON 完整但文件组不一致。
    (directory / export_store.CSV_FILENAME).write_text(
        "id,name,amount\n1,张三,1250.50\n2,李四,2340.00\n3,王五,999.99\n",
        encoding="utf-8",
    )

    with pytest.raises(export_store.PublicationCorrupted):
        export_store.read_publication(1)

    with pytest.raises(export_store.PublicationCorrupted):
        export_store.read_publication(1, directory.name)


def test_publication_records_survive_only_complete_groups(env):
    export_store = env
    _publish(export_store, _rows("王五"))
    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="json_written")

    records = export_store.list_publications(1)
    assert len(records) == 1
    assert records[0]["verified"] is True

    summary = export_store.recover_publications(clean_staging_older_than=0)
    assert summary["ok"] is True
    assert summary["current"][0]["publication_id"] == records[0]["publication_id"]


def test_export_task_execute_returns_publication_credential(env):
    from app.store import store
    from app.services.export_task import ExportTaskService

    service = ExportTaskService()
    result = service.execute_export(1)
    assert result["ok"] is True
    assert result["publication_id"]
    assert result["digest"]
    assert result["row_count"] == result["rows_count"] == 3

    record = env.read_publication(1, result["publication_id"])
    assert record["verified"] is True
    assert len(record["rows"]) == 3
    assert store.find("export_task", 1)["status"] == "已完成"


def test_export_task_failed_publish_returns_error_and_keeps_previous(env):
    from app.services.export_task import ExportTaskService

    service = ExportTaskService()
    first = service.execute_export(1)
    assert first["ok"]

    result = service.execute_export(1, failpoint="json_written")
    assert result["ok"] is False
    assert "发布失败" in result["message"]
    current = env.read_publication(1)
    assert current["publication_id"] == first["publication_id"]


def test_download_endpoint_exposes_publication_headers(env):
    from fastapi.testclient import TestClient

    from app.main import app

    export_store = env
    first = _publish(export_store, _rows("王五"))
    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="json_written")

    client = TestClient(app)

    response = client.get("/api/export_task/1/download?format=csv")
    assert response.status_code == 200
    assert response.headers["x-export-publication"] == first["publication_id"]
    assert response.headers["x-export-digest"] == first["digest"]
    assert response.headers["x-export-row-count"] == "3"
    assert response.text.splitlines()[3] == "3,王五,890.75"

    # 显式指定发布版本下载，仍是同一份凭据。
    pinned = client.get(
        f"/api/export_task/1/download?format=json&publication_id={first['publication_id']}"
    )
    assert pinned.status_code == 200
    assert pinned.headers["x-export-publication"] == first["publication_id"]
    assert json.loads(pinned.text)["rows"] == _values("王五")

    # 不存在的发布版本明确 404，不会退回到任意可用版本。
    missing = client.get("/api/export_task/1/download?publication_id=does-not-exist")
    assert missing.status_code == 404

    listing = client.get("/api/export_task/1/publications")
    assert listing.status_code == 200
    payload = listing.json()
    assert payload["total"] == 1
    assert payload["items"][0]["publication_id"] == first["publication_id"]
    assert payload["items"][0]["current"] is True

    # 归并凭据里的下载地址可以直接下载同一发布。
    merged = _merge(export_store)
    source = merged["receipt"]["sources"][0]
    download = client.get(source["download"]["csv"])
    assert download.status_code == 200
    assert download.headers["x-export-publication"] == source["publication_id"]
    rows = list(csv.reader(io.StringIO(download.text)))[1:]
    assert rows == [[row["id"], row["name"], row["amount"]] for row in merged["rows"]]


def test_merge_endpoint_receipts_match_download(env):
    from fastapi.testclient import TestClient

    from app.main import app
    from app.store import store

    export_store = env
    store.find("data_merge", 3).update({
        "归并键": "id",
        "冲突策略": "从源覆盖",
        "去重策略": "按键去重",
        "结果排序": "id",
        "status": "已生效",
    })

    first = _publish(export_store, _rows("王五"))
    with pytest.raises(export_store.InjectedPublishFailure):
        _publish(export_store, _rows("王五改"), failpoint="json_written")

    client = TestClient(app)
    merged = client.get("/api/data_merge/3/merge")
    assert merged.status_code == 200
    payload = merged.json()
    source = payload["receipt"]["sources"][0]
    assert source["publication_id"] == first["publication_id"]

    download = client.get(source["download"]["csv"])
    assert download.status_code == 200
    downloaded = list(csv.reader(io.StringIO(download.text)))[1:]
    assert downloaded == [
        [row["id"], row["name"], row["amount"]] for row in payload["rows"]
    ]

    # 恢复完成后，归并接口与下载同步切到新发布。
    second = _publish(export_store, _rows("王五改"))
    recovered = client.get("/api/data_merge/3/merge").json()
    assert recovered["receipt"]["sources"][0]["publication_id"] == second["publication_id"]
    assert recovered["rows"][2]["name"] == "王五改"

    # 显式固定旧发布时，归并与下载仍然一致。
    pinned = client.get(
        f"/api/data_merge/3/merge?publication_id={first['publication_id']}"
    ).json()
    pinned_download = client.get(pinned["receipt"]["sources"][0]["download"]["csv"])
    assert pinned_download.text.splitlines()[3] == "3,王五,890.75"


def test_recovery_ignores_unfinished_staging_directory(env):
    export_store = env
    first = _publish(export_store, _rows("王五"))

    # 模拟进程崩溃：暂存目录只写了一半，指针也没更新。
    staging = export_store.staging_root() / "000001-crashed-partial"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / export_store.JSON_FILENAME).write_text(
        json.dumps({
            "export_id": 1,
            "columns": ["id", "name", "amount"],
            "rows": [["1", "张三", "1250.50"]],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    os.remove(export_store.pointer_path(1))

    recovered = export_store.read_publication(1)
    assert recovered["publication_id"] == first["publication_id"]
    assert recovered["rows"] == _values("王五")

    summary = export_store.recover_publications(clean_staging_older_than=0)
    assert summary["ok"] is True
    assert not staging.exists()


def test_concurrent_readers_never_see_partial_publication(env):
    import threading

    export_store = env
    first = _publish(export_store, _rows("王五"))

    versions = {
        "王五": _values("王五"),
        "王五改": _values("王五改"),
        "王五再改": _values("王五再改"),
    }
    stop = threading.Event()
    errors: list[str] = []
    seen: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                record = export_store.read_publication(1)
                rows = record["rows"]
                if rows not in versions.values():
                    errors.append(f"读到混合版本: {rows}")
                    return
                if record["payload"]["rows"] != rows:
                    errors.append("发布记录内的 JSON 与归并取数行不一致")
                    return

                # 单次下载调用必须返回与自身凭据一致的文件组。
                csv_columns, csv_rows, csv_record = _csv_rows(export_store)
                if csv_rows != csv_record["rows"] or csv_columns != csv_record["columns"]:
                    errors.append("一次下载内的 CSV 与凭据不一致")
                    return
                if csv_record["rows"] not in versions.values():
                    errors.append(f"下载读到混合版本: {csv_record['rows']}")
                    return

                # 按归并/下载共用的凭据固定版本时，读到的一定是同一版。
                pinned_columns, pinned_rows, pinned_record = _csv_rows(
                    export_store, csv_record["publication_id"]
                )
                if (
                    pinned_record["publication_id"] != csv_record["publication_id"]
                    or pinned_rows != csv_rows
                    or pinned_columns != csv_columns
                ):
                    errors.append("固定发布凭据后读到不同内容")
                    return
                seen.append(record["publication_id"])
            except Exception as exc:  # noqa: BLE001 - 线程里收集错误
                errors.append(f"读取异常: {exc}")
                return

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for thread in readers:
        thread.start()
    try:
        for name in ("王五改", "王五再改", "王五"):
            try:
                _publish(export_store, _values(name), failpoint="json_written")
            except export_store.InjectedPublishFailure:
                pass
            _publish(export_store, _values(name))
    finally:
        stop.set()
        for thread in readers:
            thread.join(timeout=5)

    assert not errors, errors[:3]
    assert len(set(seen)) >= 2
    assert export_store.read_publication(1)["publication_id"] is not None
