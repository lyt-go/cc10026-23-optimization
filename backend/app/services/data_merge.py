"""数据归并领域服务：管理多API数据源的归并、冲突处理和去重策略。

跨模块数据流：
- 消费 api_registry 快照 -> 归并源API核验
- 消费 data_filter 快照 -> 各源数据归并前先过滤
- 被 export_task 消费 -> 归并后数据导出
- 归并结果写入共享快照
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MergeResult(list):
    """归并结果行，同时携带本次取数的发布凭据。

    保持 ``list`` 语义以兼容既有调用方，同时提供 ``receipt`` / ``publications``
    方便调用方核对归并结果与可下载成果来自同一次发布。
    """

    receipt: dict[str, Any] = {}
    publications: list[dict[str, Any]] = []


class DataMergeService(BaseModuleService):
    MODULE = "data_merge"
    ENTITY = "归并规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "主API", "从API", "归并键", "冲突策略", "去重策略", "结果排序", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "主API", "从API"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def merge_sources(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                      source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按归并规则合并两个数据源。"""
        rule = store.find(self.MODULE, rule_id)
        if rule is None or rule.get("status") != "已生效":
            return source_a_rows + source_b_rows

        merge_key = rule.get("归并键", "id")
        conflict_policy = rule.get("冲突策略", "主源优先")
        dedup_policy = rule.get("去重策略", "全保留")

        merged = {}
        for row in source_a_rows:
            key = str(row.get(merge_key, ""))
            merged[f"a_{key}"] = {"_source": "primary", **row}

        for row in source_b_rows:
            key = str(row.get(merge_key, ""))
            pk = f"a_{key}"
            if pk in merged:
                if conflict_policy == "主源优先":
                    continue
                elif conflict_policy == "从源覆盖":
                    merged[pk] = {"_source": "secondary", **row}
                elif conflict_policy == "合并字段":
                    merged[pk].update({"from_secondary": row})
            else:
                merged[f"b_{key}"] = {"_source": "secondary", **row}

        result = list(merged.values())

        # 去重
        if dedup_policy == "按键去重":
            seen = set()
            deduped = []
            for row in result:
                key = str(row.get(merge_key, ""))
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            result = deduped

        # 排序
        sort_field = rule.get("结果排序", "")
        if sort_field:
            result = sorted(result, key=lambda r: str(r.get(sort_field, "")))

        return result

    def merge_and_audit(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                        source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """归并两个数据源并写入审计记录。

        跨模块增强：归并完成后自动写入call_audit审计。
        """
        merged = self.merge_sources(rule_id, source_a_rows, source_b_rows)
        rule = store.find(self.MODULE, rule_id)

        # 联动增强：归并结果写入审计
        from app.services.data_manager import write_audit, DATA_ROOT
        audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
        write_audit(audit_id, {
            "audit_id": audit_id,
            "api_name": rule.get("主API", "") if rule else "",
            "caller": "data_merge",
            "event": "merge_completed",
            "primary_count": len(source_a_rows),
            "secondary_count": len(source_b_rows),
            "merged_count": len(merged),
            "rule_no": rule.get("规则编号", "") if rule else "",
        })
        return merged

    def merge_publications(
        self,
        rule_id: int,
        publication_ids: list[str] | None = None,
        *,
        verify: bool = True,
    ) -> dict[str, Any]:
        """从导出发布取数并归并，返回行和发布凭据。

        - 指定 ``publication_ids`` 时严格读取这些发布版本，损坏直接报错；
        - 未指定时取每个导出任务当前生效的发布，按发布时间倒序取前两个；
        - 所有来源都来自同一套 ``export_store`` 读取路径，和文件下载共用凭据，
          因此归并返回行与下载成果可以逐值核对。
        """
        from app.services import export_store

        records: list[dict[str, Any]] = []
        if publication_ids:
            for publication_id in publication_ids:
                record = export_store.find_publication(
                    str(publication_id), verify=verify
                )
                if record is None:
                    raise export_store.PublicationNotFound(
                        f"发布版本 {publication_id} 不存在"
                    )
                records.append(record)
        else:
            records = export_store.list_current_exports(verify=verify)
            records.sort(
                key=lambda item: str(item.get("published_at") or ""), reverse=True
            )

        source_a = records[0] if len(records) > 0 else None
        source_b = records[1] if len(records) > 1 else None

        def _rows(record: dict[str, Any] | None) -> list[dict[str, Any]]:
            if record is None:
                return []
            columns = record.get("columns") or []
            return [
                {column: value for column, value in zip(columns, row)}
                for row in record.get("rows", [])
            ]

        rows_a = _rows(source_a)
        rows_b = _rows(source_b)
        merged = self.merge_sources(rule_id, rows_a, rows_b)

        rule = store.find(self.MODULE, rule_id)
        sources = [
            self._source_receipt(record)
            for record in (source_a, source_b)
            if record is not None
        ]
        receipt = {
            "rule_id": rule_id,
            "rule_no": rule.get("规则编号", "") if rule else "",
            "sources": sources,
            "merged_count": len(merged),
            "rows_digest": self._rows_digest(merged),
            "generated_at": self._now_iso(),
        }

        # 审计里固定写入本次取数的发布编号与摘要，恢复期间也能追踪来源。
        from app.services.data_manager import write_audit, DATA_ROOT
        audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
        write_audit(audit_id, {
            "audit_id": audit_id,
            "api_name": rule.get("主API", "") if rule else "",
            "caller": "data_merge",
            "event": "merge_completed",
            "primary_count": len(rows_a),
            "secondary_count": len(rows_b),
            "merged_count": len(merged),
            "rule_no": rule.get("规则编号", "") if rule else "",
            "publications": [
                {
                    "export_id": item["export_id"],
                    "publication_id": item["publication_id"],
                    "digest": item["digest"],
                    "source": item.get("source"),
                }
                for item in sources
            ],
            "merged_digest": receipt["rows_digest"],
        })
        return {"rows": merged, "receipt": receipt}

    @staticmethod
    def _source_receipt(record: dict[str, Any]) -> dict[str, Any]:
        from app.services import export_store

        return {
            "export_id": record["export_id"],
            "publication_id": record["publication_id"],
            "digest": record["digest"],
            "data_digest": record.get("data_digest", record["digest"]),
            "published_at": record.get("published_at"),
            "source": record.get("source"),
            "row_count": record.get("row_count", len(record.get("rows", []))),
            "columns": list(record.get("columns") or []),
            "rows_digest": export_store._data_digest(
                list(record.get("columns") or []),
                [
                    [str(value) for value in row]
                    for row in record.get("rows", [])
                ],
            ),
            "download": {
                "csv": (
                    f"/api/export_task/{record['export_id']}/download"
                    f"?publication_id={record['publication_id']}&format=csv"
                ),
                "json": (
                    f"/api/export_task/{record['export_id']}/download"
                    f"?publication_id={record['publication_id']}&format=json"
                ),
            },
        }

    @staticmethod
    def _rows_digest(rows: list[dict[str, Any]]) -> str:
        from app.services import export_store

        columns: list[str] = []
        for row in rows:
            for key in row.keys():
                text = str(key)
                if text not in columns:
                    columns.append(text)
        values = [[str(row.get(column, "")) for column in columns] for row in rows]
        return export_store._data_digest(columns, values)

    @staticmethod
    def _now_iso() -> str:
        from app.services.data_manager import _now_iso

        return _now_iso()

    def merge_from_exports(
        self,
        rule_id: int,
        publication_ids: list[str] | None = None,
        *,
        verify: bool = True,
    ) -> list[dict[str, Any]]:
        """从导出发布直接读取两个数据源并进行归并。

        返回行为普通列表，同时通过 ``MergeResult`` 暴露本次取数的发布凭据
        （``.receipt`` / ``.sources``），便于调用方和下载结果对账。
        """
        result = self.merge_publications(
            rule_id, publication_ids, verify=verify
        )
        merged = MergeResult(result["rows"])
        merged.receipt = result["receipt"]
        merged.publications = result["receipt"]["sources"]
        return merged

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记生效":
            from app.services.data_manager import read_api_snapshot
            registry = read_api_snapshot()
            primary_api = entry.get("主API", "")
            secondary_api = entry.get("从API", "")
            api_names = {v.get("api_name") for v in (registry.values() if isinstance(registry, dict) else []) if isinstance(v, dict)}
            if primary_api not in api_names and secondary_api not in api_names:
                return None, "至少一个源API尚未注册"

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        merge_list = []
        for row in rows:
            merge_list.append({
                "id": row.get("id"),
                "rule_no": row.get("规则编号"),
                "primary_api": row.get("主API"),
                "secondary_api": row.get("从API"),
                "merge_key": row.get("归并键"),
                "conflict_policy": row.get("冲突策略"),
                "dedup_policy": row.get("去重策略"),
                "sort": row.get("结果排序"),
                "version": row.get("规则版本"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(merge_list),
            "merges": merge_list,
            "active_count": sum(1 for r in merge_list if r.get("status") == "已生效"),
        }