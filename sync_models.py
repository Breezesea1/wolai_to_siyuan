from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import mimetypes
import os
import posixpath
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import requests

SIYUAN_URL = os.environ.get("SIYUAN_URL", "http://127.0.0.1:6806")
SIYUAN_TOKEN = os.environ.get("SIYUAN_TOKEN", "")
MD_ROOT = Path(os.environ.get("WOLAI_ROOT", "."))
TARGET_ROOT = os.environ.get("SIYUAN_TARGET_ROOT", "/迁移/wolai")
NOTEBOOK_ID = os.environ.get("SIYUAN_NOTEBOOK_ID", "")
WOLAI_API_BASE = os.environ.get("WOLAI_API_BASE", "https://openapi.wolai.com")
WOLAI_APP_ID = os.environ.get("WOLAI_APP_ID", "")
WOLAI_APP_KEY = os.environ.get("WOLAI_APP_KEY", "")
WOLAI_TOKEN = os.environ.get("WOLAI_TOKEN", "")
SUMMARY_PATH = Path(".omx/logs/wolai-sync-summary.json")
MAX_HPATH_SEGMENTS = int(os.environ.get("SIYUAN_MAX_HPATH_SEGMENTS", "0"))

MARKDOWN_LINK_RE = re.compile(r"(!?)\[([^\]]*)\]\(([^)\n]+)\)")
WOLAI_WRAPPED_LINK_RE = re.compile(r"(!?)\[\s*([^\]]*?)\s*\]\(\s*([^)]+?)\s*\)", re.MULTILINE)
ANGLE_LINK_RE = re.compile(r"<([^<>\n]+)>")
WOLAI_SUFFIX_RE = re.compile(r"^(?P<base>.+?)_(?P<tail>[A-Za-z0-9]{16,})$")
INVALID_HPATH_CHARS_RE = re.compile(r'[\\/:*?"<>|]')
SPACE_RE = re.compile(r"\s+")
MARKDOWN_ESCAPE_RE = re.compile(r"\\([\\`*_{}\[\]()#+\-.!|<>])")


class SyncError(RuntimeError):
    pass


@dataclass(slots=True)
class LinkRef:
    source_rel: Path
    original_target: str
    normalized_target: str
    kind: str
    resolved_rel: Path | None = None
    anchor: str | None = None
    display_text: str | None = None
    is_image: bool = False
    wrapped_by_angle: bool = False
    line_number: int | None = None
    malformed_reason: str | None = None


@dataclass(slots=True)
class TableShape:
    line_index: int
    columns: int


@dataclass(slots=True)
class PageEntry:
    source_rel: Path
    abs_path: Path
    title: str
    target_hpath: str
    markdown: str
    links: list[LinkRef] = field(default_factory=list)
    table_shapes: list[TableShape] = field(default_factory=list)
    doc_id: str | None = None
    root_id: str | None = None
    database_plan: DatabaseTablePlan | None = None


@dataclass(slots=True)
class AssetEntry:
    source_rel: Path
    abs_path: Path
    digest: str
    upload_name: str
    target_path: str | None = None


@dataclass(slots=True)
class Manifest:
    pages: dict[Path, PageEntry] = field(default_factory=dict)
    assets: dict[Path, AssetEntry] = field(default_factory=dict)
    pages_by_hpath: dict[str, PageEntry] = field(default_factory=dict)


@dataclass(slots=True)
class DatabaseCell:
    raw_markdown: str
    text_value: str | None = None
    date_value: str | None = None
    asset_rel: Path | None = None
    asset_name: str | None = None
    doc_rel: Path | None = None
    doc_label: str | None = None


@dataclass(slots=True)
class DatabaseRow:
    primary_text: str
    primary_doc_rel: Path | None
    cells: list[DatabaseCell]


@dataclass(slots=True)
class DatabaseColumnPlan:
    index: int
    name: str
    key_type: str
    key_id: str | None = None


@dataclass(slots=True)
class DatabaseTablePlan:
    av_id: str
    columns: list[DatabaseColumnPlan]
    rows: list[DatabaseRow]


@dataclass(slots=True)
class SyncSummary:
    preflight: dict[str, object] = field(default_factory=dict)
    candidates_to_delete: list[dict[str, str]] = field(default_factory=list)
    deleted_docs: list[str] = field(default_factory=list)
    failed_deletes: list[dict[str, str]] = field(default_factory=list)
    created_docs: list[dict[str, str]] = field(default_factory=list)
    path_collisions: list[dict[str, str]] = field(default_factory=list)
    missing_assets: list[dict[str, str]] = field(default_factory=list)
    unresolved_doc_links: list[dict[str, str]] = field(default_factory=list)
    unresolved_malformed: list[dict[str, str]] = field(default_factory=list)
    unsupported_features: list[dict[str, str]] = field(default_factory=list)
    upload_failed: list[dict[str, str]] = field(default_factory=list)
    uploaded_assets: list[dict[str, str]] = field(default_factory=list)
    rewritten_asset_refs: int = 0
    rewritten_doc_refs: int = 0
    succmap_consumed: int = 0
    writeback_mode: str | None = None
    doc_id_map: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "preflight": self.preflight,
                "candidates_to_delete": self.candidates_to_delete,
                "deleted_count": len(self.deleted_docs),
                "failed_deletes": self.failed_deletes,
                "created_docs": self.created_docs,
                "path_collisions": self.path_collisions,
                "missing_assets": self.missing_assets,
                "unresolved_doc_links": self.unresolved_doc_links,
                "unresolved_malformed": self.unresolved_malformed,
                "unsupported_features": self.unsupported_features,
                "upload_failed": self.upload_failed,
                "uploaded_assets": self.uploaded_assets,
                "rewritten_asset_refs": self.rewritten_asset_refs,
                "rewritten_doc_refs": self.rewritten_doc_refs,
                "succmap_consumed": self.succmap_consumed,
                "writeback_mode": self.writeback_mode,
                "doc_id_map": self.doc_id_map,
            },
            ensure_ascii=False,
            indent=2,
        )


