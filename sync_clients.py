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


from sync_models import SyncError

class SiYuanClient:
    def __init__(self, base_url: str, token: str, timeout: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Token {token}"})

    def post_json(self, api: str, payload: dict | None = None) -> dict:
        response = self.session.post(
            f"{self.base_url}{api}",
            json=payload or {},
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise SyncError(f"{api} failed: {data}")
        return data

    def list_notebooks(self) -> list[dict]:
        return self.post_json("/api/notebook/lsNotebooks")["data"]["notebooks"]

    def create_doc_with_md(self, notebook: str, path: str, markdown: str) -> str:
        return self.post_json(
            "/api/filetree/createDocWithMd",
            {"notebook": notebook, "path": path, "markdown": markdown},
        )["data"]

    def get_ids_by_hpath(self, notebook: str, path: str) -> list[str]:
        return self.post_json(
            "/api/filetree/getIDsByHPath",
            {"notebook": notebook, "path": path},
        )["data"]

    def remove_doc_by_id(self, notebook: str, doc_id: str) -> None:
        self.post_json("/api/filetree/removeDocByID", {"notebook": notebook, "id": doc_id})

    def sql(self, stmt: str) -> list[dict]:
        return self.post_json("/api/query/sql", {"stmt": stmt})["data"]

    def update_block(self, block_id: str, markdown: str) -> None:
        self.post_json(
            "/api/block/updateBlock",
            {"dataType": "markdown", "data": markdown, "id": block_id},
        )

    def get_child_blocks(self, block_id: str) -> list[dict]:
        return self.post_json("/api/block/getChildBlocks", {"id": block_id})["data"]

    def delete_block(self, block_id: str) -> None:
        self.post_json("/api/block/deleteBlock", {"id": block_id})

    def append_block(self, parent_id: str, markdown: str) -> None:
        self.post_json(
            "/api/block/appendBlock",
            {"dataType": "markdown", "data": markdown, "parentID": parent_id},
        )

    def upload_asset(self, path: Path, upload_name: str) -> dict[str, str]:
        mime, _ = mimetypes.guess_type(path.name)
        with path.open("rb") as handle:
            response = self.session.post(
                f"{self.base_url}/api/asset/upload",
                files=[("file[]", (upload_name, handle, mime or "application/octet-stream"))],
                timeout=self.timeout,
            )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise SyncError(f"/api/asset/upload failed: {data}")
        return data["data"].get("succMap") or {}

    def render_attribute_view(self, av_id: str, block_id: str, create_if_not_exist: bool = True) -> dict:
        return self.post_json(
            "/api/av/renderAttributeView",
            {
                "id": av_id,
                "blockID": block_id,
                "createIfNotExist": create_if_not_exist,
                "page": 1,
                "pageSize": -1,
            },
        )["data"]

    def get_attribute_view(self, av_id: str) -> dict | None:
        return self.post_json("/api/av/getAttributeView", {"id": av_id})["data"].get("av")

    def add_attribute_view_key(self, av_id: str, key_id: str, key_name: str, key_type: str, previous_key_id: str) -> None:
        self.post_json(
            "/api/av/addAttributeViewKey",
            {
                "avID": av_id,
                "keyID": key_id,
                "keyName": key_name,
                "keyType": key_type,
                "keyIcon": "",
                "previousKeyID": previous_key_id,
            },
        )

    def remove_attribute_view_key(self, av_id: str, key_id: str) -> None:
        self.post_json(
            "/api/av/removeAttributeViewKey",
            {"avID": av_id, "keyID": key_id, "removeRelationDest": False},
        )

    def append_attribute_view_detached_blocks_with_values(self, av_id: str, blocks_values: list[list[dict]]) -> None:
        self.post_json(
            "/api/av/appendAttributeViewDetachedBlocksWithValues",
            {"avID": av_id, "blocksValues": blocks_values},
        )

    def set_attribute_view_block_attr(self, av_id: str, key_id: str, item_id: str, value: dict) -> None:
        self.post_json(
            "/api/av/setAttributeViewBlockAttr",
            {"avID": av_id, "keyID": key_id, "itemID": item_id, "value": value},
        )


class WolaiClient:
    def __init__(self, base_url: str, token: str, timeout: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "Authorization": token})

    def get_json(self, api: str, params: dict | None = None) -> dict:
        response = self.session.get(f"{self.base_url}{api}", params=params or {}, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and data.get("code") not in (None, 0):
            raise SyncError(f"{api} failed: {data}")
        return data

    @classmethod
    def from_app_credentials(cls, base_url: str, app_id: str, app_key: str, timeout: int = 60) -> "WolaiClient":
        session = requests.Session()
        response = session.post(
            f"{base_url.rstrip('/')}/v1/token",
            json={"appId": app_id, "appSecret": app_key},
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") not in (None, 0):
            raise SyncError(f"/v1/token failed: {data}")
        token = data.get("data", {}).get("app_token") or data.get("data", {}).get("token")
        if not token:
            raise SyncError(f"/v1/token returned no app token: {data}")
        return cls(base_url, token, timeout=timeout)

    def get_block(self, block_id: str) -> dict:
        return self.get_json(f"/v1/blocks/{block_id}")

    def get_block_children(self, block_id: str) -> list[dict]:
        data = self.get_json(f"/v1/blocks/{block_id}/children")
        if isinstance(data, dict):
            for key in ("data", "items", "blocks", "children", "result"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        if isinstance(data, list):
            return data
        return []


