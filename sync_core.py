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


from sync_clients import SiYuanClient, WolaiClient
from sync_models import *

def clean_title_text(name: str) -> str:
    name = html.unescape(name)
    name = MARKDOWN_ESCAPE_RE.sub(r"\1", name)
    name = strip_inline_markdown(name)
    return SPACE_RE.sub(" ", name).strip()


def clean_path_part(name: str) -> str:
    name = clean_title_text(name)
    name = INVALID_HPATH_CHARS_RE.sub("-", name.strip())
    name = SPACE_RE.sub(" ", name)
    return name or "未命名"


def strip_wolai_suffix(stem: str) -> str:
    match = WOLAI_SUFFIX_RE.match(stem)
    if not match:
        return stem
    return match.group("base")


def extract_wolai_block_id(source_rel: Path) -> str | None:
    stem = source_rel.stem
    match = WOLAI_SUFFIX_RE.match(stem)
    if not match:
        return None
    return match.group("tail")


def path_to_hpath(source_rel: Path) -> str:
    parts = list(source_rel.with_suffix("").parts)
    if parts and parts[0] == "pages":
        parts = parts[1:]
    cleaned = [clean_path_part(strip_wolai_suffix(part)) for part in parts]
    return TARGET_ROOT.rstrip("/") + "/" + "/".join(cleaned)


def title_to_hpath_part(title: str) -> str:
    return clean_path_part(strip_wolai_suffix(title))


def normalize_doc_key(value: str) -> str:
    unescaped = value.replace("\\#", "#")
    return title_to_hpath_part(unescaped)


def normalize_hpath_depth(hpath: str, summary: SyncSummary | None = None, source_rel: Path | None = None) -> str:
    parts = [part for part in hpath.split("/") if part]
    if MAX_HPATH_SEGMENTS <= 0 or len(parts) <= MAX_HPATH_SEGMENTS:
        return hpath
    kept = parts[: MAX_HPATH_SEGMENTS - 1]
    merged_tail = " ⟫ ".join(parts[MAX_HPATH_SEGMENTS - 1 :])
    kept.append(clean_path_part(merged_tail))
    normalized = "/" + "/".join(kept)
    if summary is not None and source_rel is not None:
        summary.unsupported_features.append(
            {
                "source_rel": source_rel.as_posix(),
                "reason": "hpath_depth_compressed",
                "original_hpath": hpath,
                "compressed_hpath": normalized,
            }
        )
    return normalized


def first_heading(markdown: str) -> str:
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return clean_title_text(stripped.lstrip("#")) or "未命名"
    return "未命名"


def compute_sha1(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_target(raw_target: str) -> tuple[str, str | None]:
    target = raw_target.strip()
    title_match = re.match(r'^(?P<path><[^>]+>|[^"\']+?\S)\s+(?:"[^"]*"|\'[^\']*\')\s*$', target)
    if title_match:
        target = title_match.group("path").strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1].strip()
    target = MARKDOWN_ESCAPE_RE.sub(r"\1", target)
    anchor = None
    if "#" in target:
        target, anchor = target.split("#", 1)
    return target.strip(), anchor


def normalize_wolai_wrapped_links(markdown: str) -> str:
    return WOLAI_WRAPPED_LINK_RE.sub(
        lambda match: f"{match.group(1)}[{SPACE_RE.sub(' ', match.group(2)).strip()}]({SPACE_RE.sub(' ', match.group(3)).strip()})",
        markdown,
    )


def inside_code_fence(markdown: str, position: int) -> bool:
    in_fence = False
    for match in re.finditer(r"(^|\n)```", markdown):
        if match.start() >= position:
            break
        in_fence = not in_fence
    return in_fence


def resolve_export_relative(source_rel: Path, target: str) -> Path | None:
    if not target:
        return None
    candidates: list[Path] = []
    for raw_candidate in (
        posixpath.normpath((PurePosixPath("/") / source_rel.parent.as_posix() / target).as_posix()),
        posixpath.normpath((PurePosixPath("/") / target).as_posix()),
    ):
        if raw_candidate in {".", "/"}:
            continue
        if raw_candidate.startswith("/"):
            raw_candidate = raw_candidate[1:]
        if raw_candidate.startswith("../"):
            continue
        candidates.append(Path(raw_candidate))
    if not candidates:
        return None
    target_posix = target.replace("\\", "/")
    if target_posix.startswith(("pages/", "image/", "file/", "video/")):
        target_root = target_posix.split("/", 1)[0]
        for candidate in candidates:
            if candidate.parts and candidate.parts[0] == target_root:
                return candidate
        for candidate in candidates:
            if candidate.parts and candidate.parts[0] in {"pages", "image", "file", "video"}:
                return candidate
    for candidate in candidates:
        if (MD_ROOT / candidate).exists():
            return candidate
    return candidates[0]


def classify_link(source_rel: Path, original_target: str, text: str, is_image: bool, wrapped_by_angle: bool, line_number: int | None) -> LinkRef:
    normalized_target, anchor = normalize_target(original_target)
    if not normalized_target:
        return LinkRef(
            source_rel=source_rel,
            original_target=original_target,
            normalized_target=normalized_target,
            kind="malformed",
            display_text=text,
            is_image=is_image,
            wrapped_by_angle=wrapped_by_angle,
            line_number=line_number,
            malformed_reason="empty_target",
        )

    resolved_rel = resolve_export_relative(source_rel, normalized_target)
    suffix = PurePosixPath(normalized_target).suffix.lower()
    if suffix == ".md":
        if resolved_rel is None:
            return LinkRef(
                source_rel=source_rel,
                original_target=original_target,
                normalized_target=normalized_target,
                kind="malformed",
                anchor=anchor,
                display_text=text,
                is_image=is_image,
                wrapped_by_angle=wrapped_by_angle,
                line_number=line_number,
                malformed_reason="unresolvable_doc_target",
            )
        return LinkRef(
            source_rel=source_rel,
            original_target=original_target,
            normalized_target=normalized_target,
            kind="doc",
            resolved_rel=resolved_rel,
            anchor=anchor,
            display_text=text,
            is_image=is_image,
            wrapped_by_angle=wrapped_by_angle,
            line_number=line_number,
        )

    if resolved_rel is not None:
        root = resolved_rel.parts[0] if resolved_rel.parts else ""
        if root in {"image", "file", "video"}:
            return LinkRef(
                source_rel=source_rel,
                original_target=original_target,
                normalized_target=normalized_target,
                kind=root,
                resolved_rel=resolved_rel,
                display_text=text,
                is_image=is_image,
                wrapped_by_angle=wrapped_by_angle,
                line_number=line_number,
            )

    return LinkRef(
        source_rel=source_rel,
        original_target=original_target,
        normalized_target=normalized_target,
        kind="external",
        resolved_rel=resolved_rel,
        anchor=anchor,
        display_text=text,
        is_image=is_image,
        wrapped_by_angle=wrapped_by_angle,
        line_number=line_number,
    )


def collect_links(markdown: str, source_rel: Path) -> list[LinkRef]:
    markdown = normalize_wolai_wrapped_links(markdown)
    links: list[LinkRef] = []
    occupied_spans: list[tuple[int, int]] = []
    for match in MARKDOWN_LINK_RE.finditer(markdown):
        if inside_code_fence(markdown, match.start()):
            continue
        occupied_spans.append(match.span())
        line_number = markdown[: match.start()].count("\n") + 1
        links.append(
            classify_link(
                source_rel=source_rel,
                original_target=match.group(3),
                text=match.group(2),
                is_image=match.group(1) == "!",
                wrapped_by_angle=False,
                line_number=line_number,
            )
        )

    for match in ANGLE_LINK_RE.finditer(markdown):
        if inside_code_fence(markdown, match.start()):
            continue
        if any(start <= match.start() < end for start, end in occupied_spans):
            continue
        target = match.group(1)
        if not any(marker in target for marker in (".md", "image/", "file/", "video/")):
            continue
        line_number = markdown[: match.start()].count("\n") + 1
        links.append(
            classify_link(
                source_rel=source_rel,
                original_target=target,
                text=Path(target.split("#", 1)[0]).stem,
                is_image=False,
                wrapped_by_angle=True,
                line_number=line_number,
            )
        )
    return links


def collect_table_shapes(markdown: str) -> list[TableShape]:
    shapes: list[TableShape] = []
    lines = markdown.splitlines()
    idx = 0
    while idx < len(lines) - 1:
        current = lines[idx]
        next_line = lines[idx + 1]
        if "|" in current and re.fullmatch(r"\s*\|?[\s:|\-]+\|?\s*", next_line):
            columns = max(len(split_table_row(current)), len(split_table_row(next_line)))
            shapes.append(TableShape(line_index=idx, columns=columns))
            idx += 2
            while idx < len(lines) and "|" in lines[idx]:
                idx += 1
            continue
        idx += 1
    return shapes


def split_table_row(line: str) -> list[str]:
    stripped = line.strip().strip("|")
    return [cell.strip() for cell in stripped.split("|")] if stripped else []


def extract_first_table(markdown: str) -> tuple[list[str], list[list[str]], tuple[int, int]] | None:
    lines = markdown.splitlines()
    idx = 0
    while idx < len(lines) - 1:
        current = lines[idx]
        next_line = lines[idx + 1]
        if "|" in current and re.fullmatch(r"\s*\|?[\s:|\-]+\|?\s*", next_line):
            header = split_table_row(current)
            rows: list[list[str]] = []
            end = idx + 2
            while end < len(lines) and "|" in lines[end]:
                rows.append(split_table_row(lines[end]))
                end += 1
            return header, rows, (idx, end)
        idx += 1
    return None


def strip_inline_markdown(text: str) -> str:
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    return text.strip()


def parse_cell_link(page: PageEntry, raw_cell: str) -> LinkRef | None:
    match = MARKDOWN_LINK_RE.search(raw_cell)
    if match:
        return classify_link(
            source_rel=page.source_rel,
            original_target=match.group(3),
            text=match.group(2),
            is_image=match.group(1) == "!",
            wrapped_by_angle=False,
            line_number=None,
        )
    match = ANGLE_LINK_RE.search(raw_cell)
    if match and any(marker in match.group(1) for marker in (".md", "image/", "file/", "video/")):
        return classify_link(
            source_rel=page.source_rel,
            original_target=match.group(1),
            text=Path(match.group(1).split("#", 1)[0]).stem,
            is_image=False,
            wrapped_by_angle=True,
            line_number=None,
        )
    return None


def parse_date_text(raw: str) -> str | None:
    value = strip_inline_markdown(raw)
    if not value:
        return None
    normalized = value.replace(".", "-").replace("/", "-")
    try:
        parsed = dt.datetime.strptime(normalized, "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.strftime("%Y/%m/%d")


def is_substantive_non_table_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("#"):
        return False
    if stripped in {"***", "---"}:
        return False
    return True


def stable_node_id(seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    numeric = str(int(digest[:14], 16))
    base = numeric[:14].rjust(14, "2")
    suffix = digest[14:21]
    return f"{base}-{suffix}"


def infer_database_column_type(column_name: str, cells: list[DatabaseCell], is_primary: bool) -> str:
    if is_primary:
        return "block"
    non_empty = [cell for cell in cells if cell.text_value or cell.asset_rel or cell.date_value or cell.doc_rel]
    if not non_empty:
        return "text"
    if all(cell.asset_rel for cell in non_empty):
        return "mAsset"
    if all(cell.date_value for cell in non_empty):
        return "date"
    if all((cell.text_value and not cell.doc_rel and not cell.asset_rel and not cell.date_value) for cell in non_empty):
        distinct = {cell.text_value for cell in non_empty if cell.text_value}
        selectish_name = any(token in column_name.lower() for token in ("类型", "状态", "标签", "分类"))
        if selectish_name and 0 < len(distinct) <= 12 and all(len(item) <= 16 for item in distinct):
            return "mSelect"
    return "text"


def detect_database_table_candidate(page: PageEntry, manifest: Manifest) -> DatabaseTablePlan | None:
    extracted = extract_first_table(page.markdown)
    if not extracted:
        return None
    header, raw_rows, (start, end) = extracted
    if not header or not raw_rows:
        return None
    if len(raw_rows) < 2 and len(header) < 3:
        return None
    lines = page.markdown.splitlines()
    if any(is_substantive_non_table_line(line) for line in lines[:start] if not line.strip().startswith("#")):
        return None
    if any(is_substantive_non_table_line(line) for line in lines[end:]):
        return None

    normalized_rows: list[list[str]] = []
    for raw_row in raw_rows:
        row = list(raw_row[: len(header)])
        while len(row) < len(header):
            row.append("")
        normalized_rows.append(row)

    rows: list[DatabaseRow] = []
    column_cells: list[list[DatabaseCell]] = [[] for _ in header]
    for raw_row in normalized_rows:
        cells: list[DatabaseCell] = []
        primary_text = strip_inline_markdown(raw_row[0])
        primary_doc_rel: Path | None = None
        for idx, raw_cell in enumerate(raw_row):
            link = parse_cell_link(page, raw_cell)
            cell = DatabaseCell(raw_markdown=raw_cell)
            if link and link.kind == "doc" and link.resolved_rel is not None:
                cell.doc_rel = link.resolved_rel
                cell.doc_label = link.display_text or strip_inline_markdown(raw_cell)
                if idx == 0:
                    primary_doc_rel = link.resolved_rel
                    primary_text = cell.doc_label or primary_text
                else:
                    cell.text_value = cell.doc_label
            elif link and link.kind in {"image", "file", "video"} and link.resolved_rel is not None:
                cell.asset_rel = link.resolved_rel
                cell.asset_name = link.display_text or Path(link.normalized_target).name
            else:
                parsed_date = parse_date_text(raw_cell)
                if parsed_date:
                    cell.date_value = parsed_date
                else:
                    stripped = strip_inline_markdown(raw_cell)
                    cell.text_value = stripped or None
            cells.append(cell)
            column_cells[idx].append(cell)
        if not primary_text:
            return None
        rows.append(DatabaseRow(primary_text=primary_text, primary_doc_rel=primary_doc_rel, cells=cells))

    columns: list[DatabaseColumnPlan] = []
    for idx, name in enumerate(header):
        clean_name = clean_path_part(name)
        columns.append(
            DatabaseColumnPlan(
                index=idx,
                name=clean_name,
                key_type=infer_database_column_type(clean_name, column_cells[idx], is_primary=idx == 0),
            )
        )
    return DatabaseTablePlan(av_id=stable_node_id(f"{page.target_hpath}#av"), columns=columns, rows=rows)


def parse_standalone_markdown_link_line(line: str) -> tuple[str, str] | None:
    if not line.startswith("[") or "](" not in line or not line.endswith(")"):
        return None
    split_at = line.find("](")
    label = line[1:split_at]
    target = line[split_at + 2 : -1]
    return label, target


def find_unique_page_by_title(manifest: Manifest, title: str) -> PageEntry | None:
    target_key = normalize_doc_key(title)
    matches = [page for page in manifest.pages.values() if normalize_doc_key(page.title) == target_key]
    if len(matches) == 1:
        return matches[0]
    matches = [page for page in manifest.pages.values() if page.title == title]
    if len(matches) == 1:
        return matches[0]
    return None


def resolve_page_from_link(link: LinkRef, manifest: Manifest) -> PageEntry | None:
    if link.kind == "doc" and link.resolved_rel is not None:
        page = manifest.pages.get(link.resolved_rel)
        if page is not None:
            return page

    candidates: list[str] = []
    if link.display_text:
        candidates.append(link.display_text)
    if link.normalized_target:
        candidates.append(Path(link.normalized_target.split("#", 1)[0]).stem)
        candidates.append(link.normalized_target)

    for candidate in candidates:
        page = find_unique_page_by_title(manifest, candidate)
        if page is not None:
            return page
    return None


def extract_doc_links_from_table_row(row: str, source_rel: Path, manifest: Manifest) -> list[Path]:
    structural: list[Path] = []
    for cell in split_table_row(row):
        link = parse_cell_link(
            PageEntry(
                source_rel=source_rel,
                abs_path=Path(),
                title="",
                target_hpath="",
                markdown="",
            ),
            cell,
        )
        if link:
            page = resolve_page_from_link(link, manifest)
            if page is not None:
                structural.append(page.source_rel)
    return structural


def extract_structural_doc_links(markdown: str, source_rel: Path, manifest: Manifest) -> list[Path]:
    markdown = normalize_wolai_wrapped_links(markdown)
    structural: list[Path] = []
    for raw_line in markdown.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped in {"***", "---"}:
            continue
        candidate = re.sub(r"^(?:[-*+]\s+|\d+\.\s+)", "", stripped)
        parsed = parse_standalone_markdown_link_line(candidate)
        if parsed:
            label, target = parsed
            link = classify_link(
                source_rel=source_rel,
                original_target=target,
                text=label,
                is_image=False,
                wrapped_by_angle=False,
                line_number=None,
            )
            page = resolve_page_from_link(link, manifest)
            if page is not None:
                structural.append(page.source_rel)
            continue
        if "|" in candidate:
            structural.extend(extract_doc_links_from_table_row(candidate, source_rel, manifest))
            continue
        angle = ANGLE_LINK_RE.fullmatch(candidate)
        if angle:
            link = classify_link(
                source_rel=source_rel,
                original_target=angle.group(1),
                text=Path(angle.group(1)).stem,
                is_image=False,
                wrapped_by_angle=True,
                line_number=None,
            )
            page = resolve_page_from_link(link, manifest)
            if page is not None:
                structural.append(page.source_rel)
    return structural


def build_navigation_parents(manifest: Manifest) -> dict[Path, Path]:
    parents: dict[Path, Path] = {}
    root_rel = Path("bzs.md")
    if root_rel not in manifest.pages:
        return parents
    queue: list[Path] = [root_rel]
    visited: set[Path] = set()
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        current_page = manifest.pages[current]
        for child_rel in extract_structural_doc_links(current_page.markdown, current_page.source_rel, manifest):
            if child_rel == current or child_rel not in manifest.pages:
                continue
            if child_rel not in parents:
                parents[child_rel] = current
            queue.append(child_rel)
    return parents


def build_wolai_api_parents(client: WolaiClient, manifest: Manifest) -> dict[Path, Path]:
    parents: dict[Path, Path] = {}
    block_to_rel: dict[str, Path] = {}
    for page in manifest.pages.values():
        block_id = extract_wolai_block_id(page.source_rel)
        if block_id:
            block_to_rel[block_id] = page.source_rel

    for block_id, source_rel in block_to_rel.items():
        try:
            data = client.get_block(block_id)
        except Exception:  # noqa: BLE001
            continue
        block = data.get("data") if isinstance(data, dict) else None
        if not isinstance(block, dict):
            continue
        parent_id = block.get("parent_id")
        if not isinstance(parent_id, str) or not parent_id:
            continue
        parent_rel = block_to_rel.get(parent_id)
        if parent_rel is None:
            continue
        if source_rel != parent_rel:
            parents[source_rel] = parent_rel
    return parents


def assign_navigation_hpaths(manifest: Manifest, parents: dict[Path, Path], summary: SyncSummary) -> None:
    manifest.pages_by_hpath.clear()
    used_hpaths: set[str] = set()
    root_rel = Path("bzs.md")

    def lineage(page_rel: Path) -> list[Path]:
        chain: list[Path] = [page_rel]
        current = page_rel
        seen: set[Path] = set()
        while current in parents:
            parent_rel = parents[current]
            if parent_rel in seen or parent_rel == root_rel:
                break
            seen.add(parent_rel)
            chain.append(parent_rel)
            current = parent_rel
        return list(reversed(chain))

    for page_rel, page in manifest.pages.items():
        if page_rel == root_rel:
            page.target_hpath = normalize_hpath_depth(
                TARGET_ROOT.rstrip("/") + "/bzs",
                summary=summary,
                source_rel=page.source_rel,
            )
            manifest.pages_by_hpath[page.target_hpath] = page
            used_hpaths.add(page.target_hpath)
            continue
        if page_rel in parents:
            chain = lineage(page_rel)
            parts = [title_to_hpath_part(manifest.pages[item].title) for item in chain]
            base_hpath = TARGET_ROOT.rstrip("/") + "/" + "/".join(parts)
        else:
            base_hpath = TARGET_ROOT.rstrip("/") + "/" + title_to_hpath_part(page.title)
        target_hpath = normalize_hpath_depth(base_hpath, summary=summary, source_rel=page.source_rel)
        suffix = 1
        while target_hpath in used_hpaths:
            suffix += 1
            target_hpath = normalize_hpath_depth(
                f"{base_hpath}-{suffix}",
                summary=summary,
                source_rel=page.source_rel,
            )
        if target_hpath != base_hpath:
            summary.path_collisions.append({"source_rel": page.source_rel.as_posix(), "hpath": target_hpath})
        page.target_hpath = target_hpath
        used_hpaths.add(target_hpath)
        manifest.pages_by_hpath[target_hpath] = page


def build_manifest(md_root: Path, summary: SyncSummary) -> Manifest:
    manifest = Manifest()
    asset_digests: dict[str, AssetEntry] = {}
    for md_file in sorted(md_root.rglob("*.md")):
        source_rel = md_file.relative_to(md_root)
        markdown = md_file.read_text(encoding="utf-8")
        page = PageEntry(
            source_rel=source_rel,
            abs_path=md_file,
            title=first_heading(markdown) if source_rel.name != "bzs.md" else "bzs",
            target_hpath="",
            markdown=markdown,
            links=collect_links(markdown, source_rel),
            table_shapes=collect_table_shapes(markdown),
        )
        for link in page.links:
            if link.kind == "malformed":
                summary.unresolved_malformed.append(
                    {
                        "source_rel": source_rel.as_posix(),
                        "target": link.original_target,
                        "reason": link.malformed_reason or "unknown",
                    }
        )
        manifest.pages[source_rel] = page

    for asset_file in sorted(md_root.rglob("*")):
        if not asset_file.is_file():
            continue
        source_rel = asset_file.relative_to(md_root)
        if source_rel.suffix.lower() == ".md":
            continue
        if not source_rel.parts or source_rel.parts[0] not in {"image", "file", "video"}:
            continue
        digest = compute_sha1(asset_file)
        if digest in asset_digests:
            manifest.assets[source_rel] = asset_digests[digest]
            continue
        upload_name = f"{source_rel.parts[0]}-{digest[:12]}{asset_file.suffix.lower()}"
        entry = AssetEntry(source_rel=source_rel, abs_path=asset_file, digest=digest, upload_name=upload_name)
        asset_digests[digest] = entry
        manifest.assets[source_rel] = entry
    parents = build_navigation_parents(manifest)
    if WOLAI_TOKEN or (WOLAI_APP_ID and WOLAI_APP_KEY):
        try:
            wolai_client = (
                WolaiClient(WOLAI_API_BASE, WOLAI_TOKEN)
                if WOLAI_TOKEN
                else WolaiClient.from_app_credentials(WOLAI_API_BASE, WOLAI_APP_ID, WOLAI_APP_KEY)
            )
            parents.update(build_wolai_api_parents(wolai_client, manifest))
        except Exception as exc:  # noqa: BLE001
            summary.unsupported_features.append({"reason": "wolai_api_parent_discovery_failed", "error": str(exc)})
    assign_navigation_hpaths(manifest, parents, summary)
    for page in manifest.pages.values():
        page.database_plan = detect_database_table_candidate(page, manifest)
    return manifest


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


def discover_target_docs(client: SiYuanClient, notebook_id: str, target_root: str) -> list[dict]:
    root = sql_quote(target_root.rstrip("/"))
    notebook = sql_quote(notebook_id)
    stmt = (
        "select id, hpath from blocks "
        f"where box = '{notebook}' and type = 'd' and (hpath = '{root}' or hpath like '{root}/%') "
        "order by hpath asc, id asc"
    )
    return client.sql(stmt)


def discover_exact_doc_ids(client: SiYuanClient, notebook_id: str, hpath: str) -> list[str]:
    notebook = sql_quote(notebook_id)
    hpath_q = sql_quote(hpath)
    stmt = (
        "select id from blocks "
        f"where box = '{notebook}' and type = 'd' and hpath = '{hpath_q}' "
        "order by id asc"
    )
    return [row["id"] for row in client.sql(stmt)]


def safe_cleanup_target_root(
    client: SiYuanClient,
    notebook_id: str,
    target_root: str,
    summary: SyncSummary,
    desired_hpaths: set[str],
    enabled: bool = True,
) -> dict[str, str]:
    docs = discover_target_docs(client, notebook_id, target_root)
    existing_kept: dict[str, str] = {}
    delete_candidates: list[dict] = []
    for doc in docs:
        hpath = doc["hpath"]
        if hpath in desired_hpaths:
            if hpath not in existing_kept:
                existing_kept[hpath] = doc["id"]
                continue
            continue
        if hpath == target_root:
            continue
        delete_candidates.append(doc)
    for doc in docs:
        hpath = doc["hpath"]
        if hpath in desired_hpaths and existing_kept.get(hpath) != doc["id"]:
            delete_candidates.append(doc)
    summary.candidates_to_delete = [{"doc_id": doc["id"], "hpath": doc["hpath"]} for doc in delete_candidates]
    if not enabled:
        return existing_kept
    for doc in delete_candidates:
        hpath = doc["hpath"]
        if not (hpath == target_root or hpath.startswith(target_root.rstrip("/") + "/")):
            raise SyncError(f"Refusing to delete out-of-bound doc: {hpath}")
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                client.remove_doc_by_id(notebook_id, doc["id"])
                summary.deleted_docs.append(doc["id"])
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if "indexing" in str(exc).lower() and attempt < 2:
                    time.sleep(1.5)
                    continue
                break
        if last_error is not None:
            summary.failed_deletes.append({"doc_id": doc["id"], "hpath": hpath, "error": str(last_error)})
    return existing_kept


def has_desired_ancestor(hpath: str, desired_hpaths: set[str]) -> bool:
    parts = [part for part in hpath.split("/") if part]
    for index in range(1, len(parts)):
        ancestor = "/" + "/".join(parts[:index])
        if ancestor in desired_hpaths:
            return True
    return False


def safe_cleanup_existing_desired_docs(
    client: SiYuanClient,
    notebook_id: str,
    target_root: str,
    summary: SyncSummary,
    desired_hpaths: set[str],
    enabled: bool = True,
) -> dict[str, str]:
    """Replace only existing docs whose hpaths are owned by this Wolai manifest."""
    docs = discover_target_docs(client, notebook_id, target_root)
    delete_candidates = [
        doc
        for doc in docs
        if doc["hpath"] in desired_hpaths and not has_desired_ancestor(doc["hpath"], desired_hpaths)
    ]
    delete_candidates.sort(key=lambda doc: (len(PurePosixPath(doc["hpath"]).parts), doc["hpath"]))
    summary.candidates_to_delete = [{"doc_id": doc["id"], "hpath": doc["hpath"]} for doc in delete_candidates]
    if not enabled:
        return {doc["hpath"]: doc["id"] for doc in docs if doc["hpath"] in desired_hpaths}
    for doc in delete_candidates:
        hpath = doc["hpath"]
        if target_root != "/" and not (hpath == target_root or hpath.startswith(target_root.rstrip("/") + "/")):
            raise SyncError(f"Refusing to delete out-of-bound doc: {hpath}")
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                client.remove_doc_by_id(notebook_id, doc["id"])
                summary.deleted_docs.append(doc["id"])
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if "indexing" in str(exc).lower() and attempt < 2:
                    time.sleep(1.5)
                    continue
                break
        if last_error is not None:
            summary.failed_deletes.append({"doc_id": doc["id"], "hpath": hpath, "error": str(last_error)})
    return {}


def create_docs(
    client: SiYuanClient,
    notebook_id: str,
    manifest: Manifest,
    summary: SyncSummary,
    existing_doc_ids: dict[str, str],
    placeholder_markdown: str = "# Placeholder\n",
) -> None:
    pages_in_creation_order = sorted(
        manifest.pages.values(),
        key=lambda page: (len(PurePosixPath(page.target_hpath).parts), page.target_hpath),
    )
    for page in pages_in_creation_order:
        doc_id = existing_doc_ids.get(page.target_hpath)
        if doc_id is None:
            exact_ids = discover_exact_doc_ids(client, notebook_id, page.target_hpath)
            if exact_ids:
                doc_id = exact_ids[0]
            else:
                doc_id = client.create_doc_with_md(notebook_id, page.target_hpath, placeholder_markdown)
                summary.created_docs.append({"source_rel": page.source_rel.as_posix(), "doc_id": doc_id, "hpath": page.target_hpath})
            existing_doc_ids[page.target_hpath] = doc_id
        page.doc_id = doc_id
        page.root_id = doc_id
        summary.doc_id_map[page.source_rel.as_posix()] = doc_id


def upload_assets(client: SiYuanClient, manifest: Manifest, summary: SyncSummary) -> None:
    uploaded_by_digest: dict[str, str] = {}
    for source_rel, entry in manifest.assets.items():
        if entry.digest in uploaded_by_digest:
            entry.target_path = uploaded_by_digest[entry.digest]
            continue
        try:
            succ_map = client.upload_asset(entry.abs_path, entry.upload_name)
        except Exception as exc:  # noqa: BLE001
            summary.upload_failed.append({"source_rel": source_rel.as_posix(), "error": str(exc)})
            continue
        if entry.upload_name not in succ_map:
            summary.upload_failed.append(
                {"source_rel": source_rel.as_posix(), "error": f"succMap missing key {entry.upload_name}", "succMap": json.dumps(succ_map, ensure_ascii=False)}
            )
            continue
        entry.target_path = succ_map[entry.upload_name]
        uploaded_by_digest[entry.digest] = entry.target_path
        summary.uploaded_assets.append({"source_rel": source_rel.as_posix(), "target_path": entry.target_path})


def rewrite_markdown(markdown: str, page: PageEntry, manifest: Manifest, summary: SyncSummary) -> str:
    markdown = normalize_wolai_wrapped_links(markdown)
    replacements: list[tuple[int, int, str]] = []
    for match in MARKDOWN_LINK_RE.finditer(markdown):
        if inside_code_fence(markdown, match.start()):
            continue
        link = classify_link(
            source_rel=page.source_rel,
            original_target=match.group(3),
            text=match.group(2),
            is_image=match.group(1) == "!",
            wrapped_by_angle=False,
            line_number=markdown[: match.start()].count("\n") + 1,
        )
        replacement = rewrite_link(link, manifest, summary)
        if replacement is not None:
            replacements.append((match.start(), match.end(), replacement))
    for match in ANGLE_LINK_RE.finditer(markdown):
        if inside_code_fence(markdown, match.start()):
            continue
        target = match.group(1)
        if not any(marker in target for marker in (".md", "image/", "file/", "video/")):
            continue
        if any(start <= match.start() < end for start, end, _ in replacements):
            continue
        link = classify_link(
            source_rel=page.source_rel,
            original_target=target,
            text=Path(target.split("#", 1)[0]).stem,
            is_image=False,
            wrapped_by_angle=True,
            line_number=markdown[: match.start()].count("\n") + 1,
        )
        replacement = rewrite_link(link, manifest, summary)
        if replacement is not None:
            replacements.append((match.start(), match.end(), replacement))

    if not replacements:
        return markdown
    replacements.sort(key=lambda item: item[0])
    out: list[str] = []
    cursor = 0
    for start, end, replacement in replacements:
        out.append(markdown[cursor:start])
        out.append(replacement)
        cursor = end
    out.append(markdown[cursor:])
    return "".join(out)


def rewrite_link(link: LinkRef, manifest: Manifest, summary: SyncSummary) -> str | None:
    if link.kind == "doc":
        if link.anchor:
            summary.unsupported_features.append(
                {
                    "source_rel": link.source_rel.as_posix(),
                    "target": link.original_target,
                    "reason": "doc_anchor_dropped",
                }
            )
        if not link.resolved_rel or link.resolved_rel not in manifest.pages:
            summary.unresolved_doc_links.append(
                {"source_rel": link.source_rel.as_posix(), "target": link.original_target}
            )
            return None
        target_doc = manifest.pages[link.resolved_rel]
        if not target_doc.doc_id:
            summary.unresolved_doc_links.append(
                {"source_rel": link.source_rel.as_posix(), "target": link.original_target, "reason": "missing_doc_id"}
            )
            return None
        label = link.display_text or target_doc.title
        summary.rewritten_doc_refs += 1
        return f"[{label}](siyuan://blocks/{target_doc.doc_id})"

    if link.kind == "external":
        target_doc = resolve_page_from_link(link, manifest)
        if target_doc and target_doc.doc_id and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", link.normalized_target):
            label = link.display_text or target_doc.title
            summary.rewritten_doc_refs += 1
            return f"[{label}](siyuan://blocks/{target_doc.doc_id})"

    if link.kind in {"image", "file", "video"}:
        if not link.resolved_rel or link.resolved_rel not in manifest.assets:
            summary.missing_assets.append(
                {"source_rel": link.source_rel.as_posix(), "target": link.original_target}
            )
            return None
        asset_entry = manifest.assets[link.resolved_rel]
        if not asset_entry.target_path:
            summary.missing_assets.append(
                {
                    "source_rel": link.source_rel.as_posix(),
                    "target": link.original_target,
                    "reason": "upload_missing",
                }
            )
            return None
        summary.rewritten_asset_refs += 1
        summary.succmap_consumed += 1
        label = link.display_text or Path(link.normalized_target).name
        if link.kind == "image" or link.is_image:
            return f"![{label}]({asset_entry.target_path})"
        return f"[{label}]({asset_entry.target_path})"

    if link.kind == "malformed":
        return None

    return None


def verify_table_shapes(before: str, after: str) -> list[str]:
    before_shapes = collect_table_shapes(before)
    after_shapes = collect_table_shapes(after)
    errors: list[str] = []
    if len(before_shapes) != len(after_shapes):
        errors.append("table_count_changed")
        return errors
    for idx, (left, right) in enumerate(zip(before_shapes, after_shapes, strict=True)):
        if left.columns != right.columns:
            errors.append(f"table_{idx}_column_count_changed")
    return errors


def discover_doc_root_id(
    client: SiYuanClient,
    notebook_id: str,
    hpath: str,
    retries: int = 3,
    retry_delay: float = 0.5,
) -> str:
    last_hpath_ids: list[str] = []
    last_sql_ids: list[str] = []
    for attempt in range(retries):
        last_hpath_ids = client.get_ids_by_hpath(notebook_id, hpath)
        if last_hpath_ids:
            return last_hpath_ids[0]
        last_sql_ids = discover_exact_doc_ids(client, notebook_id, hpath)
        if last_sql_ids:
            return last_sql_ids[0]
        if attempt < retries - 1:
            time.sleep(retry_delay)
    raise SyncError(
        f"No doc ID found for hpath {hpath}; "
        f"getIDsByHPath={last_hpath_ids!r}, sql_exact={last_sql_ids!r}"
    )


def discover_attribute_view_block_id(client: SiYuanClient, root_id: str, av_id: str, retries: int = 6) -> str:
    av_markdown = f'<div data-type="NodeAttributeView" data-av-id="{av_id}" data-av-type="table"></div>'
    stmt = (
        "select id from blocks "
        f"where root_id = '{sql_quote(root_id)}' and type = 'av' and markdown = '{sql_quote(av_markdown)}' "
        "order by id asc"
    )
    last_rows: list[dict] = []
    for _ in range(retries):
        last_rows = client.sql(stmt)
        if last_rows:
            return last_rows[0]["id"]
        time.sleep(1)
    raise SyncError(f"Attribute view block not found for root {root_id} / av {av_id}: {last_rows}")


def date_string_to_millis(value: str) -> int:
    parsed = dt.datetime.strptime(value.replace("/", "-"), "%Y-%m-%d")
    return int(parsed.timestamp() * 1000)


def build_database_row_payload(row: DatabaseRow, plan: DatabaseTablePlan, manifest: Manifest) -> list[dict]:
    payload: list[dict] = []
    for column, cell in zip(plan.columns, row.cells, strict=True):
        assert column.key_id
        if column.key_type == "block":
            payload.append({"keyID": column.key_id, "block": {"content": row.primary_text}})
        elif column.key_type == "mAsset":
            if not cell.asset_rel or cell.asset_rel not in manifest.assets:
                continue
            asset_entry = manifest.assets[cell.asset_rel]
            if not asset_entry.target_path:
                continue
            asset_type = "image" if cell.asset_rel.parts[0] == "image" else "file"
            payload.append(
                {
                    "keyID": column.key_id,
                    "mAsset": [
                        {
                            "type": asset_type,
                            "name": cell.asset_name or Path(asset_entry.target_path).name,
                            "content": asset_entry.target_path,
                        }
                    ],
                }
            )
        elif column.key_type == "date":
            if not cell.date_value:
                continue
            millis = date_string_to_millis(cell.date_value)
            payload.append(
                {
                    "keyID": column.key_id,
                    "date": {
                        "content": millis,
                        "isNotEmpty": True,
                        "isNotTime": True,
                        "hasEndDate": False,
                        "formattedContent": cell.date_value.replace("/", "-"),
                    },
                }
            )
        elif column.key_type == "mSelect":
            if not cell.text_value:
                continue
            payload.append(
                {"keyID": column.key_id, "mSelect": [{"content": cell.text_value, "color": "1"}]}
            )
        else:
            text_value = cell.text_value or ""
            if not text_value:
                continue
            payload.append({"keyID": column.key_id, "text": {"content": text_value}})
    return payload


def extract_attribute_view_row_ids(rendered_or_av_data: dict) -> list[str]:
    view = rendered_or_av_data.get("view")
    if isinstance(view, dict) and isinstance(view.get("rows"), list):
        return [row["id"] for row in view["rows"] if isinstance(row, dict) and row.get("id")]
    for view_item in rendered_or_av_data.get("views", []):
        table = view_item.get("table")
        if isinstance(table, dict) and table.get("rowIds"):
            return list(table["rowIds"])
        if view_item.get("itemIds"):
            return list(view_item["itemIds"])
    return []


def configure_database_for_page(client: SiYuanClient, page: PageEntry, manifest: Manifest, summary: SyncSummary) -> None:
    if not page.database_plan or not page.root_id:
        return
    plan = page.database_plan
    av_block_id = discover_attribute_view_block_id(client, page.root_id, plan.av_id)
    render_data = client.render_attribute_view(plan.av_id, av_block_id, create_if_not_exist=True)
    default_columns = render_data["view"]["columns"]
    if len(default_columns) < 2:
        raise SyncError(f"Unexpected default AV columns for {page.source_rel}")
    block_key_id = default_columns[0]["id"]
    default_select_key_id = default_columns[1]["id"]
    plan.columns[0].key_id = block_key_id

    previous_key_id = default_select_key_id
    created_key_ids: list[str] = []
    for column in plan.columns[1:]:
        column.key_id = stable_node_id(f"{page.target_hpath}#{column.index}#{column.name}#{column.key_type}")
        client.add_attribute_view_key(plan.av_id, column.key_id, column.name, column.key_type, previous_key_id)
        created_key_ids.append(column.key_id)
        previous_key_id = column.key_id
    client.remove_attribute_view_key(plan.av_id, default_select_key_id)

    blocks_values = [build_database_row_payload(row, plan, manifest) for row in plan.rows]
    blocks_values = [row for row in blocks_values if row]
    if blocks_values:
        client.append_attribute_view_detached_blocks_with_values(plan.av_id, blocks_values)

    rendered_after_append = client.render_attribute_view(plan.av_id, av_block_id, create_if_not_exist=False)
    row_ids = extract_attribute_view_row_ids(rendered_after_append)
    if len(row_ids) != len(plan.rows):
        summary.unsupported_features.append(
            {
                "source_rel": page.source_rel.as_posix(),
                "reason": "database_row_count_mismatch_after_append",
                "expected": str(len(plan.rows)),
                "actual": str(len(row_ids)),
            }
        )
        return
    for row_id, row in zip(row_ids, plan.rows, strict=True):
        if not row.primary_doc_rel or row.primary_doc_rel not in manifest.pages:
            continue
        target_page = manifest.pages[row.primary_doc_rel]
        if not target_page.doc_id:
            continue
        try:
            client.set_attribute_view_block_attr(
                plan.av_id,
                block_key_id,
                row_id,
                {"isDetached": False, "block": {"id": target_page.doc_id, "content": row.primary_text}},
            )
        except Exception as exc:  # noqa: BLE001
            summary.unsupported_features.append(
                {
                    "source_rel": page.source_rel.as_posix(),
                    "target": row.primary_doc_rel.as_posix(),
                    "reason": f"database_primary_binding_failed:{exc}",
                }
            )


def probe_writeback_mode(client: SiYuanClient, notebook_id: str) -> str:
    probe_root = "/__omx_probe__/wolai_sync_ralph_probe"
    created_id = client.create_doc_with_md(notebook_id, probe_root, "# Probe\n\nfirst")
    try:
        try:
            client.update_block(created_id, "# Probe\n\nsecond")
            children = client.get_child_blocks(created_id)
            if not children or children[1]["markdown"] != "second":
                raise SyncError("updateBlock probe did not rewrite expected content")
            return "updateBlock"
        except Exception:
            children = client.get_child_blocks(created_id)
            for child in children:
                client.delete_block(child["id"])
            client.append_block(created_id, "# Probe\n\nfallback")
            fallback_children = client.get_child_blocks(created_id)
            if not fallback_children or fallback_children[1]["markdown"] != "fallback":
                raise SyncError("appendBlock fallback probe did not rewrite expected content")
            return "appendBlock"
    finally:
        try:
            client.remove_doc_by_id(notebook_id, created_id)
        except Exception:
            time.sleep(1.5)
            client.remove_doc_by_id(notebook_id, created_id)


def preflight(client: SiYuanClient, notebook_id: str, summary: SyncSummary) -> str:
    notebooks = client.list_notebooks()
    if not any(item["id"] == notebook_id for item in notebooks):
        raise SyncError(f"Notebook not found: {notebook_id}")
    mode = probe_writeback_mode(client, notebook_id)
    summary.preflight = {
        "notebook_found": True,
        "writeback_mode": mode,
        "target_root": TARGET_ROOT,
    }
    summary.writeback_mode = mode
    return mode


def write_back_page(client: SiYuanClient, page: PageEntry, markdown: str, mode: str) -> None:
    if not page.root_id:
        raise SyncError(f"Page {page.source_rel} has no root_id")
    if mode == "updateBlock":
        client.update_block(page.root_id, markdown)
        return
    child_blocks = client.get_child_blocks(page.root_id)
    for child in child_blocks:
        client.delete_block(child["id"])
    client.append_block(page.root_id, markdown)


def sync_all(
    client: SiYuanClient,
    md_root: Path,
    notebook_id: str,
    target_root: str,
    dry_run: bool,
    skip_cleanup: bool,
    cleanup_mode: str = "target-root",
) -> SyncSummary:
    global TARGET_ROOT
    TARGET_ROOT = target_root
    summary = SyncSummary()
    writeback_mode = preflight(client, notebook_id, summary)
    manifest = build_manifest(md_root, summary)
    if dry_run:
        summary.doc_id_map = {page.source_rel.as_posix(): page.target_hpath for page in manifest.pages.values()}
        return summary

    desired_hpaths = {page.target_hpath for page in manifest.pages.values()}
    if cleanup_mode == "desired-existing":
        existing_doc_ids = safe_cleanup_existing_desired_docs(
            client, notebook_id, target_root, summary, desired_hpaths=desired_hpaths, enabled=not skip_cleanup
        )
    else:
        existing_doc_ids = safe_cleanup_target_root(
            client, notebook_id, target_root, summary, desired_hpaths=desired_hpaths, enabled=not skip_cleanup
        )
    create_docs(client, notebook_id, manifest, summary, existing_doc_ids=existing_doc_ids)
    upload_assets(client, manifest, summary)

    for page in manifest.pages.values():
        page.root_id = discover_doc_root_id(client, notebook_id, page.target_hpath)
        if page.database_plan:
            page.database_plan.av_id = stable_node_id(f"{page.root_id}#av")
        if page.database_plan:
            rewritten = f'# {page.title}\n\n<div data-type="NodeAttributeView" data-av-id="{page.database_plan.av_id}" data-av-type="table"></div>\n'
        else:
            rewritten = rewrite_markdown(page.markdown, page, manifest, summary)
            shape_errors = verify_table_shapes(page.markdown, rewritten)
            for error in shape_errors:
                summary.unsupported_features.append({"source_rel": page.source_rel.as_posix(), "reason": error})
        write_back_page(client, page, rewritten, writeback_mode)
        refreshed_id = discover_doc_root_id(client, notebook_id, page.target_hpath)
        if refreshed_id != page.root_id:
            raise SyncError(f"Doc root ID changed for {page.source_rel}: {page.root_id} -> {refreshed_id}")
        if page.database_plan:
            configure_database_for_page(client, page, manifest, summary)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Wolai markdown export into SiYuan.")
    parser.add_argument("--source-root", type=Path, default=MD_ROOT)
    parser.add_argument("--target-root", default=TARGET_ROOT)
    parser.add_argument("--notebook-id", default=NOTEBOOK_ID)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-cleanup", action="store_true")
    parser.add_argument(
        "--cleanup-mode",
        choices=("target-root", "desired-existing"),
        default="target-root",
        help=(
            "target-root removes stale docs under target root; desired-existing only replaces "
            "existing docs whose hpaths are in the current Wolai manifest."
        ),
    )
    parser.add_argument("--summary-path", type=Path, default=SUMMARY_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not SIYUAN_TOKEN:
        raise SyncError("SIYUAN_TOKEN is required. Set it in the environment before syncing.")
    if not args.notebook_id:
        raise SyncError("SIYUAN_NOTEBOOK_ID is required. Set it in the environment or pass --notebook-id.")
    client = SiYuanClient(SIYUAN_URL, SIYUAN_TOKEN)
    summary = sync_all(
        client=client,
        md_root=args.source_root,
        notebook_id=args.notebook_id,
        target_root=args.target_root,
        dry_run=args.dry_run,
        skip_cleanup=args.skip_cleanup,
        cleanup_mode=args.cleanup_mode,
    )
    args.summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload = summary.to_json()
    args.summary_path.write_text(payload, encoding="utf-8")
    try:
        print(payload)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((payload + "\n").encode("utf-8", errors="replace"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
