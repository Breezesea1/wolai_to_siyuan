from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import main


class SyncNotesTests(unittest.TestCase):
    def test_strip_wolai_suffix(self) -> None:
        self.assertEqual(main.strip_wolai_suffix("Algorithm_fNPgcKVFyDD588mqmPWF9G"), "Algorithm")
        self.assertEqual(main.strip_wolai_suffix("plain-name"), "plain-name")

    def test_collect_links_covers_markdown_and_angle_links(self) -> None:
        markdown = """# Demo

normal [Doc](SiblingPage_abcdefghijk123456.md)
table | [Asset](file/book_abcd1234567890.pdf) | <AnotherPage_abcdefghijk123456.md> |
image ![alt](../image/pic.png)
"""
        links = main.collect_links(markdown, Path("pages/Root_abcdefghijk123456.md"))
        kinds = sorted(link.kind for link in links)
        self.assertEqual(kinds.count("doc"), 2)
        self.assertIn("file", kinds)
        self.assertIn("image", kinds)

    def test_classify_link_marks_missing_doc_as_doc_reference(self) -> None:
        link = main.classify_link(
            source_rel=Path("pages/Root_abcdefghijk123456.md"),
            original_target="MissingDoc_abcdefghijk123456.md",
            text="Missing",
            is_image=False,
            wrapped_by_angle=False,
            line_number=1,
        )
        self.assertEqual(link.kind, "doc")
        self.assertEqual(link.resolved_rel, Path("pages/MissingDoc_abcdefghijk123456.md"))

    def test_table_shape_preserved_when_rewriting(self) -> None:
        before = "| Name | Link |\n| --- | --- |\n| A | [Doc](pages/Doc_abcdefghijk123456.md) |\n"
        after = "| Name | Link |\n| --- | --- |\n| A | [Doc](siyuan://blocks/2025) |\n"
        self.assertEqual(main.verify_table_shapes(before, after), [])

    def test_build_manifest_collects_malformed_and_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pages").mkdir()
            (root / "image").mkdir()
            (root / "pages" / "Child_abcdefghijk123456.md").write_text("# Child\n", encoding="utf-8")
            (root / "image" / "pic.png").write_bytes(b"png")
            (root / "pages" / "Root_abcdefghijk123456.md").write_text(
                "# Root\n\n[Child](Child_abcdefghijk123456.md)\n[Bad](Missing_abcdefghijk123456.md)\n![Pic](../image/pic.png)\n",
                encoding="utf-8",
            )
            summary = main.SyncSummary()
            manifest = main.build_manifest(root, summary)
            self.assertEqual(len(manifest.pages), 2)
            self.assertFalse(summary.unresolved_malformed)
            self.assertIn(Path("image/pic.png"), manifest.assets)

    def test_probe_writeback_mode_falls_back_when_update_block_fails(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.children = [
                    {"id": "h1", "markdown": "# Probe"},
                    {"id": "p1", "markdown": "first"},
                ]

            def create_doc_with_md(self, notebook: str, path: str, markdown: str) -> str:
                return "doc-root"

            def update_block(self, block_id: str, markdown: str) -> None:
                raise main.SyncError("update failed")

            def get_child_blocks(self, block_id: str) -> list[dict]:
                return self.children

            def delete_block(self, block_id: str) -> None:
                self.children = [child for child in self.children if child["id"] != block_id]

            def append_block(self, parent_id: str, markdown: str) -> None:
                self.children = [
                    {"id": "h2", "markdown": "# Probe"},
                    {"id": "p2", "markdown": "fallback"},
                ]

            def remove_doc_by_id(self, notebook: str, doc_id: str) -> None:
                return None

        mode = main.probe_writeback_mode(FakeClient(), "nb")
        self.assertEqual(mode, "appendBlock")

    def test_discover_doc_root_id_falls_back_to_sql_when_hpath_api_is_empty(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.hpath_calls: list[tuple[str, str]] = []
                self.sql_calls: list[str] = []

            def get_ids_by_hpath(self, notebook: str, path: str) -> list[str]:
                self.hpath_calls.append((notebook, path))
                return []

            def sql(self, stmt: str) -> list[dict]:
                self.sql_calls.append(stmt)
                return [{"id": "sql-doc-id"}]

        client = FakeClient()
        doc_id = main.discover_doc_root_id(
            client,
            "nb",
            "/迁移/wolai/DCC/Houdini/VFX/Axis/00-_Basic",
            retries=1,
            retry_delay=0,
        )

        self.assertEqual(doc_id, "sql-doc-id")
        self.assertEqual(client.hpath_calls, [("nb", "/迁移/wolai/DCC/Houdini/VFX/Axis/00-_Basic")])
        self.assertEqual(len(client.sql_calls), 1)
        self.assertIn("hpath = '/迁移/wolai/DCC/Houdini/VFX/Axis/00-_Basic'", client.sql_calls[0])

    def test_extract_structural_doc_links_ignores_table_links(self) -> None:
        markdown = """# CSPath

[软件架构](软件架构_cAqhyLbKBDreP5TU43bL3G.md "软件架构")
[分布式](分布式_mmNFxoVzX2TXpYsw4CvVWE.md "分布式")

| State | Course |
| --- | --- |
| 待进行 | [课程A](课程A_abcdefghijk123456.md "课程A") |

***

[Coding TODO (deprecated)](<Coding TODO (deprecated)_pLcYuK2uzPPH2bw4mg722X.md> "Coding TODO (deprecated)")
"""
        manifest = main.Manifest(
            pages={
                Path("pages/软件架构_cAqhyLbKBDreP5TU43bL3G.md"): main.PageEntry(
                    source_rel=Path("pages/软件架构_cAqhyLbKBDreP5TU43bL3G.md"),
                    abs_path=Path(),
                    title="软件架构",
                    target_hpath="",
                    markdown="",
                ),
                Path("pages/分布式_mmNFxoVzX2TXpYsw4CvVWE.md"): main.PageEntry(
                    source_rel=Path("pages/分布式_mmNFxoVzX2TXpYsw4CvVWE.md"),
                    abs_path=Path(),
                    title="分布式",
                    target_hpath="",
                    markdown="",
                ),
                Path("pages/课程A_abcdefghijk123456.md"): main.PageEntry(
                    source_rel=Path("pages/课程A_abcdefghijk123456.md"),
                    abs_path=Path(),
                    title="课程A",
                    target_hpath="",
                    markdown="",
                ),
                Path("pages/Coding TODO (deprecated)_pLcYuK2uzPPH2bw4mg722X.md"): main.PageEntry(
                    source_rel=Path("pages/Coding TODO (deprecated)_pLcYuK2uzPPH2bw4mg722X.md"),
                    abs_path=Path(),
                    title="Coding TODO (deprecated)",
                    target_hpath="",
                    markdown="",
                ),
            }
        )
        rels = main.extract_structural_doc_links(markdown, Path("pages/CSPath_abcdefghijk123456.md"), manifest)
        self.assertEqual(
            rels,
            [
                Path("pages/软件架构_cAqhyLbKBDreP5TU43bL3G.md"),
                Path("pages/分布式_mmNFxoVzX2TXpYsw4CvVWE.md"),
                Path("pages/课程A_abcdefghijk123456.md"),
                Path("pages/Coding TODO (deprecated)_pLcYuK2uzPPH2bw4mg722X.md"),
            ],
        )

    def test_build_navigation_parents_from_bzs_and_section_links(self) -> None:
        manifest = main.Manifest()
        bzs = main.PageEntry(
            source_rel=Path("bzs.md"),
            abs_path=Path("bzs.md"),
            title="bzs",
            target_hpath="/迁移/wolai/bzs",
            markdown="# bzs\n\n* [library](pages/library_nyTuuqs79PniXL6wPBx6g4.md)\n* [OS](pages/OS_wuoU6oKTh14gPGBBz9Ftm9.md)\n",
        )
        library = main.PageEntry(
            source_rel=Path("pages/library_nyTuuqs79PniXL6wPBx6g4.md"),
            abs_path=Path("pages/library_nyTuuqs79PniXL6wPBx6g4.md"),
            title="library",
            target_hpath="/迁移/wolai/library",
            markdown="# library\n\n#### `资源`\n\n[应用软件](应用软件_afmrjnW4WKJZfNNEkzG3js.md \"应用软件\")\n\n[图书馆](图书馆_oT2FzU8RX8w7YDTgnjpwq8.md \"图书馆\")\n",
        )
        app = main.PageEntry(
            source_rel=Path("pages/应用软件_afmrjnW4WKJZfNNEkzG3js.md"),
            abs_path=Path("pages/应用软件_afmrjnW4WKJZfNNEkzG3js.md"),
            title="应用软件",
            target_hpath="/迁移/wolai/应用软件",
            markdown="# 应用软件\n\n| 软件名 | 文件 |\n| --- | --- |\n| [QT翻译软件](QT翻译软件_pSaM6XdZTnEXEoJL9ScxFJ.md) | [qtranslate](file/qtranslate-6-10-0_uvmQS75WRZ.exe) |\n",
        )
        os_page = main.PageEntry(
            source_rel=Path("pages/OS_wuoU6oKTh14gPGBBz9Ftm9.md"),
            abs_path=Path("pages/OS_wuoU6oKTh14gPGBBz9Ftm9.md"),
            title="OS",
            target_hpath="/迁移/wolai/OS",
            markdown="# OS\n\n#### `Basis`\n\n[进程管理](进程管理_3rFUiZKj1xUdYMWCDuG5qY.md \"进程管理\")\n",
        )
        proc = main.PageEntry(
            source_rel=Path("pages/进程管理_3rFUiZKj1xUdYMWCDuG5qY.md"),
            abs_path=Path("pages/进程管理_3rFUiZKj1xUdYMWCDuG5qY.md"),
            title="进程管理",
            target_hpath="/迁移/wolai/进程管理",
            markdown="# 进程管理\n",
        )
        manifest.pages = {
            bzs.source_rel: bzs,
            library.source_rel: library,
            app.source_rel: app,
            os_page.source_rel: os_page,
            proc.source_rel: proc,
        }
        parents = main.build_navigation_parents(manifest)
        self.assertEqual(parents[library.source_rel], bzs.source_rel)
        self.assertEqual(parents[app.source_rel], library.source_rel)
        self.assertEqual(parents[proc.source_rel], os_page.source_rel)

    def test_build_navigation_parents_includes_table_doc_links(self) -> None:
        manifest = main.Manifest()
        root = main.PageEntry(
            source_rel=Path("bzs.md"),
            abs_path=Path("bzs.md"),
            title="bzs",
            target_hpath="/迁移/wolai/bzs",
            markdown="# bzs\n\n[基础](pages/基础_8m3BckAjMYL9ceekWvApbH.md)\n",
        )
        base = main.PageEntry(
            source_rel=Path("pages/基础_8m3BckAjMYL9ceekWvApbH.md"),
            abs_path=Path("pages/基础_8m3BckAjMYL9ceekWvApbH.md"),
            title="基础",
            target_hpath="/迁移/wolai/基础",
            markdown="""# 基础

| [\\_](新页面_4DEVqxkT9FFbLFZrr4FGQp.md "_") | **`Single()`** |
| [\\_](新页面_5erii59gwu7jmhRo8aKGUp.md "_") | **`Where`** |
""",
        )
        child_a = main.PageEntry(
            source_rel=Path("pages/新页面_4DEVqxkT9FFbLFZrr4FGQp.md"),
            abs_path=Path("pages/新页面_4DEVqxkT9FFbLFZrr4FGQp.md"),
            title="新页面",
            target_hpath="/迁移/wolai/新页面",
            markdown="# 新页面\n",
        )
        child_b = main.PageEntry(
            source_rel=Path("pages/新页面_5erii59gwu7jmhRo8aKGUp.md"),
            abs_path=Path("pages/新页面_5erii59gwu7jmhRo8aKGUp.md"),
            title="新页面",
            target_hpath="/迁移/wolai/新页面-2",
            markdown="# 新页面\n",
        )
        manifest.pages = {
            root.source_rel: root,
            base.source_rel: base,
            child_a.source_rel: child_a,
            child_b.source_rel: child_b,
        }
        parents = main.build_navigation_parents(manifest)
        self.assertEqual(parents[base.source_rel], root.source_rel)
        self.assertEqual(parents[child_a.source_rel], base.source_rel)
        self.assertEqual(parents[child_b.source_rel], base.source_rel)

    def test_navigation_hpaths_preserve_deep_page_tree_segments(self) -> None:
        root = main.PageEntry(
            source_rel=Path("bzs.md"),
            abs_path=Path("bzs.md"),
            title="bzs",
            target_hpath="",
            markdown="# bzs\n\n[Coding](pages/Coding_abcdefghijklmnop.md)\n",
        )
        coding = main.PageEntry(
            source_rel=Path("pages/Coding_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="Coding",
            target_hpath="",
            markdown="[dotnet](dotnet_abcdefghijklmnop.md)\n",
        )
        dotnet = main.PageEntry(
            source_rel=Path("pages/dotnet_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="dotnet",
            target_hpath="",
            markdown="[Core](Core_abcdefghijklmnop.md)\n",
        )
        core = main.PageEntry(
            source_rel=Path("pages/Core_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="Core",
            target_hpath="",
            markdown="[基础库](基础库_abcdefghijklmnop.md)\n",
        )
        basic = main.PageEntry(
            source_rel=Path("pages/基础库_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="基础库",
            target_hpath="",
            markdown="[Time](Time_abcdefghijklmnop.md)\n",
        )
        time = main.PageEntry(
            source_rel=Path("pages/Time_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="Time",
            target_hpath="",
            markdown="[TimeSpan](TimeSpan_abcdefghijklmnop.md)\n",
        )
        timespan = main.PageEntry(
            source_rel=Path("pages/TimeSpan_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="TimeSpan",
            target_hpath="",
            markdown="# TimeSpan\n",
        )
        manifest = main.Manifest(
            pages={
                page.source_rel: page
                for page in [root, coding, dotnet, core, basic, time, timespan]
            }
        )

        parents = main.build_navigation_parents(manifest)
        summary = main.SyncSummary()
        main.assign_navigation_hpaths(manifest, parents, summary)

        self.assertEqual(
            timespan.target_hpath,
            "/迁移/wolai/Coding/dotnet/Core/基础库/Time/TimeSpan",
        )
        self.assertNotIn("⟫", timespan.target_hpath)
        self.assertFalse(
            [item for item in summary.unsupported_features if item.get("reason") == "hpath_depth_compressed"]
        )

    def test_create_docs_creates_parents_before_children(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.created_paths: list[str] = []

            def sql(self, stmt: str) -> list[dict]:
                return []

            def create_doc_with_md(self, notebook: str, path: str, markdown: str) -> str:
                self.created_paths.append(path)
                return f"doc-{len(self.created_paths)}"

        parent = main.PageEntry(
            source_rel=Path("pages/Time_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="Time",
            target_hpath="/迁移/wolai/Coding/dotnet/Core/基础库/Time",
            markdown="# Time\n",
        )
        child = main.PageEntry(
            source_rel=Path("pages/TimeSpan_abcdefghijklmnop.md"),
            abs_path=Path(),
            title="TimeSpan",
            target_hpath="/迁移/wolai/Coding/dotnet/Core/基础库/Time/TimeSpan",
            markdown="# TimeSpan\n",
        )
        manifest = main.Manifest(pages={child.source_rel: child, parent.source_rel: parent})

        client = FakeClient()
        main.create_docs(client, "nb", manifest, main.SyncSummary(), existing_doc_ids={})

        self.assertEqual(
            client.created_paths,
            [
                "/迁移/wolai/Coding/dotnet/Core/基础库/Time",
                "/迁移/wolai/Coding/dotnet/Core/基础库/Time/TimeSpan",
            ],
        )

    def test_rewrite_markdown_resolves_suffixless_local_page_link_by_unique_title(self) -> None:
        source = main.PageEntry(
            source_rel=Path("pages/Blazor_tU9BAscRyMJzGvT3cvouF9.md"),
            abs_path=Path(),
            title="Blazor",
            target_hpath="/迁移/wolai/Core/Blazor",
            markdown="",
            doc_id="source-id",
        )
        target = main.PageEntry(
            source_rel=Path("pages/JS调用C#_vn3bSTutXkUj3SijnMcZxX.md"),
            abs_path=Path(),
            title="JS调用C#",
            target_hpath="/迁移/wolai/Core/Blazor/JS调用C#",
            markdown="",
            doc_id="target-id",
        )
        manifest = main.Manifest(pages={source.source_rel: source, target.source_rel: target})
        summary = main.SyncSummary()
        rewritten = main.rewrite_markdown('[JS调用C#](JS调用C "JS调用C#")', source, manifest, summary)
        self.assertEqual(rewritten, "[JS调用C#](siyuan://blocks/target-id)")
        self.assertEqual(summary.rewritten_doc_refs, 1)

    def test_wrapped_wolai_link_is_normalized_for_navigation(self) -> None:
        parent = main.PageEntry(
            source_rel=Path("pages/Filter_hCDhvTWKyKRVARyKjCBNeJ.md"),
            abs_path=Path(),
            title="Filter",
            target_hpath="/迁移/wolai/Coding/dotnet/Core/Linq/Filter",
            markdown="""# Filter

[ Distinct，DistinctBy
](Distinct，DistinctBy-_ut9Rdht2gWbZuPCUwAdg3n.md " Distinct，DistinctBy
")
""",
        )
        child = main.PageEntry(
            source_rel=Path("pages/Distinct，DistinctBy-_ut9Rdht2gWbZuPCUwAdg3n.md"),
            abs_path=Path(),
            title="`Distinct，DistinctBy`",
            target_hpath="/迁移/wolai/`Distinct，DistinctBy`",
            markdown="",
        )
        manifest = main.Manifest(pages={parent.source_rel: parent, child.source_rel: child})
        rels = main.extract_structural_doc_links(parent.markdown, parent.source_rel, manifest)
        self.assertEqual(rels, [child.source_rel])

    def test_navigation_resolves_suffixless_local_page_link_by_unique_title(self) -> None:
        parent = main.PageEntry(
            source_rel=Path("pages/Blazor_tU9BAscRyMJzGvT3cvouF9.md"),
            abs_path=Path(),
            title="Blazor",
            target_hpath="/迁移/wolai/Coding/dotnet/Core/Blazor",
            markdown='[JS调用C#](JS调用C "JS调用C#")',
        )
        child = main.PageEntry(
            source_rel=Path("pages/JS调用C#_vn3bSTutXkUj3SijnMcZxX.md"),
            abs_path=Path(),
            title="JS调用C\\#",
            target_hpath="/迁移/wolai/JS调用C-#",
            markdown="",
        )
        manifest = main.Manifest(pages={parent.source_rel: parent, child.source_rel: child})
        rels = main.extract_structural_doc_links(parent.markdown, parent.source_rel, manifest)
        self.assertEqual(rels, [child.source_rel])

    def test_navigation_resolves_escaped_bracket_page_links(self) -> None:
        parent = main.PageEntry(
            source_rel=Path("pages/Blazor_tU9BAscRyMJzGvT3cvouF9.md"),
            abs_path=Path(),
            title="Blazor",
            target_hpath="/迁移/wolai/Coding/dotnet/Core/Blazor",
            markdown='[\\[JSImport\\]和\\[JSExport\\]](\\[JSImport]和\\[JSExport]_sGK28eRKKYUVmJA6X4taJG.md "\\[JSImport]和\\[JSExport]")',
        )
        child = main.PageEntry(
            source_rel=Path("pages/[JSImport]和[JSExport]_sGK28eRKKYUVmJA6X4taJG.md"),
            abs_path=Path(),
            title="[JSImport]和[JSExport]",
            target_hpath="/迁移/wolai/[JSImport]和[JSExport]",
            markdown="",
        )
        manifest = main.Manifest(pages={parent.source_rel: parent, child.source_rel: child})

        rels = main.extract_structural_doc_links(parent.markdown, parent.source_rel, manifest)

        self.assertEqual(rels, [child.source_rel])

    def test_title_to_hpath_part_decodes_html_entities_and_markdown_escapes(self) -> None:
        self.assertEqual(main.title_to_hpath_part("\\[JSImport]和\\[JSExport]"), "[JSImport]和[JSExport]")
        self.assertEqual(main.title_to_hpath_part("设计模式&#x20;"), "设计模式")
        self.assertEqual(main.title_to_hpath_part("&#x20;"), "未命名")

    def test_parse_database_candidate_for_table_only_page(self) -> None:
        markdown = """# 图书馆

| 书名 | 文件 | 类型 | 结束日期 | 简介 |
| --- | --- | --- | --- | --- |
| [Docker Deep Dive](Docker Deep Dive_rTJ5seiQvmQxKoGuVJhhJk.md) | [Docker.Deep.Dive.2024.5.pdf](file/Docker.Deep.Dive.2024.5_lBbOZX5Pbi.pdf) | 编程 | 2025/02/10 | Docker的基础入门 |
"""
        page = main.PageEntry(
            source_rel=Path("pages/图书馆_oT2FzU8RX8w7YDTgnjpwq8.md"),
            abs_path=Path("pages/图书馆_oT2FzU8RX8w7YDTgnjpwq8.md"),
            title="图书馆",
            target_hpath="/迁移/wolai/library/图书馆",
            markdown=markdown,
            links=main.collect_links(markdown, Path("pages/图书馆_oT2FzU8RX8w7YDTgnjpwq8.md")),
            table_shapes=main.collect_table_shapes(markdown),
        )
        manifest = main.Manifest(
            pages={page.source_rel: page},
            pages_by_hpath={page.target_hpath: page},
            assets={},
        )

        plan = main.detect_database_table_candidate(page, manifest)
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual([column.name for column in plan.columns], ["书名", "文件", "类型", "结束日期", "简介"])
        self.assertEqual(plan.columns[0].key_type, "block")
        self.assertEqual(plan.columns[1].key_type, "mAsset")
        self.assertEqual(plan.columns[2].key_type, "mSelect")
        self.assertEqual(plan.columns[3].key_type, "date")
        self.assertEqual(plan.columns[4].key_type, "text")
        self.assertEqual(plan.rows[0].primary_text, "Docker Deep Dive")
        self.assertEqual(plan.rows[0].primary_doc_rel, Path("pages/Docker Deep Dive_rTJ5seiQvmQxKoGuVJhhJk.md"))
        self.assertEqual(
            plan.rows[0].cells[1].asset_rel,
            Path("file/Docker.Deep.Dive.2024.5_lBbOZX5Pbi.pdf"),
        )
        self.assertEqual(plan.rows[0].cells[2].text_value, "编程")
        self.assertEqual(plan.rows[0].cells[3].date_value, "2025/02/10")

    def test_database_candidate_rejects_pages_with_extra_body_content(self) -> None:
        markdown = """# 示例

这是正文说明，不应该强转数据库。

| A | B |
| --- | --- |
| 1 | 2 |
"""
        page = main.PageEntry(
            source_rel=Path("pages/示例_abcdefghijk123456.md"),
            abs_path=Path("pages/示例_abcdefghijk123456.md"),
            title="示例",
            target_hpath="/迁移/wolai/示例",
            markdown=markdown,
            links=main.collect_links(markdown, Path("pages/示例_abcdefghijk123456.md")),
            table_shapes=main.collect_table_shapes(markdown),
        )
        manifest = main.Manifest(pages={page.source_rel: page}, pages_by_hpath={page.target_hpath: page}, assets={})
        self.assertIsNone(main.detect_database_table_candidate(page, manifest))


if __name__ == "__main__":
    unittest.main()
