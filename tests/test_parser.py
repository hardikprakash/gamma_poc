"""
Tests for M1: PDF Parser helper functions.
Covers: pymupdf4llm markdown parsing, table validation, pdfplumber fallback helpers.
"""

import pytest


class TestTableToMarkdown:
    """Test _table_to_markdown conversion (pdfplumber fallback path)."""

    def test_basic_table(self):
        from pipeline.ingestion.parser import _table_to_markdown
        headers = ["Metric", "2023", "2022"]
        rows = [["Revenue", "383,285", "394,328"], ["Net Income", "96,995", "99,803"]]
        md = _table_to_markdown(headers, rows)
        assert "| Metric |" in md
        assert "| Revenue |" in md
        assert "383,285" in md
        assert md.count("\n") >= 3  # header + separator + 2 data rows

    def test_empty_table(self):
        from pipeline.ingestion.parser import _table_to_markdown
        md = _table_to_markdown([], [])
        assert md == "" or md.strip() == ""

    def test_single_row(self):
        from pipeline.ingestion.parser import _table_to_markdown
        headers = ["A", "B"]
        rows = [["1", "2"]]
        md = _table_to_markdown(headers, rows)
        assert "| A |" in md
        assert "| 1 |" in md

    def test_short_row_padded(self):
        from pipeline.ingestion.parser import _table_to_markdown
        headers = ["A", "B", "C"]
        rows = [["1"]]  # fewer cells than headers
        md = _table_to_markdown(headers, rows)
        assert "| A | B | C |" in md


class TestFontProfile:
    """Basic smoke tests for font-related helpers."""

    def test_heading_level_map_returns_dict(self):
        from pipeline.ingestion.parser import _build_heading_level_map
        profile = {"body": (10, "normal"), "all": {
            (10, "normal"): 5000,
            (14, "bold"): 200,
            (12, "bold"): 300,
            (10, "bold"): 100,
        }}
        result = _build_heading_level_map(profile)
        assert isinstance(result, dict)
        # 14pt bold → level 1, 12pt bold → level 2, 10pt bold → level 3
        assert result[(14, "bold")] == 1
        assert result[(12, "bold")] == 2
        assert result[(10, "bold")] == 3


class TestMarkdownTableValidation:
    """Test _validate_markdown_table."""

    def test_valid_table(self):
        from pipeline.ingestion.parser import _validate_markdown_table
        md = (
            "| Metric | 2023 | 2022 |\n"
            "| --- | --- | --- |\n"
            "| Revenue | 383,285 | 394,328 |\n"
            "| Net Income | 96,995 | 99,803 |"
        )
        assert _validate_markdown_table(md) is True

    def test_missing_separator(self):
        from pipeline.ingestion.parser import _validate_markdown_table
        md = (
            "| Metric | 2023 |\n"
            "| Revenue | 383,285 |"
        )
        assert _validate_markdown_table(md) is False

    def test_inconsistent_columns(self):
        from pipeline.ingestion.parser import _validate_markdown_table
        md = (
            "| A | B | C |\n"
            "| --- | --- | --- |\n"
            "| 1 | 2 | 3 | 4 | 5 |\n"  # too many columns
            "| 6 | 7 | 8 |"
        )
        assert _validate_markdown_table(md) is False

    def test_all_empty_cells_rejected(self):
        from pipeline.ingestion.parser import _validate_markdown_table
        md = (
            "| A | B |\n"
            "| --- | --- |\n"
            "|  |  |"
        )
        assert _validate_markdown_table(md) is False

    def test_too_few_lines(self):
        from pipeline.ingestion.parser import _validate_markdown_table
        assert _validate_markdown_table("| A | B |") is False


class TestMarkdownPageParsing:
    """Test _parse_markdown_page."""

    def test_headings_extracted(self):
        from pipeline.ingestion.parser import _parse_markdown_page
        md = "# Revenue Discussion\n\nTotal revenue increased by 15%.\n\n## Regional Breakdown\n\nAsia Pacific grew."
        blocks, tables = _parse_markdown_page(md, page_idx=0)
        headings = [b for b in blocks if b.is_heading]
        assert len(headings) == 2
        assert headings[0].heading_level == 1
        assert headings[0].text == "Revenue Discussion"
        assert headings[1].heading_level == 2
        assert headings[1].text == "Regional Breakdown"

    def test_tables_extracted(self):
        from pipeline.ingestion.parser import _parse_markdown_page
        md = (
            "Some text before.\n\n"
            "| Metric | Value |\n"
            "| --- | --- |\n"
            "| Revenue | 100 |\n\n"
            "Some text after."
        )
        blocks, tables = _parse_markdown_page(md, page_idx=0)
        assert len(tables) == 1
        assert tables[0].headers == ["Metric", "Value"]
        assert tables[0].rows == [["Revenue", "100"]]

    def test_prose_blocks_created(self):
        from pipeline.ingestion.parser import _parse_markdown_page
        md = "This is a paragraph.\n\nAnother paragraph."
        blocks, tables = _parse_markdown_page(md, page_idx=0)
        prose = [b for b in blocks if not b.is_heading]
        assert len(prose) >= 1
        combined_text = " ".join(b.text for b in prose)
        assert "paragraph" in combined_text

    def test_empty_page(self):
        from pipeline.ingestion.parser import _parse_markdown_page
        blocks, tables = _parse_markdown_page("", page_idx=0)
        assert blocks == []
        assert tables == []


class TestStripMarkdownFormatting:
    """Test _strip_markdown_formatting utility."""

    def test_strips_headings(self):
        from pipeline.ingestion.parser import _strip_markdown_formatting
        result = _strip_markdown_formatting("## My Heading\nSome text")
        assert "##" not in result
        assert "My Heading" in result

    def test_strips_bold(self):
        from pipeline.ingestion.parser import _strip_markdown_formatting
        result = _strip_markdown_formatting("This is **bold** text")
        assert "**" not in result
        assert "bold" in result
