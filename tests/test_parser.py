"""
Tests for M1: PDF Parser helper functions.
"""

import pytest


class TestTableToMarkdown:
    """Test _table_to_markdown conversion."""

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

    def test_none_values(self):
        from pipeline.ingestion.parser import _table_to_markdown
        headers = ["A", "B"]
        rows = [["1", None], [None, "2"]]
        md = _table_to_markdown(headers, rows)
        # Should not crash — None values converted to empty strings
        assert isinstance(md, str)


class TestFontProfile:
    """Basic smoke tests for font-related helpers (require fitz mock or skip)."""

    def test_heading_level_map_returns_dict(self):
        from pipeline.ingestion.parser import _build_heading_level_map
        profile = {"body_size": 10.0, "heading_sizes": [14.0, 12.0, 11.0]}
        result = _build_heading_level_map(profile)
        assert isinstance(result, dict)
        # Larger fonts → lower heading level
        if 14.0 in result and 11.0 in result:
            assert result[14.0] <= result[11.0]
