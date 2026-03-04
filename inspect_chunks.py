"""
Chunk Inspector — Streamlit UI for auditing how a PDF is chunked before ingest.

Runs M1 (parse) + optionally M2 (structure via LLM) + M3 (chunk) on a PDF
and visualises the resulting chunks: token distribution, page spans,
section breakdown, and full content viewer.

Usage:
    streamlit run inspect_chunks.py

No Neo4j, no embeddings — pure pipeline inspection.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

# ── make project root importable ────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

# ── Page config must be first Streamlit call ─────────────────────────────────
st.set_page_config(
    page_title="Chunk Inspector",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Lazy imports (avoid slow imports on every rerun) ────────────────────────
@st.cache_resource(show_spinner=False)
def _get_encoder():
    import tiktoken
    return tiktoken.get_encoding("cl100k_base")


# ── Pipeline helpers ─────────────────────────────────────────────────────────

def _run_m1_m3(pdf_path: str, company: str, ticker: str, fiscal_year: int):
    """M1 + M3 only (no LLM): fast mode."""
    from pipeline.ingestion.parser import parse_pdf
    from pipeline.ingestion.chunker import chunk_document
    from models.response import StructuredDocument

    with st.spinner("M1 — Parsing PDF…"):
        t0 = time.time()
        parsed_doc = parse_pdf(pdf_path)
        parse_time = time.time() - t0

    with st.spinner("M3 — Chunking (no structure inference)…"):
        t0 = time.time()
        structured_doc = StructuredDocument(
            doc_type="other",
            sections=[],          # triggers single-section fallback in chunker
            table_classifications={},
            parsed_doc=parsed_doc,
            company=company,
            ticker=ticker,
            fiscal_year=fiscal_year,
        )
        chunks = chunk_document(structured_doc)
        chunk_time = time.time() - t0

    return parsed_doc, chunks, parse_time, chunk_time


async def _run_m1_m2_m3_async(pdf_path: str, company: str, ticker: str, fiscal_year: int):
    """M1 + M2 (LLM) + M3: full mode."""
    from pipeline.ingestion.parser import parse_pdf
    from pipeline.ingestion.structure import infer_structure
    from pipeline.ingestion.chunker import chunk_document

    with st.spinner("M1 — Parsing PDF…"):
        t0 = time.time()
        parsed_doc = parse_pdf(pdf_path)
        parse_time = time.time() - t0

    with st.spinner("M2 — Inferring structure (3 LLM calls)…"):
        t0 = time.time()
        structured_doc = await infer_structure(parsed_doc, company, fiscal_year, ticker)
        struct_time = time.time() - t0

    with st.spinner("M3 — Chunking…"):
        t0 = time.time()
        chunks = chunk_document(structured_doc)
        chunk_time = time.time() - t0

    return parsed_doc, structured_doc, chunks, parse_time, struct_time, chunk_time


def _run_full(pdf_path: str, company: str, ticker: str, fiscal_year: int):
    return asyncio.run(
        _run_m1_m2_m3_async(pdf_path, company, ticker, fiscal_year)
    )


# ── Colour helpers ───────────────────────────────────────────────────────────

TYPE_COLOURS = {
    "prose": "#4C9BE8",
    "table": "#E8954C",
    "footnote": "#A0A0A0",
}

CAT_PALETTE = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
    "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
]


def _badge(text: str, colour: str) -> str:
    return (
        f'<span style="background:{colour};color:#fff;padding:2px 8px;'
        f'border-radius:4px;font-size:0.78rem;font-weight:600">{text}</span>'
    )


# ── Stat cards ───────────────────────────────────────────────────────────────

def _metric_card(label: str, value: str, sub: str = "") -> str:
    return f"""
    <div style="background:#1e2130;border-radius:8px;padding:14px 18px;
                border-left:4px solid #4C9BE8;margin-bottom:4px">
      <div style="color:#aaa;font-size:0.78rem;text-transform:uppercase;
                  letter-spacing:.06em">{label}</div>
      <div style="color:#fff;font-size:1.6rem;font-weight:700;line-height:1.2">{value}</div>
      {"<div style='color:#888;font-size:0.75rem'>" + sub + "</div>" if sub else ""}
    </div>"""


# ── Main UI ──────────────────────────────────────────────────────────────────

def main():
    st.title("🔬 Chunk Inspector")
    st.caption("Audit how a PDF is chunked before it enters the knowledge graph.")

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Document")

        uploaded = st.file_uploader("Upload PDF", type=["pdf"])
        pdf_dir = st.text_input("…or path to PDF file", placeholder="/path/to/file.pdf")

        st.divider()
        company    = st.text_input("Company name", value="Infosys Limited")
        ticker     = st.text_input("Ticker",       value="INFY")
        fiscal_year = st.number_input("Fiscal year", min_value=2000, max_value=2035, value=2022, step=1)

        st.divider()
        fast_mode = st.checkbox(
            "Fast mode (skip LLM structure inference)",
            value=True,
            help="Skip M2 — chunks off a single fallback section. "
                 "Uncheck to run full M1→M2→M3 (requires OpenRouter key).",
        )

        run_btn = st.button("▶  Run pipeline", type="primary", use_container_width=True)

    # ── Resolve PDF path ─────────────────────────────────────────────────────
    pdf_path = None
    tmp_file = None

    if uploaded is not None:
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        tmp_file.write(uploaded.read())
        tmp_file.flush()
        pdf_path = tmp_file.name
    elif pdf_dir.strip():
        pdf_path = pdf_dir.strip()

    # ── Run pipeline ─────────────────────────────────────────────────────────
    if run_btn:
        if not pdf_path:
            st.error("Please upload a PDF or enter a file path.")
            st.stop()
        if not os.path.isfile(pdf_path):
            st.error(f"File not found: `{pdf_path}`")
            st.stop()
        if not company or not ticker or not fiscal_year:
            st.error("Company, ticker, and fiscal year are required.")
            st.stop()

        try:
            if fast_mode:
                parsed_doc, chunks, parse_time, chunk_time = _run_m1_m3(
                    pdf_path, company, str(ticker), int(fiscal_year)
                )
                struct_time = None
                structured_doc = None
            else:
                parsed_doc, structured_doc, chunks, parse_time, struct_time, chunk_time = _run_full(
                    pdf_path, company, str(ticker), int(fiscal_year)
                )
        except Exception as exc:
            st.error(f"Pipeline error: {exc}")
            st.exception(exc)
            st.stop()
        finally:
            if tmp_file:
                try:
                    os.unlink(tmp_file.name)
                except Exception:
                    pass

        st.session_state["chunks"]       = chunks
        st.session_state["parsed_doc"]   = parsed_doc
        st.session_state["structured"]   = structured_doc
        st.session_state["parse_time"]   = parse_time
        st.session_state["struct_time"]  = struct_time
        st.session_state["chunk_time"]   = chunk_time
        st.session_state["fast_mode"]    = fast_mode
        st.session_state["pdf_name"]     = (
            uploaded.name if uploaded else Path(pdf_path).name
        )

    # ── Display results ───────────────────────────────────────────────────────
    if "chunks" not in st.session_state:
        st.info("Upload a PDF and click **▶ Run pipeline** to begin.")
        st.stop()

    chunks       = st.session_state["chunks"]
    parsed_doc   = st.session_state["parsed_doc"]
    structured   = st.session_state["structured"]
    parse_time   = st.session_state["parse_time"]
    struct_time  = st.session_state["struct_time"]
    chunk_time   = st.session_state["chunk_time"]
    fast_mode    = st.session_state["fast_mode"]
    pdf_name     = st.session_state["pdf_name"]

    enc = _get_encoder()

    # ── Header bar ────────────────────────────────────────────────────────────
    st.markdown(f"### `{pdf_name}`")

    timing_parts = [
        f"Parse {parse_time:.1f}s",
        f"Chunk {chunk_time:.1f}s",
    ]
    if struct_time is not None:
        timing_parts.insert(1, f"Structure {struct_time:.1f}s")
    mode_tag = "fast (no LLM)" if fast_mode else "full (LLM)"
    st.caption(f"Mode: {mode_tag}  ·  " + "  ·  ".join(timing_parts))

    # ── Computed stats ────────────────────────────────────────────────────────
    import statistics

    token_counts  = [c.token_count for c in chunks]
    page_spans    = [c.page_end - c.page_start + 1 for c in chunks]
    multi_page    = sum(1 for s in page_spans if s > 1)
    type_counts   = {}
    cat_counts    = {}
    for c in chunks:
        type_counts[c.chunk_type] = type_counts.get(c.chunk_type, 0) + 1
        cat_counts[c.semantic_category] = cat_counts.get(c.semantic_category, 0) + 1

    total_tokens = sum(token_counts)
    avg_tokens   = statistics.mean(token_counts) if token_counts else 0
    median_tokens = statistics.median(token_counts) if token_counts else 0
    max_tokens   = max(token_counts) if token_counts else 0
    min_tokens   = min(token_counts) if token_counts else 0

    # ── Stat cards ────────────────────────────────────────────────────────────
    cols = st.columns(5)
    stats = [
        ("Total chunks",    str(len(chunks)),             f"{parsed_doc.total_pages} pages"),
        ("Total tokens",    f"{total_tokens:,}",          f"avg {avg_tokens:.0f} / median {median_tokens:.0f}"),
        ("Min tokens",      str(min_tokens),               "smallest chunk"),
        ("Max tokens",      str(max_tokens),               "largest chunk"),
        ("Multi-page",      str(multi_page),               f"{100*multi_page/max(len(chunks),1):.0f}% span >1 page"),
    ]
    for col, (label, value, sub) in zip(cols, stats):
        with col:
            st.markdown(_metric_card(label, value, sub), unsafe_allow_html=True)

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_overview, tab_browse, tab_pages = st.tabs(
        ["📊 Overview", "🗂 Browse Chunks", "📄 Page Coverage"]
    )

    # ════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ════════════════════════════════════════════════════════════
    with tab_overview:
        import plotly.graph_objects as go
        import plotly.express as px

        col_left, col_right = st.columns(2)

        # Token distribution histogram
        with col_left:
            st.subheader("Token count distribution")
            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=token_counts,
                nbinsx=40,
                marker_color="#4C9BE8",
                opacity=0.85,
                name="chunks",
            ))
            fig.add_vline(x=avg_tokens,    line_dash="dash", line_color="#FFA15A",
                          annotation_text=f"avg {avg_tokens:.0f}", annotation_position="top right")
            fig.add_vline(x=median_tokens, line_dash="dot",  line_color="#00CC96",
                          annotation_text=f"median {median_tokens:.0f}", annotation_position="top left")
            fig.update_layout(
                height=300, margin=dict(t=10, b=30, l=0, r=0),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="#ccc",
                xaxis_title="Tokens", yaxis_title="Chunks",
                bargap=0.05,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Page span distribution
        with col_right:
            st.subheader("Page span distribution")
            span_freq: dict[int, int] = {}
            for s in page_spans:
                span_freq[s] = span_freq.get(s, 0) + 1
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=list(span_freq.keys()),
                y=list(span_freq.values()),
                marker_color="#AB63FA",
                opacity=0.85,
            ))
            fig2.update_layout(
                height=300, margin=dict(t=10, b=30, l=0, r=0),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="#ccc",
                xaxis_title="Pages spanned", yaxis_title="Chunks",
                xaxis=dict(dtick=1),
            )
            st.plotly_chart(fig2, use_container_width=True)

        col_left2, col_right2 = st.columns(2)

        # Chunk type pie
        with col_left2:
            st.subheader("Chunk type")
            colours = [TYPE_COLOURS.get(t, "#888") for t in type_counts.keys()]
            fig3 = go.Figure(go.Pie(
                labels=list(type_counts.keys()),
                values=list(type_counts.values()),
                marker_colors=colours,
                hole=0.45,
                textinfo="label+percent",
            ))
            fig3.update_layout(
                height=280, margin=dict(t=10, b=10, l=0, r=0),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="#ccc", showlegend=False,
            )
            st.plotly_chart(fig3, use_container_width=True)

        # Semantic category bar
        with col_right2:
            st.subheader("Semantic category")
            sorted_cats = sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)
            fig4 = go.Figure(go.Bar(
                x=[v for _, v in sorted_cats],
                y=[k for k, _ in sorted_cats],
                orientation="h",
                marker_color=CAT_PALETTE[:len(sorted_cats)],
                opacity=0.85,
            ))
            fig4.update_layout(
                height=280, margin=dict(t=10, b=10, l=0, r=0),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="#ccc",
                xaxis_title="Chunks",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig4, use_container_width=True)

        # Quality flags
        st.subheader("Quality flags")
        oversized  = [c for c in chunks if c.token_count > 600]
        undersized = [c for c in chunks if c.token_count < 50]
        wide_span  = [c for c in chunks if (c.page_end - c.page_start) > 4]

        fl1, fl2, fl3 = st.columns(3)
        with fl1:
            colour = "#E8504C" if oversized else "#00CC96"
            st.markdown(
                _metric_card("Oversized (>600 tok)", str(len(oversized)),
                             "may truncate in context" if oversized else "all OK"),
                unsafe_allow_html=True,
            )
        with fl2:
            colour = "#E8954C" if undersized else "#00CC96"
            st.markdown(
                _metric_card("Undersized (<50 tok)", str(len(undersized)),
                             "very short chunks" if undersized else "all OK"),
                unsafe_allow_html=True,
            )
        with fl3:
            st.markdown(
                _metric_card("Wide span (>4 pages)", str(len(wide_span)),
                             "large section blocks" if wide_span else "all OK"),
                unsafe_allow_html=True,
            )

    # ════════════════════════════════════════════════════════════
    # TAB 2 — BROWSE CHUNKS
    # ════════════════════════════════════════════════════════════
    with tab_browse:
        # ── Filter bar ───────────────────────────────────────────────────────
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])
        with fc1:
            type_filter = st.multiselect(
                "Type", options=sorted(type_counts.keys()), default=list(type_counts.keys())
            )
        with fc2:
            cat_filter = st.multiselect(
                "Category", options=sorted(cat_counts.keys()), default=list(cat_counts.keys())
            )
        with fc3:
            tok_min, tok_max = st.slider(
                "Token range",
                min_value=0, max_value=max(max_tokens, 1),
                value=(0, max(max_tokens, 1)),
                step=10,
            )
        with fc4:
            search_text = st.text_input("Search content", placeholder="keyword…")

        # ── Apply filters ─────────────────────────────────────────────────────
        filtered = [
            c for c in chunks
            if c.chunk_type in type_filter
            and c.semantic_category in cat_filter
            and tok_min <= c.token_count <= tok_max
            and (not search_text or search_text.lower() in c.content.lower())
        ]

        st.caption(f"Showing **{len(filtered)}** of {len(chunks)} chunks")

        if not filtered:
            st.warning("No chunks match the current filters.")
        else:
            for idx, chunk in enumerate(filtered):
                page_span = chunk.page_end - chunk.page_start + 1
                type_col  = TYPE_COLOURS.get(chunk.chunk_type, "#888")
                span_col  = "#E8504C" if page_span > 4 else ("#E8954C" if page_span > 1 else "#4C9BE8")
                tok_col   = "#E8504C" if chunk.token_count > 600 else ("#E8954C" if chunk.token_count < 50 else "#00CC96")

                header_html = (
                    f"<div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap'>"
                    f"<code style='font-size:0.75rem'>{chunk.chunk_id}</code>"
                    f"&nbsp;"
                    + _badge(chunk.chunk_type, type_col)
                    + _badge(f"{chunk.token_count} tok", tok_col)
                    + _badge(
                        f"pp {chunk.page_start+1}–{chunk.page_end+1}"
                        + (f" ({page_span} pages)" if page_span > 1 else ""),
                        span_col,
                    )
                    + _badge(chunk.semantic_category, "#444")
                    + f"</div>"
                )

                with st.expander(
                    f"#{idx+1}  pp {chunk.page_start+1}–{chunk.page_end+1}"
                    f"  ·  {chunk.token_count} tok  ·  {chunk.chunk_type}"
                    f"  ·  {chunk.section_path[:60]}",
                    expanded=False,
                ):
                    st.markdown(header_html, unsafe_allow_html=True)
                    st.markdown(
                        f"**Section path:** `{chunk.section_path}`  "
                        f"| **Category:** `{chunk.semantic_category}`  "
                        f"| **Doc type:** `{chunk.doc_type}`"
                    )
                    if chunk.chunk_type == "table":
                        st.markdown(chunk.content)
                    else:
                        st.text_area(
                            "Content",
                            value=chunk.content,
                            height=180,
                            key=f"chunk_content_{chunk.chunk_id}_{idx}",
                            disabled=True,
                            label_visibility="collapsed",
                        )

    # ════════════════════════════════════════════════════════════
    # TAB 3 — PAGE COVERAGE
    # ════════════════════════════════════════════════════════════
    with tab_pages:
        import plotly.graph_objects as go

        total_pages = parsed_doc.total_pages

        # Build per-page stats
        page_chunk_count  = [0] * total_pages
        page_token_total  = [0] * total_pages
        page_has_table    = [False] * total_pages

        for c in chunks:
            for p in range(c.page_start, min(c.page_end + 1, total_pages)):
                page_chunk_count[p] += 1
                page_token_total[p] += c.token_count
                if c.chunk_type == "table":
                    page_has_table[p] = True

        page_nums = list(range(1, total_pages + 1))
        bar_colours = [
            "#E8954C" if page_has_table[p] else "#4C9BE8"
            for p in range(total_pages)
        ]

        st.subheader("Chunks per page")
        st.caption("Blue = prose/footnote only · Orange = page has at least one table chunk")

        fig5 = go.Figure()
        fig5.add_trace(go.Bar(
            x=page_nums,
            y=page_chunk_count,
            marker_color=bar_colours,
            opacity=0.85,
            hovertemplate=(
                "Page %{x}<br>"
                "Chunks: %{y}<br>"
                "Tokens: %{customdata}<extra></extra>"
            ),
            customdata=page_token_total,
        ))
        fig5.update_layout(
            height=320, margin=dict(t=10, b=30, l=0, r=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#ccc",
            xaxis_title="Page (1-indexed)", yaxis_title="Chunk count",
            bargap=0.1,
        )
        st.plotly_chart(fig5, use_container_width=True)

        # Zero-coverage pages
        empty_pages = [p + 1 for p in range(total_pages) if page_chunk_count[p] == 0]
        if empty_pages:
            st.warning(
                f"**{len(empty_pages)} page(s) have no chunks:** "
                + (", ".join(str(p) for p in empty_pages[:30]))
                + ("…" if len(empty_pages) > 30 else "")
            )
        else:
            st.success("All pages have at least one chunk.")

        # Heatmap of tokens per page
        st.subheader("Token density per page")
        import plotly.express as px

        n = total_pages
        cols_grid = min(50, n)
        rows_grid = (n + cols_grid - 1) // cols_grid
        grid = []
        for row in range(rows_grid):
            grid_row = []
            for col in range(cols_grid):
                p = row * cols_grid + col
                grid_row.append(page_token_total[p] if p < n else None)
            grid.append(grid_row)

        fig6 = go.Figure(go.Heatmap(
            z=grid,
            colorscale="Blues",
            showscale=True,
            colorbar=dict(title="Tokens"),
            hovertemplate="Page %{text}<br>Tokens: %{z}<extra></extra>",
            text=[
                [str(row * cols_grid + col + 1) if (row * cols_grid + col) < n else ""
                 for col in range(cols_grid)]
                for row in range(rows_grid)
            ],
        ))
        fig6.update_layout(
            height=max(120, rows_grid * 28),
            margin=dict(t=10, b=10, l=0, r=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#ccc",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        st.plotly_chart(fig6, use_container_width=True)

        # Multi-page chunk table
        wide_chunks = [(c, c.page_end - c.page_start + 1) for c in chunks if c.page_end > c.page_start]
        if wide_chunks:
            st.subheader(f"Multi-page chunks ({len(wide_chunks)})")
            import pandas as pd
            rows = [
                {
                    "chunk_id":   c.chunk_id,
                    "type":       c.chunk_type,
                    "category":   c.semantic_category,
                    "page_start": c.page_start + 1,
                    "page_end":   c.page_end + 1,
                    "pages_span": span,
                    "tokens":     c.token_count,
                    "section":    c.section_path[:60],
                }
                for c, span in sorted(wide_chunks, key=lambda x: x[1], reverse=True)
            ]
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "pages_span": st.column_config.NumberColumn("span", format="%d pp"),
                    "tokens":     st.column_config.NumberColumn("tokens"),
                },
            )


if __name__ == "__main__":
    main()
