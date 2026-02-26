"""
Streamlit frontend — thin client for the Financial Filings Graph RAG Agent.
Communicates with the FastAPI backend via HTTP.

Entry point: streamlit run frontend/app.py
"""

import streamlit as st
import httpx
import os
from datetime import datetime
from collections import defaultdict

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Financial Filings Agent",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session State ────────────────────────────────────────────────────────────
if "query_history" not in st.session_state:
    st.session_state.query_history = []


# ── Helpers ──────────────────────────────────────────────────────────────────

def fetch_corpus():
    """Fetch corpus info from backend."""
    try:
        response = httpx.get(f"{BACKEND_URL}/corpus", timeout=10.0)
        if response.status_code == 200:
            return response.json().get("documents", [])
    except Exception:
        pass
    return []


# ── Page 1: Query ────────────────────────────────────────────────────────────

def render_query_page():
    st.title("📊 Financial Filings Agent")
    st.caption("Ask questions about financial filings with full source citation.")

    # Sidebar
    corpus = fetch_corpus()
    available_companies = sorted(set(d.get("company", "") for d in corpus if d.get("company")))
    available_years = sorted(set(d.get("fiscal_year", 0) for d in corpus if d.get("fiscal_year")))

    with st.sidebar:
        st.markdown("### Filters")

        selected_companies = st.multiselect(
            "Filter by company",
            options=available_companies,
            default=available_companies,
        )

        selected_years = st.multiselect(
            "Filter by fiscal year",
            options=available_years,
            default=available_years,
        )

        min_confidence = st.select_slider(
            "Minimum confidence to display",
            options=["LOW", "MEDIUM", "HIGH"],
            value="LOW",
        )

        st.markdown("---")
        st.markdown("**About**")
        st.caption(
            "Graph RAG agent for financial document Q&A. "
            "Uses Neo4j knowledge graph with structured fact extraction "
            "and hybrid retrieval for accurate, cited answers."
        )

        # Recent queries
        if st.session_state.query_history:
            st.markdown("---")
            st.markdown("**Recent queries**")
            for item in reversed(st.session_state.query_history[-5:]):
                if st.button(item["query"][:60] + "...", key=f"hist_{item['timestamp']}"):
                    st.session_state["rerun_query"] = item["query"]

    # Main area
    rerun_query = st.session_state.pop("rerun_query", None)
    query = st.text_area(
        "Ask a question about the financial filings",
        value=rerun_query or "",
        placeholder="e.g. How did Company A's gross margin trend from 2020 to 2023?",
        height=100,
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        submit = st.button("Ask", type="primary")
    with col2:
        st.caption(f"{len(query)} characters")

    if submit and query.strip():
        with st.spinner("Retrieving and generating answer..."):
            try:
                response = httpx.post(
                    f"{BACKEND_URL}/query",
                    json={
                        "query": query,
                        "companies": selected_companies,
                        "years": selected_years,
                    },
                    timeout=30.0,
                )
            except httpx.ConnectError:
                st.error("Cannot connect to backend. Is the FastAPI server running?")
                return
            except Exception as e:
                st.error(f"Request failed: {e}")
                return

        if response.status_code == 200:
            data = response.json()

            # Check confidence threshold
            conf_label = data.get("retrieval_confidence", {}).get("label", "LOW")
            conf_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
            if conf_order.get(conf_label, 0) < conf_order.get(min_confidence, 0):
                st.warning(
                    f"Answer confidence ({conf_label}) is below your threshold ({min_confidence}). "
                    "Showing anyway with warning."
                )

            render_answer(data)

            # Save to history
            st.session_state.query_history.append({
                "query": query,
                "response": data,
                "timestamp": datetime.now().isoformat(),
            })

        elif response.status_code == 422:
            st.error(f"Query error: {response.json().get('detail', 'Unknown error')}")
        else:
            st.error(f"Backend error: {response.status_code}")


def render_answer(data: dict):
    """Render the agent's response."""
    # 1. Confidence badge
    confidence = data.get("retrieval_confidence", {})
    label = confidence.get("label", "LOW")
    color = {"HIGH": "green", "MEDIUM": "orange", "LOW": "red"}.get(label, "red")
    st.markdown(
        f"**Confidence:** :{color}[{label}] "
        f"({confidence.get('answered_by_facts', 0)} facts, "
        f"{confidence.get('answered_by_chunks', 0)} text sources, "
        f"{confidence.get('unanswered', 0)} unanswered)"
    )

    # 2. Answer text
    st.markdown("---")
    st.markdown(data.get("answer", "No answer generated."))

    # 3. Unanswerable sub-questions
    unanswerable = data.get("unanswerable_sub_questions", [])
    if unanswerable:
        with st.expander(f"⚠ {len(unanswerable)} sub-question(s) could not be answered", expanded=True):
            for q in unanswerable:
                st.markdown(f"- {q}")

    # 4. Conflicts
    conflicts = data.get("conflicts_detected", [])
    if conflicts:
        with st.expander(f"⚠ {len(conflicts)} data conflict(s) detected", expanded=True):
            for c in conflicts:
                st.warning(c)

    # 5. Citations panel
    st.markdown("---")
    st.markdown("**Sources used:**")
    render_citations(data.get("resolved_citations", []))

    # 6. Debug expander
    with st.expander("Debug: full response"):
        st.json(data)


def render_citations(citations: list[dict]):
    """Render citations grouped by company and year."""
    if not citations:
        st.caption("No citations.")
        return

    groups = defaultdict(list)
    for c in citations:
        groups[(c.get("company", ""), c.get("fiscal_year", 0))].append(c)

    for (company, year), cites in sorted(groups.items()):
        st.markdown(f"**{company} — FY{year}**")
        for c in cites:
            confidence_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(
                c.get("confidence", ""), "⚪"
            )
            with st.expander(
                f"{confidence_icon} `{c.get('key', '')}` · "
                f"{c.get('section_path', '')} · p{c.get('page', '')} · "
                f"{c.get('chunk_type', '')}",
                expanded=False,
            ):
                st.caption(c.get("content_preview", ""))


# ── Page 2: Corpus ───────────────────────────────────────────────────────────

def render_corpus_page():
    st.title("📁 Corpus Management")

    tab1, tab2 = st.tabs(["Ingested Documents", "Ingest New"])

    with tab1:
        render_corpus_tab()

    with tab2:
        render_ingest_tab()


def render_corpus_tab():
    """Show all ingested documents."""
    try:
        response = httpx.get(f"{BACKEND_URL}/corpus", timeout=10.0)
        docs = response.json().get("documents", [])
    except Exception:
        st.error("Cannot connect to backend.")
        return

    if not docs:
        st.info("No documents ingested yet. Go to 'Ingest New' to add documents.")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Documents", len(docs))
    col2.metric("Companies", len(set(d.get("company", "") for d in docs)))
    col3.metric("Years covered", len(set(d.get("fiscal_year", 0) for d in docs)))
    col4.metric("Total chunks", sum(d.get("chunk_count", 0) for d in docs))

    # Documents table
    import pandas as pd

    df = pd.DataFrame(docs)
    display_cols = [c for c in ["company", "ticker", "fiscal_year", "doc_type", "chunk_count", "fact_count", "ingest_timestamp"] if c in df.columns]
    if display_cols:
        df = df[display_cols].sort_values(["company", "fiscal_year"])
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_ingest_tab():
    """Upload and ingest new PDFs."""
    st.markdown("Upload one or more PDF financial filings to add them to the corpus.")

    col1, col2 = st.columns(2)
    with col1:
        company_name = st.text_input("Company name", placeholder="Apple Inc.")
        ticker = st.text_input("Ticker / short ID", placeholder="AAPL")
    with col2:
        fiscal_year = st.number_input("Fiscal year", min_value=2000, max_value=2030, value=2023)
        doc_type_hint = st.selectbox(
            "Document type hint (optional — auto-detected if unsure)",
            options=["auto-detect", "20-F", "10-K", "annual_report", "earnings_release", "other"],
        )

    uploaded_files = st.file_uploader(
        "Upload PDF(s)",
        type=["pdf"],
        accept_multiple_files=True,
    )

    if st.button("Start Ingest", type="primary", disabled=not uploaded_files or not company_name):
        progress = st.progress(0, text="Starting ingest...")
        status = st.empty()

        for i, file in enumerate(uploaded_files):
            status.text(f"Ingesting {file.name}...")
            try:
                response = httpx.post(
                    f"{BACKEND_URL}/ingest",
                    files={"file": (file.name, file.getvalue(), "application/pdf")},
                    data={
                        "company": company_name,
                        "ticker": ticker,
                        "fiscal_year": str(fiscal_year),
                        "doc_type_hint": doc_type_hint if doc_type_hint != "auto-detect" else "",
                    },
                    timeout=300.0,
                )
                progress.progress((i + 1) / len(uploaded_files))

                if response.status_code == 200:
                    result = response.json()
                    st.success(
                        f"{file.name}: {result.get('chunks_created', 0)} chunks, "
                        f"{result.get('facts_created', 0)} facts, "
                        f"{result.get('entities_created', 0)} entities"
                    )
                else:
                    st.error(f"{file.name}: ingest failed — {response.text}")
            except Exception as e:
                st.error(f"{file.name}: error — {e}")

        progress.progress(1.0, text="Ingest complete.")
        st.balloons()


# ── Page Routing ─────────────────────────────────────────────────────────────
page = st.sidebar.radio("Navigation", ["Query", "Corpus"], label_visibility="collapsed")

if page == "Query":
    render_query_page()
elif page == "Corpus":
    render_corpus_page()
