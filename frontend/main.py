"""
Streamlit Chat Interface
========================
A single chat UI with a backend selector (Graph RAG / PageIndex).

Run:
    streamlit run frontend/main.py
or:
    python scripts/run_frontend.py
"""

import sys
import os
import logging

# ── Path setup ────────────────────────────────────────────────────────
_frontend_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_frontend_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(name)s - %(message)s")
logging.getLogger("neo4j").setLevel(logging.WARNING)

from app.backends.graphrag.agent import QueryAgent as GraphRAGAgent
from app.backends.pageindex.agent import PageIndexAgent

# ── Constants ─────────────────────────────────────────────────────────
BACKENDS = {
    "Graph RAG": {
        "class": GraphRAGAgent,
        "icon": "🕸️",
        "description": (
            "Queries a Neo4j knowledge graph built from company SEC filings. "
            "Uses a 4-step agentic workflow: *assess → plan → fetch → answer*."
        ),
    },
    "PageIndex": {
        "class": PageIndexAgent,
        "icon": "📑",
        "description": (
            "Uses PageIndex JSON table-of-contents for page-level retrieval. "
            "*(Skeleton — not yet implemented.)*"
        ),
    },
}


# ── Page config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Financial Filing Assistant",
    page_icon="📊",
    layout="centered",
)

st.title("📊 Financial Filing Assistant")
st.caption("Ask questions about company financial filings across multiple years.")


# ── Session state ─────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []

if "active_backend" not in st.session_state:
    st.session_state.active_backend = None

if "agent" not in st.session_state:
    st.session_state.agent = None


def _init_agent(backend_name: str):
    """Instantiate (or re-instantiate) the agent for the chosen backend."""
    # Close existing agent if switching
    if st.session_state.agent is not None:
        try:
            st.session_state.agent.close()
        except Exception:
            pass

    cfg = BACKENDS[backend_name]
    st.session_state.agent = cfg["class"]()
    st.session_state.active_backend = backend_name


# ── Sidebar ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Backend")

    backend_choice = st.radio(
        "Retrieval approach",
        list(BACKENDS.keys()),
        index=0,
        format_func=lambda name: f"{BACKENDS[name]['icon']}  {name}",
    )

    # (Re-)init agent when selection changes
    if st.session_state.active_backend != backend_choice:
        _init_agent(backend_choice)
        # Clear chat when switching backends
        st.session_state.messages = []

    st.markdown(BACKENDS[backend_choice]["description"])

    st.divider()
    st.header("Controls")

    if st.button("🗑️  Clear chat"):
        st.session_state.messages = []
        st.rerun()

    if backend_choice == "Graph RAG" and st.button("📋  Show graph schema"):
        schema = st.session_state.agent.retriever.get_schema_summary()
        st.code(schema, language="text")

    st.divider()
    st.markdown(
        "**About**\n\n"
        "This assistant answers questions about company financial filings "
        "(10-K, 20-F, annual reports) using one of the selectable retrieval "
        "backends above."
    )


# ── Render chat history ──────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        if msg["role"] == "assistant" and msg.get("meta"):
            meta = msg["meta"]
            with st.expander("Details", expanded=False):
                col1, col2 = st.columns(2)
                col1.metric("Confidence", meta.get("confidence", "—"))
                col2.metric("Tool calls", len(meta.get("tool_calls", [])))

                if meta.get("has_sufficient_data") is False:
                    st.warning(f"Missing data: {meta.get('missing_data', 'unknown')}")

                if meta.get("assessment"):
                    st.markdown("**Assessment**")
                    st.json(meta["assessment"])
                if meta.get("plan"):
                    st.markdown("**Retrieval plan**")
                    st.json(meta["plan"])
                if meta.get("tool_calls"):
                    st.markdown("**Tool calls**")
                    st.json(meta["tool_calls"])


# ── Handle user input ────────────────────────────────────────────────
if prompt := st.chat_input("Ask a question about the filings…"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            result = st.session_state.agent.query(prompt)

        answer = result.get("answer", "(no answer)")
        st.markdown(answer)

        # Inline details (Graph RAG provides richer metadata)
        meta = {
            "confidence": result.get("confidence"),
            "has_sufficient_data": result.get("has_sufficient_data"),
            "missing_data": result.get("missing_data"),
            "assessment": result.get("assessment"),
            "plan": result.get("plan"),
            "tool_calls": result.get("tool_calls", []),
        }

        with st.expander("Details", expanded=False):
            col1, col2 = st.columns(2)
            col1.metric("Confidence", meta.get("confidence", "—"))
            col2.metric("Tool calls", len(meta.get("tool_calls", [])))

            if meta.get("has_sufficient_data") is False:
                st.warning(f"Missing data: {meta.get('missing_data', 'unknown')}")

            if meta.get("assessment"):
                st.markdown("**Assessment**")
                st.json(meta["assessment"])
            if meta.get("plan"):
                st.markdown("**Retrieval plan**")
                st.json(meta["plan"])
            if meta.get("tool_calls"):
                st.markdown("**Tool calls**")
                st.json(meta["tool_calls"])

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "meta": meta,
    })
