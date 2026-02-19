"""
Streamlit Chat Interface
========================
A simple chat UI for the financial filing query agent.

Run:
    streamlit run frontend/app.py
or:
    python scripts/run_frontend.py
"""

import sys
import os
import logging

# ── Path setup ────────────────────────────────────────────────────────
# Ensure the project root is importable regardless of working directory.
_frontend_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_frontend_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(name)s - %(message)s")
logging.getLogger("neo4j").setLevel(logging.WARNING)

from app.agent.agent import QueryAgent

# ── Page config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Financial Filing Assistant",
    page_icon="📊",
    layout="centered",
)

st.title("📊 Financial Filing Assistant")
st.caption("Ask questions about ingested SEC filings.  Data is retrieved from a Neo4j knowledge graph.")


# ── Session state ─────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    st.session_state.agent = QueryAgent()


def get_agent() -> QueryAgent:
    return st.session_state.agent


# ── Sidebar ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Controls")
    if st.button("🗑️  Clear chat"):
        st.session_state.messages = []
        st.rerun()

    if st.button("📋  Show graph schema"):
        schema = get_agent().retriever.get_schema_summary()
        st.code(schema, language="text")

    st.divider()
    st.markdown("**About**")
    st.markdown(
        "This assistant queries a Neo4j knowledge graph built from "
        "company SEC filings (10-K, 20-F, annual reports).  "
        "It follows a 4-step workflow: *assess → plan → fetch → answer*."
    )


# ── Render chat history ──────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        # Show metadata expander for assistant messages
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
    # Show & record user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Run agent
    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            result = get_agent().query(prompt)

        st.markdown(result["answer"])

        # Inline details
        with st.expander("Details", expanded=False):
            col1, col2 = st.columns(2)
            col1.metric("Confidence", result.get("confidence", "—"))
            col2.metric("Tool calls", len(result.get("tool_calls", [])))

            if result.get("has_sufficient_data") is False:
                st.warning(f"Missing data: {result.get('missing_data', 'unknown')}")

            if result.get("assessment"):
                st.markdown("**Assessment**")
                st.json(result["assessment"])
            if result.get("plan"):
                st.markdown("**Retrieval plan**")
                st.json(result["plan"])
            if result.get("tool_calls"):
                st.markdown("**Tool calls**")
                st.json(result["tool_calls"])

    # Record assistant message
    st.session_state.messages.append({
        "role": "assistant",
        "content": result["answer"],
        "meta": {
            "confidence": result.get("confidence"),
            "has_sufficient_data": result.get("has_sufficient_data"),
            "missing_data": result.get("missing_data"),
            "assessment": result.get("assessment"),
            "plan": result.get("plan"),
            "tool_calls": result.get("tool_calls", []),
        },
    })
