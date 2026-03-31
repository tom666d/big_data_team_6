import streamlit as st
import json
import os

# ── Page config ──────────────────────────────────────
st.set_page_config(
    page_title="Data Quality Remediation Assistant",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Data Quality Remediation Assistant")
st.caption("AI-Driven Anomaly Detection & ETL Fix Generation at Scale")

# ── Load issues ──────────────────────────────────────
try:
    with open("data/issues_with_suggestions.json") as f:
        issues = json.load(f)
except FileNotFoundError:
    st.error("Please run detector.py and suggester.py first")
    st.stop()

# ── Summary metrics ──────────────────────────────────
st.divider()
col1, col2, col3 = st.columns(3)
col1.metric("Issues Detected", len(issues))
col2.metric("High Severity", sum(1 for i in issues if i["severity"] == "HIGH"))
col3.metric("Medium Severity", sum(1 for i in issues if i["severity"] == "MEDIUM"))
st.divider()

# ── Session state for decisions ──────────────────────
if "decisions" not in st.session_state:
    st.session_state.decisions = {}

# ── Issue cards ──────────────────────────────────────
st.subheader("📋 Detected Issues")

for i, issue in enumerate(issues):
    color = "🔴" if issue["severity"] == "HIGH" else "🟡"
    with st.expander(
        f"{color} [{issue['severity']}] {issue['column']} — {issue['issue_type']}",
        expanded=True
    ):
        st.write(f"**Detail:** {issue['detail']}")
        st.write(f"**Sample Values:** `{issue['sample_values']}`")
        st.divider()
        st.write("**💡 AI Suggestions:**")

        for s in issue["suggestions"]:
            col_a, col_b = st.columns([4, 1])
            with col_a:
                st.write(f"**Option {s['option']}** (Confidence: {s['confidence']}%)")
                st.write(f"✏️ `{s['action']}`")
                st.caption(s["rationale"])
            with col_b:
                if st.button(
                    "✅ Accept",
                    key=f"accept_{i}_{s['option']}"
                ):
                    st.session_state.decisions[issue["column"]] = {
                        "action": s["action"],
                        "confidence": s["confidence"]
                    }
                    st.success("Accepted!")

# ── Decision log ─────────────────────────────────────
if st.session_state.decisions:
    st.divider()
    st.subheader("📝 Remediation Decisions")
    for col_name, decision in st.session_state.decisions.items():
        st.write(f"- **{col_name}** → `{decision['action']}` (confidence: {decision['confidence']}%)")