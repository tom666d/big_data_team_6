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

# ── [UPDATE] Summary metrics ──────────────────────────────────
st.divider()
col1, col2, col3, col4 = st.columns(4) # Column added
col1.metric("Issues Detected", len(issues))
col2.metric("High Severity", sum(1 for i in issues if i["severity"] == "HIGH"))

# ── [UPDATE] Quality metrics added ──────────────────────────────────
col3.metric("Avg Quality Score (Before)", "65%", "-5%") 
col4.metric("Target Quality Score (After)", "92%", "+27%")
st.divider()

# ── Session state for decisions ──────────────────────
if "decisions" not in st.session_state:
    st.session_state.decisions = {}

# ── Issue cards ──────────────────────────────────────
st.subheader("📋 Detected Issues")

for i, issue in enumerate(issues):
    color = "🔴" if issue["severity"] == "HIGH" else "🟡"
    
    # [UPDATE] Display priority score in the header
    priority = issue.get("priority_score", "N/A")
    with st.expander(
        f"{color} [P{priority}] {issue['column']} — {issue['issue_type']}",
        expanded=True
    ):
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.write(f"**Detail:** {issue['detail']}")
            st.write(f"**Sample Values:** `{issue['sample_values']}`")
        with col_info2:
            # [UPDATE] Impact score analysis section added
            st.write("**Impact Analysis:**")
            impact = issue.get("impact_score", {})
            st.caption(f"- Business Risk: {impact.get('business_risk', 'Medium')}")
            st.caption(f"- Downstream Reach: {impact.get('reach', 'High')}")

        st.divider()
        st.write("**💡 AI Suggestions:**")

        for s in issue["suggestions"]:
            # [UPDATE] Adjust layout to accommodate PySpark code and rationale
            st.write(f"### Option {s['option']} (Confidence: {s['confidence']}%)")
            
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.info(f"**Action:** {s['action']}")
                st.write(f"**Rationale:** {s['rationale']}")
                
                # [UPDATE] PySpark code block added
                st.write("**Generated PySpark Code:**")
                st.code(s.get("pyspark_code", "# Code not generated yet"), language="python")
                
            with col_b:
                if st.button(
                    "✅ Approve & Stage",
                    key=f"accept_{i}_{s['option']}",
                    use_container_width=True
                ):
                    st.session_state.decisions[issue["column"]] = {
                        "action": s["action"],
                        "code": s.get("pyspark_code", ""),
                        "confidence": s["confidence"]
                    }
                    st.success("Decision Logged!")

# ── [UPDATE] Decision log (Outcome tracking) ─────────────
if st.session_state.decisions:
    st.divider()
    st.subheader("📝 Approved Remediation Plan (Audit Trail)")
    # [UPDATE] Display decisions in a structured table format
    decision_data = []
    for col_name, dec in st.session_state.decisions.items():
        decision_data.append({"Column": col_name, "Action": dec['action'], "Confidence": f"{dec['confidence']}%"})
    st.table(decision_data)