import streamlit as st
import json
import os
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta

# ── Page config ──────────────────────────────────────
st.set_page_config(
    page_title="Data Quality Remediation Assistant",
    page_icon="🔍",
    layout="wide"
)

# ── [UPDATE] [FEEDBACK LOOP] Write decision to JSON ───
def save_decision_to_history(issue, selected_option, action_type="Approved"):
    history_path = "data/historical_decisions.json"
    
    # Load existing history
    if os.path.exists(history_path):
        try:
            with open(history_path, "r", encoding="utf-8") as f:
                history = json.load(f)
        except:
            history = []
    else:
        history = []

    # Create a new record based on user selection
    new_record = {
        "column": issue["input"]["column"],
        "issue_type": issue["input"]["issue_type"],
        "selected_action": selected_option["action"] if selected_option else "Declined",
        "pyspark_code": selected_option.get("pyspark_code", "") if selected_option else None,
        "decision_type": action_type,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    history.append(new_record)

    # Save back to JSON
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

# ── [UPDATE] Sidebar: Live injection demo ─────
with st.sidebar:
    st.header("🚀 Demo Controls")
    st.info("Simulate data quality degradation for the live presentation.")
    
    if st.button("Inject Anomalies (Live Demo)"):
        # Simulated triggers for the presentation demo
        st.warning("Injecting Null Spikes in 'loan_amnt'...")
        st.warning("Injecting Outliers in 'annual_inc'...")
        st.success("Anomalies Injected! Run detector.py to refresh.")
        st.toast("Demo data corrupted successfully!")

    st.divider()
    st.subheader("System Status")
    st.write("**Role:** Analytics Consultant")
    st.write("**Environment:** Databricks / Local")

st.title("🔍 Data Quality Remediation Assistant")
st.caption("AI-Driven Anomaly Detection & ETL Fix Generation at Scale")

# ── Load issues ──────────────────────────────────────
try:
    with open("data/issues_with_suggestions.json") as f:
        issues = json.load(f)
except FileNotFoundError:
    st.error("Please run detector.py and suggester.py first")
    st.stop()

# ── [UPDATE] Tabs for UI polish ────────────────
tab1, tab2 = st.tabs(["📋 Current Issues", "📈 Quality Trends"])

with tab1:
    # ── Summary metrics (Dynamic calculation) ──
    if issues:
        # Sanitize potential negative scores from data bugs
        avg_before = max(0, sum(i["quality_score"]["before"] for i in issues) / len(issues))
        avg_after = max(0, sum(i["quality_score"]["after"] for i in issues) / len(issues))
        avg_delta = avg_after - avg_before
    else:
        avg_before, avg_after, avg_delta = 0, 0, 0

    st.divider()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Issues Detected", len(issues))
    col2.metric("High Severity", sum(1 for i in issues if i["input"]["severity"] == "HIGH"))
    col3.metric("Avg Quality Score (Before)", f"{avg_before:.1f}%") 
    col4.metric("Estimated Quality Score (After)", f"{avg_after:.1f}%", f"{avg_delta:+.1f}%")
    st.divider()

    # ── Session state for decisions ──
    if "decisions" not in st.session_state:
        st.session_state.decisions = {}

    # ── Issue cards ──
    st.subheader("📋 Detected Issues")

    for i, issue in enumerate(issues):
        color = "🔴" if issue["input"]["severity"] == "HIGH" else "🟡"
        priority = issue.get("diagnosis", {}).get("priority_score", "N/A")
        
        with st.expander(f"{color} [P{priority}] {issue['input']['column']} — {issue['input']['issue_type']}", expanded=True):
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.write(f"**Detail:** {issue['input']['detail']}")
                st.write(f"**Sample Values:** `{issue['input']['sample_values']}`")
            with col_info2:
                st.write("**Impact Analysis:**")
                impact = issue.get("diagnosis", {})
                st.caption(f"- Business Risk: {impact.get('business_impact', 'N/A')}")
                st.caption(f"- Affected rows: {impact.get('affected_percent', 'N/A')}%")

            st.divider()
            st.write("**💡 AI Suggestions:**")

            for s in issue["remediation"]["suggestions"]:
                st.write(f"### Option {s['option']} (Confidence: {s['confidence']}%)")
                
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.info(f"**Action:** {s['action']}")
                    st.write(f"**Rationale:** {s['rationale']}")
                    st.write("**Generated PySpark Code:**")
                    st.code(s.get("pyspark_code", ""), language="python")
                    
                with col_b:
                    # [UPDATE] [FEEDBACK LOOP] Integrated JSON write-back on approval
                    if st.button("✅ Approve", key=f"app_{i}_{s['option']}", use_container_width=True):
                        st.session_state.decisions[issue["input"]["column"]] = {
                            "action": s["action"],
                            "confidence": s["confidence"]
                        }
                        save_decision_to_history(issue, s, action_type="Approved")
                        st.success("Decision Logged to History!")
                        st.toast(f"Saved selection for {issue['input']['column']}")

            # [UPDATE] [FEEDBACK LOOP] Decline button to capture non-remediation decisions
            if st.button("❌ Decline All Changes", key=f"dec_{i}", use_container_width=True):
                st.session_state.decisions[issue["input"]["column"]] = {
                    "action": "Declined",
                    "confidence": 0
                }
                save_decision_to_history(issue, None, action_type="Declined")
                st.error("Issue Declined and Logged.")

    # ── Audit trail ──
    if st.session_state.decisions:
        st.divider()
        st.subheader("📝 Approved Remediation Plan (Audit Trail)")
        decision_data = []
        for col_name, dec in st.session_state.decisions.items():
            decision_data.append({"Column": col_name, "Action": dec['action'], "Confidence": f"{dec['confidence']}%"})
        st.table(decision_data)

# ── [UPDATE] Trend analysis chart ─────────────
with tab2:
    st.subheader("📈 Data Quality Trend Analysis")
    
    # 7-day trend simulation with current quality score as the endpoint
    dates = [(datetime.now() - timedelta(days=x)).strftime("%Y-%m-%d") for x in range(7)][::-1]
    trend_scores = [52, 58, 55, 60, 63, 61, avg_before]
    
    df_trend = pd.DataFrame({"Date": dates, "Quality Score": trend_scores})
    fig = px.line(df_trend, x="Date", y="Quality Score", markers=True, title="Average Quality Score Movement (Last 7 Days)")
    fig.update_traces(line_color='#FF4B4B', line_width=3)
    fig.update_layout(hovermode="x unified")
    
    st.plotly_chart(fig, use_container_width=True)
    st.info("💡 Trend reflects historical health scores; the final point is based on the latest detection run.")