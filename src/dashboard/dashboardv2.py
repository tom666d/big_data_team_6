import streamlit as st
import json
import os
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import subprocess
import sys

# ── Page config ──────────────────────────────────────
st.set_page_config(
    page_title="Data Quality Remediation Assistant",
    page_icon="🔍",
    layout="wide"
)

st.markdown("""
<style>
    [data-testid="stMetric"] {
        text-align: center;
        display: flex;
        flex-direction: column;
        align-items: center;
    }
</style>
""", unsafe_allow_html=True)

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
    "chosen_action": selected_option["action"] if selected_option else "Declined",
    "chosen_rationale": selected_option.get("rationale", "") if selected_option else "",
    "chosen_caveats": selected_option.get("caveats", "") if selected_option else "",
    "chosen_confidence": selected_option.get("confidence", 0) if selected_option else 0,
    "detail_summary": issue["input"]["detail"],
    "pyspark_code": selected_option.get("pyspark_code", "") if selected_option else "",
    "decision_type": action_type,
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}
    
    history.append(new_record)

    # Save back to JSON
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

# ── [UPDATE] Sidebar: Live injection demo ─────
with st.sidebar:
    st.image("src/streamlit_assets/genie20.png", caption="The Data Genie")
    st.header("🚀 Demo Controls")
    st.info("Simulate data quality degradation for the live presentation.")

    # ── Dataset toggle ────────────────────────────────
    demo = st.toggle("Use Demo Dataset", value=True,
                      help="ON = use demo_lendingclub.csv  •  OFF = use your uploaded file")
    
    if st.button("💉 Inject Anomalies (Live Demo)"):
        
        try:
            import pandas as pd
            import numpy as np

            df = pd.read_csv("data/demo_lendingclub.csv")

            # Inject Null Spike into loan_amnt (~40% nulls)
            null_idx = df.sample(frac=0.4, random_state=42).index
            df.loc[null_idx, "loan_amnt"] = np.nan

            # Inject Outliers into annual_inc
            outlier_idx = df.sample(frac=0.08, random_state=99).index
            df.loc[outlier_idx, "annual_inc"] = 9999999

            # Inject Format Inconsistency into issue_d
            if "issue_d" in df.columns:
                mix_idx = df.sample(frac=0.3, random_state=7).index
                df.loc[mix_idx, "issue_d"] = "01/2020"

            df.to_csv("data/demo_lendingclub.csv", index=False)

            st.warning("⚠️ Null Spikes injected into 'loan_amnt'")
            st.warning("⚠️ Outliers injected into 'annual_inc'")
            st.warning("⚠️ Format errors injected into 'issue_d'")
            st.success("✅ Done — now run detector.py to refresh")
            st.toast("Demo data corrupted successfully!")

        except FileNotFoundError:
            st.error("demo_lendingclub.csv not found in data/")
        except Exception as e:
            st.error(f"Injection failed: {e}")

    st.divider()
    st.subheader("System Status")
    st.write("**Role:** Analytics Consultant")
    st.write("**Environment:** Databricks / Local")
    st.write(f"**Dataset Mode:** {'🟢 Demo' if demo else '🔵 Uploaded'}")

col1, col2 = st.columns([1, 8])

with col1:
    # Use st.image for the logo
    st.image("src/streamlit_assets/magiclamp.png")#, width=60)

with col2:
    # Use st.title for the text
    st.title("The Data Genie")

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

    st.subheader("📁 Upload Data File")
    uploaded_file = st.file_uploader("Upload data here:", type=["csv", "json", "parquet"])

    if uploaded_file:
        # Save to the expected location
        os.makedirs("data", exist_ok=True)
        save_path = os.path.join("data", uploaded_file.name)
        with open(save_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.success(f"Saved to {save_path}")

    st.subheader("⚙️ Run Pipeline")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Step 1: 🔎 Run Detector", use_container_width=True):
             with st.spinner("Running detector..."):
                # This version prints all the outputs from the original file
                # More informative, but it does clutter the dashboard a bit
                # I made the other button just use the spinning icon to show what that looks like
                # But either one can easily be changed
                if demo:
                    process = subprocess.Popen(
                    ["python", "-u", "src/detection/detector_dashboard.py", "data/demo_lendingclub.csv"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                else:
                    if not uploaded_file:
                        st.warning("⚠️ No file uploaded — toggle 'Use Demo Dataset' on, or upload a file first.")
                        st.stop()
                    process = subprocess.Popen(
                        ["python", "-u", "src/detection/detector_dashboard.py", save_path],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        text=True,
                    )
                
                output_area = st.empty()
                log = ""
                
                for line in process.stdout:
                    log += line
                    output_area.code(log, language="text")
                
                process.wait()
                
                if process.returncode == 0:
                    st.success("Detector finished!")
                else:
                    st.error(f"Detector failed:\n{process.stderr.read()}")

    with col2:
        if st.button("Step 2: \n💡 Run Suggester", use_container_width=True):
            with st.spinner("Running suggester..."):
                result = subprocess.run(
                    ["python", "src/llm/suggester_dashboard.py"],
                    capture_output=True, text=True
                )
            if result.returncode == 0:
                st.success("Suggester finished!")
                st.code(result.stdout, language="text")
            else:
                st.error(f"Suggester failed:\n{result.stderr}")
                st.code(result.stderr, language="text")
    


    if os.path.exists("data/issues_with_suggestions.json"):
        with open("data/issues_with_suggestions.json") as f:
            issues = json.load(f)
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
                st.caption(f"- Affected rows: {impact.get('affected_rows_percent', 'N/A')}%")

            st.divider()
            st.write("**💡 AI Suggestions:**")

            for s in issue["remediation"]["suggestions"]:
                st.write(f"### Option {s['option']} (Confidence: {s['confidence']}%)")
                
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.info(f"**Action:** {s['action']}")
                    st.write(f"**Rationale:** {s['rationale']}")
                    st.caption(f"⚠️ Caveats: {s.get('caveats', 'N/A')}") 
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