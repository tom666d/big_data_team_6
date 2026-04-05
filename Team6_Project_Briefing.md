# Team 6 — Data Quality Remediation Assistant
## Project Briefing

---

## What We're Building

A system that **automatically detects data quality issues** in large datasets using Apache Spark, then uses **AI agents to generate runnable fix code**, and lets a human analyst review and approve everything through a dashboard.

> "Bad data in → AI diagnoses the problem → generates PySpark fix code → human approves → clean data out"

---

## Current Status

The **End-to-End Skeleton is complete and running.** All three core components are connected and working on local machine. Next step is to upgrade each component based on professor feedback, then move to Databricks with real datasets.

---

## File Structure

```
big_data_team_6/
├── src/
│   ├── detection/
│   │   └── detector.py       ← Spark scans data, flags issues
│   ├── llm/
│   │   └── suggester.py      ← GPT generates fix suggestions
│   └── dashboard/
│       └── dashboard.py      ← Streamlit UI for human review
├── data/
│   ├── sample.csv                     ← Sample data (will be replaced)
│   ├── issues_output.json             ← detector.py output
│   └── issues_with_suggestions.json  ← suggester.py output
├── .env                      ← API keys (NOT on GitHub)
└── .gitignore
```

### How the 3 files connect

```
detector.py → issues_output.json → suggester.py → issues_with_suggestions.json → dashboard.py
```

Each file has a clear input and output. **You only need to work on your assigned file.**

---

## System Architecture

```
┌─────────────────────────────────────────────────────┐
│                    DATA SOURCES                      │
│   Lending Club (2.5M) + Chicago Payments (5M)       │
│              + NYSE Historical (3M)                  │
└──────────────────────┬──────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│              LAYER 1: DETECTION                      │
│                  detector.py                         │
│         Apache Spark / PySpark                       │
│   • Null Spike Detection (z-score)                   │
│   • Statistical Outlier Detection (IQR)              │
│   • Compute Data Quality Score                       │
│   Output: issues_output.json                         │
└──────────────────────┬──────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│              LAYER 2: AI AGENTS                      │
│                 suggester.py                         │
│            GPT-4o-mini (OpenAI API)                  │
│                                                      │
│   Agent 1 — Diagnosis                               │
│   "What is the root cause? How urgent? Impact?"      │
│         ↓ passes diagnosis to Agent 2                │
│   Agent 2 — Remediation                             │
│   "Generate runnable PySpark fix code"               │
│                                                      │
│   Output: priority score + impact score + code       │
└──────────────────────┬──────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│              LAYER 3: HUMAN REVIEW                   │
│                 dashboard.py                         │
│                   Streamlit                          │
│   • View detected issues + AI suggestions            │
│   • See priority score + business impact             │
│   • Copy runnable PySpark fix code                   │
│   • Accept / Reject each suggestion                  │
│   • Track Before/After quality score                 │
└──────────────────────┬──────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│              LAYER 4: AUDIT TRAIL                    │
│          Delta Lake / Snowflake                      │
│   • All decisions logged                             │
│   • Quality score history                            │
│   • LLM inputs/outputs recorded                      │
└─────────────────────────────────────────────────────┘
```

---

## Team Roles & Tasks

| Role | Main File | Key Tasks |
|------|-----------|-----------|
| **LLM Engineer** | `suggester.py` | Upgrade to 2-step agentic flow, add Priority Score + Impact Score, generate PySpark fix code |
| **Dashboard Developer** | `dashboard.py` | Display priority/impact scores, show PySpark code block, add Before/After quality score, add Live Demo button |
| **Data Engineer** | `detector.py` | Switch to real Lending Club dataset, focus on Null Spike + Outlier, compute Quality Score, set up Databricks |
| **Integration & QA** | All files | Connect all 3 modules, error handling, full pipeline testing, edge cases |
| **Business & Presentation** | Docs | README, 2-page handout, demo script, prepare for evaluator Q&A |

---

## What Each Person Needs to Do This Week

**LLM Engineer (Most urgent)**
- Upgrade `suggester.py` from single GPT call → 2-step agent
- Agent 1 outputs: root cause, priority score (1-10), affected % of records
- Agent 2 takes Agent 1 output → generates runnable PySpark fix code
- Test that JSON output format stays consistent

**Dashboard Developer**
- Add Priority Score + Impact Score display to each issue card
- Add PySpark code block (copyable) under each suggestion
- Add Quality Score metric (before/after placeholder for now)

**Data Engineer**
- Download Lending Club dataset from Kaggle (start with 100K rows)
- Update column names in `detector.py` to match real dataset
- Keep focus on 2 issue types: Null Spike + Statistical Outlier
- Email Carlson IT today to request Databricks resources

**Integration & QA**
- Make sure all 3 files still connect after upgrades
- Test full pipeline end-to-end after each teammate's changes
- Handle edge cases: what if GPT API fails? What if data is empty?

**Business & Presentation**
- Update README with architecture description
- Draft demo narrative (the "story" we tell evaluators)
- Research: what do companies like Monte Carlo, Databricks do? (for opening pitch)

---

## Timeline

```
Week 1 (Now)    → Upgrade skeleton based on professor feedback
Week 2          → Real datasets + Before/After tracking + Databricks
Week 3          → Live Demo button + Streaming + Final polish
Week 4          → Demo rehearsal + Handout + GitHub cleanup
```

---

## How to Run (Local)

```bash
# Install dependencies
pip install pyspark openai streamlit pandas python-dotenv

# Step 1: Detect issues
python src/detection/detector.py

# Step 2: Generate AI suggestions
python src/llm/suggester.py

# Step 3: Launch dashboard
streamlit run src/dashboard/dashboard.py
```

Make sure you have a `.env` file in the root directory:
```
OPENAI_API_KEY=your_key_here
```

---

## Key Design Principles

**Why 2-step agents?**
Each agent does one job well. Diagnosis Agent understands the problem deeply. Remediation Agent uses that understanding to generate accurate fix code. Better output quality than asking one LLM to do everything at once.

**Why Human-in-the-Loop?**
No fix is ever executed automatically. Every suggestion requires analyst approval. This is both a safety feature and a key differentiator — most automated tools don't have this.

**Why grounded prompts?**
LLM only sees concrete statistics (null rate %, sample values, column type). It cannot hallucinate because it has no room to speculate — every suggestion must be based on the numbers provided.

---

*GitHub: https://github.com/tom666d/big_data_team_6*
