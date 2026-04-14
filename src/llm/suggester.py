import json
import os
import re
from pathlib import Path
from typing import Any

import openai
from dotenv import load_dotenv

# =========================
# Path and environment setup
# =========================
project_root = Path(__file__).resolve().parents[2]
load_dotenv(project_root / ".env")

client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

input_path = project_root / "data" / "issues_output.json" # change #issues_output
output_path = project_root / "data" / "issues_with_suggestions.json"
historical_path = project_root / "data" / "historical_decisions.json"

# Temporary assumption for sample dataset
DEFAULT_TOTAL_ROWS = 10


# =========================
# Load detector output
# =========================
with open(input_path, "r", encoding="utf-8") as f:
    issues = json.load(f)

print(f"📂 Loaded {len(issues)} issue(s) from issues_output.json\n")


# =========================
# Helper functions
# =========================
def extract_affected_rows(detail: str) -> int | None:
    """
    Try to extract affected row count from issue detail text.

    Supported patterns:
    1. 'Null rate: 40% (4/10 rows)' -> 4
    2. '1 outlier(s) detected ...' -> 1
    3. 'Mixed formats: 7 rows as YYYY-MM-DD, 3 rows as MM/DD/YYYY' -> 3
       (take the smaller inconsistent group as affected rows)
    """
    if not detail:
        return None

    # Pattern like (4/10 rows)
    match_fraction = re.search(r"\((\d+)\s*/\s*(\d+)\s*rows\)", detail)
    if match_fraction:
        return int(match_fraction.group(1))

    # Pattern like "1 outlier(s) detected"
    match_outlier = re.search(r"(\d+)\s+outlier\(s\)", detail, re.IGNORECASE)
    if match_outlier:
        return int(match_outlier.group(1))

    # Pattern like "7 rows as ..., 3 rows as ..."
    row_counts = re.findall(r"(\d+)\s+rows", detail, re.IGNORECASE)
    if row_counts:
        counts = [int(x) for x in row_counts]
        return min(counts)

    return None


def compute_affected_percent(detail: str, total_rows: int = DEFAULT_TOTAL_ROWS) -> float | None:
    """
    Compute affected percent using affected_rows / total_rows * 100.
    """
    affected_rows = extract_affected_rows(detail)
    if affected_rows is None or total_rows <= 0:
        return None

    percent = (affected_rows / total_rows) * 100
    return round(percent, 2)


def severity_to_score(severity: str) -> int:
    """
    Map detector severity to a 1-10 style base score.
    """
    severity_map = {
        "LOW": 3,
        "MEDIUM": 6,
        "HIGH": 9,
    }
    return severity_map.get(str(severity).upper(), 5)


def affected_percent_to_score(affected_percent: float | None) -> int:
    """
    Convert affected percent to a 1-10 style score.
    """
    if affected_percent is None:
        return 5
    if affected_percent < 5:
        return 1
    if affected_percent < 15:
        return 3
    if affected_percent < 30:
        return 5
    if affected_percent < 50:
        return 7
    return 9


def compute_priority_score(severity: str, affected_percent: float | None) -> int:
    """
    Priority score = 0.6 * severity_score + 0.4 * affected_score
    Final score is rounded and clipped to 1-10.
    """
    severity_score = severity_to_score(severity)
    affected_score = affected_percent_to_score(affected_percent)

    weighted_score = round(0.6 * severity_score + 0.4 * affected_score)
    weighted_score = max(1, min(10, weighted_score))
    return weighted_score

    # Week 2 temporary assumption:
    # selected remediation fully resolves the current issue
    after_rows = 0
    after_percent = 0.0 if total_rows > 0 else None

    chosen_action = None
    suggestions = remediation.get("suggestions", [])
    for s in suggestions:
        if s.get("option") == 1:
            chosen_action = s.get("action")
            break
    if chosen_action is None and suggestions:
        chosen_action = suggestions[0].get("action")

    return {
        "before": {
            "affected_rows": affected_rows,
            "affected_percent": before_percent,
            "detail": issue.get("detail"),
            "sample_values": issue.get("sample_values")
        },
        "after": {
            "affected_rows": after_rows,
            "affected_percent": after_percent,
            "detail": f"Expected issue resolved after applying: {chosen_action}"
        },
        "comparison_summary": {
            "affected_rows_delta": after_rows - affected_rows,
            "affected_percent_delta": round(after_percent - before_percent, 2) if before_percent is not None else None,
            "chosen_action": chosen_action
        }
    }

# =========================
# Week 2: Data Quality Score
# =========================
# Week 2 temporary assumption:
# the sample dataset has 10 rows and 4 total columns,
# so total_cells = 40.
#
# In the future, total_rows and total_columns should not be hardcoded.
# They should be read from detector metadata or from the source dataframe shape.
DEFAULT_TOTAL_ROWS = 10
DEFAULT_TOTAL_COLUMNS = 4


def compute_total_cells(
    total_rows: int = DEFAULT_TOTAL_ROWS,
    total_columns: int = DEFAULT_TOTAL_COLUMNS
) -> int:
    """
    Week 2 temporary assumption for the sample dataset:
    - total_rows = 10
    - total_columns = 4

    Therefore:
    total_cells = 10 * 4 = 40

    Future improvement:
    this should be replaced by metadata from the detector output
    or by directly reading the source dataframe shape.
    """
    return total_rows * total_columns


def compute_total_problem_cells(issues_list: list[dict[str, Any]]) -> int:
    """
    Sum affected rows across all issues.
    We treat one issue affecting one column, so affected rows = affected cells for that issue.
    """
    total_problem_cells = 0
    for issue in issues_list:
        affected_rows = extract_affected_rows(issue.get("detail", ""))
        if affected_rows is not None:
            total_problem_cells += affected_rows
    return total_problem_cells


def compute_quality_scores_for_issue(
    current_issue: dict[str, Any],
    issues_list: list[dict[str, Any]],
    total_rows: int = DEFAULT_TOTAL_ROWS,
    total_columns: int = DEFAULT_TOTAL_COLUMNS
) -> dict[str, float]:
    """
    Cell-level quality score logic:

    before = good_cells_before / total_cells
    after  = good_cells_after / total_cells
    delta  = after - before

    Assumption:
    - current issue fix successfully resolves all affected cells of this issue
    - other issues remain unchanged
    """
    total_cells = compute_total_cells(total_rows, total_columns)
    total_problem_cells_before = compute_total_problem_cells(issues_list)

    current_issue_affected = extract_affected_rows(current_issue.get("detail", "")) or 0

    good_cells_before = total_cells - total_problem_cells_before
    good_cells_after = total_cells - max(0, total_problem_cells_before - current_issue_affected)

    before = round((good_cells_before / total_cells) * 100, 2) if total_cells > 0 else 0.0
    after = round((good_cells_after / total_cells) * 100, 2) if total_cells > 0 else 0.0
    delta = round(after - before, 2)

    return {
        "before": before,
        "after": after,
        "delta": delta,
    }


# =========================
# Week 2: Historical Feedback
# =========================
def load_historical_feedback(path: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Load historical decisions grouped by issue_type.
    Return empty dict if file does not exist, is empty, or is invalid JSON.
    """
    if not path.exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except json.JSONDecodeError:
        return {}


def save_historical_feedback(path: Path, history: dict[str, list[dict[str, Any]]]) -> None:
    """
    Save historical decisions grouped by issue_type.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def format_historical_context(issue_type: str, history: dict[str, list[dict[str, Any]]]) -> str:
    """
    Build short historical context for the current issue type only.
    """
    records = history.get(issue_type, [])
    if not records:
        return "No historical decisions are available for this issue type."

    lines = []
    for idx, record in enumerate(records[-5:], start=1):
       lines.append(
            f"{idx}. Column: {record.get('column')}; "
            f"Chosen Action: {record.get('chosen_action')}; "
            f"Rationale: {record.get('chosen_rationale')}; "
            f"Caveats: {record.get('chosen_caveats')}; "
            f"Confidence: {record.get('chosen_confidence')}; "
            f"Detail Summary: {record.get('detail_summary')}"
        )

    return "\n".join(lines)


def append_default_historical_decision(
    history: dict[str, list[dict[str, Any]]],
    issue: dict[str, Any],
    remediation: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """
    Week 2 temporary assumption:
    - automatically treat suggestion option 1 as the chosen action
    """
    issue_type = issue["issue_type"]
    suggestions = remediation.get("suggestions", [])

    chosen = None
    for suggestion in suggestions:
        if suggestion.get("option") == 1:
            chosen = suggestion
            break

    if chosen is None and suggestions:
        chosen = suggestions[0]

    if chosen is None:
        return history

def append_default_historical_decision(
    history: dict[str, list[dict[str, Any]]],
    issue: dict[str, Any],
    remediation: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """
    Week 2 temporary assumption:
    - automatically treat suggestion option 1 as the chosen action
    """
    issue_type = issue["issue_type"]
    suggestions = remediation.get("suggestions", [])

    chosen = None
    for suggestion in suggestions:
        if suggestion.get("option") == 1:
            chosen = suggestion
            break

    if chosen is None and suggestions:
        chosen = suggestions[0]

    if chosen is None:
        return history

    record = {
        "column": issue["column"],
        "chosen_option": chosen.get("option"),
        "chosen_action": chosen.get("action"),
        "chosen_rationale": chosen.get("rationale"),
        "chosen_caveats": chosen.get("caveats"),
        "chosen_confidence": chosen.get("confidence"),
        "detail_summary": issue.get("detail")
    }

    if issue_type not in history:
        history[issue_type] = []

    history[issue_type].append(record)
    return history


# =========================
# Agent 1: Root Cause only
# =========================
def diagnose_root_cause(issue: dict[str, Any]) -> dict[str, Any]:
    """
    Agent 1:
    Use LLM to generate root_cause and business_impact.
    Other diagnosis fields are computed by Python logic.
    """
    prompt = f"""
You are Agent 1 in a two-agent data quality remediation workflow.

Your task is to analyze the issue and return:
- likely root cause
- likely business impact

Issue input:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Instructions:
- Explain the likely upstream or operational cause.
- Do NOT simply repeat the issue type.
- Be specific and concise.
- business_impact should explain the likely downstream impact on analytics, reporting, or modeling.
- Keep both fields concise (1-2 short sentences each).
- Return JSON only.

Return exactly this format:
{{
  "root_cause": "string",
  "business_impact": "string"
}}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a precise data quality diagnosis agent. Return valid JSON only."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return json.loads(response.choices[0].message.content)


# =========================
# Agent 2: Remediation
# =========================
def generate_remediation(
    issue: dict[str, Any],
    diagnosis: dict[str, Any],
    historical_context: str
) -> dict[str, Any]:
    """
    Agent 2:
    Input = detector issue + Agent 1 diagnosis + historical feedback
    Output = remediation suggestions with runnable PySpark fix code
    """
    prompt = f"""
You are Agent 2 in a two-agent data quality remediation workflow.

Your task is to generate remediation suggestions with runnable PySpark fix code,
based on the original issue, Agent 1 diagnosis, and historical remediation preferences.

Original issue:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Agent 1 diagnosis:
Root Cause: {diagnosis['root_cause']}
Business Impact: {diagnosis['business_impact']}
Priority Score: {diagnosis['priority_score']}
Affected Percent: {diagnosis['affected_percent']}

Historical decisions for this issue type:
{historical_context}

Requirements:
1. Generate exactly 2 remediation suggestions.
2. Each suggestion must include:
   - option
   - action
   - confidence
   - rationale
   - caveats
   - pyspark_code

Field rules:
- option: integer (1 or 2)
- action: short remediation title
- confidence: integer from 0 to 100
- rationale: one or two concise sentences
- caveats: one concise sentence describing assumptions, limitations, or risks
- pyspark_code:
  - must be runnable PySpark-style code
  - must reference a dataframe named df
  - must be relevant to the issue
  - must not be pseudo-code
  - must be copyable as a code block string

Confidence definition:
- 80–100: highly reliable, standard practice, safe to apply directly
- 60–79: generally valid but may depend on data context
- 40–59: usable but has clear trade-offs or risks
- below 40: weak, risky, or not recommended in most cases

Confidence scoring guidance:
- Give higher confidence to standard, widely used, low-risk fixes
- Give lower confidence if the method may drop rows, distort values, or require strong assumptions
- If the fix is conservative and preserves data safely, confidence should usually be higher
- If the fix may cause data loss, confidence should usually be lower

Important:
- The suggestions must be informed by Agent 1 diagnosis.
- Use historical decisions as preference signals when reasonable.
- If similar issue types were solved before, prefer consistent remediation strategies unless there is a strong reason not to.
- Historical decisions should influence both action selection and confidence scoring.
- Avoid generic advice with no executable action.
- Make the code practical and directly tied to the issue.
- Return JSON only.

Return exactly this format:
{{
  "suggestions": [
    {{
      "option": 1,
      "action": "string",
      "confidence": 85,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "string"
    }},
    {{
      "option": 2,
      "action": "string",
      "confidence": 65,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "string"
    }}
  ]
}}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a PySpark remediation agent. Return valid JSON only."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return json.loads(response.choices[0].message.content)


# =========================
# Build layered output
# =========================
def build_output_record(
    issue: dict[str, Any],
    diagnosis: dict[str, Any],
    remediation: dict[str, Any],
    quality_score: dict[str, float]
) -> dict[str, Any]:
    return {
        "input": {
            "column": issue["column"],
            "issue_type": issue["issue_type"],
            "severity": issue["severity"],
            "detail": issue["detail"],
            "sample_values": issue["sample_values"]
        },
        "diagnosis": {
            "root_cause": diagnosis["root_cause"],
            "business_impact": diagnosis["business_impact"],
            "priority_score": diagnosis["priority_score"],
            "affected_percent": diagnosis["affected_percent"]
        },
        "remediation": {
            "suggestions": remediation.get("suggestions", [])
        },
        "quality_score": quality_score
    }


# =========================
# Main pipeline
# =========================
print("🤖 Running Week 2 Agent pipeline...\n")

# historical_feedback = load_historical_feedback(historical_path) #formal version
historical_feedback = {} #training version
results = []

for issue in issues:
    print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")

    # Agent 1: root cause from LLM
    root_cause_result = diagnose_root_cause(issue)

    # Deterministic diagnosis fields
    affected_percent = compute_affected_percent(issue.get("detail", ""), DEFAULT_TOTAL_ROWS)
    priority_score = compute_priority_score(issue.get("severity", ""), affected_percent)

    diagnosis_result = {
        "root_cause": root_cause_result.get("root_cause"),
        "business_impact": root_cause_result.get("business_impact"),
        "priority_score": priority_score,
        "affected_percent": affected_percent
    }

    print(
        f"  Agent 1 done → priority_score={diagnosis_result['priority_score']}, "
        f"affected_percent={diagnosis_result['affected_percent']}"
    )

    # Historical context for Agent 2
    historical_context = format_historical_context(issue["issue_type"], historical_feedback)

    # Agent 2
    remediation_result = generate_remediation(issue, diagnosis_result, historical_context)
    print(f"  Agent 2 done → generated {len(remediation_result.get('suggestions', []))} suggestion(s)")

    # Week 2 quality score
    quality_score_result = compute_quality_scores_for_issue(issue, issues, DEFAULT_TOTAL_ROWS)
    print(
        f"  Quality score → before={quality_score_result['before']}, "
        f"after={quality_score_result['after']}, "
        f"delta={quality_score_result['delta']}"
    )


    # Save output record
    output_record = build_output_record(
        issue=issue,
        diagnosis=diagnosis_result,
        remediation=remediation_result,
        quality_score=quality_score_result
    )
    results.append(output_record)

    # Update memory with default chosen action = option 1
    historical_feedback = append_default_historical_decision(
        historical_feedback,
        issue,
        remediation_result
    )

    print()

# Sort by priority_score descending
results = sorted(
    results,
    key=lambda x: x["diagnosis"]["priority_score"],
    reverse=True
)

# Save final JSON output
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# Save updated historical memory
save_historical_feedback(historical_path, historical_feedback)

print("✅ issues_with_suggestions.json saved")
print("✅ historical_decisions.json updated")