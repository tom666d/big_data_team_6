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

input_path = project_root / "data" / "issues_output.json"
output_path = project_root / "data" / "issues_with_suggestions.json"
historical_path = project_root / "data" / "historical_decisions.json"
df_shape_path = project_root / "data" / "df_shape.json"


# =========================
# Load detector output
# =========================
with open(input_path, "r", encoding="utf-8") as f:
    issues = json.load(f)


# =========================
# Helper functions
# =========================
def load_df_shape(path: Path) -> dict[str, int]:
    """
    Load dataframe shape metadata from JSON.

    Expected format:
    {
        "total_rows": 100000,
        "total_columns": 152
    }
    """
    if not path.exists():
        raise FileNotFoundError(f"df_shape file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        shape_info = json.load(f)

    total_rows = shape_info.get("total_rows")
    total_columns = shape_info.get("total_columns")

    if not isinstance(total_rows, int) or total_rows <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_rows'")

    if not isinstance(total_columns, int) or total_columns <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_columns'")

    return {
        "total_rows": total_rows,
        "total_columns": total_columns
    }


df_shape = load_df_shape(df_shape_path)
TOTAL_ROWS = df_shape["total_rows"]
TOTAL_COLUMNS = df_shape["total_columns"]

print(f"📂 Loaded {len(issues)} issue(s) from issues_output.json")
print(f"📐 Loaded dataframe shape: rows={TOTAL_ROWS}, columns={TOTAL_COLUMNS}\n")


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

    match_fraction = re.search(r"\((\d+)\s*/\s*(\d+)\s*rows\)", detail)
    if match_fraction:
        return int(match_fraction.group(1))

    match_outlier = re.search(r"(\d+)\s+outlier\(s\)", detail, re.IGNORECASE)
    if match_outlier:
        return int(match_outlier.group(1))

    row_counts = re.findall(r"(\d+)\s+rows", detail, re.IGNORECASE)
    if row_counts:
        counts = [int(x) for x in row_counts]
        return min(counts)

    return None


def extract_affected_rows_percent(detail: str) -> float | None:
    """
    Extract affected row percentage from issue detail text.

    Supported patterns:
    1. '35215 outlier(s) (1.6% of rows)' -> 1.6
    2. 'Null rate: 40% (4/10 rows)' -> 40.0
    """
    if not detail:
        return None

    match_percent_of_rows = re.search(r"(\d+(?:\.\d+)?)%\s+of\s+rows", detail, re.IGNORECASE)
    if match_percent_of_rows:
        return float(match_percent_of_rows.group(1))

    match_null_rate = re.search(r"Null rate:\s*(\d+(?:\.\d+)?)%", detail, re.IGNORECASE)
    if match_null_rate:
        return float(match_null_rate.group(1))

    return None


def compute_affected_rows_percent(detail: str, total_rows: int) -> float | None:
    """
    Compute affected row percentage.

    Priority:
    1. Use the percentage directly from detail text if available.
    2. Otherwise, compute from affected row count / total_rows.
    """
    percent_from_detail = extract_affected_rows_percent(detail)
    if percent_from_detail is not None:
        return round(percent_from_detail, 2)

    affected_rows = extract_affected_rows(detail)
    if affected_rows is None or total_rows <= 0:
        return None

    return round((affected_rows / total_rows) * 100, 2)


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


def affected_rows_percent_to_score(affected_rows_percent: float | None) -> int:
    """
    Convert affected row percentage to a 1-10 style score.
    """
    if affected_rows_percent is None:
        return 5
    if affected_rows_percent < 5:
        return 1
    if affected_rows_percent < 15:
        return 3
    if affected_rows_percent < 30:
        return 5
    if affected_rows_percent < 50:
        return 7
    return 9


def compute_priority_score(severity: str, affected_rows_percent: float | None) -> int:
    """
    Priority score = 0.6 * severity_score + 0.4 * affected_rows_percent_score.
    Final score is rounded and clipped to 1-10.
    """
    severity_score = severity_to_score(severity)
    affected_score = affected_rows_percent_to_score(affected_rows_percent)

    weighted_score = round(0.6 * severity_score + 0.4 * affected_score)
    weighted_score = max(1, min(10, weighted_score))
    return weighted_score


# =========================
# Week 2: Data Quality Score
# =========================
def compute_total_affected_rows(
    issues_list: list[dict[str, Any]],
    total_rows: int
) -> int:
    """
    Estimate total affected rows across all issues by summing affected row percentages.

    Note:
    - This is a simplified approximation.
    - Different issues may overlap on the same rows, so this can overcount.
    - The final percentage is capped at 100%.
    """
    total_affected_percent = 0.0

    for issue in issues_list:
        issue_percent = compute_affected_rows_percent(issue.get("detail", ""), total_rows)
        if issue_percent is not None:
            total_affected_percent += issue_percent

    total_affected_percent = min(total_affected_percent, 100.0)
    total_affected_rows = round((total_affected_percent / 100) * total_rows)

    return total_affected_rows


def compute_quality_scores_for_issue(
    current_issue: dict[str, Any],
    issues_list: list[dict[str, Any]],
    total_rows: int
) -> dict[str, float]:
    """
    Row-based quality score logic.

    before:
    - overall row quality before fixing the current issue

    after:
    - overall row quality after assuming the current issue is fully resolved

    delta:
    - improvement contributed by fixing the current issue

    Note:
    - This uses affected_rows_percent as the common unit.
    - The total affected percent is capped at 100%.
    """
    total_affected_rows_before = compute_total_affected_rows(issues_list, total_rows)

    current_issue_percent = compute_affected_rows_percent(
        current_issue.get("detail", ""),
        total_rows
    ) or 0.0
    current_issue_affected_rows = round((current_issue_percent / 100) * total_rows)

    affected_rows_after = max(0, total_affected_rows_before - current_issue_affected_rows)

    good_rows_before = total_rows - total_affected_rows_before
    good_rows_after = total_rows - affected_rows_after

    before = round((good_rows_before / total_rows) * 100, 2) if total_rows > 0 else 0.0
    after = round((good_rows_after / total_rows) * 100, 2) if total_rows > 0 else 0.0
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
    if not path.exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.loads(content)

            
            if isinstance(data, list):
                converted = {}
                for record in data:
                    itype = record.get("issue_type", "unknown")
                    converted.setdefault(itype, []).append(record)
                return converted

            
            if isinstance(data, dict):
                return data

            return {}

    except json.JSONDecodeError:
        return {}


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
    Input = detector issue + Agent 1 diagnosis + historical feedback.
    Output = remediation suggestions with runnable PySpark fix code.
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
Affected Rows Percent: {diagnosis['affected_rows_percent']}

Historical decisions for this issue type:
{historical_context}

Requirements:
1. Generate exactly 3 remediation suggestions.
2. Option 1 and Option 2 must be actual remediation actions.
3. Option 3 must always be "Decline changes".
4. Each suggestion must include:
   - option
   - action
   - confidence
   - rationale
   - caveats
   - pyspark_code

Field rules:
- option: integer (1, 2, or 3)
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

Special rule for option 3:
- option 3 must always represent "Decline changes"
- action must be exactly "Decline changes"
- rationale should explain that no remediation will be applied and the decision is still captured for audit purposes
- caveats should explain that the issue remains unresolved
- pyspark_code should be a no-op comment such as "# No change applied to df"
- confidence can be 100 because this is a user decision rather than a technical remediation estimate

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
    }},
    {{
      "option": 3,
      "action": "Decline changes",
      "confidence": 100,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "# No change applied to df"
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
            "affected_rows_percent": diagnosis["affected_rows_percent"]
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

historical_feedback = load_historical_feedback(historical_path)
results = []

for issue in issues:
    print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")

    # Agent 1: root cause from LLM
    root_cause_result = diagnose_root_cause(issue)

    # Deterministic diagnosis fields
    affected_rows_percent = compute_affected_rows_percent(issue.get("detail", ""), TOTAL_ROWS)
    priority_score = compute_priority_score(issue.get("severity", ""), affected_rows_percent)

    diagnosis_result = {
        "root_cause": root_cause_result.get("root_cause"),
        "business_impact": root_cause_result.get("business_impact"),
        "priority_score": priority_score,
        "affected_rows_percent": affected_rows_percent
    }

    print(
        f"  Agent 1 done → priority_score={diagnosis_result['priority_score']}, "
        f"affected_rows_percent={diagnosis_result['affected_rows_percent']}"
    )

    # Historical context for Agent 2
    historical_context = format_historical_context(issue["issue_type"], historical_feedback)

    # Agent 2
    remediation_result = generate_remediation(issue, diagnosis_result, historical_context)
    print(f"  Agent 2 done → generated {len(remediation_result.get('suggestions', []))} suggestion(s)")

    # Week 2 quality score
    quality_score_result = compute_quality_scores_for_issue(
        issue,
        issues,
        TOTAL_ROWS
    )
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

print("✅ issues_with_suggestions.json saved")