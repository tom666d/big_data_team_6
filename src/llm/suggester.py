import openai
import json
from dotenv import load_dotenv
import os
# ── Load detection results from Day 1 ───────────────
with open("data/issues_output.json") as f:
    issues = json.load(f)

print(f"📂 Loaded {len(issues)} issue(s) from issues_output.json\n")

# ── GPT API setup ────────────────────────────────────
load_dotenv()
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_llm_suggestion(issue):
    prompt = f"""
    You are a data quality expert. A data pipeline has detected the following issue:

    Column: {issue['column']}
    Issue Type: {issue['issue_type']}
    Severity: {issue['severity']}
    Detail: {issue['detail']}
    Sample Values: {issue['sample_values']}

    Provide exactly 2 remediation options in this JSON format:
    {{
        "suggestions": [
            {{
                "option": 1,
                "action": "short action description",
                "confidence": 85,
                "rationale": "one sentence explanation"
            }},
            {{
                "option": 2,
                "action": "short action description",
                "confidence": 65,
                "rationale": "one sentence explanation"
            }}
        ]
    }}
    Return JSON only, no other text.
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[{"role": "user", "content": prompt}]
    )

    return json.loads(response.choices[0].message.content)

# ── Call GPT for each issue ──────────────────────────
print("🤖 Generating LLM suggestions...\n")

for issue in issues:
    print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")
    result = get_llm_suggestion(issue)
    issue["suggestions"] = result["suggestions"]

    for s in result["suggestions"]:
        print(f"  Option {s['option']} (confidence {s['confidence']}%): {s['action']}")
    print()

# ── Save updated results ─────────────────────────────
with open("data/issues_with_suggestions.json", "w") as f:
    json.dump(issues, f, indent=2, ensure_ascii=False)

print("✅ issues_with_suggestions.json saved")