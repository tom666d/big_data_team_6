from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_extract, when, trim
from pyspark.sql.types import DoubleType
import json

# ── Start Spark ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("DataQualityDetector") \
    .getOrCreate()

# ── Load data ────────────────────────────────────────────────────────────────
df = spark.table("workspace.team6.lendingclub_full")
print("✅ Step 1: Data loaded successfully")

total = df.count()
issues = []

# ── Helper: parse emp_length strings → numeric ────────────────────────────────
def parse_emp_length(df, col_name="emp_length"):
    return df.withColumn(
        col_name + "_numeric",
        when(trim(col(col_name)) == "10+ years", 10.0)
        .when(trim(col(col_name)).rlike("^< *1 year$"), 0.0)
        .when(trim(col(col_name)).rlike(r"^(\d+) years?$"),
              regexp_extract(trim(col(col_name)), r"^(\d+)", 1).cast(DoubleType()))
        .otherwise(None)
    )

df = parse_emp_length(df)
print("✅ Step 2: emp_length parsed to numeric (emp_length_numeric)")

# ── Detection 1: Null Spike (targeted columns only, threshold: >30%) ─────────
print("\n🔍 Detecting Null Spikes...")

NULL_COLS = [
    "loan_amnt", "funded_amnt", "int_rate", "grade",
    "emp_length", "annual_inc", "loan_status", "purpose",
    "revol_util", "tot_cur_bal"
]

for col_name in NULL_COLS:
    if col_name not in df.columns:
        print(f"  ⏭️  {col_name}: column not found, skipping")
        continue

    null_count = df.filter(col(col_name).isNull()).count()
    null_rate = null_count / total

    if null_rate > 0.3:
        sample = (
            df.select(col_name).dropna().limit(3)
            .toPandas()[col_name].tolist()
        )
        issues.append({
            "column": col_name,
            "issue_type": "Null Spike",
            "severity": "HIGH",
            "detail": f"Null rate: {null_rate:.0%} ({null_count}/{total} rows)",
            "sample_values": str(sample)
        })
        print(f"  ⚠️  {col_name}: null rate = {null_rate:.0%}")
    else:
        print(f"  ✅ {col_name}: null rate = {null_rate:.0%}, within threshold")

# ── Detection 2: Statistical Outlier (3×IQR, flag threshold: >5%) ────────────
print("\n🔍 Detecting Statistical Outliers (IQR)...")

NUMERIC_COLS = [
    "loan_amnt",
    "funded_amnt",
    "funded_amnt_inv",
    "int_rate",
    "emp_length_numeric",
]

for col_name in NUMERIC_COLS:
    if col_name not in df.columns:
        print(f"  ⏭️  {col_name}: column not found, skipping")
        continue

    quantiles = df.select(col_name).dropna().approxQuantile(col_name, [0.25, 0.75], 0.01)
    if len(quantiles) < 2:
        continue

    q1, q3 = quantiles
    iqr = q3 - q1
    if iqr == 0:
        continue

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    outlier_count = df.filter(
        col(col_name).isNotNull() &
        ((col(col_name) < lower) | (col(col_name) > upper))
    ).count()

    outlier_rate = outlier_count / total

    if outlier_rate > 0.05:  # raised from 0.01 → 0.05
        sample = (
            df.filter((col(col_name) < lower) | (col(col_name) > upper))
            .select(col_name).limit(5)
            .toPandas()[col_name].tolist()
        )
        issues.append({
            "column": col_name,
            "issue_type": "Statistical Outlier",
            "severity": "HIGH" if outlier_rate > 0.05 else "MEDIUM",
            "detail": (
                f"{outlier_count} outlier(s) ({outlier_rate:.1%} of rows). "
                f"IQR bounds: [{lower:.2f}, {upper:.2f}]"
            ),
            "sample_values": str(sample)
        })
        print(f"  ⚠️  {col_name}: {outlier_count} outliers ({outlier_rate:.1%}) "
              f"| bounds [{lower:.2f}, {upper:.2f}]")
    else:
        print(f"  ✅ {col_name}: clean ({outlier_count} outliers, below threshold)")

# ── Detection 3: Format Inconsistency (issue_date) ──────────────────────────
print("\n🔍 Detecting Format Inconsistencies...")
if "issue_date" in df.columns:
    iso_pattern = r"^\d{4}-\d{2}-\d{2}$"
    us_pattern  = r"^\d{2}/\d{2}/\d{4}$"

    iso_count = df.filter(regexp_extract(col("issue_date"), iso_pattern, 0) != "").count()
    us_count  = df.filter(regexp_extract(col("issue_date"), us_pattern,  0) != "").count()

    if iso_count > 0 and us_count > 0:
        sample = df.select("issue_date").limit(5).toPandas()["issue_date"].tolist()
        issues.append({
            "column": "issue_date",
            "issue_type": "Format Inconsistency",
            "severity": "MEDIUM",
            "detail": (
                f"Mixed formats: {iso_count} rows as YYYY-MM-DD, "
                f"{us_count} rows as MM/DD/YYYY"
            ),
            "sample_values": str(sample)
        })
        print(f"  ⚠️  issue_date: mixed date formats detected")
else:
    print("  ⏭️  issue_date: column not found, skipping")

# ── Compute Data Quality Score ───────────────────────────────────────────────
deductions = {"HIGH": 8, "MEDIUM": 4, "LOW": 2}  # softened from 15/7/3
raw_score = 100 - sum(deductions.get(i["severity"], 4) for i in issues)
quality_score = max(0, min(100, raw_score))

print(f"\n📊 Data Quality Score: {quality_score}/100  |  {len(issues)} issue(s) found")

# --- Output -----------------------------------

print(f"\n✅ Detection complete — {len(issues)} issue(s) found")
for idx, issue in enumerate(issues):
    print(f" Issue {idx+1}: [{issue['severity']}] {issue['column']} - {issue['issue_type']}")

from pathlib import Path
import os

repo_root = Path(os.getcwd())

issues_path = repo_root / "data" / "issues_output.json"
score_path  = repo_root / "data" / "quality_score.json"

issues_path.parent.mkdir(parents=True, exist_ok=True)

with open(issues_path, "w") as f:
    json.dump(issues, f, indent=2, ensure_ascii=False)

with open(score_path, "w") as f:
    json.dump({"quality_score": quality_score}, f, indent=2)

print("\n✅ issues_output.json saved")
print("✅ quality_score.json saved")