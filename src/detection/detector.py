from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
import json

# ── Start Spark ──────────────────────────────────────
spark = SparkSession.builder \
    .appName("DataQualityDetector") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Create sample data (will be replaced with real dataset later) ──
import pandas as pd

pd.DataFrame({
    "loan_amount":       [5000, 10000, None, 15000, None, None, 8000, 12000, None, 3000],
    "interest_rate":     [0.05, 0.08, 0.06, 0.99, 0.07, -0.01, 0.05, 0.09, 0.06, 0.04],
    "employment_length": [2, 5, 3, None, None, None, 4, 6, None, 1],
    "issue_date":        ["2023-01-01", "01/02/2023", "2023-03-01",
                          "2023-04-01", "04/05/2023", "2023-06-01",
                          "2023-07-01", "08/01/2023", "2023-09-01", "2023-10-01"]
}).to_csv("data/sample.csv", index=False)

# ── Load data ────────────────────────────────────────
df = spark.read.csv("data/sample.csv", header=True, inferSchema=True)
print("✅ Step 1: Data loaded successfully")
df.show()

total = df.count()
issues = []

# ── Detection 1: Null Spike ──────────────────────────
print("\n🔍 Detecting Null Spikes...")
for col_name in df.columns:
    null_count = df.filter(col(col_name).isNull()).count()
    null_rate = null_count / total
    if null_rate > 0.3:
        issues.append({
            "column": col_name,
            "issue_type": "Null Spike",
            "severity": "HIGH",
            "detail": f"Null rate: {null_rate:.0%} ({null_count}/{total} rows)",
            "sample_values": str(df.select(col_name).dropna().limit(3).toPandas()[col_name].tolist())
        })
        print(f"  ⚠️  {col_name}: null rate = {null_rate:.0%}")

# ── Detection 2: Statistical Outlier ────────────────
print("\n🔍 Detecting Statistical Outliers...")
for col_name in ["loan_amount", "interest_rate", "employment_length"]:
    outliers = df.filter(
        (col(col_name) > 1) | (col(col_name) < 0)
    ).count()
    if col_name == "interest_rate" and outliers > 0:
        issues.append({
            "column": col_name,
            "issue_type": "Statistical Outlier",
            "severity": "HIGH",
            "detail": f"{outliers} outlier(s) detected (values > 1 or < 0)",
            "sample_values": str(df.select(col_name).dropna().limit(5).toPandas()[col_name].tolist())
        })
        print(f"  ⚠️  {col_name}: {outliers} outliers detected")

# ── Detection 3: Format Inconsistency ───────────────
print("\n🔍 Detecting Format Inconsistencies...")
iso_pattern = r"^\d{4}-\d{2}-\d{2}$"
us_pattern  = r"^\d{2}/\d{2}/\d{4}$"

iso_count = df.filter(regexp_extract(col("issue_date"), iso_pattern, 0) != "").count()
us_count  = df.filter(regexp_extract(col("issue_date"), us_pattern,  0) != "").count()

if iso_count > 0 and us_count > 0:
    issues.append({
        "column": "issue_date",
        "issue_type": "Format Inconsistency",
        "severity": "MEDIUM",
        "detail": f"Mixed formats: {iso_count} rows as YYYY-MM-DD, {us_count} rows as MM/DD/YYYY",
        "sample_values": str(df.select("issue_date").limit(5).toPandas()["issue_date"].tolist())
    })
    print(f"  ⚠️  issue_date: mixed date formats detected")

# ── Print results ────────────────────────────────────
print(f"\n✅ Step 2: Detection complete — {len(issues)} issue(s) found")
for i, issue in enumerate(issues):
    print(f"  Issue {i+1}: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")

# Save results for next step
with open("data/issues_output.json", "w") as f:
    json.dump(issues, f, indent=2, ensure_ascii=False)

print("\n✅ issues_output.json saved")
spark.stop()