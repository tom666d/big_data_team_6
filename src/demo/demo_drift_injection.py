from pyspark.sql.functions import col, rand, when

SOURCE_TABLE = "workspace.team6.lendingclub_full"
DEMO_TABLE   = "workspace.team6.demo_lendingclub"

df = spark.table(SOURCE_TABLE)
print(f"✅ Loaded {df.count()} rows from {SOURCE_TABLE}")

# Inject 1: Null Spike on loan_amnt (40% nulls)
df = df.withColumn("loan_amnt",
    when(rand() < 0.4, None).otherwise(col("loan_amnt")))
print("✅ Injected null spike into loan_amnt")

# Inject 2: Outliers on funded_amnt (10% extreme values)
df = df.withColumn("funded_amnt",
    when(rand() < 0.1, 9999999.0).otherwise(col("funded_amnt")))
print("✅ Injected outliers into funded_amnt")

df.write.mode("overwrite").saveAsTable(DEMO_TABLE)
print(f"✅ Demo table saved to {DEMO_TABLE}")