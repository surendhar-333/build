# Databricks notebook source
# MAGIC %md
# MAGIC # Hello, Databricks 👋
# MAGIC This notebook was created locally and synced via GitHub.
# MAGIC If you can see this rendered as a notebook in your Git folder, the round-trip works.

# COMMAND ----------

# A simple smoke test that runs on Free Edition serverless compute.
print("Connected. Spark version:", spark.version)

# COMMAND ----------

# Create a tiny DataFrame to confirm Spark works end to end.
df = spark.createDataFrame(
    [(1, "alpha"), (2, "beta"), (3, "gamma")],
    ["id", "name"],
)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ If the cells above ran, you're ready to build. Tell Claude what to create next.
