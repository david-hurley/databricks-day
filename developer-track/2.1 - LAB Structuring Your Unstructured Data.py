# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - CLONE THIS NOTEBOOK INTO YOUR PERSONAL WORKSPACE FOLDER BEFORE RUNNING
# MAGIC
# MAGIC # 2 - START/ATTACH NOTEBOOK CLUSTER USING YOUR BU POLICY WITH DBR 17.3

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: AI-Powered Event Planning Weather Assessment
# MAGIC
# MAGIC In the demo we used AI SQL Functions to assess flight suitability. In this lab you'll use the same functions on the same weather dataset, but for a different scenario: **helping an event planner decide whether to hold an outdoor event**.
# MAGIC
# MAGIC **Tips:**
# MAGIC - Use the **Databricks Data Science Agent** (cmd/ctrl + I) to help you write code
# MAGIC - All exercises use SQL — remember to start cells with `%sql`
# MAGIC - Each exercise describes *what* to do — it's up to you *how* to write it
# MAGIC - AI functions can take 10–30 seconds to run — that's normal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Run the cells below to configure your notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_been2c_catalog", "Catalog")
dbutils.widgets.text("schema", "weather", "Schema")


# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print("catalog:", catalog)
print("schema:", schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Classify Event Suitability
# MAGIC
# MAGIC Use `ai_classify` on the `phrase_long` column to label each day as one of:
# MAGIC
# MAGIC **`Perfect`** · **`Manageable`** · **`Indoor Only`** · **`Dangerous`**
# MAGIC
# MAGIC Return `phrase_long` and the classification. Limit to 10 rows.
# MAGIC
# MAGIC Function signature: `ai_classify(content, ARRAY('label1', 'label2', ...))`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify phrase_long into event suitability categories
# MAGIC -- Table: utils.zz_temp_training.us_postal_daily_metric') OR enter the full table path
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Extract Event Planning Details
# MAGIC
# MAGIC Use `ai_extract` on `phrase_long` to pull out these fields:
# MAGIC - `recommended_clothing`
# MAGIC - `outdoor_risk`
# MAGIC - `best_activity`
# MAGIC
# MAGIC Save the result as a **temp view**, then query it and flatten the struct columns using dot notation.
# MAGIC
# MAGIC Function signature: `ai_extract(content, ARRAY('field1', 'field2', ...))`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view that extracts recommended_clothing, outdoor_risk, best_activity from phrase_long
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the temp view and flatten the struct into individual columns using dot notation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Generate Event Recommendations with `ai_query`
# MAGIC
# MAGIC Use `ai_query` to generate a short recommendation for an **outdoor wedding** based on each weather phrase. Include the `temperature_avg` and `wind_speed_avg` columns alongside `phrase_long` in your prompt to give the model more context.
# MAGIC
# MAGIC Pick any foundation model (e.g. `databricks-claude-sonnet-4`).
# MAGIC
# MAGIC Function signature: `ai_query('model-name', 'your prompt string')`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use ai_query to generate outdoor wedding recommendations
# MAGIC -- Concatenate phrase_long, temperature_avg, and wind_speed_avg into the prompt for richer context
# MAGIC -- Table: utils.zz_temp_training.us_postal_daily_metric')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge: Wrap it in a Reusable UC Function
# MAGIC
# MAGIC Take your `ai_query` prompt from Exercise 3 and register it as a **Unity Catalog function** so any notebook or SQL editor can call it by name.
# MAGIC
# MAGIC Requirements:
# MAGIC - Accept `phrase STRING`, `temperature DOUBLE`, and `wind_speed DOUBLE` as parameters
# MAGIC - Use `responseFormat` to return **structured JSON** with at least two fields (e.g. `recommendation` and `risk_level`)
# MAGIC - Register with your unique suffix: `fx_event_advisor_{username}`
# MAGIC
# MAGIC Then call the function against the weather table to verify it works.
# MAGIC
# MAGIC Refer back to the demo notebook for the `responseFormat` JSON schema syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create your UC function: fx_event_advisor_{username}
# MAGIC -- It should accept phrase, temperature, and wind_speed parameters
# MAGIC -- Return structured JSON using responseFormat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Call your function against the weather table to verify
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Nice work!** You've used AI SQL Functions to classify, extract, and generate event planning insights from raw weather text — and optionally wrapped it in a reusable UC function.
