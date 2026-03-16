# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - CLONE THIS NOTEBOOK INTO YOUR PERSONAL WORKSPACE FOLDER BEFORE RUNNING
# MAGIC
# MAGIC # 2 - START/ATTACH NOTEBOOK CLUSTER USING YOUR BU POLICY WITH DBR 17.3

# COMMAND ----------

# MAGIC %md
# MAGIC # Structuring Your Unstructured Data with AI SQL Functions
# MAGIC
# MAGIC Most real-world data isn't clean rows and columns — it's free-form text, PDFs, and documents. In this notebook we'll use Databricks **AI SQL Functions** to turn that unstructured data into structured, queryable tables using a single SQL expression.
# MAGIC
# MAGIC **Why batch inference matters:**
# MAGIC - **Analysts** get structured columns they can filter, aggregate, and visualize
# MAGIC - **Classical ML** gets clean features without manual labelling pipelines
# MAGIC - **Agentic systems** get structured memory and tool-ready outputs
# MAGIC
# MAGIC **What we'll cover:**
# MAGIC 1. AI SQL Functions overview
# MAGIC 2. Structuring free-form text (`ai_classify`, `ai_extract`, `ai_query`)
# MAGIC 3. Wrapping a custom prompt in a reusable Unity Catalog function
# MAGIC 4. Parsing documents (`ai_parse` on a PDF)
# MAGIC 5. Evaluating AI function outputs
# MAGIC 6. Challenge activity

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook Setup

# COMMAND ----------

# DO NOT MODIFY - FOR DEMO ONLY
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
unique_table_suffix = current_user.split('@')[0].replace('.', '_')

# COMMAND ----------

# create notebook widgets so we can pass dynamic parameters in jobs
dbutils.widgets.text("catalog", "classic_stable_been2c_catalog", "Catalog")
dbutils.widgets.text("schema", "weather", "Schema")


# COMMAND ----------

# get widgets values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")


print("catalog: ", catalog)
print("schema: ", schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. AI SQL Functions Overview
# MAGIC
# MAGIC Databricks AI SQL Functions are built-in SQL functions that call foundation models directly inside a query. There's no model deployment, no API key management, and no separate inference pipeline — the model call happens inline, one row at a time, as part of your SQL or PySpark expression.
# MAGIC
# MAGIC Under the hood they use the **Databricks Foundation Model API**, which is served through Unity Catalog and governed like any other data asset.
# MAGIC
# MAGIC **Key properties:**
# MAGIC - Works in SQL, PySpark (`expr()`), and Databricks notebooks
# MAGIC - Scales to millions of rows via Spark parallelism
# MAGIC - Results can be written straight to a Delta table — making them available to dashboards, Genie, and downstream pipelines
# MAGIC
# MAGIC
# MAGIC **Note that there is additional cost when using a LLM, which is included in these functions - we are monitoring use so please use responsibly on pre-filtered or pre-aggregated data**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **2a. `ai_classify` — Assign a label from a fixed list**
# MAGIC
# MAGIC `ai_classify` picks the single best-matching label from the list you provide. It's deterministic and fast — ideal for categorisation tasks where you control the label set.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     phrase_long,
# MAGIC     ai_classify(phrase_long, ARRAY('Clear', 'Cloudy', 'Precipitation', 'Severe')) AS weather_category
# MAGIC FROM 
# MAGIC     classic_stable_been2c_catalog.weather.us_postal_daily_metric 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **2b. `ai_extract` — Pull named attributes from text**
# MAGIC
# MAGIC `ai_extract` reads free-form text and returns a struct of named fields. You define what you want to extract — the model figures out how to find it in the text.

# COMMAND ----------

df = sql(f"""
SELECT
    phrase_long,
    ai_extract(
        phrase_long,
        ARRAY('precipitation_type', 'sky_condition', 'intensity')
    ) AS extracted
FROM 
    {catalog}.{schema}.us_postal_daily_metric
LIMIT 5;
"""
)

df.display()

# COMMAND ----------

# use dot notation on the struct column to flatten - we can do in sql or python
display(
    df.select(
        "phrase_long",
        "extracted.precipitation_type",
        "extracted.sky_condition",
        "extracted.intensity"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **2c. `ai_query` — Custom prompt, maximum flexibility**
# MAGIC
# MAGIC `ai_query` gives you a direct line to the model. You pass the model name and a full prompt string — useful when classify and extract don't cover your use case.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     phrase_long,
# MAGIC     ai_query(
# MAGIC         'databricks-claude-sonnet-4', -- choose any foundation model
# MAGIC         "Is this weather condition suitable for flying a small aircraft? Condition: " || phrase_long -- this is our prompt
# MAGIC     ) AS flying_suitability
# MAGIC FROM 
# MAGIC     classic_stable_been2c_catalog.weather.us_postal_daily_metric
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating a resusable Unity Catalog registered User Defined Function (UDF)
# MAGIC
# MAGIC Repeating a long prompt string in every notebook is fragile — if the prompt changes you have to find and update every reference. A better pattern is to wrap the `ai_query` call in a **Python function registered to Unity Catalog**.
# MAGIC
# MAGIC Benefits:
# MAGIC - **Reusable** — call it from any notebook, SQL editor, or job with `catalog.schema.function_name()`
# MAGIC - **Governed** — versioned and permissioned like any other UC asset
# MAGIC - **Consistent** — the prompt logic lives in one place
# MAGIC
# MAGIC The function below analyses a weather phrase and returns a **structured JSON string** with three fields: `suitable` (bool), `risk_level` (High / Medium / Low), and `reason` (one sentence). Returning JSON rather than plain text makes it easy to parse the output into columns downstream.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.fx_small_aircraft_flight_suitability_{unique_table_suffix}(phrase STRING)
    RETURNS STRING
    COMMENT "Determines flight suitability based on weather"
    RETURN 
        SELECT ai_query(
        "databricks-claude-sonnet-4",
        "Is this weather condition suitable for flying a small aircraft? Condition: " || phrase,
        responseFormat => '{{
            "type": "json_schema",
            "json_schema": {{
            "name": "small_aircraft_flight_suitability",
            "schema": {{
                "type": "object",
                "properties": {{
                "suitability": {{"type": "string"}},
                "reason": {{"type": "string"}}
                }}
            }},
            "strict": true
            }}
        }}'
        )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Call the registered UC function from SQL**
# MAGIC
# MAGIC Once registered, anyone with `EXECUTE` permission on the function can call it by its three-part name.

# COMMAND ----------

display(spark.sql(f"""
  SELECT
    phrase_long,
    -- dynamically pointing to our function above, you could just hardcode as well
    {catalog}.{schema}.fx_small_aircraft_flight_suitability_{unique_table_suffix}(phrase_long) AS assessment
  FROM 
    {catalog}.{schema}.us_postal_daily_metric
  LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parsing Documents with `ai_parse_document`
# MAGIC
# MAGIC So far we've worked with short text strings. But a lot of valuable information lives in documents — PDFs, reports, manuals. `ai_parse_document` reads a binary file from a Databricks Volume and extracts its content as structured text.
# MAGIC
# MAGIC We'll use the **RCAF Weather Manual** — a document describing aviation weather standards — as our example.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_parse_document(content) AS parsed
# MAGIC FROM READ_FILES('/Volumes/' || :catalog || '/' || :schema || '/files/RCAF-Weather-Manual.pdf', format => 'binaryFile')

# COMMAND ----------

# MAGIC %md
# MAGIC **We can create a single column of text content**

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed_documents AS (
# MAGIC     SELECT
# MAGIC       path,
# MAGIC       ai_parse_document(
# MAGIC         content
# MAGIC       ) AS parsed
# MAGIC     FROM READ_FILES('/Volumes/' || :catalog || '/' || :schema || '/files/RCAF-Weather-Manual.pdf', format => 'binaryFile')
# MAGIC   ),
# MAGIC   
# MAGIC   parsed_text AS (
# MAGIC     SELECT
# MAGIC       path,
# MAGIC       concat_ws(
# MAGIC         '\n\n',
# MAGIC         transform(
# MAGIC           try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC           element -> try_cast(element:content AS STRING)
# MAGIC         )
# MAGIC       ) AS text
# MAGIC     FROM parsed_documents
# MAGIC     WHERE try_cast(parsed:error_status AS STRING) IS NULL
# MAGIC   )
# MAGIC
# MAGIC SELECT * FROM parsed_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evaluating AI Function Outputs
# MAGIC
# MAGIC AI SQL functions are powerful, but their outputs are probabilistic — the same input can produce slightly different results, and the model can make mistakes. Before trusting batch inference results in a production pipeline, it's worth thinking about evaluation.
# MAGIC
# MAGIC **Three-step approach:**
# MAGIC
# MAGIC **1. Create a labelled benchmark**
# MAGIC Hand-label 50–100 rows with the correct answer. This becomes your ground truth. Keep it small enough to do manually, but large enough to be statistically meaningful. Store it as a Delta table.
# MAGIC
# MAGIC **2. Run your AI function against the benchmark**
# MAGIC Apply `ai_classify`, `ai_extract`, or your UDF to the benchmark rows and compare the outputs to your labels. Calculate accuracy, precision, recall, or whatever metric fits your use case.
# MAGIC
# MAGIC **3. Use `ai_query` as an LLM judge**
# MAGIC For tasks where there's no single right answer (e.g. free-text generation), use a second `ai_query` call to evaluate the quality of the first. The judge prompt asks the model to score or compare outputs. This is called **LLM-as-a-judge**.
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: use ai_query to judge whether a classification was correct
# MAGIC SELECT
# MAGIC     phrase_long,
# MAGIC     predicted_category,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'A weather classifier labelled the phrase "', phrase_long, '" as "', predicted_category, '". ',
# MAGIC             'Is this label correct? Answer only: Correct or Incorrect.'
# MAGIC         )
# MAGIC     ) AS judge_verdict
# MAGIC FROM benchmark_results
# MAGIC ```
# MAGIC
# MAGIC **Practical tips:**
# MAGIC - Re-evaluate after prompt changes — small wording changes can shift accuracy meaningfully
# MAGIC - Track agreement rate (% where the judge agrees with your label) over time as a quality signal
# MAGIC - For production pipelines, fail the job or alert if accuracy drops below a threshold
