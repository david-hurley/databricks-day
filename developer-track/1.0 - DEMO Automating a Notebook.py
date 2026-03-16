# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - CLONE THIS NOTEBOOK INTO YOUR PERSONAL WORKSPACE FOLDER BEFORE RUNNING
# MAGIC
# MAGIC # 2 - START/ATTACH NOTEBOOK CLUSTER USING YOUR BU POLICY WITH DBR 17.3

# COMMAND ----------

# MAGIC %md
# MAGIC # Building and Automating an ETL Notebook in Databricks
# MAGIC
# MAGIC In this notebook we will build a ETL pipeline — loading raw weather data, transforming it, and persisting curated results as a Delta table. Once built, this notebook can be scheduled as a Databricks Job to run automatically on a cadence.
# MAGIC
# MAGIC **Components:**
# MAGIC 1. Load the data
# MAGIC 2. Transform the data
# MAGIC 3. Persist the data
# MAGIC 4. Review table transaction logs

# COMMAND ----------

# DBTITLE 1,Quick Tour of the Databricks Notebook UI
# MAGIC %md
# MAGIC    
# MAGIC ## Quick Tour of the Databricks Notebook UI
# MAGIC
# MAGIC Before diving into the code, here's a quick orientation of the key areas of the notebook interface:
# MAGIC
# MAGIC - **Top Bar** — notebook title, compute selector, and the Run All / Schedule buttons
# MAGIC - **Sidebar (left)** — workspace file browser, search, and catalog explorer
# MAGIC - **Cell Toolbar** — each cell has controls to run, move, delete, or change language (Python, SQL, Scala, R, Markdown)
# MAGIC - **Widgets Bar** — parameters defined with `dbutils.widgets` appear here for interactive input
# MAGIC - **Results Panel** — execution output renders below each cell (tables, charts, text, errors)
# MAGIC - **Right Sidebar** — Genie Code, comments, revision history, and environment panel (libraries, Spark config)
# MAGIC - **Status Bar (bottom)** — cluster state, Spark UI link, and cell execution progress
# MAGIC - **Web Terminal** - can be accessed from >_ logo in bottom right, use for adding secrets
# MAGIC
# MAGIC > **Tip:** You can mix languages in a single notebook by adding a magic command (`%sql`, `%python`, `%sh`, `%md`) at the top of any cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook Setup - Overview
# MAGIC - installing dependencies
# MAGIC - using secrets
# MAGIC - defining parameters with widgets

# COMMAND ----------

# DO NOT MODIFY - FOR DEMO ONLY
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
unique_table_suffix = current_user.split('@')[0].replace('.', '_')

# COMMAND ----------

# MAGIC %md
# MAGIC Check what groups or you are with the code below or using [this dashboard](https://adb-5421949986596593.13.azuredatabricks.net/dashboardsv3/01f00024ee011b99a69ca4913429bef3/published/pages/935f186b?o=5421949986596593)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM corporate_function.audit.group_details where member = current_user()

# COMMAND ----------

# MAGIC %md
# MAGIC **Notebook dependencies can be installed in a cell or in the notebook environment panel**
# MAGIC
# MAGIC A common practice is to keep project or team level dependencies in a `requirements.txt` file and store in a volume, this file can be users across many notebooks and ensure consistent package use

# COMMAND ----------

# DBTITLE 1,How to install dependencies
# MAGIC %sh
# MAGIC # Databricks compute comes pre-loaded with many libraries - this is just a example of how to install missing libraries
# MAGIC pip install --quiet pandas
# MAGIC pip install --quiet numpy

# COMMAND ----------

# MAGIC %md
# MAGIC **You may need to access secrets, such as API keys or CLIENT-ID**
# MAGIC
# MAGIC Databricks uses secret scopes to store secret values. 
# MAGIC
# MAGIC Use these CLI commands in the web terminal to add a secret 
# MAGIC
# MAGIC `databricks secrets put-secret bu_your_business_unit your_unique_secret_name`
# MAGIC
# MAGIC The terminal will then prompt you to add the secret value
# MAGIC
# MAGIC Use of the web terminal or CLI is needed when entering as notebooks save the history of every previous state
# MAGIC
# MAGIC [See docs here](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/example-secret-workflow)

# COMMAND ----------

# DBTITLE 1,How to load secret scopes
secret_scope_name = "bu_your_business_unit"
secret_key = "your_unique_secret_name"

# example only - if the scope and key existed you can retrieve with the following
# my_secret_value = dbutils.secrets.get(scope=secret_scope_name, key=secret_key)

# COMMAND ----------

# MAGIC %md
# MAGIC **We can add notebook parameters that can be set through widgets and job parameters**
# MAGIC
# MAGIC You may want to use a notebook across Dev/QA/Prod environments and point to different catalog and schema. Or be able to alter the value of a variable dynamically in a job or between tasks. Parameters help us do this. 

# COMMAND ----------

# DBTITLE 1,How to create widgets
# create notebook widgets so we can pass dynamic parameters in jobs
dbutils.widgets.text("catalog", "classic_stable_been2c_catalog", "Catalog")
dbutils.widgets.text("schema", "weather", "Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC **If we change the widget value and run the below cell multiple times the output will also change**

# COMMAND ----------

# DBTITLE 1,How to fetch widget values
# get widgets values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print("catalog: ", catalog)
print("schema: ", schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load in Data
# MAGIC
# MAGIC There are several ways to bring data into a notebook. We'll cover three common patterns:
# MAGIC - **SQL temp view** — query Unity Catalog tables using SQL
# MAGIC - **PySpark DataFrame** — read a table directly into a Spark Dataframe
# MAGIC - **File-based** — read CSV or Excel from a Databricks Volume

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 1: Create a Temp View and Query with SQL**
# MAGIC
# MAGIC Temp views exist only for the duration of the session. They're useful when you want to break SQL work into reusable chunks without writing anything to the catalog.
# MAGIC
# MAGIC We are also using our dynamic catalog and schema - it will become apparent why when we create a job.

# COMMAND ----------

# DBTITLE 1,Query with SQL and Create a Temp View
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view AS
# MAGIC SELECT * FROM classic_stable_been2c_catalog.weather.us_postal_daily_metric;
# MAGIC
# MAGIC SELECT * FROM temp_view LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 2a: Read a Unity Catalog Table into a PySpark DataFrame**
# MAGIC
# MAGIC Reading directly into a DataFrame gives you the full power of PySpark for transformations. `spark.table()` takes a three-level Unity Catalog name: `catalog.schema.table`.

# COMMAND ----------

# DBTITLE 1,Load with PySpark into a Dataframe
df = spark.table(f'{catalog}.{schema}.us_postal_daily_metric')

display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 15b
# MAGIC %md
# MAGIC    
# MAGIC **Option 2b: Use Python to Pass Parameters into SQL**
# MAGIC
# MAGIC You can also run SQL from Python using f-strings to inject widget values and variables directly into the query. This gives you the flexibility of SQL with the control of Python — useful when filters or table references need to be set dynamically.

# COMMAND ----------

## Example using python to pass parameters to SQL
station_code_filter = 'NQI'

df = sql(

    f"""
    
    SELECT * FROM {catalog}.{schema}.us_postal_daily_metric
    WHERE station_code = '{station_code_filter}'
    
    """
)

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Option 2c: Convert the PySpark Dataframe into Pandas Dataframe**
# MAGIC
# MAGIC Generally we recommend keeping dataframes in PySpark, but sometimes you may be more comfortable working in Pandas. This is suitable for small datasets.  

# COMMAND ----------

df_pandas = df.toPandas()

display(df_pandas.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 3a: Read a CSV from a Databricks Volume**
# MAGIC
# MAGIC Databricks Volumes provide governed storage for files. You can upload CSV, Excel, JSON, and other files to a Volume and read them directly in a notebook using the `/Volumes/` path prefix.

# COMMAND ----------

df = spark.read.csv(f"/Volumes/{catalog}/{schema}/files/weather.csv", header=True, inferSchema=True)

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 3b: Read an Excel file from a Databricks Volume**
# MAGIC
# MAGIC Excel files can also be read from Volumes. Databricks handles the parsing — no extra library installation needed.
# MAGIC
# MAGIC See docs here https://learn.microsoft.com/en-us/azure/databricks/query/formats/excel
# MAGIC
# MAGIC As this is a newer functionality the assitant is sometimes not in sync with the latest docs

# COMMAND ----------

df = spark.read.format("excel").option("headerRows", 1).load(f"/Volumes/{catalog}/{schema}/files/weather.xlsx")

display(df.limit(5))

# COMMAND ----------

# create a view onto the excel
sql(
    f"""
CREATE OR REPLACE VIEW  {catalog}.{schema}.weather_excel_{unique_table_suffix} AS
SELECT * FROM read_files(
  '/Volumes/{catalog}/{schema}/files/weather.xlsx',
  format => "excel",
  headerRows => 1,
  schemaEvolutionMode => "none"
)
"""
)

# COMMAND ----------

# query the view
display(sql(f"select * from {catalog}.{schema}.weather_excel_{unique_table_suffix} limit 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform the Data
# MAGIC
# MAGIC Using PySpark we'll clean and aggregate the raw data into a curated dataset. We'll work through three common transformation steps: selecting relevant columns, removing bad data, and aggregating to a useful grain.

# COMMAND ----------

# MAGIC %md
# MAGIC **Start with a fresh copy of the source data**
# MAGIC
# MAGIC It's good practice to reload from the source at the start of your transformation chain so the notebook is idempotent — it produces the same result every time it runs.

# COMMAND ----------

df = spark.table(f"{catalog}.{schema}.us_postal_daily_metric")

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1: Select only the columns you need**
# MAGIC
# MAGIC Dropping unused columns early reduces memory usage and keeps downstream logic easier to follow.

# COMMAND ----------

from pyspark.sql.types import DoubleType

df = df.select(
    "latitude", "longitude", "station_code", "date", "phrase_long", "temperature_avg", "wind_speed_avg", "rain_lwe_total"
)

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Remove bad data**
# MAGIC
# MAGIC Drop rows with nulls and filter out known bad records. In a production pipeline you'd typically log the dropped rows rather than silently discard them.

# COMMAND ----------

df = df.dropna().filter(df.station_code != "NYCG")

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Aggregate to monthly averages**
# MAGIC
# MAGIC Roll up the daily records to monthly averages per station. This is the curated grain we'll persist as a Delta table.

# COMMAND ----------

from pyspark.sql.functions import month, avg

df_monthly_avg = df.withColumn("month", month("date")) \
    .groupBy("station_code", "month") \
    .agg(
        avg("temperature_avg").alias("avg_temperature"),
        avg("wind_speed_avg").alias("avg_wind_speed"),
        avg("rain_lwe_total").alias("avg_rain_lwe")
    )

display(df_monthly_avg.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persist the Data
# MAGIC
# MAGIC After transforming the data, we need to write it somewhere durable so other users and tools can access it. There are three main patterns:
# MAGIC
# MAGIC | Pattern | When to use |
# MAGIC |---|---|
# MAGIC | **View** | Ad-hoc / lightweight — no data stored, transformation re-runs on every query |
# MAGIC | **Overwrite table** | Full refresh — simplest option, replaces the entire table each job run |
# MAGIC | **Delta merge (upsert)** | Incremental updates — adds new rows and updates changed ones, most production-ready |

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 1: Create a View**
# MAGIC
# MAGIC A view stores the SQL definition, not the data. Every query re-runs the transformation against the source table — useful for always-fresh, ad-hoc datasets. For scheduled jobs that serve downstream users, a persisted table is usually a better choice.

# COMMAND ----------

sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.monthly_avg_view_{unique_table_suffix} AS
SELECT
    station_code,
    MONTH(date) AS month,
    AVG(temperature_avg) AS avg_temperature,
    AVG(wind_speed_avg) AS avg_wind_speed,
    AVG(precipitation_probability) AS avg_precip_probability,
    AVG(rain_lwe_total) AS avg_rain_lwe
FROM {catalog}.{schema}.us_postal_daily_metric
GROUP BY station_code, MONTH(date)
""")

# read back in to show
display(sql(f"SELECT * FROM {catalog}.{schema}.monthly_avg_view_{unique_table_suffix} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 2: Overwrite — Write a Full Table on Every Run**
# MAGIC
# MAGIC The simplest persistence pattern: replace the entire table each time the job runs. Works well when the source dataset is small enough to fully reprocess and you don't need to preserve history.

# COMMAND ----------

df_monthly_avg.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}")

# read back in to show
df = spark.table(f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}")

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 3: Delta Merge — Upsert New and Changed Rows**
# MAGIC
# MAGIC When new data arrives incrementally, a merge (upsert) is more efficient than a full overwrite. Delta's merge operation matches records on a key, updates rows that already exist, and inserts rows that are new.

# COMMAND ----------

from pyspark.sql import Row
from delta.tables import DeltaTable

# Simulating incoming data - first row exists and we are updating, next row is new
new_data = [
    Row(station_code="ORNY", month=4, avg_temperature=14.174545, avg_wind_speed=2.803636, avg_precip_probability=17.90909090909091, avg_rain_lwe=0.078509091),
    Row(station_code="ABC",  month=4, avg_temperature=15.132727, avg_wind_speed=2.845455, avg_precip_probability=41.81818181818182, avg_rain_lwe=2.496131818)
]

df_new = spark.createDataFrame(new_data)

display(df_new)

# COMMAND ----------

# Use merge to update and insert new records
delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}")

delta_table.alias("target").merge(
    df_new.alias("source"),
    "target.station_code = source.station_code AND target.month = source.month"
).whenMatchedUpdate(set={
    "avg_temperature": "source.avg_temperature",
    "avg_wind_speed": "source.avg_wind_speed",
    "avg_rain_lwe": "source.avg_rain_lwe"
}).whenNotMatchedInsert(values={
    "station_code": "source.station_code",
    "month": "source.month",
    "avg_temperature": "source.avg_temperature",
    "avg_wind_speed": "source.avg_wind_speed",
    "avg_rain_lwe": "source.avg_rain_lwe"
}).execute()

# read back in to show
display(spark.table(f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option 4: Metric Views**
# MAGIC Metric views provide a centralized way to define and manage consistent, reusable, and governed core business metrics.

# COMMAND ----------

# ask Genie Code: 
# 
# in cell 49 - create a metric view of avg temperature with grouping for month, day, week using my us_postal_daily_metric table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta Transaction Logs
# MAGIC
# MAGIC Every write to a Delta table is recorded in a transaction log. This gives you a full audit trail of every change — who wrote what, when, and using which operation (overwrite, merge, etc.).
# MAGIC
# MAGIC You can also use this log to **time-travel**: query the table as it looked at any previous version.

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}")

display(delta_table.history().limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel — Query a Previous Version**
# MAGIC
# MAGIC You can read any previous version of the table using `VERSION AS OF` in SQL, or the `.option("versionAsOf", ...)` reader option in PySpark.

# COMMAND ----------

df_v0 = spark.read.format("delta").option("versionAsOf", 0).table(f"{catalog}.{schema}.monthly_avg_{unique_table_suffix}")

display(df_v0.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Grant Use of New Table to a Group**
# MAGIC
# MAGIC You can grant a group `SELECT` or more permissions using SQL or through the Unity Catalog UI
# MAGIC
# MAGIC **BEST PRACTICE IS TO DEFINE ACCESS AND UPDATE OWNERSHIP IN SAME CELL/SCRIPT AS THE VIEW/TABLE CREATION**
# MAGIC
# MAGIC **CREATE OR REPLACE WILL OVERWRITE THESE SETTINGS TO YOU AS OWNER AND DEFAULT INHERITED PERMISSIONS, USE ALTER COMMAND IF YOU ONLY WANT TO UPDATE CERTAIN PARTS OF TABLE/VIEW**
# MAGIC
# MAGIC

# COMMAND ----------

# Grant access
# CATALOG ACCESS MANAGED THROUGH AD GROUPS AS PART OF CATALOG SETUP

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `my_group`")

spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.monthly_avg_{unique_table_suffix} TO `my_group`")

# Update object owner example
spark.sql(f"ALTER TABLE {catalog}.{schema}.monthly_avg_{unique_table_suffix} OWNER TO `my_group`")
spark.sql(f"ALTER VIEW {catalog}.{schema}.monthly_avg_view_{unique_table_suffix} OWNER TO `my_group`")

# View current grants
display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.{schema}.monthly_avg_{unique_table_suffix}"))
