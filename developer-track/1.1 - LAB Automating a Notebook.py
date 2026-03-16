# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - CLONE THIS NOTEBOOK INTO YOUR PERSONAL WORKSPACE FOLDER BEFORE RUNNING
# MAGIC
# MAGIC # 2 - START/ATTACH NOTEBOOK CLUSTER USING YOUR BU POLICY WITH DBR 17.3

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Build an Extreme Heat Report with Genie Code
# MAGIC
# MAGIC In the demo we built an ETL pipeline that aggregated weather data to monthly averages. In this lab you'll tackle a different problem using the same dataset: **find which stations experienced the most extreme heat** by joining two tables, filtering on temperature, and aggregating.
# MAGIC
# MAGIC **Tips:**
# MAGIC - Use the **Databricks Genie Code** (cmd/ctrl + I) to help you write code
# MAGIC - The source tables live in `{catalog}.{schema}` — use the widgets below
# MAGIC - Each exercise describes *what* to do — it's up to you *how* to do it

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Run the cells below to configure your notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_been2c_catalog", "Catalog")
dbutils.widgets.text("schema", "weather", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print("catalog:", catalog)
print("schema:", schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Load & Join
# MAGIC
# MAGIC The setup script created two normalized tables:
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `station_location` | One row per station — `station_code`, postal code, country, lat, long |
# MAGIC | `daily_weather_measurement` | One row per station per day — all weather observations |
# MAGIC
# MAGIC Load both tables into DataFrames and **join them on `station_code`** so each measurement row has its station's latitude and longitude.

# COMMAND ----------

# Load station_location and daily_weather_measurement
# Join them on station_code
# Display a sample to verify the join worked

# TIP - you can use the '@' sybmol in the Data Science Agent to point to a specific cell or the whole notebook


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Filter to Extreme Heat Days
# MAGIC
# MAGIC From the joined DataFrame:
# MAGIC - convert temperature to Celsius from Farenheit
# MAGIC - keep only rows where **`temperature_avg` > 30** (Celsius).
# MAGIC
# MAGIC Display a few rows and a count so you can see how much data you're working with.

# COMMAND ----------

# Convert temperature to Celsius
# Filter to rows where temperature_avg > 30
# Display a sample and print the row count


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Aggregate a Heat Summary
# MAGIC
# MAGIC Group by `station_code` and compute a heat summary with these columns:
# MAGIC
# MAGIC | Output column | Aggregation |
# MAGIC |---|---|
# MAGIC | `hot_days` | count of rows |
# MAGIC | `max_temperature` | max of `temperature_max` |
# MAGIC | `avg_wind_speed` | average of `wind_speed_avg` |
# MAGIC | `avg_humidity` | average of `humidity_relative_avg` |
# MAGIC
# MAGIC Order the result by `hot_days` descending.

# COMMAND ----------

# Group by station_code
# Compute: hot_days (count), max_temperature (max), avg_wind_speed (avg), avg_humidity (avg)
# Order by hot_days descending
# Display the result


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Save as a Delta Table
# MAGIC
# MAGIC Write your heat summary DataFrame as a Delta table named:
# MAGIC
# MAGIC `{catalog}.{schema}.heat_summary_{unique_table_suffix}`
# MAGIC
# MAGIC Then read it back and display it to confirm.

# COMMAND ----------

# Write the heat summary as a Delta table (overwrite mode)
# Read it back and display to verify


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge: Filter by Proximity with Geospatial SQL
# MAGIC
# MAGIC **Runtime: 17.3 DBR Required**
# MAGIC
# MAGIC Databricks has built-in [geospatial functions](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-st-geospatial-functions) that work directly in SQL and PySpark.
# MAGIC
# MAGIC Go back to your joined DataFrame (before the heat filter) and use `ST_DISTANCE` and `ST_POINT` to **keep only stations within 100 km of Raleigh, NC** (latitude `35.7796`, longitude `-78.6382`).
# MAGIC
# MAGIC Then re-run your heat filter and aggregation on just those nearby stations and save as `{catalog}.{schema}.heat_summary_raleigh_{unique_table_suffix}`.
# MAGIC
# MAGIC Useful functions:
# MAGIC - `ST_POINT(longitude, latitude)` — creates a geometry point
# MAGIC - `ST_DISTANCE(point1, point2)` — returns distance in meters

# COMMAND ----------

# Filter your joined DataFrame to stations within 100 km of Raleigh, NC
# Raleigh: latitude = 35.7796, longitude = -78.6382
# Then apply the heat filter and aggregation from Exercises 2-3
# Save as heat_summary_raleigh_{unique_table_suffix}


# COMMAND ----------

# Save as a Delta table and display
# How do the results compare to the full-country summary?


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Nice work!** You've built a pipeline that joins, filters, aggregates, and persists weather data — and optionally added geospatial proximity filtering. 
# MAGIC
# MAGIC This notebook is ready to be scheduled as a Databricks Job to run on a regular cadence. Try setting up a job and trigger.
