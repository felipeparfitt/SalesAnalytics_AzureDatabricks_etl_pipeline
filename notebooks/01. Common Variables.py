# Databricks notebook source
# MAGIC %md
# MAGIC # Common Variables

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# Getting the External Locations path
checkpoints_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION dbproj_checkpoints_{env}").select('url').collect()[0][0]
landing_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION dbproj_landing_{env}").select('url').collect()[0][0]
medallion_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION dbproj_medallion_{env}").select('url').collect()[0][0]

bronze_path = medallion_path+'bronze/'
silver_path = medallion_path+'silver/'
gold_path = medallion_path+'gold/'
