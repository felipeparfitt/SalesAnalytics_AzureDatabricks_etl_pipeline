# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Bronze to Silver Layer
# MAGIC
# MAGIC This notebook provides functions to handle reading, transforming, and writing data between the bronze and silver layers, supporting both batch and streaming modes.
# MAGIC
# MAGIC ## Batch Mode
# MAGIC - **Reading Data from Bronze:** Reads data from a specified table in the bronze layer of a given environment. It returns the DataFrame for further processing, ensuring the data is successfully retrieved.
# MAGIC
# MAGIC - **Writing Data to Silver:** Writes a DataFrame to the silver layer of a specified environment in batch mode, overwriting any existing data. It ensures that the transformed data is stored correctly.
# MAGIC
# MAGIC ## Streaming Mode
# MAGIC - **Reading Data from Bronze:** Reads data from a specified table in the bronze layer of a given environment in streaming mode. It supports continuous data ingestion and returns the streaming DataFrame.
# MAGIC
# MAGIC - **Transforming Data in Batch/Streaming Mode:** Transforms data from the bronze layer by applying a SQL query to a temporary view of the DataFrame. It handles both batch and streaming data, ensuring the transformations are applied correctly.
# MAGIC
# MAGIC - **Writing Data to Silver:** Writes the transformed DataFrame to the silver layer of a specified environment in streaming mode. It uses checkpointing and triggers real-time data updates, ensuring the data is consistently stored.

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# Importing common variables
%run "./01. Common Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions (Batch/Streaming):

# COMMAND ----------

# Reading data in batch mode:
def read_from_bronze_batch(environment, table_name):
    print(f"(BATCH) Reading the {table_name} table from dbproj_{environment}.bronze: ", end='')

    # Reading the data from bronze
    read_df = (
        spark.read
             .table(f"dbproj_{environment}.bronze.{table_name}")
    )
    print('Success !!')
    print("*******************************")
    return read_df
    
# Writing data in batch mode:
def write_to_silver_batch(df, environment, table_name):
    print(f"(BATCH) Write {table_name} to dbproj_{environment}.silver: ", end='')
    (
        df.write
          .format('delta')
          .mode('overwrite')
          .saveAsTable(f"dbproj_{environment}.silver.{table_name}")
    )
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# Reading data in stream mode:
def read_from_bronze_stream(environment, table_name):
    print(f"(STREAM) Reading the {table_name} table from dbproj_{environment}.bronze: ", end='')

    # Reading the data from 'layer'
    read_df = (
        spark.readStream
             .table(f"dbproj_{environment}.bronze.{table_name}")
    )
    print('Success !!')
    return read_df

# Transforming data in batch/stream mode:
def transforming_bronze_tables(df, table_name, sql_query):
    print(f"Transforming {table_name} table: ", end='')

    # Updating the table name
    temp_view_name = f"{table_name}_view"
    updated_sql_query = sql_query.replace(f"FROM {table_name}", f"FROM {temp_view_name}")
    # Creating a temp view / transforming data
    df.createOrReplaceTempView(temp_view_name)
    df_tranformed = spark.sql(updated_sql_query)
    
    print("Success !!")
    print("************************************")
    return df_tranformed
        
# Writing data in stream mode:
def write_to_silver_stream(df, environment, table_name):
    print(f"(STREAM) Writing the {table_name} table to dbproj_{environment}.silver: ", end='')

    # Writing the data to silver layer
    writeSilver_df = (
        df.writeStream
          .queryName(f'silver_{table_name}_writeStream')
          .format('delta')
          .option('checkpointLocation', f"{checkpoints_path}/silver_{table_name}_load")
          .outputMode('append')
          .trigger(availableNow=True)
          .toTable(f"dbproj_{environment}.silver.{table_name}")
    )
    writeSilver_df.awaitTermination()

    print("Success !!")
    print("************************************")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing all tables from bronze to silver layer:

# COMMAND ----------

# (BATCH) SQL tranformations:
silver_transformation_batch = {
    "products": """
        SELECT
            *
        FROM products
    """,
    "sales_people": """
        SELECT
            salesperson_id,
            firstname,
            lastname,
            CONCAT(firstname, ' ', lastname) AS full_name,
            email,
            phone_number,
            extract_time
        FROM sales_people
    """
}

# (STREAM) SQL tranformations:
silver_transformation_stream = {
    "clients": """
        SELECT 
            client_id,
            firstname,
            lastname,
            CONCAT(firstname, ' ', lastname) AS full_name,
            birth_date,
            FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS age,
            CASE 
                WHEN FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) < 15 THEN 'children'
                WHEN FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) < 25 THEN 'teenager'
                WHEN FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) < 65 THEN 'adult'
                ELSE 'senior'
            END AS age_group,
            email,
            phone,
            extract_time
        FROM clients
    """,
    "sales": """
        SELECT
            sale_id,
            client_id,
            salesperson_id,
            sale_date,
            MONTH(sale_date) AS sale_month,
            DAY(sale_date) AS sale_day,
            total_amount,
            extract_time
        FROM sales
    """,
    "sales_items": """
        SELECT
            *
        FROM sales_items
    """
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### a) Batch mode:

# COMMAND ----------

for table_name, sql_transf_query in silver_transformation_batch.items():
    # Reading data from bronze layer
    df_bronze_batch = read_from_bronze_batch(env, table_name)
    # Combining/Tranforming bronze layer 
    df_bronze_transformed_batch = transforming_bronze_tables(df_bronze_batch, table_name, sql_transf_query)
    # Writing to silver layer
    write_to_silver_batch(df_bronze_transformed_batch, env, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### a) Stream mode:

# COMMAND ----------

for table_name, sql_transf_query in silver_transformation_stream.items():
    # Reading data from bronze layer
    df_bronze_stream = read_Table(env, table_name)
    # Combining/Tranforming bronze layer
    df_bronze_transformed_stream = transforming_bronze_tables(df_bronze_stream, table_name, sql_transf_query)
    # Writing to silver layer
    write_to_silver_stream(df_bronze_transformed_stream, env, table_name)
