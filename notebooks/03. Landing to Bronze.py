# Databricks notebook source
# MAGIC %md
# MAGIC # Landing zone to Bronze Layer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./01. Common Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions (Batch/Streaming)
# MAGIC - creating read function;
# MAGIC - creating write function;

# COMMAND ----------

# Reading data in batch mode:
def read_landing_data_batch(table_name, schema):
    print(f"(BATCH) Reading the landing {table_name} table: ", end='')
    landing_df = (
        spark.read
        .format("csv")
        .option('header', 'true')
        .schema(schema)
        .load(f"{landing_path}/{table_name}")
        .withColumn('extract_time', current_timestamp())
    )
    print("Success !!")
    print("*******************************")
    return landing_df
    
# Writing data in batch mode:
def write_bronze_data_batch(df, environment, table_name):
    print(f"(BATCH) Write {table_name} to dbproj_{environment}.bronze: ", end='')
    bronze_df = (
        df.write
          .format('delta')
          .mode('overwrite')
          .saveAsTable(f"dbproj_{environment}.bronze.{table_name}")
    )
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# Reading data in streaming mode:
def read_lading_data_stream(table_name, schema):
    print(f"(STREAM) Reading the landing {table_name} table: ", end='')
    landing_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoints_path}/bronze_{table_name}_load/schemaInfer")
        .option('header', 'true')
        .schema(schema)
        .load(f"{landing_path}/{table_name}")
        .withColumn('extract_time', current_timestamp())
    )
    print("Success !!")
    print("*******************************")
    return landing_df

# Writing data in streaming mode:
def write_bronze_data_stream(df, environment, table_name):
    print(f"(STREAM) Write {table_name} to dbproj_{environment}.bronze: ", end='')
    bronze_df = (
        df.writeStream
          .queryName(f"bronze{table_name}WriteStream")
          .format('delta')
          .option('checkpointLocation', f"{checkpoints_path}/bronze_{table_name}_load/checkpoint")
          .outputMode('append')
          .trigger(availableNow=True)
          .toTable(f"dbproj_{environment}.bronze.{table_name}")
    )
    bronze_df.awaitTermination()
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Writing all tables to bronze layer

# COMMAND ----------

# Dict used to store all table schemas
table_schemas_batch = {
    "clients": """
            client_id INT,
            firstname STRING,
            lastname STRING,
            birth_date DATE,
            email STRING,
            phone STRING
    """,
    "client_addresses": """
            address_id INT,
            client_id INT,
            state STRING,
            city STRING,
            street STRING,
            zip_code STRING
    """,
    "sales": """
            sale_id INT,
            client_id INT,
            salesperson_id INT,
            sale_date DATE,
            total_amount DECIMAL(10, 2)
    """,
    "sales_items": """
            item_id INT,
            sale_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            discount DECIMAL(10, 2)
    """
}

table_schemas_stream = {
        "products": """
                product_id INT,
                product_name STRING,
                description STRING,
                price DECIMAL(10, 2)
        """,
        "sales_people": """
                salesperson_id INT,
                firstname STRING,
                lastname STRING,
                email STRING,
                phone_number STRING
        """
}


# COMMAND ----------

# MAGIC %md
# MAGIC #### a) Batch mode:

# COMMAND ----------

for table_name_batch, schema_batch in table_schemas_batch.items():
    df_batch = read_landing_data_batch(table_name_batch, schema_batch)
    write_bronze_data_batch(df_batch, env, table_name_batch)

# COMMAND ----------

# MAGIC %md
# MAGIC #### b) Stream mode:

# COMMAND ----------

for table_name_stream, schema_stream in table_schemas_stream.items():
    df_stream = read_lading_data_stream(table_name_stream, schema_stream)
    write_bronze_data_stream(df_stream, env, table_name_stream)
