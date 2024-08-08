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
# MAGIC ### Creating Useful Functions
# MAGIC - creating read function;
# MAGIC - creating write function;

# COMMAND ----------

def read_lading_data(table_name, schema):
    print(f"Reading the landing {table_name} table: ", end='')
    landing_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoints_path}/bronze{table_name}Load/schemaInfer")
        .option('header', 'true')
        .schema(schema)
        .load(f"{landing_path}/{table_name}")
        .withColumn('extract_time', current_timestamp())
    )
    print("Success !!")
    print("*******************************")
    return landing_df

def write_bronze_data(df, environment, table_name):
    print(f"Write {table_name} to dbproj_{environment}.bronze: ", end='')
    bronze_df = (
        df.writeStream
          .queryName(f"bronze{table_name}WriteStream")
          .format('delta')
          .option('checkpointLocation', f"{checkpoints_path}/bronze{table_name}Load/checkpoint")
          .outputMode('append')
          .trigger(availableNow=True)
          .toTable(f"dbproj_{environment}.bronze.{table_name}")
    )
    bronze_df.awaitTermination()
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# Dict used to store all table schemas
table_schemas = {
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
    "sales_people": """
            salesperson_id INT,
            firstname STRING,
            lastname STRING,
            email STRING,
            phone_number STRING
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
    """,
    "products": """
            product_id INT,
            product_name STRING,
            description STRING,
            price DECIMAL(10, 2)
    """
}


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading/Writing all tables to bronze layer

# COMMAND ----------

for table_name, schema in table_schemas.items():
    df = read_lading_data(table_name, schema)
    write_bronze_data(df, env, table_name)
