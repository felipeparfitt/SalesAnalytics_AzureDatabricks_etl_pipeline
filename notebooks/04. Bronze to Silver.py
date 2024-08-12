# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Transforming

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./01. Common Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions
# MAGIC - creating read function;
# MAGIC - creating transforming function;
# MAGIC - creating writing function;

# COMMAND ----------

def read_Table(environment, layer, table_name):
    print(f"Reading the {table_name} table from dbproj_{environment}.{layer}: ", end='')

    # Reading the data from 'layer'
    read_df = (
        spark.readStream
             .table(f"dbproj_{environment}.{layer}.{table_name}")
    )
    print('Success !!')
    return read_df

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
        

def write_to_silver(df, environment, table_name):
    print(f"Writing the {table_name} table to dbproj_{environment}.silver: ", end='')

    # Writing the data to silver layer
    (
        df.writeStream
          .queryName(f'silver_{table_name}_writeStream')
          .format('delta')
          .option('checkpointLocation', f"{checkpoints_path}/silver_{table_name}_load")
          .outputMode('append')
          .trigger(availableNow=True)
          .toTable(f"dbproj_{environment}.silver.{table_name}")
    )

    print("Success !!")
    print("************************************")

# COMMAND ----------

# Dict with sql queries to transform data from bronze to silver
silver_transformation_sql = {
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
    """,
    "products": """
        SELECT
            *
        FROM products
    """
}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Bronze to Silver Transforming

# COMMAND ----------

for table_name, sql_transf_query in silver_transformation_sql.items():
    # Reading data from bronze layer
    df_bronze = read_Table(env, 'bronze', table_name)
    # Transform each bronze table
    df_bronze_transformed = transforming_bronze_tables(df_bronze, table_name, sql_transf_query)
    # Writing to silver layer
    write_to_silver(df_bronze_transformed, env, table_name)
