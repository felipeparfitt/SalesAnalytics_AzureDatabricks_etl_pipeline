# Databricks notebook source
# MAGIC %md
# MAGIC # Project Setup Notebook

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./01. Common Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions
# MAGIC - creating catalog function;
# MAGIC - creating schema function;
# MAGIC - creating table function;

# COMMAND ----------

def create_catalog(environment):
    catalog_name = f"dbproj_{environment}"
    print(f"Creating the {catalog_name} Catalog")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
    print("************************************")

def create_schema(environment, path, layer):
    #schema_n = f"dbproj_{environment}"
    catalog_name = f"dbproj_{environment}"
    print(f'Creating {layer} schema in {catalog_name}')
    spark.sql(f"""
              CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer}
              MANAGED LOCATION '{path}';
              """)
    print("************************************")

def creating_tables(environment, layer, table_dict):
    catalog_name = f"dbproj_{environment}"
    print(f"Using {catalog_name} catalog")
    spark.sql(f"USE CATALOG {catalog_name};")

    print(f"Using {layer} schema")
    spark.sql(f"USE SCHEMA {layer};")

    for table_name, create_table_sql in table_dict.items():
        print(f"Creating {table_name} table in {catalog_name}.{layer}: ", end='')
        spark.sql(create_table_sql)
        print("Success !!")
        print("************************************")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating Catalog, Schemas and Tables

# COMMAND ----------

# Dict used to create the tables on bronze layer
sql_tables = {
    "clients": """
        CREATE TABLE IF NOT EXISTS clients (
            client_id INT,
            firstname VARCHAR(30),
            lastname VARCHAR(30),
            birth_date DATE,
            email STRING,
            phone CHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "client_addresses": """
        CREATE TABLE IF NOT EXISTS client_addresses (
            address_id INT,
            client_id INT,
            state CHAR(2),
            city VARCHAR(100),
            street VARCHAR(150),
            zip_code VARCHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "sales_people": """
        CREATE TABLE IF NOT EXISTS sales_people (
            salesperson_id INT,
            firstname VARCHAR(30),
            lastname VARCHAR(30),
            email VARCHAR(100),
            phone_number VARCHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "sales": """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INT,
            client_id INT,
            salesperson_id INT,
            sale_date DATE,
            total_amount DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """,
    "sales_items": """
        CREATE TABLE IF NOT EXISTS sales_items (
            item_id INT,
            sale_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            discount DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT,
            product_name VARCHAR(150),
            description STRING,
            price DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """
}

# COMMAND ----------

# Creating the catalog
create_catalog(env)

# Creating the schemas in a managed location
create_schema(env, bronze_path, 'bronze')
create_schema(env, silver_path, 'silver')
create_schema(env, gold_path, 'gold')

# Creating all tables needed
creating_tables(env, 'bronze', sql_tables)
