# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold Transforming

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./01. Common Variables"

# COMMAND ----------

def read_from_gold_batch(environment, layer, used_tables):
    # Reading the data from 'layer' and creating temp view
    for temp_view_name in used_tables:
        print(f"Reading the {temp_view_name} table from dbproj_{environment}.{layer}: ", end='')

        read_df = (
            spark.read
                 .table(f"dbproj_{environment}.{layer}.{temp_view_name}")
        )
        read_df.createOrReplaceTempView(temp_view_name) 

        print('Success !!')

def transforming_silver_tables(sql_query):
    print(f"Transforming/Combining tables: ", end='')

    # Transforming silver data
    df_tranformed = spark.sql(sql_query)
    
    print("Success !!")
    print("************************************")
    return df_tranformed
        

def write_to_gold_batch(df, environment, table_name, comparative_keys):
    # Gold table name:
    gold_table_name = f"dbproj_{environment}.gold.{table_name}"

    # Checking if the gold table exists
    print(f"Is {gold_table_name} table exists? ", end='')
    gold_table_exists = spark.catalog.tableExists(gold_table_name)
    print(f"{gold_table_exists}")


    print(f"Writing the {table_name} table to dbproj_{environment}.gold: ", end='')
    if gold_table_exists:
        df_gold = spark.read.table(gold_table_exists)

        # Merging SOURCE INTO TARGET (targ.id=sorc.id): 
        #   1) when targ.id=sorc.id, then update with source row
        #   2) when sorc.id not corresp. any targ.id, then insert source row
        #   3) when targ.id not corresp. any sorc.id, then delete target row (table sync)
        (
            df_gold.alias('target')
                   .merge(
                       df.alias('source'),
                       comparative_keys
                   )
                   .whenMatchedUpdateAll()
                   .whenNotMatchedInsertAll()
                   .whenNotMatchedBySourceDelete()
                   .execute()
        )
    else:
        (
            df.write
            .format('delta')
            .saveAsTable(f"dbproj_{environment}.gold.{table_name}")
        )

    print("Success !!")
    print("************************************")

# COMMAND ----------

gold_transformation = {
    "sales_people_by_total":{
        "tables": ['sales', 'sales_people'],
        "sql": """
            SELECT 
                s.salesperson_id,
                sp.full_name,
                SUM(s.total_amount) AS total_sales
            FROM 
                sales AS s
            LEFT JOIN 
                sales_people AS sp 
            ON 
                s.salesperson_id = sp.salesperson_id
            GROUP BY 
                s.salesperson_id,
                sp.full_name
            ORDER BY 
                total_sales DESC
        """,
        "comparative_keys": "target.salesperson_id = source.salesperson_id" 
    },
    "sales_people_by_month":{
        "tables": ['sales', 'sales_people'],
        "sql":"""
            SELECT
                s.salesperson_id, 
                sp.full_name,
                s.sale_month,
                SUM(s.total_amount) AS total_sales
            FROM 
                sales AS s
            LEFT JOIN 
                sales_people AS sp 
            ON 
                s.salesperson_id = sp.salesperson_id
            GROUP BY 
                s.salesperson_id, 
                sp.full_name,
                s.sale_month
            ORDER BY 
                total_sales DESC
        """,
        "comparative_keys": """
            target.salesperson_id = source.salesperson_id AND
            target.sale_month = source.sale_month              
         """
    },
    "sales_people_by_product":{
        "tables": ['sales', 'sales_people', 'sales_items', 'products'],
        "sql":"""
            SELECT
                s.salesperson_id,
                sp.full_name,
                p.product_name,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN sales_people AS sp ON s.salesperson_id = sp.salesperson_id
            LEFT JOIN sales_items AS si ON s.sale_id = si.sale_id
            LEFT JOIN products AS p ON si.product_id = p.product_id
            GROUP BY 1, 2, 3
            ORDER BY 4 DESC
        """,
        "comparative_keys": """
            target.salesperson_id = source.salesperson_id AND
            target.product_name = source.product_name              
         """
    },
    "sales_people_by_product_month":{
        "tables": ['sales', 'sales_people', 'sales_items', 'products'],
        "sql":"""
            SELECT
                s.salesperson_id,
                sp.full_name,
                p.product_name,
                s.sale_month,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN sales_people AS sp ON s.salesperson_id = sp.salesperson_id
            LEFT JOIN sales_items AS si ON s.sale_id = si.sale_id
            LEFT JOIN products AS p ON si.product_id = p.product_id
            GROUP BY 1, 2, 3, 4
            ORDER BY 5 DESC
        """,
        "comparative_keys": """
            target.salesperson_id = source.salesperson_id AND
            target.product_name = source.product_name AND
            target.sale_month = source.sale_month
         """
    },
    "top_selling_products":{
        "tables": ['sales', 'sales_people', 'sales_items', 'products'],
        "sql":"""
            SELECT
                si.product_id,
                p.product_name,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN sales_people AS sp ON s.salesperson_id = sp.salesperson_id
            LEFT JOIN sales_items AS si ON s.sale_id = si.sale_id
            LEFT JOIN products AS p ON si.product_id = p.product_id
            GROUP BY 1, 2
            ORDER BY 3 DESC
        """,
        "comparative_keys": "target.product_id = source.product_id"
    },
    "top_selling_products_by_month":{
        "tables": ['sales', 'sales_people', 'sales_items', 'products'],
        "sql":"""
            SELECT
                si.product_id,
                p.product_name,
                s.sale_month,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN sales_people AS sp ON s.salesperson_id = sp.salesperson_id
            LEFT JOIN sales_items AS si ON s.sale_id = si.sale_id
            LEFT JOIN products AS p ON si.product_id = p.product_id
            GROUP BY 1, 2, 3
            ORDER BY 4 DESC
        """,
        "comparative_keys": """
            target.product_id = source.product_id AND
            target.sale_month = source.sale_month
         """
    },
    "top_spending_clients":{
        "tables": ['sales', 'clients'],
        "sql":"""
            SELECT
                s.client_id,
                c.full_name,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN clients AS c ON s.client_id = c.client_id
            GROUP BY 1, 2
            ORDER BY 3 DESC
        """,
        "comparative_keys": "target.client_id = source.client_id"
    },
    "top_spending_clients_by_age_group":{
        "tables": ['sales', 'clients'],
        "sql":"""
            SELECT
                c.age_group,
                SUM(s.total_amount) AS total_sales
            FROM sales AS s
            LEFT JOIN clients AS c ON s.client_id = c.client_id
            GROUP BY 1
            ORDER BY 2 DESC
        """,
        "comparative_keys": "target.age_group = source.age_group"
    }
}

# COMMAND ----------

for table_name, sql_transf_query in gold_transformation.items():
    # Reading data from silver layer
    read_from_gold_batch(env, 'silver', sql_transf_query["tables"])
    # Combining/Tranforming silver layer 
    df_silver_transformed = transforming_silver_tables(sql_transf_query["sql"])
    # Writing to gold layer
    write_to_gold_batch(df_silver_transformed, env, table_name, sql_transf_query["comparative_keys"])

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`sales_people_by_month` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`sales_people_by_product` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`sales_people_by_product_month` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`sales_people_by_total` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`top_selling_products` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`top_selling_products_by_month` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`top_spending_clients` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `dbproj_dev`; select * from `gold`.`top_spending_clients_by_age_group` limit 100;
