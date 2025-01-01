# Databricks notebook source
from delta.tables import *

stage_table_name = "incremental_load.default.orders_stage"
target_table_name = "incremental_load.default.orders_target"

# COMMAND ----------

df = spark.read.table(stage_table_name)

# COMMAND ----------

if not spark._jsparkSession.catalog().tableExists(target_table_name):
    df.write.format("delta").saveAsTable(target_table_name)

else:
    # delta_table = DeltaTable.forName(spark, target_table_name)
    target_table = spark.read.format("delta").table(target_table_name)
    merge_condition = "stage.tracking_num = target.tracking_num"

    target_table.alias("target")\
        .merge(df.alias("stage"), merge_condition)\
        .whenMatchedDelete() \
        .execute()

    # target_table.alias("target")\
        # .merge(df.alias("stage"), merge_condition)\
        # .whenMatchedUpdate(set={"target.status": "stage.status"})\
        # .whenNotMatchedInsertAll()\       
        # .execute()

    target_table.write.format("delta").mode("append").saveAsTable(target_table_name)

