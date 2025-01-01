# Databricks notebook source
source_dir = "/Volumes/incremental_load/default/order_data/source/"
archive_dir = "/Volumes/incremental_load/default/order_data/archive/"
stage_table = "incremental_load.default.orders_stage"

# COMMAND ----------

df = spark.read.csv(source_dir, header=True, inferSchema=True)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(stage_table)

# COMMAND ----------

files = dbutils.fs.ls(source_dir)

for file in files:
    path = file.path

    target_path = archive_dir + path.split("/")[-1]

    dbutils.fs.mv(path, target_path)


# COMMAND ----------


