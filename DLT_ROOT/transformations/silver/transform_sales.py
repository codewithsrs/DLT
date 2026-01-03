import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "sales_stg_trans"
)

def sales_srg_trans():
    df = spark.readStream.table("append_sales")
    df = df.withColumn("total_amount",col("quantity")*col("amount"))
    return df


dlt.create_streaming_table(
    name = "sales_trans"
)


dlt.create_auto_cdc_flow(
    target = "sales_trans",
    source = "sales_stg_trans",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    ignore_null_updates = False,
    stored_as_scd_type = 1
)