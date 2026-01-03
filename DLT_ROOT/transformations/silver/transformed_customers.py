import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
    name = "customers_trans"
)

def customer_stg_trans():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("CustomerNameUp",upper(col("customer_name")))
    return df


dlt.create_streaming_table(
    name = "customer_transformed"
)


dlt.create_auto_cdc_flow(
    target = "customer_transformed",
    source = "customers_trans",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    stored_as_scd_type = 1
)