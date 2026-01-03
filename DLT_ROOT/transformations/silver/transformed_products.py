import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
    name = "products_trans"
)

def sales_stg_trans():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price",col("price").cast(IntegerType()))
    return df


dlt.create_streaming_table(
    name = "product_transformed"
)


dlt.create_auto_cdc_flow(
    target = "product_transformed",
    source = "products_trans",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    stored_as_scd_type = 1
)