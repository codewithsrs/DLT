import dlt

dlt.create_streaming_table(
    name = "fact_sales"
)


dlt.create_auto_cdc_flow(
    target = "fact_sales",
    source = "sales_stg_trans",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    stored_as_scd_type=1
)