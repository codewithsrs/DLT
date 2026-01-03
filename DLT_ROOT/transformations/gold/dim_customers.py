import dlt

dlt.create_streaming_table(
    name = "dim_customers"
)


dlt.create_auto_cdc_flow(
    target = "dim_customers",
    source = "customers_trans",
    keys = ["customer_id"],
    sequence_by = "last_updated",
    stored_as_scd_type=2
)