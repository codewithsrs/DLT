import dlt


sales_rules = {
    "rule_1":"sales_id is not null"
}

#create empty streaming table
dlt.create_streaming_table(
    name = "append_sales",
    expect_all_or_drop=sales_rules
)

#creating east sales flow
@dlt.append_flow(target="append_sales")
def east_sales():
    df = spark.readStream.table("dltsrs.source.sales_east")
    return df


@dlt.append_flow(target="append_sales")
def west_sales():
    df = spark.readStream.table("dltsrs.source.sales_west")
    return df