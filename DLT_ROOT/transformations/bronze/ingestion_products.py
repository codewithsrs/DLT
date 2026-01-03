import dlt

products_rules = {
    "rule_1":"product_id IS NOT NULL",
    "rule_2":"price >= 0"
}

@dlt.table(
    name = "products_stg"
)
@dlt.expect_all_or_drop(products_rules)
def product_stg():
    df = spark.readStream.table("dltsrs.source.products")
    return df