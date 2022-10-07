import pyspark.pandas as ps


def model(dbt, session):

    # upstream data
    order_items = dbt.ref("stg_order_items").to_pandas_on_spark()
    products = dbt.ref("stg_products").to_pandas_on_spark()
    orders = dbt.ref("stg_orders").to_pandas_on_spark()

    # merge
    merged = order_items.merge(products, on="product_id")

    # pivot
    pivoted = merged.pivot(
        index="order_id", columns="product_id", values="product_price"
    )

    # fill NaNs
    pivoted = pivoted.fillna(0)

    # rename columns
    renames = {col: f"subtotal_{col}" for col in pivoted.columns}
    pivoted = pivoted.rename(columns=renames)

    # compute subtotals
    # YOLO: https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/options.html#operations-on-different-dataframes
    ps.set_option("compute.ops_on_diff_frames", True)
    pivoted["subtotal"] = pivoted.sum(axis=1)

    # more merging
    final = pivoted.merge(orders, left_index=True, right_on="order_id").set_index(
        "order_id"
    )

    return final
