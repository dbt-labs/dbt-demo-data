def model(dbt, session):

    # get upstream data
    orders = dbt.ref("orders").to_pandas()
    products = dbt.ref("stg_products").to_pandas()
    order_items = dbt.ref("stg_order_items").to_pandas()

    # list of unique product ids
    product_ids = sorted(list(set(products["product_id".upper()].unique())))

    # get the subtotal for each product_id for each order_id
    order_item_product_subtotals = (
        order_items.merge(products, on="product_id".upper())
        .groupby(
            ["order_id".upper(), "product_id".upper()],
            as_index=False,
        )
        .agg(SUBTOTAL=("product_price".upper(), "sum"))
        .reset_index()
        .pivot(
            index="order_id".upper(),
            columns="product_id".upper(),
            values="subtotal".upper(),
        )
        .reset_index()
    )

    # rename the product_id columns to include "subtotal_"
    renames = {
        product_id: f"subtotal_{product_id}".upper() for product_id in product_ids
    }
    order_item_product_subtotals = order_item_product_subtotals.rename(columns=renames)

    # fill NaNs with 0s
    order_item_product_subtotals = order_item_product_subtotals.fillna(0)

    # merge with the existing orders mart
    orders_with_subtotals = orders.merge(
        order_item_product_subtotals, on="order_id".upper()
    )

    return orders_with_subtotals
