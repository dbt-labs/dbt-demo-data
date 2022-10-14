def model(dbt, session):
    """
    In our scenario for the purposes of this demo, we can only
    allow one unique customer_name per customer_id.
    Imagine we're an ultra-hip, ultra-cool, ultra-exclusive
    coffee shop and our one-name-per-customer policy is
    a big part of our brand.
    """

    # get upstream data
    stg_customers = dbt.ref("stg_customers").to_pandas()

    # drop duplicate customer_name and their ids.
    # as as an ultra fancy shop, we only allow one customer per name.
    int_customers = stg_customers.drop_duplicates(
        subset=["customer_name".upper()], keep="first"
    )

    return int_customers
