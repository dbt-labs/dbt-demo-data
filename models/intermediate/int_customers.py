def model(dbt, session):

    # get upstream data
    stg_customers = dbt.ref("stg_customers").to_pandas()

    # drop duplicate customer_name and their ids.
    # as as an ultra fancy shop, we only allow one customer per name.
    int_customers = stg_customers.drop_duplicates(
        subset=["customer_name".upper()], keep="first"
    )

    return int_customers
