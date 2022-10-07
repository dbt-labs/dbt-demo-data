def model(dbt, session):

    # get upstream data
    orders = dbt.ref("orders").to_pandas_on_spark()

    # describe the data
    described = orders.describe()

    # insert the index as the first
    described.insert(0, "metric", described.index)

    return described
