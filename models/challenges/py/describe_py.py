def model(dbt, session):

    # get upstream data
    orders = dbt.ref("orders")

    # describe the data
    described = orders.describe()

    return described