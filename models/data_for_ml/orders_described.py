#imports
import pyspark.pandas as ps


def model(dbt, session):

    # get upstream data
    orders = dbt.ref("orders").pandas_api()

    # describe data
    described = orders.describe()

    # insert index as the first column named metrics
    described.insert(0, "metric", described.index)

    return described
