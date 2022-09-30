import pyspark.pandas as ps


def model(dbt, session):

    # upstream data
    df = dbt.ref("orders").to_pandas_on_spark()

    # describe the data
    df = df.describe()

    # insert the index as a column in the first position
    df.insert(0, "metric", df.index)

    return df
