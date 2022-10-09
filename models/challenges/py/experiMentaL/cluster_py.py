import pandas as pd

from random import random
from sklearn.cluster import KMeans


def model(dbt, session):

    # dbt configuration
    dbt.config(packages=["pandas", "scikit-learn"])

    # get variables
    n_clusters = dbt.config.get("suspected_personas")

    # get upstream data
    orders_with_subtotals = (
        dbt.ref("pivot_sql").to_pandas()
        if random() < 0.5
        else dbt.ref("pivot_py").to_pandas()
    )

    # feature engineering
    X = orders_with_subtotals.select_dtypes(include=["number"]).fillna(0).values

    # train the ML model
    model = KMeans(n_clusters=n_clusters)
    model = model.fit(X)

    # score the ML model
    cluster_labels = model.predict(X)

    # add the cluster labels to orders_with_subtotals
    temp = pd.DataFrame(data=cluster_labels, columns=["cluster_label".upper()])
    orders_with_subtotals_and_clusters = orders_with_subtotals.merge(
        temp, left_index=True, right_index=True
    )

    return orders_with_subtotals_and_clusters
