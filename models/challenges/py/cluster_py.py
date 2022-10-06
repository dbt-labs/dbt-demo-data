import pandas as pd

from sklearn.cluster import KMeans


def model(dbt, session):

    # dbt configuration
    dbt.config(packages=["pandas", "scikit-learn"])

    # get variables
    n_clusters = dbt.config.get("suspected_personas")

    # get upstream data
    orders_with_subtotals = dbt.ref("pivot_py").to_pandas()

    # feature engineering
    df = orders_with_subtotals
    # drop non-numeric columns programmatically
    X = df.select_dtypes(include=["number"]).values

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
