# imports
import mlflow

import pyspark.pandas as ps

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA


def model(dbt, session):

    # MLflow control
    experiment_name = "/Users/cody.peterson@dbtlabs.com/demo"
    model_name = "dbt-databricks-demo"
    train_new_model = False
    register_model = train_new_model and False

    # get upstream data
    orders_with_subtotals = dbt.ref("orders_with_subtotals").pandas_api()

    # drop non-numeric columns TODO: programmatic?
    X = orders_with_subtotals.drop(
        columns=[
            "order_id",
            "location_id",
            "customer_id",
            "ordered_at",
            "location_name",
            "is_first_order",
            "is_food_order",
            "is_drink_order",
        ],
        axis=1,
    )

    if train_new_model:
        # log ML stuff
        mlflow.set_experiment(experiment_name)
        mlflow.start_run()
        mlflow.autolog()

        # train model
        kmeans = KMeans(n_clusters=5)
        model = kmeans.fit(X.to_numpy())

        if register_model:
            # register ML model
            artifact_uri = mlflow.get_artifact_uri()
            mlflow.register_model(artifact_uri + "/model", model_name)

    else:
        # load the latest registered model
        version = get_latest_model_version(model_name)
        model_uri = f"models:/{model_name}/{version}"
        model = mlflow.pyfunc.load_model(model_uri)

    # score model
    cluster_labels = model.predict(X.to_numpy())

    # add cluster labels to orders_with_subtotals
    temp = ps.DataFrame(data=cluster_labels)
    temp = temp.rename(columns={0: "cluster_label"})
    orders_with_subtotals_and_clusters = orders_with_subtotals.merge(
        temp, left_index=True, right_index=True
    )

    return orders_with_subtotals_and_clusters


def get_latest_model_version(model_name):

    # assume default incremental integer versioning
    latest_version = 1

    # get the MLflow client
    mlflow_client = mlflow.MlflowClient()

    # get the latest model version
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int

    return latest_version
