# NOTE: you need to install this package into your cluster or job configuration
from thefuzz.process import extractOne as match_str


def model(dbt, session):

    # get upstream data
    customers = dbt.ref("customers").pandas_api()
    fuzzed = dbt.ref("fuzz_py").pandas_api()

    # get actual list of customer names
    customer_names = sorted(list(set(customers["customer_name"].unique().to_numpy())))

    # match customer names to unfuzz
    unfuzzed = fuzzed
    unfuzzed["customer_name_unfuzeed"] = fuzzed["customer_name"].apply(
        lambda x: match_str(x, customer_names)[0]
    )

    return unfuzzed
