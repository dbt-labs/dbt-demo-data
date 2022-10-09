# TODO: update to thefuzz
try:
    from thefuzz.process import extractOne as match_str
except:
    from fuzzywuzzy.process import extractOne as match_str


def model(dbt, session):

    # configure dbt
    # TODO: update to thefuzz
    dbt.config(packages=["fuzzywuzzy"])

    # get upstream data
    customers = dbt.ref("int_customers").to_pandas()
    orders = dbt.ref("int_orders").to_pandas()

    # take a sample, otherwise this can take days...
    unfuzzed = orders.sample(100)

    # get list of possible matches
    names = sorted(list(customers["customer_name".upper()].unique()))

    # match customer names to unfuzz
    unfuzzed[
        ["customer_name_match".upper(), "match_likelihood".upper()]
    ] = unfuzzed.apply(
        lambda x: match_str(x["customer_name".upper()], names),
        axis=1,
        result_type="expand",
    )

    return unfuzzed
