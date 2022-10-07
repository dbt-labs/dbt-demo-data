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
    customers = dbt.ref("customers").to_pandas()
    fuzzed = dbt.ref("fuzz_py").to_pandas()

    customer_names = sorted(list(customers["customer_name".upper()].unique()))

    # match customer names to unfuzz
    fuzzed["customer_name_unfuzzed".upper()] = fuzzed[
        "customer_name_fuzzed".upper()
    ].apply(lambda x: match_str(x, customer_names)[0])

    # add in the matched customer names
    unfuzzed = fuzzed

    return unfuzzed
