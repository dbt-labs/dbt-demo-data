import itertools

# TODO: update to thefuzz
try:
    from thefuzz import fuzz
except:
    from fuzzywuzzy import fuzz


def model(dbt, session):

    # configure dbt
    # TODO: update to thefuzz
    dbt.config(packages=["fuzzywuzzy"])

    # get upstream data
    customers = dbt.ref("customers").to_pandas()

    # mark potential duplicates for manual review
    unfuzzed = customers
    unfuzzed["likely_duplicate".upper()] = [False] * len(unfuzzed)
    unfuzzed["likely_duplicate_ids".upper()] = [set({})] * len(unfuzzed)

    for i, j in itertools.combinations(range(len(unfuzzed)), 2):
        if i != j:
            name1, id1 = unfuzzed.iloc[i][
                ["customer_name".upper(), "customer_id".upper()]
            ]
            name2, id2 = unfuzzed.iloc[j][
                ["customer_name".upper(), "customer_id".upper()]
            ]

            if fuzz.ratio(name1, name2) > 95:
                unfuzzed.iloc[i][["likely_duplicate".upper()]] = True
                unfuzzed.iloc[i][["likely_duplicate_ids".upper()]].values[0].add(id2)
                unfuzzed.iloc[j][["likely_duplicate".upper()]] = True
                unfuzzed.iloc[j][["likely_duplicate_ids".upper()]].values[0].add(id1)

    return unfuzzed
