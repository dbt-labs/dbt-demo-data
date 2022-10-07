from random import random, randint


def model(dbt, session):

    # get upstream data
    orders = dbt.ref("orders").pandas_api()
    customers = dbt.ref("customers").pandas_api()

    # imagine instead of clean customer_ids in the orders,
    # we only have the names written by our employees and
    # processed by our AI. we want to match these
    # to our known customers from the rewards program by name.

    # drop columns related to the customer besides their name
    drop_cols = ["customer_id", "customer_order_index", "is_first_order"]
    drop_cols.extend([col for col in customers.columns if col != "customer_name"])

    # merge in customers with orders by customer_id and drop the columns
    fuzzed = orders.merge(customers, on="customer_id").drop(drop_cols, axis=1)

    # fuzz names and add them in
    names = fuzzed["customer_name"]
    fuzzed_names = [fuzz_name(name) for name in names.to_numpy()]
    fuzzed["customer_name_fuzzed"] = fuzzed_names

    return fuzzed


def fuzz_name(name):

    fuzz_name = ""
    names = name.split(" ")

    for name in names:
        if random() < 0.5:
            # employee or AI is decisive
            if random() < 0.5:
                # and loves all caps
                name = name.upper()
            else:
                # or all lowercase
                name = name.lower()

        if random() < 0.2:
            # AI dropped the first or last letter probably :/
            if random() < 0.5:
                # first letter dropped, whoops
                name = name[1:]
            else:
                # last letter dropped, whoops
                name = name[:-1]

        if random() < 0.1:
            # a solar flare hit the datacenter in all regions,
            # no multi-region resiliency could have saved it :(
            for char in name:
                if random() < 0.3:
                    name = name.replace(char, chr(ord(char) + randint(-5, 5)))

        fuzz_name += name + " "

    return fuzz_name.strip()
