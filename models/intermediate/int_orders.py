from random import random, randint


def model(dbt, session):

    # get upstream data
    stg_orders = dbt.ref("stg_orders").pandas_api()
    int_customers = dbt.ref("int_customers").pandas_api()

    # imagine instead of clean customer_ids in the orders,
    # we only have the names written by our employees and
    # recorded by our AI in the database. we want to match these
    # to our known customers.
    # to boost the exclusiveness of our shop, we strictly allow
    # only one customer per name, regardless of location. this is
    # our differentiator from the competition.
    int_orders = stg_orders.merge(int_customers, on="customer_id").drop(
        "customer_id", axis=1
    )

    # names fuzzed by humans and AI
    int_orders["customer_name"] = int_orders["customer_name"].apply(
        lambda x: fuzz_name(x)
    )

    return int_orders


def fuzz_name(name):

    fuzz_name = ""
    names = name.split(" ")

    for name in names:
        if random() < 0.8:
            if random() < 0.5:
                # all caps
                name = name.upper()
            elif random() > 0.5:
                # all lowercase
                name = name.lower()
            elif random() < 0.5:
                # title case?
                name = name.title()
            elif random() < 0.1:
                # lIKE tHiS?
                name = "".join(
                    [c.upper() if i % 2 == 1 else c.lower() for i, c in enumerate(name)]
                )
            else:
                # swapcase?
                name = name.swapcase()

        if random() < 0.2:
            # AI dropped the first or last letter probably :/
            if random() < 0.5:
                # first letter dropped, whoops
                name = name[1:]
            else:
                # last letter dropped, whoops
                name = name[:-1]

        if random() < 0.2:
            # a solar flare hit the datacenter in all regions,
            # no multi-region resiliency could have saved it :(
            for char in name:
                if random() < 0.3:
                    name = name.replace(char, chr(ord(char) + randint(-5, 5)))

        fuzz_name += name + " "

    return fuzz_name.strip()
