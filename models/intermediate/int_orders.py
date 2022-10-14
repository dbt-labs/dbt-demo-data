from random import random, randint


def model(dbt, session):
    """
    In our scenario for the purposes of this demo, we get the
    customer_name column from a datacenter located on Mercury
    subject to solar flares. The customer name is first heard by
    our employee, who writes it down on a coffee cup. An AI-powered
    camera 50 feet away reads the coffee cup and transcribes the
    name into the database on Mercury.
    """

    # get upstream data
    stg_orders = dbt.ref("stg_orders").to_pandas()
    int_customers = dbt.ref("int_customers").to_pandas()

    # swap customer_id with customer_name and prepare to fuzz
    int_orders = stg_orders.merge(int_customers, on="customer_id".upper()).drop(
        "customer_id".upper(), axis=1
    )

    # names fuzzed by humans, AI, and solar flares
    int_orders["customer_name".upper()] = int_orders["customer_name".upper()].apply(
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
            # and we only bought single-planet resiliency.
            for char in name:
                if random() < 0.3:
                    name = name.replace(char, chr(ord(char) + randint(-5, 5)))

        fuzz_name += name + " "

    return fuzz_name.strip()
