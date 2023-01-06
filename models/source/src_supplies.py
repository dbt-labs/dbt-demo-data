# code stolen from internal Hex notebook

# imports
import uuid
import random
import datetime

import numpy as np
import pandas as pd

from faker import Faker

# OOP
class Curve:
    @classmethod
    def eval(cls, date):
        x = cls.TranslateDomain(date)
        x_mod = x % len(cls.Domain)
        x_translated = cls.Domain[x_mod]
        return cls.Expr(x_translated)


class AnnualCurve(Curve):
    Domain = np.linspace(0, 2 * np.pi, 365)
    TranslateDomain = lambda date: date.timetuple().tm_yday
    Expr = lambda x: (np.cos(x) + 1) / 10 + 0.8


class WeekendCurve(Curve):
    Domain = tuple(range(6))
    TranslateDomain = lambda date: date.weekday() - 1
    Expr = lambda x: 0.6 if x >= 6 else 1


class GrowthCurve(Curve):
    Domain = tuple(range(500))
    TranslateDomain = lambda date: (date.year - 2016) * 12 + date.month
    # ~ aim for ~20% growth/year
    Expr = lambda x: 1 + (x / 12) * 0.2


class Day(object):
    EPOCH = datetime.datetime(year=2016, month=9, day=1)
    SEASONAL_MONTHLY_CURVE = AnnualCurve()
    WEEKEND_CURVE = WeekendCurve()
    GROWTH_CURVE = GrowthCurve()

    def __init__(self, date_index, minutes=0):
        self.date_index = date_index
        self.date = self.EPOCH + datetime.timedelta(days=date_index, minutes=minutes)

        self.day_of_week = self._get_day_of_week(self.date)
        self.is_weekend = self._is_weekend(self.date)
        self.season = self._get_season(self.date)

        self.effects = [
            self.SEASONAL_MONTHLY_CURVE.eval(self.date),
            self.WEEKEND_CURVE.eval(self.date),
            self.GROWTH_CURVE.eval(self.date),
        ]

    def at_minute(self, minutes):
        return Day(self.date_index, minutes=minutes)

    def get_effect(self):
        total = 1
        for effect in self.effects:
            total = total * effect
        return total

        # weekend_effect = 0.8 if date.is_weekend else 1
        # summer_effect = 0.7 if date.season == 'summer' else 1

    def _get_day_of_week(self, date):
        return date.weekday()

    def _is_weekend(self, date):
        # 5 + 6 are weekends
        return date.weekday() >= 5

    def _get_season(self, date):
        month_no = date.month
        day_no = date.day

        if month_no in (1, 2) or (month_no == 3 and day_no < 21):
            return "winter"
        elif month_no in (3, 4, 5) or (month_no == 6 and day_no < 21):
            return "spring"
        elif month_no in (6, 7, 8) or (month_no == 9 and day_no < 21):
            return "summer"
        elif month_no in (9, 10, 11) or (month_no == 12 and day_no < 21):
            return "fall"
        else:
            return "winter"


class Store(object):
    def __init__(
        self, store_id, name, base_popularity, hours_of_operation, opened_date, tax_rate
    ):
        self.store_id = store_id
        self.name = name
        self.base_popularity = base_popularity
        self.hours_of_operation = hours_of_operation
        self.opened_date = opened_date
        self.tax_rate = tax_rate

    def p_buy(self, date):
        date_effect = date.get_effect()
        return self.base_popularity * date_effect

    def minutes_open(self, date):
        return self.hours_of_operation.minutes_open(date)

    def iter_minutes_open(self, date):
        yield from self.hours_of_operation.iter_minutes(date)

    def is_open(self, date):
        return date.date >= self.opened_date.date

    def is_open_at(self, date):
        return self.hours_of_operation.is_open(date)

    def days_since_open(self, date):
        return date.date_index - self.opened_date.date_index

    def opens_at(self, date):
        return self.hours_of_operation.opens_at(date)

    def closes_at(self, date):
        return self.hours_of_operation.closes_at(date)

    def to_dict(self):
        return {
            "id": self.store_id,
            "name": self.name,
            "opened_at": self.opened_date.date.isoformat(),
            "tax_rate": self.tax_rate,
        }


class Item(object):
    def __init__(self, sku, name, description, type, price):
        self.sku = sku
        self.name = name
        self.description = description
        self.type = type
        self.price = price

    def __str__(self):
        return f"<{self.name} @ ${self.price}>"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            "sku": self.sku,
            "name": self.name,
            "type": self.type,
            "price": int(self.price * 100),
            "description": self.description,
        }


class Inventory(object):
    inventory = {}

    @classmethod
    def update(cls, inventory_list):
        cls.inventory["jaffle"] = []
        cls.inventory["beverage"] = []
        for item in inventory_list:
            ttype = item.type
            cls.inventory[ttype].append(item)

    @classmethod
    def get_food(cls, count=1):
        return [random.choice(cls.inventory["jaffle"]) for i in range(count)]

    @classmethod
    def get_drink(cls, count=1):
        return [random.choice(cls.inventory["beverage"]) for i in range(count)]

    @classmethod
    def to_dict(cls):
        all_items = []
        for key in cls.inventory:
            all_items += [item.to_dict() for item in cls.inventory[key]]
        return all_items


class Supply(object):
    def __init__(self, id, name, cost, perishable, skus):
        self.id = id
        self.name = name
        self.cost = cost
        self.perishable = perishable
        self.skus = skus

    def __str__(self):
        return f"<{self.name} @ ${self.cost}>"

    def __repr__(self):
        return self.__str__()

    def to_dict(self, sku):
        return {
            "id": self.id,
            "name": self.name,
            "cost": int(self.cost * 100),
            "perishable": self.perishable,
            "sku": sku,
        }


class Stock(object):
    stock = {}

    @classmethod
    def update(cls, stock_list):
        for supply in stock_list:
            skus = supply.skus
            for sku in skus:
                if sku not in cls.stock:
                    cls.stock[sku] = []
                cls.stock[sku].append(supply)

    @classmethod
    def to_dict(cls):
        all_items = []
        for key in cls.stock:
            all_items += [item.to_dict(key) for item in cls.stock[key]]
        return all_items


class OrderItem(object):
    def __init__(self, order_id, item):
        self.item_id = str(uuid.uuid4())
        self.order_id = order_id
        self.item = item

    def to_dict(self):
        return {
            "id": self.item_id,
            "order_id": self.order_id,
            "sku": self.item.sku,
        }


class Order(object):
    def __init__(self, customer, items, store, day):
        self.order_id = str(uuid.uuid4())
        self.customer = customer
        self.items = [OrderItem(self.order_id, item) for item in items]
        self.store = store
        self.day = day
        self.subtotal = sum(i.item.price for i in self.items)
        self.tax_paid = store.tax_rate * self.subtotal
        self.order_total = self.subtotal + self.tax_paid

    def __str__(self):
        return f"{self.customer.name} bought {str(self.items)} at {self.day}"

    def to_dict(self):
        return {
            "id": self.order_id,
            "customer": self.customer.customer_id,
            "ordered_at": self.day.date.isoformat(),
            # "order_month": self.day.date.strftime("%Y-%m"),
            "store_id": self.store.store_id,
            "subtotal": int(self.subtotal * 100),
            "tax_paid": int(self.tax_paid * 100),
            "order_total": int(self.order_total * 100),
        }

    def items_to_dict(self):
        return [i.to_dict(self.order_id) for i in self.items]


class Customer(object):
    def __init__(self, store):
        self.customer_id = str(uuid.uuid4())
        self.store = store
        self.name = Faker(123456789).name()  # TODO not ideal
        self.favorite_number = int(np.random.rand() * 100)

    def p_buy_season(self, day):
        return self.store.p_buy(day)

    def p_buy(self, day):
        p_buy_season = self.p_buy_season(day)
        p_buy_persona = self.p_buy_persona(day)
        p_buy_on_day = (p_buy_season * p_buy_persona) ** 0.5
        return p_buy_on_day

    def get_order(self, day):
        items = self.get_order_items(day)
        order_time_delta = self.get_order_time(day)

        order_time = day.at_minute(order_time_delta + self.store.opens_at(day))
        if not self.store.is_open_at(order_time):
            return None

        return Order(self, items, self.store, order_time)

    def get_order_items(self, day):
        raise NotImplemented()

    def get_order_time(self, store, day):
        raise NotImplemented()

    def p_buy_persona(self, day):
        raise NotImplemented()

    def sim_day(self, day):
        p_buy = self.p_buy(day)
        p_buy_threshold = np.random.random()

        if p_buy_threshold < p_buy:
            return self.get_order(day)
        else:
            return None

    def to_dict(self):
        return {
            "id": self.customer_id,
            "name": self.name,
        }


class RemoteWorker(Customer):
    "This person works from a coffee shop"

    def p_buy_persona(self, day):
        buy_propensity = (self.favorite_number / 100) * 0.4
        return 0.001 if day.is_weekend else buy_propensity

    def get_order_time(self, day):
        # most likely to order in the morning
        # exponentially less likely to order in the afternoon
        avg_time = 420
        order_time = np.random.normal(loc=avg_time, scale=180)
        return max(0, int(order_time))

    def get_order_items(self, day):
        num_drinks = 1
        food = []

        if random.random() > 0.7:
            num_drinks = 2

        if random.random() > 0.7:
            food = Inventory.get_food(1)

        return Inventory.get_drink(num_drinks) + food


class BrunchCrowd(Customer):
    "Do you sell mimosas?"

    def p_buy_persona(self, day):
        buy_propensity = 0.2 + (self.favorite_number / 100) * 0.2
        return buy_propensity if day.is_weekend else 0

    def get_order_time(self, day):
        # most likely to order in the early afternoon
        avg_time = 300 + ((self.favorite_number - 50) / 50) * 120
        order_time = np.random.normal(loc=avg_time, scale=120)
        return max(0, int(order_time))

    def get_order_items(self, day):
        num_customers = 1 + int(self.favorite_number / 20)
        return Inventory.get_drink(num_customers) + Inventory.get_food(num_customers)


class Commuter(Customer):
    "the regular, thanks"

    def p_buy_persona(self, day):
        buy_propensity = 0.5 + (self.favorite_number / 100) * 0.3
        return 0.001 if day.is_weekend else buy_propensity

    def get_order_time(self, day):
        # most likely to order in the morning
        # exponentially less likely to order in the afternoon
        avg_time = 60
        order_time = np.random.normal(loc=avg_time, scale=30)
        return max(0, int(order_time))

    def get_order_items(self, day):
        return Inventory.get_drink(1)


class Student(Customer):
    "coffee might help"

    def p_buy_persona(self, day):
        if day.season == "summer":
            return 0
        else:
            buy_propensity = 0.1 + (self.favorite_number / 100) * 0.4
            return buy_propensity

    def get_order_time(self, day):
        # later is better
        avg_time = 9 * 60
        order_time = np.random.normal(loc=avg_time, scale=120)
        return max(0, int(order_time))

    def get_order_items(self, day):
        food = []
        if random.random() > 0.5:
            food = Inventory.get_food(1)

        return Inventory.get_drink(1) + food


class Casuals(Customer):
    "just popping in"

    def p_buy_persona(self, day):
        return 0.1

    def get_order_time(self, day):
        avg_time = 5 * 60
        order_time = np.random.normal(loc=avg_time, scale=120)
        return max(0, int(order_time))

    def get_order_items(self, day):
        num_drinks = int(random.random() * 10 / 3)
        num_food = int(random.random() * 10 / 3)
        return Inventory.get_drink(num_drinks) + Inventory.get_food(num_food)


class Market(object):
    PersonaMix = [
        (Commuter, 0.25),
        (RemoteWorker, 0.2),
        (BrunchCrowd, 0.1),
        (Student, 0.2),
        (Casuals, 0.25),
    ]

    def __init__(self, store, num_customers, days_to_penetration=365):
        self.store = store
        self.num_customers = num_customers
        self.days_to_penetration = days_to_penetration

        self.addressable_customers = []

        for (Persona, weight) in self.PersonaMix:
            num_customers = int(weight * self.num_customers)
            for i in range(num_customers):
                self.addressable_customers.append(Persona(store))

        random.shuffle(self.addressable_customers)

        self.active_customers = []

    def sim_day(self, day):
        days_since_open = self.store.days_since_open(day)
        if days_since_open < 0:
            yield None
            return
        elif days_since_open < 7:
            pct_penetration = min(days_since_open / self.days_to_penetration, 1)
            market_penetration = min(np.log(1.2 + pct_penetration * (np.e - 1.2)), 1)
        else:
            pct_penetration = min(days_since_open / self.days_to_penetration, 1)
            market_penetration = min(np.log(1 + pct_penetration * (np.e - 1)), 1)

        num_desired_customers = market_penetration * len(self.addressable_customers)
        customers_to_add = int(num_desired_customers - len(self.active_customers))

        for i in range(customers_to_add):
            customer = self.addressable_customers.pop()
            self.active_customers.append(customer)

        for customer in self.active_customers:
            order = customer.sim_day(day)
            yield order


class HoursOfOperation(object):
    def __init__(self, weekday_range, weekend_range):
        self.weekday_range = weekday_range
        self.weekend_range = weekend_range

    def minutes_open(self, date):
        if date.is_weekend:
            return self.weekend_range[1] - self.weekend_range[0]
        else:
            return self.weekday_range[1] - self.weekday_range[0]

    def opens_at(self, date):
        if date.is_weekend:
            return self.weekend_range[0]
        else:
            return self.weekday_range[0]

    def closes_at(self, date):
        if date.is_weekend:
            return self.weekend_range[1]
        else:
            return self.weekday_range[1]

    def is_open(self, date):
        opens_at = self.opens_at(date)
        closes_at = self.closes_at(date)

        dt = date.date.hour * 60 + date.date.minute
        return dt >= opens_at and dt < closes_at

    def iter_minutes(self, date):
        if date.is_weekend:
            start, end = self.weekend_range
        else:
            start, end = self.weekday_range

        for i in range(end - start):
            yield start + i


# define dbt model
def model(dbt, session):

    # configure dbt
    dbt.config(packages=["numpy", "pandas", "Faker"])

    # constants
    T_7AM = 60 * 7
    T_8AM = 60 * 8
    T_2PM = 60 * 14
    T_8PM = 60 * 20

    # setup seeds
    rng = np.random.default_rng()

    # simulate data
    inventory = Inventory()
    inventory.update(
        [
            Item(
                sku="JAF-001",
                name="nutellaphone who dis?",
                description="nutella and banana jaffle",
                type="jaffle",
                price=11,
            ),
            Item(
                sku="JAF-002",
                name="doctor stew",
                description="house-made beef stew jaffle",
                type="jaffle",
                price=11,
            ),
            Item(
                sku="JAF-003",
                name="the krautback",
                description="lamb and pork bratwurst with house-pickled cabbage sauerkraut and mustard",
                type="jaffle",
                price=12,
            ),
            Item(
                sku="JAF-004",
                name="flame impala",
                description="pulled pork and pineapple al pastor marinated in ghost pepper sauce, kevin parker's favorite! ",
                type="jaffle",
                price=14,
            ),
            Item(
                sku="JAF-005",
                name="mel-bun",
                description="melon and minced beef bao, in a jaffle, savory and sweet",
                type="jaffle",
                price=12,
            ),
            Item(
                sku="BEV-001",
                name="tangaroo",
                description="mango and tangerine smoothie",
                type="beverage",
                price=6,
            ),
            Item(
                sku="BEV-002",
                name="chai and mighty",
                description="oatmilk chai latte with protein boost",
                type="beverage",
                price=5,
            ),
            Item(
                sku="BEV-003",
                name="vanilla ice",
                description="iced coffee with house-made french vanilla syrup",
                type="beverage",
                price=6,
            ),
            Item(
                sku="BEV-004",
                name="for richer or pourover ",
                description="daily selection of single estate beans for a delicious hot pourover",
                type="beverage",
                price=7,
            ),
            Item(
                sku="BEV-005",
                name="adele-ade",
                description="a kiwi and lime agua fresca, hello from the other side of thirst",
                type="beverage",
                price=4,
            ),
        ]
    )

    stock = Stock()
    stock.update(
        [
            Supply(
                id="SUP-001",
                name="compostable cutlery - knife",
                cost=0.07,
                perishable=False,
                skus=["JAF-001", "JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-002",
                name="cutlery - fork",
                cost=0.07,
                perishable=False,
                skus=["JAF-001", "JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-003",
                name="serving boat",
                cost=0.11,
                perishable=False,
                skus=["JAF-001", "JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-004",
                name="napkin",
                cost=0.04,
                perishable=False,
                skus=["JAF-001", "JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-005",
                name="16oz compostable clear cup",
                cost=0.13,
                perishable=False,
                skus=["BEV-001", "BEV-002", "BEV-003", "BEV-004", "BEV-005"],
            ),
            Supply(
                id="SUP-006",
                name="16oz compostable clear lid",
                cost=0.04,
                perishable=False,
                skus=["BEV-001", "BEV-002", "BEV-003", "BEV-004", "BEV-005"],
            ),
            Supply(
                id="SUP-007",
                name="biodegradable straw",
                cost=0.13,
                perishable=False,
                skus=["BEV-001", "BEV-002", "BEV-003", "BEV-004", "BEV-005"],
            ),
            Supply(
                id="SUP-008",
                name="chai mix",
                cost=0.98,
                perishable=True,
                skus=["BEV-002"],
            ),
            Supply(
                id="SUP-009",
                name="bread",
                cost=0.33,
                perishable=True,
                skus=["JAF-001", "JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-010",
                name="cheese",
                cost=0.2,
                perishable=True,
                skus=["JAF-002", "JAF-003", "JAF-004", "JAF-005"],
            ),
            Supply(
                id="SUP-011",
                name="nutella",
                cost=0.46,
                perishable=True,
                skus=["JAF-001"],
            ),
            Supply(
                id="SUP-012",
                name="banana",
                cost=0.13,
                perishable=True,
                skus=["JAF-001"],
            ),
            Supply(
                id="SUP-013",
                name="beef stew",
                cost=1.69,
                perishable=True,
                skus=["JAF-002"],
            ),
            Supply(
                id="SUP-014",
                name="lamb and pork bratwurst",
                cost=2.34,
                perishable=True,
                skus=["JAF-003"],
            ),
            Supply(
                id="SUP-015",
                name="house-pickled cabbage sauerkraut",
                cost=0.43,
                perishable=True,
                skus=["JAF-003"],
            ),
            Supply(
                id="SUP-016",
                name="mustard",
                cost=0.07,
                perishable=True,
                skus=["JAF-003"],
            ),
            Supply(
                id="SUP-017",
                name="pulled pork",
                cost=2.15,
                perishable=True,
                skus=["JAF-004"],
            ),
            Supply(
                id="SUP-018",
                name="pineapple",
                cost=0.26,
                perishable=True,
                skus=["JAF-004"],
            ),
            Supply(
                id="SUP-019", name="melon", cost=0.33, perishable=True, skus=["JAF-005"]
            ),
            Supply(
                id="SUP-020",
                name="minced beef",
                cost=1.24,
                perishable=True,
                skus=["JAF-005"],
            ),
            Supply(
                id="SUP-021",
                name="ghost pepper sauce",
                cost=0.2,
                perishable=True,
                skus=["JAF-004"],
            ),
            Supply(
                id="SUP-022", name="mango", cost=0.32, perishable=True, skus=["BEV-001"]
            ),
            Supply(
                id="SUP-023",
                name="tangerine",
                cost=0.2,
                perishable=True,
                skus=["BEV-001"],
            ),
            Supply(
                id="SUP-024",
                name="oatmilk",
                cost=0.11,
                perishable=True,
                skus=["BEV-002"],
            ),
            Supply(
                id="SUP-025",
                name="whey protein",
                cost=0.36,
                perishable=True,
                skus=["BEV-002"],
            ),
            Supply(
                id="SUP-026",
                name="coffee",
                cost=0.52,
                perishable=True,
                skus=["BEV-003", "BEV-004"],
            ),
            Supply(
                id="SUP-027",
                name="french vanilla syrup",
                cost=0.72,
                perishable=True,
                skus=["BEV-003"],
            ),
            Supply(
                id="SUP-028", name="kiwi", cost=0.2, perishable=True, skus=["BEV-005"]
            ),
            Supply(
                id="SUP-029", name="lime", cost=0.13, perishable=True, skus=["BEV-005"]
            ),
        ]
    )

    scale = 100
    stores = [
        # id | name | popularity | opened | TAM | tax
        (str(uuid.uuid4()), "Philadelphia", 0.85, 0, 9 * scale, 0.06),
        (str(uuid.uuid4()), "Brooklyn", 0.95, 192, 14 * scale, 0.04),
        (str(uuid.uuid4()), "Chicago", 0.92, 605, 12 * scale, 0.0625),
        (str(uuid.uuid4()), "San Francisco", 0.87, 615, 11 * scale, 0.075),
        (str(uuid.uuid4()), "New Orleans", 0.92, 920, 8 * scale, 0.04),
    ]

    markets = []
    for store_id, store_name, popularity, opened_date, market_size, tax in stores:
        market = Market(
            Store(
                store_id=store_id,
                name=store_name,
                base_popularity=popularity,
                hours_of_operation=HoursOfOperation(
                    weekday_range=(T_7AM, T_8PM),
                    weekend_range=(T_8AM, T_2PM),
                ),
                opened_date=Day(opened_date),
                tax_rate=tax,
            ),
            num_customers=market_size,
        )
    markets.append(market)

    customers = {}
    orders = []

    sim_days = 365 * 10
    for i in range(sim_days):

        for market in markets:
            store = market.store.name
            day = Day(i)
            for order in (o for o in market.sim_day(day) if o is not None):
                orders.append(order)
                if order.customer.customer_id not in customers:
                    customers[order.customer.customer_id] = order.customer

    person = customers[list(customers.keys())[0]]
    df_customers = pd.DataFrame.from_dict(c.to_dict() for c in customers.values())
    df_orders = pd.DataFrame.from_dict(o.to_dict() for o in orders)
    df_items = pd.DataFrame.from_dict(i.to_dict() for o in orders for i in o.items)
    df_stores = pd.DataFrame.from_dict(m.store.to_dict() for m in markets)
    df_products = pd.DataFrame.from_dict(inventory.to_dict())
    df_supplies = pd.DataFrame.from_dict(stock.to_dict())

    # combine into a single dataframe

    # for snowflake
    df_supplies.columns = [c.upper() for c in df_supplies.columns]

    # return the model
    return df_supplies


# define functions
