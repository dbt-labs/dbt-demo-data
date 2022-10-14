# dbt Python models on Snowpark demo for Coalesce 2022

This repository contains a demo of dbt Python models Snowflake via Snowpark for the Coalesce 2022 conference. It will not be actively maintained. See the repository it was forked from for a current version -- we will work to merge the Python models into the `main` branch there after Coalesce.

## Cool gifs

What a cool DAG! Python and SQL side-by-side in dbt!

![DAG](etc/dag.gif)

Python models in dbt Cloud!

![py_gif](etc/py_gif.gif)

## Get started

Follow these instructions to run yourself.

### Environment

If you're running in dbt Cloud, ensure your environment(s) that need to run Python models are on v1.3+.

To run locally, you need to update `dbt-core` and `dbt-snowflake` to 1.3 or later. We recommend creating a fresh `venv` and `pip install`ing the packages. The exact steps may vary by your platform, but as an example with an environment named `dbt_py`:

```bash
$ python3 -m venv dbt_py
$ source dbt_py/bin/activate
$ (dbt_py) pip install --upgrade dbt-core dbt-snowflake
$ (dbt_py) which python3
```

You can run `dbt --version` to ensure you have v1.3 installed.

To deactivate:

```bash
$ (dbt_py) deactivate
$ which python3
```

You may want to create an additional `venv` for locally running the Snowpark Python package. Instructions for setup are in [Untitled.ipynb](models/challenges/Untitled.ipynb), preceding local prototypes for the Python models.

### Source data

If you're a dbt Labs employee, you can skip this step -- the source data is already loaded into the Snowflake sandbox account.

Otherwise, run the [snowflake.sql](scripts/snowflake.sql) script through the Snowflake UI or locally via `snowsql`. This will create the ecommerce database and source tables from parquet files in S3. Modify the script as needed.

### Run or build

Run individual models or build the entire project!

```bash
$ (dbt_py) dbt run -s describe_py
$ (dbt_py) dbt build
```

And generate the docs!

```bash
$ (dbt_py) dbt docs generate && dbt docs serve
```

## Challenges

See [the challenges directory's README.md](models/challenges/README.md).

## Contributing

We'd welcome contributions to this demo project. However, we will likely archive this repository sometime after Coalesce 2022. Consider contributing to the repository this one is forked from instead!
