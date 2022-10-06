# see the .dbtignore in the root of the repo -- this file is ignored!


def model(dbt, session):
    return session.sql("select 1 as id")
