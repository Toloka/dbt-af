import pandas as pd


def model(dbt, session):
    # HACK: we need to define this function so that dbt can parse the model correctly
    # here we need to define all refs that are used in the model
    dbt.ref('b1')
    return pd.DataFrame()
