import numpy as np
import pandas as pd


# HACK: we need to define this function so that dbt can parse the model correctly
# here we need to define all refs that are used in the model
def model(dbt, session):
    return pd.DataFrame()  # it's required to return DataFrame in dbt python models


df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
print(df.head())

print('Hello, World!')
