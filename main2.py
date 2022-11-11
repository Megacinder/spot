import requests
import pandas as pd
import pendulum


for i in pd.date_range(pendulum.date(2022, 10, 1), pendulum.date(2022, 10, 15)):
    print(i)
