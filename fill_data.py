import requests
import pandas as pd
import urllib.parse as parse_url

df = pd.read_csv("sample_us_users.csv")
for _idx in range(len(df)):
    res = requests.post("http://0.0.0.0:5000/user?user_json={}".format(parse_url.quote(str(df.iloc[_idx].to_dict()))))
    print(res.text)