import requests
import pandas as pd
from io import StringIO

token = "5123dedd3e04a07c6380b8aec0ba30b2"
zone = "detailed-update"
format = "text"
cols = ["domain", "ns", "ip", "country", "web_server", "email", "Alexa_rank", "phone"]
api = f"https://domains-monitor.com/api/v1/{token}/get/{zone}/list/{format}/"
# print(api)

data = requests.get(api).text
df = pd.read_csv(StringIO(data), sep=";", quotechar='"', names=cols)
pd.set_option("display.max_columns", None)
print(df.head(1))
