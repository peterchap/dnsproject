import requests
import pandas as pd
from io import StringIO
from datetime import datetime

token = "5123dedd3e04a07c6380b8aec0ba30b2"
zone = "detailed-update"
format = "text"
cols = ["domain", "ns", "ip", "country", "web_server", "email", "Alexa_rank", "phone"]
api = f"https://domains-monitor.com/api/v1/{token}/get/{zone}/list/{format}/"
directory = "/root/updates/"
current_datetime = str(datetime.now().strftime("%Y-%m-%d"))
file = "domain_update_daily" + current_datetime + ".parquet"
# print(api)

data = requests.get(api).text
print(len(data))
df = pd.read_csv(StringIO(data), sep=";", quotechar='"', names=cols)
print(df.shape)
df.to_parquet(directory + file, index=False)
