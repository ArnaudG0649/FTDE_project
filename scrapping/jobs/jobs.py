import requests
import pandas as pd

response = requests.get("https://candidat.francetravail.fr/gw-metierscope/jobs/groupByFirstLetter")


if response.status_code == 200:
    Dict = response.json()
    df = pd.DataFrame(sum(Dict.values(), [])).astype({"romeCode": str, "mainName": str})
    df.to_csv("jobs.csv", index=False)
    df.to_parquet("jobs.parquet", index=False)
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
