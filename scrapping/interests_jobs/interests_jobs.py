import requests
import pandas as pd


df_interest = pd.read_parquet("../interests/interests.parquet")
list_df = []

for code in df_interest.interest_id:

    response = requests.get(f"https://candidat.francetravail.fr/gw-metierscope/interest/{code:02d}")

    if response.status_code == 200:
        print(f"Successfully retrieved the page for code {code:02d}")
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["interest_id"] = code
        list_df.append(df)
        
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

df_interest_jobs = pd.concat(list_df, ignore_index=True)[["interest_id", "romeCode"]].rename(columns={"romeCode": "jobID"}).astype({"interest_id": "int", "jobID": "str"})
df_interest_jobs.to_csv("interests_jobs.csv", index=False)
df_interest_jobs.to_parquet("interests_jobs.parquet", index=False)