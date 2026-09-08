import requests
import pandas as pd

df_extended = pd.read_csv("../job_subdomains_extended/jobs_subdomains_extended.csv", usecols=["subDomain", "romeCode"])
df_subdomains = pd.read_csv("../subdomains/subdomains.csv", usecols=["subDomain", "subDomainID"])

df_jobs_subdomains = df_extended.merge(df_subdomains, on="subDomain", how="left")[["romeCode", "subDomainID"]].astype({"romeCode": "string", "subDomainID": "int"})
df_jobs_subdomains.to_csv("jobs_subdomains.csv", index=False)
df_jobs_subdomains.to_parquet("jobs_subdomains.parquet", index=False)