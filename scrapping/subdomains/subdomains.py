import requests
import pandas as pd

df = pd.read_csv("../job_subdomains_extended/jobs_subdomains_extended.csv", usecols=["subDomain", "DomainID"])

my_unique_value = lambda x:list(x)[0]
df_subdomains = df.groupby("subDomain").agg(my_unique_value).reset_index().sort_values("DomainID")
df_subdomains["subDomainID"] = range(1, len(df_subdomains) + 1)
df_subdomains = df_subdomains[["subDomainID", "subDomain", "DomainID"]].astype({"subDomainID": "int", "subDomain": "string", "DomainID": "int"})

df_subdomains.to_csv("subdomains.csv",index=False)
df_subdomains.to_parquet("subdomains.parquet",index=False)
