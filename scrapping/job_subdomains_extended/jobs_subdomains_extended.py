import requests
import pandas as pd


df_domains = pd.read_parquet("../domains/domains.parquet")
list_df = []

for code,label,_ in df_domains.itertuples(index=False):

    response = requests.get(f"https://candidat.francetravail.fr/gw-metierscope/domain/{code:03d}")

    if response.status_code == 200:
        print(f"Successfully retrieved the page for code {code:03d}")
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["DomainID"] = code
        try:
            df["subDomain"] = df["subDomain"].map(lambda x: x["label"])
        except:
            df["subDomain"] =  label
        list_df.append(df)
        
        
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


df_extended = pd.concat(list_df, ignore_index=True)
df_extended.to_csv("jobs_subdomains_extended.csv", index=True)