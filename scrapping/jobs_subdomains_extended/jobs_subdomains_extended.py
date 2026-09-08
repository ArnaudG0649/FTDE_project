import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


df_domains = pd.read_parquet("../domains/domains.parquet")
list_df = []
list_df2 = []

for domain_id,label,_ in df_domains.itertuples(index=False):

    response = requests.get(f"https://candidat.francetravail.fr/gw-metierscope/domain/{domain_id:03d}")

    if response.status_code == 200:
        print(f"Successfully retrieved the page for code {domain_id:03d}")
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["domain_id"] = domain_id
        try:
            df["subDomain"] = df["subDomain"].map(lambda x: x["label"])
        except:
            df["subDomain"] =  label
        list_df2.append(df)


    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def fetch_jobs_subdomains(
    domain_id, label, url_base="https://candidat.francetravail.fr/gw-metierscope/domain"
):
    response = requests.get(f"{url_base}/{domain_id:03d}")

    if response.status_code == 200:
        print(f"Successfully retrieved the page for code {domain_id:03d}")
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["domain_id"] = domain_id
        try:
            df["subDomain"] = df["subDomain"].map(lambda x: x["label"])
        except:
            df["subDomain"] = label
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")



total = len(df_domains)
with ThreadPoolExecutor(max_workers=10) as executor:
            # future_to_code = [
            #     executor.submit(, code, label)
            #     for code, label, _ in df_domains.itertuples(index=False)
            #     ]
                
            list_df=executor.map(fetch_jobs_subdomains, df_domains.domain_id, df_domains.label)
            

            # completed = 0
            # for future in as_completed(future_to_code):
            #     df = future.result()
            #     # print(data)
            #     list_df.append(df)
            #     completed += 1

df_extended2 = pd.concat(list_df2, ignore_index=True)[["romeCode", "subDomain"]]
df_extended2.sort_values(by=["romeCode", "subDomain"], inplace=True)
df_extended2.reset_index(drop=True, inplace=True)
df_extended = pd.concat(list_df, ignore_index=True)[["romeCode", "subDomain"]]
df_extended.sort_values(by=["romeCode", "subDomain"], inplace=True)
df_extended.reset_index(drop=True, inplace=True)
print(df_extended2.equals(df_extended)) # testing if the dataframes are equals
df_extended.to_csv("jobs_subdomains_extended.csv", index=True)
df_extended2.to_csv("jobs_subdomains_extended2.csv", index=True)

