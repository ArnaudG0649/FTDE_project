import requests
import pandas as pd

response = requests.get("https://candidat.francetravail.fr/gw-metierscope/domains")

# code label labelUrl
# 1  J'ai envie de créer, construire, rénover  ai-envie-de-creer-construire-renover

if response.status_code == 200:
    # df = pd.DataFrame(response.json()).drop(columns=["labelUrl"]).astype({"code": int, "label": str}).sort_values("code")
    df = pd.DataFrame(response.json()).astype({"code": int, "label": str, "labelUrl": str}).sort_values("code")
    df = df.rename(columns={"code": "DomainID"})
    df.to_csv("domains.csv", index=False)
    df.to_parquet("domains.parquet", index=False)

else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
