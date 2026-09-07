import requests
import pandas as pd

response = requests.get("https://candidat.francetravail.fr/gw-metierscope/interests")

if response.status_code == 200:
    df = (
        pd.DataFrame(response.json())
        .astype({"code": int, "label": str, "labelUrl": str})
    )
    df.rename(columns={"code": "interestID", "label": "interestLabel", "labelUrl": "interestLabelUrl"}, inplace=True)
    df.to_csv("interests.csv", index=False)
    df.to_parquet("interests.parquet", index=False)

else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
