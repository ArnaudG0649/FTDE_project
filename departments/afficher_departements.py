import pandas as pd

df = pd.read_csv("departements_regions_france.csv", header=0, dtype=str)

# print(df)
df.drop(columns=["region"], inplace=True)
df.to_csv("departements_france.csv", index=False)
df.to_parquet("departements_france.parquet", index=False)

# df2 = df.head(3)

# df3 = df[df["code_departement"].isin(["04", "05", "06", "13", "83", "84"])]
# print(df3)

# print(all(df3 == df[df["region"] == "Provence-Alpes-Côte d'Azur"]))