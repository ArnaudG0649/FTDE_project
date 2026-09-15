def callable_create_and_save_dataframe():
    from os.path import join
    import pandas as pd
    
    datadict = {"id": range(4), "name": ["Alice", "Bob", "Charlie", "David"]}
    df = pd.DataFrame(datadict)
    df.to_csv(join("dags", "extract", "output.csv"), index=False)
    df.to_parquet(join("dags", "extract", "output.parquet"), index=False)
    
# print_current_datetime()