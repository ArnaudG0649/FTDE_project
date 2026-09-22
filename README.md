1. Go [here](iac/README.md) to configure your gcp project, get the json key and set up some necessary ressources with terraform.
2. In `dags/extract/params.yams`, there is some important parameters. **You must put your own gcp project id for the corresponding parameter (line 14)**. You can change some other parameters like `test_mode` that allow you to download only `n` random lines for jobs and jobs_departments tables, or `make_csv` that will put the data extracted in csv files in addition to the parquet files.
3. Execute
```shell
Docker compose build 
```
once and each time you want to open the airflow ui
```shell
Docker compose up -d
```
open http://localhost:8080/, enter airflow as both username and password and you can choose between `extract_load_transform`, `load_transform` and `transform` dags in `dags` tab. I strongly advice to use a VPN if you want to extract the France Travail data, because on this site if you try to download too much their data they might stop to answer to your request to avoid abuses (but don't worry the data you download are public).