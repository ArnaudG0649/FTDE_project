# France travail MetierScope data project

## Introduction

Did you ever wonder : 
- "What are the jobs with the most positions offers and the less applicants ?"
- "The opposite, to know which job to avoid ?"
- "Same questions but in some precise geographical zones ?"
- "Same questions again but about whole professional areas instead of precise jobs"
- "And most important : what are the most paid and underpaid jobs ? And where ?" 

For the french job market, with this data engineering project you can have answers based on the data displayed on the [MetierScope](https://candidat.francetravail.fr/metierscope/) site from France Travail. It consists of an ELT pipeline with 

## Setup

1. Go [here](iac/README.md) to know how to configure your gcp project, get the json key and set up some necessary ressources with terraform.
2. In `dags/extract/params.yams`, there is some important parameters. **You must insert your own gcp project id for the corresponding parameter (line 14)**. You can change some other parameters like `test_mode` that allow you to download the data only for `n` jobs, or `make_csv` that will put the data extracted in csv files in addition to the parquet files.
3. Execute
```shell
Docker compose build 
```
once and each time you want to open the airflow ui execute
```shell
Docker compose up -d
```
, open http://localhost:8080/, enter *airflow* as both username and password and you can choose between `extract_load_transform`, `load_transform` and `transform` dags in `dags` tab. `extract_load_transform` is scheduled every month from the 1st of October 2026. I strongly advice to use a VPN if you want to extract the France Travail data, because on this site if you try to download too much of their data they might stop to answer to your request to avoid abuses (but don't worry the data you want to download is public).