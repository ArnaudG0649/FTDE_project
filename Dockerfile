FROM apache/airflow:3.3.1

RUN pip install --no-cache-dir "apache-airflow-providers-google>=10.0.0"
RUN python -m pip install --force-reinstall -v "dbt==2.0.5"
