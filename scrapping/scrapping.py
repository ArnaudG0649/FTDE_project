from scrapping_utils import (
    download_interests,
    download_jobs_id_and_name,
    download_domains,
    download_jobs_subdomains_extended,
    collect_subdomains,
    collect_jobs_subdomains,
    download_jobs_attributes,
    download_jobs_departments,
    csv_to_parquet,
)

if __name__ == "__main__":
    csv_to_parquet()
    download_interests()
    download_jobs_id_and_name()
    download_domains()
    download_jobs_subdomains_extended()
    collect_subdomains()
    collect_jobs_subdomains()
    download_jobs_attributes()
    download_jobs_departments()