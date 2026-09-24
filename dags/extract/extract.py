def extract(config_file="dags/extract/params.yaml") : 

    import yaml
    import os.path as osp
    from extract.extract_utils import (
        csv_to_parquet,
        download_jobs_id_and_name,
        download_domains,
        download_jobs_subdomains_extended,
        collect_subdomains,
        collect_jobs_subdomains,
        download_jobs_attributes,
        download_jobs_departments,
    )

    with open(config_file, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.FullLoader)

    # Extract configuration values from the YAML file
    make_csv = yaml_data["make_csv"]
    data_dir = yaml_data["data_dir"]
    territories_dir = yaml_data["territories_dir"]

    test_mode = yaml_data["test_mode"]
    n = yaml_data["n"]

    jobs_alphabetical_url = yaml_data["jobs_alphabetical_url"]
    domains_url = yaml_data["domains_url"]
    domain_url = yaml_data["domain_url"]
    url_job = yaml_data["url_job"]

    # Perform the data extraction steps
    csv_to_parquet(territories_dir, data_dir)
    download_jobs_id_and_name(jobs_alphabetical_url, data_dir, make_csv)
    download_domains(domains_url, data_dir, make_csv)
    download_jobs_subdomains_extended(domain_url, data_dir, make_csv)
    collect_subdomains(data_dir, make_csv)
    collect_jobs_subdomains(data_dir, make_csv)
    download_jobs_attributes(data_dir, url_job, make_csv, test_mode, n)
    download_jobs_departments(data_dir, url_job, make_csv)
