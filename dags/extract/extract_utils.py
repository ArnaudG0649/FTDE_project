import requests
import pandas as pd
import re
from pathlib import Path
import os.path as osp
from concurrent.futures import ThreadPoolExecutor, as_completed


def csv_to_parquet(territories_dir,data_dir) -> None:
    """Convert CSV files of territories (departments and regions) to Parquet format."""
    
    departments_file_csv = osp.join(territories_dir,"departments.csv")
    regions_file_csv = osp.join(territories_dir,"regions.csv")
    departments_file_parquet = osp.join(data_dir,"departments.parquet")
    regions_file_parquet= osp.join(data_dir,"regions.parquet")
    pd.read_csv(departments_file_csv) \
        .astype({"department_id":"string","department_name":"string","region_id":"string"}) \
        .to_parquet(departments_file_parquet)
    pd.read_csv(regions_file_csv) \
        .astype({"region_id":"Int64","region_name":"string"}) \
        .to_parquet(regions_file_parquet)


def download_jobs_id_and_name(url, data_dir, make_csv) -> None:
    """Download jobs ID and name from the given URL (usually jobs_alphabetical_url) and save them as CSV and Parquet files."""
    response = requests.get(url)

    print(f"Downloading jobs from {url}")
    if response.status_code == 200:
        Dict = response.json()
        df = pd.DataFrame(sum(Dict.values(), [])).astype(
            {"romeCode": str, "mainName": str}
        ) #sum because the JSON response is a dictionary of lists, and we want to combine all lists into a single list for the DataFrame.
        df.rename(columns={"mainName": "job_name"}, inplace=True)
        if make_csv:
            df.to_csv(osp.join(data_dir, "jobs.csv"), index=False)
        df.to_parquet(osp.join(data_dir, "jobs.parquet"), index=False)
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def download_domains(url, data_dir, make_csv) -> None:
    """Download domains from the given URL (usually domains_url link in params.yaml) and save them as CSV and Parquet files."""

    print(f"Downloading domains from {url}")
    response = requests.get(url)

    if response.status_code == 200:
        df = (
            pd.DataFrame(response.json())
            .astype({"code": int, "label": str, "labelUrl": str})
            .sort_values("code")
        )
        df = df.rename(
            columns={
                "code": "domain_id",
                "label": "DomainName",
                "labelUrl": "DomainNameUrl",
            }
        )
        if make_csv:
            df.to_csv(osp.join(data_dir, "domains.csv"), index=False)
        df.to_parquet(osp.join(data_dir, "domains.parquet"), index=False)

    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        
        
        
def fetch_jobs_subdomains(
    domain_id, label, url_base
):
    """For the given domain ID and label, fetch the associated jobs and subdomains from the specified URL base (usually domain_url in params.yaml)."""
    response = requests.get(f"{url_base}/{domain_id:03d}")

    if response.status_code == 200:
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["domain_id"] = domain_id # add the domain_id to each row in the DataFrame
        try:
            df["subDomain"] = df["subDomain"].map(lambda x: x["label"]) #The subDomain column dtype is dictionnary, so we apply the collection of the label in the whole column.
        except:
            df["subDomain"] = label #With no subdomain, we use the domain label as the subdomain.
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

        
def download_jobs_subdomains_extended(url, data_dir, make_csv) -> None:
    """Download extended jobs subdomains relations data from the given URL (usually {domain_url}/{domain_id} links type with domain_url in params.yaml) and save them as CSV and Parquet files.
    Extended means that it includes additional information beyond the basic jobs and subdomains relations.
    Though the table made is not normalized and won't be uploaded to the data warehouse,
    it is needed for collecting the subdomains and their relations. 
    """
    print(f"Downloading jobs subdomains relations extended data from {url}/*")
    
    df_domains = pd.read_parquet(osp.join(data_dir, "domains.parquet"))
    n = len(df_domains)

    with ThreadPoolExecutor(max_workers=10) as executor: #fetch jobs subdomains couples for all domainsId
        list_df=list(executor.map(fetch_jobs_subdomains, df_domains.domain_id, df_domains.DomainName, [url for _ in range(n)]))
    
    df_extended = pd.concat(list_df, ignore_index=True)
    if make_csv:
        df_extended.to_csv(osp.join(data_dir, "jobs_subdomains_extended.csv"), index=False)
    df_extended.to_parquet(osp.join(data_dir, "jobs_subdomains_extended.parquet"), index=False)
    
def collect_subdomains(data_dir, make_csv):
    """Collect subdomains from the extended jobs subdomains relations data and save them as CSV and Parquet files."""
    df_extended_path=osp.join(data_dir, "jobs_subdomains_extended.parquet")
    df = pd.read_parquet(df_extended_path)[["subDomain", "domain_id"]]
    
    print(f"Collecting subdomains from {df_extended_path}")

    # Make a dataframe of unique subdomains with their corresponding domain_id
    my_unique_value = lambda x:list(x)[0]
    df_subdomains = df.groupby("subDomain").agg(my_unique_value).reset_index().sort_values("domain_id")
    
    df_subdomains["subdomain_id"] = range(1, len(df_subdomains) + 1) # Assign a unique ID to each subdomain
    df_subdomains = df_subdomains[["subdomain_id", "subDomain", "domain_id"]].astype({"subdomain_id": "int", "subDomain": "string", "domain_id": "int"})

    if make_csv:
        df_subdomains.to_csv(osp.join(data_dir, "subdomains.csv"),index=False)
    df_subdomains.to_parquet(osp.join(data_dir, "subdomains.parquet"),index=False)


def collect_jobs_subdomains(data_dir, make_csv):
    """Collect jobs subdomains relations from the extended jobs subdomains relations data and the subdomains data, and save them as CSV and Parquet files."""
    df_extended_path=osp.join(data_dir, "jobs_subdomains_extended.parquet")
    df_subdomains_path=osp.join(data_dir, "subdomains.parquet")
    
    print(f"Collecting jobs subdomains from {df_extended_path} and {df_subdomains_path}")
    
    df_extended = pd.read_parquet(df_extended_path)
    df_subdomains = pd.read_parquet(df_subdomains_path)

    df_jobs_subdomains = df_extended.merge(df_subdomains, on="subDomain", how="left")[["romeCode", "subdomain_id"]].astype({"romeCode": "string", "subdomain_id": "int"})
    if make_csv:
        df_jobs_subdomains.to_csv(osp.join(data_dir,"jobs_subdomains.csv"), index=False)
    df_jobs_subdomains.to_parquet(osp.join(data_dir,"jobs_subdomains.parquet"), index=False)
    
    
def fetch_job_attributes(rome_code: str, session: requests.Session, url_base) -> dict:
    """
    For one job retrieves job attributes from the two France Travail (Metierscope) APIs:
    1. {url_base}/{romeCode} for transition indicators and employment statuses
    2. {url_base}/{romeCode}/labourMarket?territory=FR for salaries and recruitment difficulty scores
    """
    url_job = f"{url_base}/{rome_code}"
    url_market = f"{url_base}/{rome_code}/labourMarket?territory=FR"

    row = {
        "romeCode": str(rome_code),
        "transitionNumerique": None,
        "transitionDemographique": None,
        "transitionEcologique": None,
        "emploiCadre": None,
        "emploiReglemente": None,
        "salaryq10": None,
        "salaryq90": None,
        "jobSeekersNational": None,
        "jobOffersNational": None,
        "recruitementDifficultyScore": None,
        "recruitementDifficultyScoreYear": None,
        "sourcePeriod": None,
    }

    # 1. General job API
    try:
        r_job = session.get(url_job, timeout=15)
        if r_job.status_code == 200:
            job_data = r_job.json()
            row["transitionNumerique"] = job_data.get("transitionNumerique")
            row["transitionDemographique"] = job_data.get("transitionDemographique")
            row["transitionEcologique"] = job_data.get("transitionEcologique")
            row["emploiCadre"] = job_data.get("emploiCadre")
            row["emploiReglemente"] = job_data.get("emploiReglemente")
        else:
            print(f"[{rome_code}] Erreur job: status code {r_job.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception during job call: {e}")

    # 2. Labour market API
    try:
        r_market = session.get(url_market, timeout=15)
        if r_market.status_code == 200:
            market_data = r_market.json()

            # Salaries
            salary_data = market_data.get("salary")
            if isinstance(salary_data, dict):
                row["salaryq10"] = salary_data.get("minSalary")
                row["salaryq90"] = salary_data.get("maxSalary")
                row["sourcePeriod"] = salary_data.get("libellePeriode")

            job_seekers_data = market_data.get("jobSeekers")
            if isinstance(job_seekers_data, dict):
                row["jobSeekersNational"] = job_seekers_data.get("nombreIndicateur")

            job_offers_data = market_data.get("jobOffers")
            if isinstance(job_offers_data, dict):
                row["jobOffersNational"] = job_offers_data.get("nombreIndicateur")

            # Recruitment difficulty
            diff_data = market_data.get("recruitmentDifficultyScore")
            if isinstance(diff_data, dict):
                row["recruitementDifficultyScore"] = diff_data.get("nombreIndicateur")
                
                # Extract the year from the recruitment difficulty score period label
                libelle_periode = diff_data.get("libellePeriode")
                if libelle_periode:
                    match = re.search(r"\b(\d{4})\b", str(libelle_periode)) # Look for a 4-digit year in the period label
                    if match:
                        row["recruitementDifficultyScoreYear"] = int(match.group(1))
        else:
            print(f"[{rome_code}] Erreur labourMarket: status code {r_market.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception during labourMarket call: {e}")

    return row


def download_jobs_attributes(data_dir, url_base, make_csv, test_mode, n):
    """Download job attributes from the France Travail (Metierscope) APIs for all jobs listed in the local jobs parquet file, and insert them into it.

    Args:
        data_dir (str): Directory where the jobs CSV and Parquet files are stored.
        url_base (str): Base URL for the France Travail APIs. Often corresponds to url_job in params.yaml.
        make_csv (bool): Whether to create a CSV file for the downloaded attributes.
        test_mode (bool): If True, only a sample of n jobs will be processed.
        n (int): Number of jobs to sample if test_mode is True.
    """
    csv_path = osp.join(data_dir, "jobs.csv")
    parquet_path = osp.join(data_dir, "jobs.parquet")

    print(f"Downloading job attributes from {url_base}/*/labourMarket?territory=FR and {url_base}/*")
    print(f"Reading : {parquet_path}")
    
    df_jobs = pd.read_parquet(parquet_path)
    if test_mode:
        df_jobs = df_jobs.sample(n=n, random_state=42)

    rome_codes = df_jobs["romeCode"].tolist()
    total = len(rome_codes)
    print(f"Number of jobs to process: {total}")

    results = {}

    # Use a thread pool to parallelize requests
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_attributes, code, session, url_base): code
                for code in rome_codes
            }

            completed = 0
            for future in as_completed(future_to_code): # Iterate over completed futures as they finish
                data = future.result() # = The row precessed by fetch_job_attributes
                results[data["romeCode"]] = data
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"Progression : {completed}/{total} ; {completed/total:.2%}")

    df_attributes = pd.DataFrame([results[code] for code in rome_codes])

    # Merge with the initial DataFrame.
    # Drop existing columns so they are updated with the retrieved values.
    cols_to_drop = [c for c in df_attributes.columns if c in df_jobs.columns and c != "romeCode"]
    if cols_to_drop:
        df_jobs = df_jobs.drop(columns=cols_to_drop)

    df_final = df_jobs.merge(df_attributes, on="romeCode", how="left") # Join the initial job DataFrame with the retrieved attributes

    # Column type conversions
    df_final = df_final.astype({
        "transitionNumerique": "boolean",
        "transitionDemographique": "boolean",
        "transitionEcologique": "boolean",
        "emploiCadre": "boolean",
        "emploiReglemente": "boolean",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "jobSeekersNational": "Int64",
        "jobOffersNational": "Int64",
        "sourcePeriod": "string",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64"
    })

    # Save the enriched data
    if make_csv:
        df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Update completed. Files saved in {csv_path} and {parquet_path}.")
    
    
def fetch_job_department_attributes(rome_code: str, department_id: str, session: requests.Session, url_base) -> dict:
    """
    Retrieves the attributes of a job in a department via the France Travail Labour Market API. url_base is often url_job in params.yaml.
    """

    url_market = f"{url_base}{rome_code}/labourMarket?territory={department_id}"

    row = {
        "romeCode": str(rome_code),
        "department_id": str(department_id),
        "jobSeekers": None,
        "jobOffers": None,
        "sourcePeriod": None,
        "salaryq10": None,
        "salaryq90": None,
        "recruitementDifficultyScore": None,
        "recruitementDifficultyScoreYear": None,
    }

    # Labour market API
    try:
        r_market = session.get(url_market, timeout=15)
        if r_market.status_code == 200:
            market_data = r_market.json()

            # Salaries
            salary_data = market_data.get("salary")
            if isinstance(salary_data, dict):
                row["salaryq10"] = salary_data.get("minSalary")
                row["salaryq90"] = salary_data.get("maxSalary")

            # Recruitment difficulty
            diff_data = market_data.get("recruitmentDifficultyScore")
            if isinstance(diff_data, dict):
                row["recruitementDifficultyScore"] = diff_data.get("nombreIndicateur")
                libelle_periode = diff_data.get("libellePeriode")
                if libelle_periode:
                    match = re.search(r"\b(\d{4})\b", str(libelle_periode)) # Look for a 4-digit year in the period label
                    if match:
                        row["recruitementDifficultyScoreYear"] = int(match.group(1))
                     
            # Job seekers
            job_seekers_data = market_data.get("jobSeekers")
            if isinstance(job_seekers_data, dict):
                row["jobSeekers"] = job_seekers_data.get("nombreIndicateur")
        
            # Job offers
            job_offers_data = market_data.get("jobOffers")
            if isinstance(job_offers_data, dict):
                row["jobOffers"] = job_offers_data.get("nombreIndicateur")
                row["sourcePeriod"] = job_offers_data.get("libellePeriode")

                
        else:
            print(f"[{rome_code}] Error labourMarket: status code {r_market.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception during labourMarket call: {e}")

    return row


def download_jobs_departments(data_dir, url_base, make_csv):
    """
    Downloads job department pair attributes from the France Travail Labour Market API.

    Args:
        data_dir (str): Directory where the data files are stored.
        url_base (str): Base URL for the France Travail APIs. Often corresponds to url_job in params.yaml.
        make_csv (bool): Whether to create a CSV file for the results.
    """

    df_department = pd.read_parquet(osp.join(data_dir, "departments.parquet"))
    df_jobs = pd.read_parquet(osp.join(data_dir, "jobs.parquet"))
    df_cross = df_jobs.merge(df_department, how='cross')[["romeCode", "department_id"]] # Create all possible job-department pairs (cartesian product)

    print(f"Retrieving job department attributes from {url_base}/*/labourMarket?territory=*")
    
    if make_csv:
        csv_path = osp.join(data_dir, "jobs_departments.csv")
    parquet_path = osp.join(data_dir, "jobs_departments.parquet")
    
    total = len(df_cross)
    print(f"Number of job-department pairs to process: {total}")

    results = {}

    # Use a thread pool to parallelize requests
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_department_attributes, code, department_id, session, url_base): code
                for code, department_id in df_cross.itertuples(index=False)
            }

            completed = 0
            for future in as_completed(future_to_code):
                data = future.result()
                # print(data)
                results[f"{data['romeCode']}_{data['department_id']}"] = data
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"Progression : {completed}/{total} ; {completed/total:.2%}")

    df_final = pd.DataFrame(results.values())

    # Column type conversions
    df_final = df_final.astype({
        "romeCode": "string",
        "department_id": "string",
        "jobSeekers": "Int64",
        "jobOffers": "Int64",
        "sourcePeriod": "string",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64",
    }).sort_values(by=["romeCode", "department_id"])

    if make_csv:
        df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Download finished. File saved in {parquet_path}.")
