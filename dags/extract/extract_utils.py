import requests
import pandas as pd
import re
from pathlib import Path
import os.path as osp
from concurrent.futures import ThreadPoolExecutor, as_completed



def csv_to_parquet(territories_dir,data_dir) -> None:
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



# def download_interests(url, data_dir, make_csv) -> None:
#     response = requests.get(url)

#     print(f"Downloading interests from {url}")
#     if response.status_code == 200:
#         df = pd.DataFrame(response.json()).astype(
#             {"code": int, "label": str, "labelUrl": str}
#         )
#         df.rename(
#             columns={
#                 "code": "interest_id",
#                 "label": "interestLabel",
#                 "labelUrl": "interestLabelUrl",
#             },
#             inplace=True,
#         )
#         if make_csv:
#             df.to_csv(osp.join(data_dir, "interests.csv"), index=False)
#         df.to_parquet(osp.join(data_dir, "interests.parquet"), index=False)

#     else:
#         print(f"Failed to retrieve the page. Status code: {response.status_code}")


def download_jobs_id_and_name(url, data_dir, make_csv) -> None:
    response = requests.get(url)

    print(f"Downloading jobs from {url}")
    if response.status_code == 200:
        Dict = response.json()
        df = pd.DataFrame(sum(Dict.values(), [])).astype(
            {"romeCode": str, "mainName": str}
        )
        df.rename(columns={"mainName": "job_name"}, inplace=True)
        if make_csv:
            df.to_csv(osp.join(data_dir, "jobs.csv"), index=False)
        df.to_parquet(osp.join(data_dir, "jobs.parquet"), index=False)
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


def download_domains(url, data_dir, make_csv) -> None:

    print(f"Downloading domains from {url}")
    response = requests.get(url)

    if response.status_code == 200:
        # df = pd.DataFrame(response.json()).drop(columns=["labelUrl"]).astype({"code": int, "label": str}).sort_values("code")
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
    response = requests.get(f"{url_base}/{domain_id:03d}")

    if response.status_code == 200:
        # print(f"Successfully retrieved the page for code {domain_id:03d}")
        Dict = response.json()["jobs"]
        df = pd.DataFrame(Dict)
        df["domain_id"] = domain_id
        try:
            df["subDomain"] = df["subDomain"].map(lambda x: x["label"])
        except:
            df["subDomain"] = label
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

        
def download_jobs_subdomains_extended(url, data_dir, make_csv) -> None:
    print(f"Downloading jobs subdomains relations extended data from {url}/*")
    
    df_domains = pd.read_parquet(osp.join(data_dir, "domains.parquet"))
    n = len(df_domains)

    with ThreadPoolExecutor(max_workers=10) as executor:
        list_df=list(executor.map(fetch_jobs_subdomains, df_domains.domain_id, df_domains.DomainName, [url for _ in range(n)]))
    
    df_extended = pd.concat(list_df, ignore_index=True)
    if make_csv:
        df_extended.to_csv(osp.join(data_dir, "jobs_subdomains_extended.csv"), index=False)
    df_extended.to_parquet(osp.join(data_dir, "jobs_subdomains_extended.parquet"), index=False)
    
def collect_subdomains(data_dir, make_csv):
    
    df_extended_path=osp.join(data_dir, "jobs_subdomains_extended.parquet")
    df = pd.read_parquet(df_extended_path)[["subDomain", "domain_id"]]
    
    print(f"Collecting subdomains from {df_extended_path}")

    my_unique_value = lambda x:list(x)[0]
    df_subdomains = df.groupby("subDomain").agg(my_unique_value).reset_index().sort_values("domain_id")
    df_subdomains["subdomain_id"] = range(1, len(df_subdomains) + 1)
    df_subdomains = df_subdomains[["subdomain_id", "subDomain", "domain_id"]].astype({"subdomain_id": "int", "subDomain": "string", "domain_id": "int"})

    if make_csv:
        df_subdomains.to_csv(osp.join(data_dir, "subdomains.csv"),index=False)
    df_subdomains.to_parquet(osp.join(data_dir, "subdomains.parquet"),index=False)


def collect_jobs_subdomains(data_dir, make_csv):
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
    Récupère les attributs d'un métier à partir des deux API de France Travail (Métierscope) :
    1. {url_base}/job/{romeCode} pour les indicateurs de transitions et statuts d'emploi
    2. {url_base}/job/{romeCode}/labourMarket?territory=FR pour les salaires et scores de difficulté de recrutement
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
        "recruitementDifficultyScore": None,
        "recruitementDifficultyScoreYear": None,
    }

    # 1. API Métier général
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

    # 2. API Marché du travail (Labour Market)
    try:
        r_market = session.get(url_market, timeout=15)
        if r_market.status_code == 200:
            market_data = r_market.json()

            # Salaires
            salary_data = market_data.get("salary")
            if isinstance(salary_data, dict):
                row["salaryq10"] = salary_data.get("minSalary")
                row["salaryq90"] = salary_data.get("maxSalary")

            # Difficulté de recrutement
            diff_data = market_data.get("recruitmentDifficultyScore")
            if isinstance(diff_data, dict):
                row["recruitementDifficultyScore"] = diff_data.get("nombreIndicateur")
                libelle_periode = diff_data.get("libellePeriode")
                if libelle_periode:
                    match = re.search(r"\b(\d{4})\b", str(libelle_periode))
                    if match:
                        row["recruitementDifficultyScoreYear"] = int(match.group(1))
        else:
            print(f"[{rome_code}] Erreur labourMarket: status code {r_market.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception during labourMarket call: {e}")

    return row


def download_jobs_attributes(data_dir, url_base, make_csv, test_mode, n):
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
    headers = {"User-Agent": "Mozilla/5.0"}

    # Utilisation d'un pool de threads pour paralléliser les requêtes
    with requests.Session() as session:
        session.headers.update(headers)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_attributes, code, session, url_base): code
                for code in rome_codes
            }

            completed = 0
            for future in as_completed(future_to_code):
                data = future.result()
                results[data["romeCode"]] = data
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"Progression : {completed}/{total} ; {completed/total:.2%}")

    df_attributes = pd.DataFrame([results[code] for code in rome_codes])

    # Fusion avec le DataFrame initial
    # Si les colonnes existent déjà dans df_jobs, on les met à jour
    cols_to_drop = [c for c in df_attributes.columns if c in df_jobs.columns and c != "romeCode"]
    if cols_to_drop:
        df_jobs = df_jobs.drop(columns=cols_to_drop)

    df_final = df_jobs.merge(df_attributes, on="romeCode", how="left")

    # Typage des colonnes
    df_final = df_final.astype({
        "transitionNumerique": "boolean",
        "transitionDemographique": "boolean",
        "transitionEcologique": "boolean",
        "emploiCadre": "boolean",
        "emploiReglemente": "boolean",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64"
    })

    # Sauvegarde des données enrichies
    if make_csv:
        df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Update completed. Files saved in {csv_path} and {parquet_path}.")
    
    
def fetch_job_department_attributes(rome_code: str, department_id: str, session: requests.Session, url_base) -> dict:
    """
    Retrieves the attributes of a job in a department via the France Travail Labour Market API.
    """

    url_market = f"{url_base}{rome_code}/labourMarket?territory={department_id}"

    row = {
        "romeCode": str(rome_code),
        "department_id": str(department_id),
        "jobSeekers": None,
        "jobOffers": None,
        "jobPeriod": None,
        "salaryq10": None,
        "salaryq90": None,
        "recruitementDifficultyScore": None,
        "recruitementDifficultyScoreYear": None,
    }

    # API Marché du travail (Labour Market)
    try:
        r_market = session.get(url_market, timeout=15)
        if r_market.status_code == 200:
            market_data = r_market.json()

            # Salaires
            salary_data = market_data.get("salary")
            if isinstance(salary_data, dict):
                row["salaryq10"] = salary_data.get("minSalary")
                row["salaryq90"] = salary_data.get("maxSalary")

            # Difficulté de recrutement
            diff_data = market_data.get("recruitmentDifficultyScore")
            if isinstance(diff_data, dict):
                row["recruitementDifficultyScore"] = diff_data.get("nombreIndicateur")
                libelle_periode = diff_data.get("libellePeriode")
                if libelle_periode:
                    match = re.search(r"\b(\d{4})\b", str(libelle_periode))
                    if match:
                        row["recruitementDifficultyScoreYear"] = int(match.group(1))
                     
            # Demande d'emploi
            job_seekers_data = market_data.get("jobSeekers")
            if isinstance(job_seekers_data, dict):
                row["jobSeekers"] = job_seekers_data.get("nombreIndicateur")
        
            # Offre d'emploi
            job_offers_data = market_data.get("jobOffers")
            if isinstance(job_offers_data, dict):
                row["jobOffers"] = job_offers_data.get("nombreIndicateur")
                row["jobPeriod"] = job_offers_data.get("libellePeriode")

                
        else:
            print(f"[{rome_code}] Error labourMarket: status code {r_market.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception during labourMarket call: {e}")

    return row


def download_jobs_departments(data_dir, url_base, make_csv, test_mode, n):

    df_department = pd.read_parquet(osp.join(data_dir, "departments.parquet"))
    df_jobs = pd.read_parquet(osp.join(data_dir, "jobs.parquet"))
    df_cross = df_jobs.merge(df_department, how='cross')[["romeCode", "department_id"]]

    print(f"Retrieving job department attributes from {url_base}/*/labourMarket?territory=*")
    if test_mode:
        #On selectionne n lignes aux hasard pour les tests
        df_cross_test = df_cross.sample(n=n, random_state=42)
    
    if make_csv:
        csv_path = osp.join(data_dir, "jobs_departments.csv")
    parquet_path = osp.join(data_dir, "jobs_departments.parquet")
    
    total = len(df_cross_test) if test_mode else len(df_cross)
    print(f"Number of job-department pairs to process: {total}")

    results = {}
    headers = {"User-Agent": "Mozilla/5.0"}

    # Utilisation d'un pool de threads pour paralléliser les requêtes
    with requests.Session() as session:
        session.headers.update(headers)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_department_attributes, code, department_id, session, url_base): code
                for code, department_id in (df_cross_test.itertuples(index=False) if test_mode else df_cross.itertuples(index=False))
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

    # Typage des colonnes
    df_final = df_final.astype({
        "romeCode": "string",
        "department_id": "string",
        "jobSeekers": "Int64",
        "jobOffers": "Int64",
        "jobPeriod": "string",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64",
    }).sort_values(by=["romeCode", "department_id"])

    if make_csv:
        df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Download finished. File saved in {parquet_path}.")
