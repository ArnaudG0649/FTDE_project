import requests
import pandas as pd
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

test_mode = True
n=1000

df_department = pd.read_csv("../../departments/departments_france.csv")
df_jobs = pd.read_csv("../jobs/jobs.csv")
df_cross = df_jobs.merge(df_department, how='cross')[["romeCode", "departmentId"]]

#On selection n lignes aux hasard pour les tests
df_cross_test = df_cross.sample(n=n, random_state=42)


def fetch_job_department_attributes(rome_code: str, department_id: str, session: requests.Session) -> dict:
    """
    Récupère les attributs d'un métier à un département via l'API Labour Market de France Travail.
    """
    url_market = f"https://candidat.francetravail.fr/gw-metierscope/job/{rome_code}/labourMarket?territory={department_id}"

    row = {
        "romeCode": str(rome_code),
        "departmentId": str(department_id),
        "jobSeekers": None,
        "jobOffers": None,
        "jobPeriod": None,
        "salaryq10": None,
        "salaryq90": None,
        "recruitementDifficultyScore": None,
        "recruitementDifficultyScoreYear": None,
    }

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
            print(f"[{rome_code}] Erreur labourMarket: status code {r_market.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception lors de l'appel labourMarket: {e}")

    return row


def main():
    base_dir = Path(__file__).parent if "__file__" in globals() else Path(".")
    csv_path = base_dir / "jobs_departments.csv"
    parquet_path = base_dir / "jobs_departments.parquet"
    
    total = len(df_cross_test) if test_mode else len(df_cross)
    print(f"Nombre de couples métier-departement à traiter : {total}")

    results = {}
    headers = {"User-Agent": "Mozilla/5.0"}

    # Utilisation d'un pool de threads pour paralléliser les requêtes
    with requests.Session() as session:
        session.headers.update(headers)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_department_attributes, code, department_id, session): code
                for code, department_id in (df_cross_test.itertuples(index=False) if test_mode else df_cross.itertuples(index=False))
            }

            completed = 0
            for future in as_completed(future_to_code):
                data = future.result()
                # print(data)
                results[f"{data['romeCode']}_{data['departmentId']}"] = data
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"Progression : {completed}/{total} ; {completed/total:.2%}")

    df_final = pd.DataFrame(results.values())

    # Fusion avec le DataFrame initial
    # Si les colonnes existent déjà dans df_jobs, on les met à jour
    # cols_to_drop = [c for c in df_attributes.columns if c in df_cross.columns and c != "romeCode" and c != "departmentId"]
    # if cols_to_drop:
    #     df_jobs = df_jobs.drop(columns=cols_to_drop)

    # df_final = df_cross.merge(df_attributes, on=["romeCode", "departmentId"], how="left")

    # Typage des colonnes
    df_final = df_final.astype({
        "romeCode": "string",
        "departmentId": "string",
        "jobSeekers": "Int64",
        "jobOffers": "Int64",
        "jobPeriod": "string",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64",
    }).sort_values(by=["romeCode", "departmentId"])

    # Sauvegarde des données enrichies
    df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Mise à jour terminée. Fichiers enregistrés dans {csv_path} et {parquet_path}.")


if __name__ == "__main__":
    main()