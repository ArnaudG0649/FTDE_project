import os
import re
import requests
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_job_attributes(rome_code: str, session: requests.Session) -> dict:
    """
    Récupère les attributs d'un métier à partir des deux API de France Travail (Métierscope) :
    1. /job/{romeCode} pour les indicateurs de transitions et statuts d'emploi
    2. /job/{romeCode}/labourMarket?territory=FR pour les salaires et scores de difficulté de recrutement
    """
    url_job = f"https://candidat.francetravail.fr/gw-metierscope/job/{rome_code}"
    url_market = f"https://candidat.francetravail.fr/gw-metierscope/job/{rome_code}/labourMarket?territory=FR"

    row = {
        "romeCode": str(rome_code),
        "transitionNumérique": None,
        "transitionDémographique": None,
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
            row["transitionNumérique"] = job_data.get("transitionNumerique")
            row["transitionDémographique"] = job_data.get("transitionDemographique")
            row["transitionEcologique"] = job_data.get("transitionEcologique")
            row["emploiCadre"] = job_data.get("emploiCadre")
            row["emploiReglemente"] = job_data.get("emploiReglemente")
        else:
            print(f"[{rome_code}] Erreur job: status code {r_job.status_code}")
    except Exception as e:
        print(f"[{rome_code}] Exception lors de l'appel job: {e}")

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
        print(f"[{rome_code}] Exception lors de l'appel labourMarket: {e}")

    return row


def main():
    base_dir = Path(__file__).parent if "__file__" in globals() else Path(".")
    csv_path = base_dir / "jobs.csv"
    parquet_path = base_dir / "jobs.parquet"

    print(f"Lecture du fichier : {csv_path}")
    df_jobs = pd.read_csv(csv_path)

    rome_codes = df_jobs["romeCode"].tolist()
    total = len(rome_codes)
    print(f"Nombre de métiers à traiter : {total}")

    results = {}
    headers = {"User-Agent": "Mozilla/5.0"}

    # Utilisation d'un pool de threads pour paralléliser les requêtes
    with requests.Session() as session:
        session.headers.update(headers)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(fetch_job_attributes, code, session): code
                for code in rome_codes
            }

            completed = 0
            for future in as_completed(future_to_code):
                data = future.result()
                results[data["romeCode"]] = data
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"Progression : {completed}/{total}")

    df_attributes = pd.DataFrame([results[code] for code in rome_codes])

    # Fusion avec le DataFrame initial
    # Si les colonnes existent déjà dans df_jobs, on les met à jour
    cols_to_drop = [c for c in df_attributes.columns if c in df_jobs.columns and c != "romeCode"]
    if cols_to_drop:
        df_jobs = df_jobs.drop(columns=cols_to_drop)

    df_final = df_jobs.merge(df_attributes, on="romeCode", how="left")

    # Typage des colonnes
    df_final = df_final.astype({
        "transitionNumérique": "boolean",
        "transitionDémographique": "boolean",
        "transitionEcologique": "boolean",
        "emploiCadre": "boolean",
        "emploiReglemente": "boolean",
        "salaryq10": "Int64",
        "salaryq90": "Int64",
        "recruitementDifficultyScore": "Int64",
        "recruitementDifficultyScoreYear": "Int64"
    })

    # Sauvegarde des données enrichies
    df_final.to_csv(csv_path, index=False)
    df_final.to_parquet(parquet_path, index=False)
    print(f"Mise à jour terminée. Fichiers enregistrés dans {csv_path} et {parquet_path}.")


if __name__ == "__main__":
    main()
