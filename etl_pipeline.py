"""
=============================================================
 HealthAI Coach — Pipeline ETL v5.0
=============================================================
 Sources :
   - nutrition_cleaned.csv  -> table aliment
   - gym_cleaned.csv        -> tables utilisateur + metrique_quotidienne
   - fitness_cleaned.csv    -> table metrique_quotidienne
   - RapidAPI ExerciseDB    -> tables exercice + exercice_muscle
=============================================================
 Lancement :
   pip install pandas sqlalchemy psycopg2-binary openpyxl requests
   
   Windows CMD :
   set DB_HOST=127.0.0.1
   set DB_NAME=healthai
   set DB_USER=postgres
   set DB_PASSWORD=postgres
   set DATA_DIR=./data
   set RAPIDAPI_KEY=e45408853cmshf699b5c6ab9114ap1b3194jsn8ed911d40811
   python etl_pipeline.py
=============================================================
"""

import os, sys, logging, hashlib, json, requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "127.0.0.1"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "healthai"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "e45408853cmshf699b5c6ab9114ap1b3194jsn8ed911d40811")
RAPIDAPI_HOST = "exercisedb.p.rapidapi.com"
RAPIDAPI_URL  = "https://exercisedb.p.rapidapi.com/exercises"

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
LOG_DIR  = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)

# =============================================================
# LOGGER
# =============================================================
def setup_logger():
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger("healthai_etl")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logger()

# =============================================================
# CONNEXION BASE DE DONNÉES
# =============================================================
def get_engine():
    url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    try:
        engine = create_engine(url, echo=False, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connexion PostgreSQL OK.")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Connexion impossible : {e}")
        sys.exit(1)

# =============================================================
# UTILITAIRES
# =============================================================
def lire_fichier(nom):
    chemin = DATA_DIR / nom
    if not chemin.exists():
        logger.error(f"Fichier introuvable : {chemin}")
        return None
    ext = chemin.suffix.lower()
    df = pd.read_csv(chemin) if ext == ".csv" else pd.read_json(chemin)
    logger.info(f"Lu : {nom} ({len(df)} lignes)")
    return df

def lire_sql(query, engine):
    """Lecture SQL compatible Python 3.14 + SQLAlchemy."""
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

def inserer(df, table, engine):
    if df is None or df.empty:
        logger.warning(f"Rien a inserer dans {table}.")
        return 0
    try:
        df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=500)
        logger.info(f"[{table}] {len(df)} lignes inserees.")
        return len(df)
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion {table} : {e}")
        return 0

def sauvegarder_rapport(rapports):
    path = LOG_DIR / f"rapport_qualite_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rapports, f, ensure_ascii=False, indent=2)
    logger.info(f"Rapport qualite : {path}")

# =============================================================
# ETL 1 — ALIMENTS (nutrition_cleaned.csv)
# =============================================================
def etl_aliments(engine):
    logger.info("=" * 50)
    logger.info("ETL 1 — Aliments (nutrition_cleaned.csv)")
    df = lire_fichier("nutrition_cleaned.csv")
    if df is None:
        return {"dataset": "aliments", "statut": "erreur"}
    df_insert = pd.DataFrame({
        "nom":            df["food_item"].str.strip().str.title(),
        "categorie":      df["category"].str.strip(),
        "calories_100g":  df["calories_kcal"],
        "proteines_g":    df["protein_g"],
        "glucides_g":     df["carbohydrates_g"],
        "lipides_g":      df["fat_g"],
        "fibres_g":       df["fiber_g"],
        "sucres_g":       df["sugars_g"],
        "sodium_mg":      df["sodium_mg"],
        "source_dataset": "kaggle_nutrition_cleaned",
    })
    df_insert = df_insert.drop_duplicates(subset=["nom"], keep="first")
    nb = inserer(df_insert, "aliment", engine)
    return {"dataset": "nutrition_cleaned.csv", "lignes_source": len(df), "lignes_inserees": nb, "statut": "succes"}

# =============================================================
# ETL 2 — UTILISATEURS + MÉTRIQUES (gym_cleaned.csv)
# =============================================================
def etl_utilisateurs_metriques(engine):
    logger.info("=" * 50)
    logger.info("ETL 2 — Utilisateurs + Metriques (gym_cleaned.csv)")
    df = lire_fichier("gym_cleaned.csv")
    if df is None:
        return {"dataset": "gym", "statut": "erreur"}

    genre_map = {"Male": "homme", "Female": "femme", "male": "homme", "female": "femme"}
    df["sexe"] = df["gender"].map(genre_map).fillna("non_renseigne")

    utilisateurs = []
    for i, row in df.iterrows():
        utilisateurs.append({
            "nom":              f"User{i+1:04d}",
            "prenom":           "HealthAI",
            "email":            f"user_{i+1:04d}@healthai.demo",
            "mdp_hash":         hashlib.sha256(f"demo_{i}".encode()).hexdigest(),
            "sexe":             row["sexe"],
            "poids_initial_kg": float(row["weight_kg"]),
            "taille_cm":        round(float(row["height_m"]) * 100),
            "abonnement":       "freemium",
            "imc":              float(row["bmi"]),
        })

    df_users = pd.DataFrame(utilisateurs)

    try:
        emails = lire_sql("SELECT email FROM utilisateur", engine)
        df_users = df_users[~df_users["email"].isin(emails["email"])]
    except Exception:
        pass

    nb_users = inserer(df_users, "utilisateur", engine)

    df_ids = lire_sql(
        "SELECT id, email FROM utilisateur WHERE email LIKE 'user_%@healthai.demo' ORDER BY id",
        engine
    )

    if df_ids.empty:
        logger.warning("Aucun utilisateur trouve pour les metriques.")
        return {"dataset": "gym_cleaned.csv", "utilisateurs": nb_users, "metriques": 0, "statut": "succes"}

    metriques = []
    today = datetime.now().date()
    for _, row_id in df_ids.iterrows():
        idx = int(row_id["email"].split("_")[1].split("@")[0]) - 1
        if idx >= len(df):
            continue
        row = df.iloc[idx]
        poids_base = float(row["weight_kg"])
        for j in range(30):
            metriques.append({
                "utilisateur_id":  int(row_id["id"]),
                "date_mesure":     today - timedelta(days=29 - j),
                "poids_kg":        round(poids_base + (j - 15) * 0.04, 2),
                "bpm_repos":       int(row["resting_bpm"]),
                "bpm_max":         int(row["max_bpm"]),
                "calories_brulees":float(row["calories_burned"]),
                "body_fat_pct":    float(row["fat_percentage"]),
                "imc_calcule":     float(row["bmi"]),
                "source":          "kaggle_gym_cleaned",
            })

    nb_m = inserer(pd.DataFrame(metriques), "metrique_quotidienne", engine)
    return {"dataset": "gym_cleaned.csv", "utilisateurs": nb_users, "metriques": nb_m, "statut": "succes"}

# =============================================================
# ETL 3 — EXERCICES (RapidAPI ExerciseDB)
# https://rapidapi.com/justin-WFnsXH_t6/api/exercisedb
# =============================================================
def etl_exercices_api(engine):
    logger.info("=" * 50)
    logger.info("ETL 3 — Exercices (RapidAPI ExerciseDB)")

    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }

    # Recuperation des exercices (limit=100 pour rester dans le quota gratuit)
    logger.info(f"Appel RapidAPI : {RAPIDAPI_URL}?limit=100&offset=0")
    try:
        response = requests.get(
            RAPIDAPI_URL,
            headers=headers,
            params={"limit": "100", "offset": "0"},
            timeout=30
        )
        response.raise_for_status()
        exercises = response.json()

        if not isinstance(exercises, list):
            logger.error(f"Format inattendu : {type(exercises)}")
            return {"dataset": "RapidAPI ExerciseDB", "statut": "erreur", "message": "format inattendu"}

        logger.info(f"RapidAPI OK — {len(exercises)} exercices recus")

    except requests.exceptions.HTTPError as e:
        logger.error(f"Erreur HTTP RapidAPI : {e}")
        if response.status_code == 429:
            logger.error("Quota depasse (429) — attendez demain ou utilisez exercises.json")
        return {"dataset": "RapidAPI ExerciseDB", "statut": "erreur", "message": str(e)}
    except requests.exceptions.Timeout:
        logger.error("Timeout RapidAPI.")
        return {"dataset": "RapidAPI ExerciseDB", "statut": "erreur", "message": "timeout"}
    except Exception as e:
        logger.error(f"Erreur RapidAPI : {e}")
        return {"dataset": "RapidAPI ExerciseDB", "statut": "erreur", "message": str(e)}

    df = pd.DataFrame(exercises)
    logger.info(f"Colonnes recues de l'API : {list(df.columns)}")

    # Mapping colonnes RapidAPI → schema PostgreSQL
    # RapidAPI ExerciseDB retourne : id, name, bodyPart, equipment,
    # gifUrl, target, secondaryMuscles, instructions
    rename_map = {
        "name":         "nom",
        "bodyPart":     "type_raw",
        "equipment":    "equipement",
        "gifUrl":       "image_url",
        "instructions": "instructions_raw",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Niveau — pas dans l'API RapidAPI, on met debutant par defaut
    df["niveau"] = "debutant"

    # Type exercice
    type_map = {
        "cardio":      "cardio",
        "chest":       "musculation",
        "back":        "musculation",
        "shoulders":   "musculation",
        "upper arms":  "musculation",
        "lower arms":  "musculation",
        "upper legs":  "musculation",
        "lower legs":  "musculation",
        "waist":       "musculation",
        "neck":        "stretching",
    }
    if "type_raw" in df.columns:
        df["type"] = df["type_raw"].str.lower().map(type_map).fillna("musculation")
    else:
        df["type"] = "musculation"

    # Instructions : l'API retourne une liste → on joint en texte
    if "instructions_raw" in df.columns:
        df["instructions"] = df["instructions_raw"].apply(
            lambda x: " ".join(x) if isinstance(x, list) else str(x) if pd.notna(x) else ""
        )

    # Nettoyage
    df["nom"] = df["nom"].str.strip().str.title()
    df = df.drop_duplicates(subset=["nom"])
    df["source_dataset"] = "rapidapi_exercisedb"

    cols = ["nom", "type", "niveau", "equipement", "instructions", "image_url", "source_dataset"]
    df_insert = df[[c for c in cols if c in df.columns]]
    nb = inserer(df_insert, "exercice", engine)

    # Association exercice <-> groupe musculaire
    if "target" in df.columns:
        df_ex = lire_sql("SELECT id, nom FROM exercice", engine)
        df_mu = lire_sql("SELECT id, nom FROM groupe_musculaire", engine)
        assoc = []
        for _, row in df.iterrows():
            match_ex = df_ex[df_ex["nom"] == row["nom"]]
            if match_ex.empty:
                continue
            ex_id = int(match_ex.iloc[0]["id"])
            muscle = str(row.get("target", "")).lower().strip()
            match_mu = df_mu[df_mu["nom"].str.lower() == muscle]
            if not match_mu.empty:
                assoc.append({
                    "exercice_id": ex_id,
                    "muscle_id":   int(match_mu.iloc[0]["id"]),
                    "role":        "principal"
                })
        if assoc:
            nb_assoc = inserer(pd.DataFrame(assoc).drop_duplicates(), "exercice_muscle", engine)
            logger.info(f"[exercice_muscle] {nb_assoc} associations inserees.")

    return {
        "dataset":         "RapidAPI ExerciseDB",
        "url":             RAPIDAPI_URL,
        "lignes_recues":   len(exercises),
        "lignes_inserees": nb,
        "tables_cibles":   ["exercice", "exercice_muscle"],
        "statut":          "succes"
    }

# =============================================================
# ETL 4 — FITNESS TRACKER (fitness_cleaned.csv)
# =============================================================
def etl_fitness_tracker(engine):
    logger.info("=" * 50)
    logger.info("ETL 4 — Fitness Tracker (fitness_cleaned.csv)")
    df = lire_fichier("fitness_cleaned.csv")
    if df is None:
        return {"dataset": "fitness_cleaned", "statut": "erreur"}

    df_ids = lire_sql(
        "SELECT id FROM utilisateur WHERE email LIKE 'user_%@healthai.demo' ORDER BY id",
        engine
    )

    if df_ids.empty:
        logger.warning("Pas d'utilisateurs. Lancez d'abord ETL 2.")
        return {"dataset": "fitness_cleaned", "statut": "ignore"}

    metriques = []
    nb_users = len(df_ids)
    for i, row in df.iterrows():
        user_id = int(df_ids.iloc[i % nb_users]["id"])
        try:
            date_m = pd.to_datetime(row["date"]).date()
        except Exception:
            date_m = datetime.now().date()
        metriques.append({
            "utilisateur_id":  user_id,
            "date_mesure":     date_m,
            "bpm_repos":       int(row["heart_rate_avg"]) if pd.notna(row.get("heart_rate_avg")) else None,
            "calories_brulees":float(row["calories_burned"]) if pd.notna(row.get("calories_burned")) else None,
            "heures_sommeil":  float(row["sleep_hours"]) if pd.notna(row.get("sleep_hours")) else None,
            "steps":           int(row["steps"]) if pd.notna(row.get("steps")) else None,
            "source":          "kaggle_fitness_cleaned",
        })

    df_m = pd.DataFrame(metriques).drop_duplicates(
        subset=["utilisateur_id", "date_mesure"], keep="first"
    )
    nb = inserer(df_m, "metrique_quotidienne", engine)
    return {"dataset": "fitness_cleaned.csv", "lignes_inserees": nb, "statut": "succes"}

# =============================================================
# ORCHESTRATEUR
# =============================================================
def run_pipeline():
    start = datetime.now()
    logger.info("Pipeline ETL HealthAI Coach v5.0 — demarrage")
    logger.info(f"Source exercices : RapidAPI ExerciseDB ({RAPIDAPI_HOST})")
    engine = get_engine()
    rapports = []

    for nom, fn in [
        ("ETL 1 - Aliments",        etl_aliments),
        ("ETL 2 - Utilisateurs",    etl_utilisateurs_metriques),
        ("ETL 3 - Exercices API",   etl_exercices_api),
        ("ETL 4 - Fitness Tracker", etl_fitness_tracker),
    ]:
        try:
            r = fn(engine)
            rapports.append(r)
            logger.info(f"{nom} -> {r.get('statut', '?')}")
        except Exception as e:
            logger.error(f"ERREUR {nom} : {e}", exc_info=True)
            rapports.append({"dataset": nom, "statut": "erreur", "message": str(e)})

    duree = (datetime.now() - start).total_seconds()
    sauvegarder_rapport(rapports)
    nb_ok = sum(1 for r in rapports if r.get("statut") in ("succes", "ignore"))
    logger.info(f"Pipeline termine en {duree:.1f}s — {nb_ok}/{len(rapports)} ETL OK")

if __name__ == "__main__":
    run_pipeline()