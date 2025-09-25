import logging
from scripts.extract import extract_movies
from scripts.transform import transform_movies, csv_to_json, json_to_csv
from scripts.validate import validate_movies
from scripts.load import load_tables, load_incremental
from scripts.monitor import log_event, send_alert
from scripts.logging_conf import configure_logging
from sqlalchemy.orm import sessionmaker
from database import engine

SessionLocal = sessionmaker(bind=engine)

logger = logging.getLogger(__name__)

def run_etl():
    session = SessionLocal()

    try:
        log_event("🚀 Starting ETL pipeline")

        # === Extract ===
        df = extract_movies("data/imdb_movies_final.csv")
        log_event(f"Extracted {len(df)} rows from CSV")

        # === Transform (CSV ↔ JSON, limpieza/enriquecimiento) ===
        csv_to_json("data/imdb_movies_final.csv", "data/raw/imdb_movies_final.json")
        tables = transform_movies(df)
        log_event("Transformation completed (CSV→JSON + cleaning)")
        json_to_csv("data/raw/imdb_movies_final.json", "data/raw/imdb_movies_back.csv")
        log_event("Back-conversion JSON→CSV done")

        # === Validate ===
        errors = validate_movies(tables["full"])
        if errors:
            log_event(f"Validation failed: {errors}", level="error")
            send_alert("ETL Validation Errors", "\n".join(errors), "admin@example.com")
            return  # aborta el ETL

        log_event("Validation passed ✅")

        # === Load (incremental) ===
        load_incremental(tables, session)
        session.commit()
        log_event("Incremental load completed")

        log_event("🎉 ETL pipeline finished successfully")

    except Exception as e:
        session.rollback()
        log_event(f"ETL failed: {str(e)}", level="error")
        send_alert("ETL Failure", str(e), "admin@example.com")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    configure_logging()
    run_etl()

