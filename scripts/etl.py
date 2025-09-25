import logging
import os
import traceback
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

# Obtener la ruta base del directorio actual para manejar rutas de archivo de forma robusta
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_etl():
    session = SessionLocal()

    try:
        log_event("🚀 Starting ETL pipeline")

        # === Extract ===
        # Construir una ruta absoluta para el archivo CSV para evitar FileNotFoundError
        csv_path = os.path.join(BASE_DIR, '..', 'data', 'imdb_movies_final.csv')
        json_raw_path = os.path.join(BASE_DIR, '..', 'data', 'raw', 'imdb_movies_final.json')
        csv_back_path = os.path.join(BASE_DIR, '..', 'data', 'raw', 'imdb_movies_back.csv')

        logging.info(f"Looking for data file at: {csv_path}")
        
        # Manejar el error de archivo no encontrado de forma explícita
        try:
            df = extract_movies(csv_path)
        except FileNotFoundError:
            error_msg = f"ERROR: ETL failed because data file was not found at '{csv_path}'."
            log_event(error_msg, level="error")
            send_alert("ETL Failure: Data File Missing", error_msg, "admin@example.com")
            raise FileNotFoundError(error_msg)

        log_event(f"Extracted {len(df)} rows from CSV")

        # === Transform (CSV ↔ JSON, limpieza/enriquecimiento) ===
        try:
            csv_to_json(csv_path, json_raw_path)
            tables = transform_movies(df)
            log_event("Transformation completed (CSV→JSON + cleaning)")
            json_to_csv(json_raw_path, csv_back_path)
            log_event("Back-conversion JSON→CSV done")
        except Exception as e:
            error_msg = f"ERROR: Transformation failed. Details: {str(e)}"
            log_event(error_msg, level="error")
            raise RuntimeError(error_msg) from e

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
        # El 'raise' es importante para que el endpoint de FastAPI lo capture
        # y devuelva un 500 con el mensaje de error
        raise
    finally:
        session.close()

if __name__ == "_main_":
    configure_logging()
    run_etl()