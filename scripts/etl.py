import logging
import time
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

def format_duration(seconds):
    """Format duration in seconds to human readable format"""
    if seconds < 1:
        return f"{seconds*1000:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"

def run_etl():
    """
    Execute ETL pipeline for PostgreSQL database with performance metrics
    """
    session = SessionLocal()
    total_start_time = time.time()
    
    try:
        log_event("Starting ETL pipeline - PostgreSQL")
        log_event(f"Process initiated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # === EXTRACT PHASE ===
        extract_start = time.time()
        log_event("Phase 1: Data Extraction")
        
        df = extract_movies("data/imdb_movies_final.csv")
        
        extract_duration = time.time() - extract_start
        log_event(f"Extraction completed: {len(df)} rows processed")
        log_event(f"Extraction time: {format_duration(extract_duration)}")
        log_event(f"Average extraction rate: {len(df)/extract_duration:.0f} rows/second")
        
        # === TRANSFORM PHASE ===
        transform_start = time.time()
        log_event("Phase 2: Data Transformation")
        
        # CSV to JSON conversion
        json_convert_start = time.time()
        csv_to_json("data/imdb_movies_final.csv", "raw/imdb_movies_final.json")
        json_convert_duration = time.time() - json_convert_start
        log_event(f"CSV to JSON conversion: {format_duration(json_convert_duration)}")
        
        # Main transformation
        main_transform_start = time.time()
        tables = transform_movies(df)
        main_transform_duration = time.time() - main_transform_start
        log_event(f"Data transformation and cleaning: {format_duration(main_transform_duration)}")
        
        # JSON to CSV back-conversion
        csv_convert_start = time.time()
        json_to_csv("raw/imdb_movies_final.json", "raw/imdb_movies_back.csv")
        csv_convert_duration = time.time() - csv_convert_start
        log_event(f"JSON to CSV back-conversion: {format_duration(csv_convert_duration)}")
        
        transform_duration = time.time() - transform_start
        log_event(f"Total transformation time: {format_duration(transform_duration)}")
        
        # === VALIDATE PHASE ===
        validate_start = time.time()
        log_event("Phase 3: Data Validation")
        
        errors = validate_movies(tables["full"])
        validate_duration = time.time() - validate_start
        
        if errors:
            log_event(f"Validation failed with {len(errors)} errors", level="error")
            log_event(f"Validation time: {format_duration(validate_duration)}")
            for idx, error in enumerate(errors[:10], 1):
                log_event(f"Error {idx}: {error}", level="error")
            send_alert("ETL Validation Errors", "\n".join(errors), "admin@example.com")
            return None
        
        log_event("Validation passed successfully")
        log_event(f"Validation time: {format_duration(validate_duration)}")
        log_event(f"Records validated: {len(tables['full'])}")
        
        # === LOAD PHASE - PostgreSQL ===
        sql_load_start = time.time()
        log_event("Phase 4: PostgreSQL Loading")
        
        # Incremental load to PostgreSQL
        load_incremental(tables, session)
        session.commit()
        
        sql_load_duration = time.time() - sql_load_start
        log_event(f"PostgreSQL load completed: {format_duration(sql_load_duration)}")
        log_event(f"PostgreSQL throughput: {len(tables['full'])/sql_load_duration:.0f} records/second")
        
        # === PIPELINE SUMMARY ===
        total_duration = time.time() - total_start_time
        log_event("ETL Pipeline Completed Successfully")
        log_event("=" * 50)
        log_event("PERFORMANCE SUMMARY:")
        log_event(f"  Total pipeline duration: {format_duration(total_duration)}")
        log_event(f"  Extraction phase: {format_duration(extract_duration)} ({extract_duration/total_duration*100:.1f}%)")
        log_event(f"  Transformation phase: {format_duration(transform_duration)} ({transform_duration/total_duration*100:.1f}%)")
        log_event(f"  Validation phase: {format_duration(validate_duration)} ({validate_duration/total_duration*100:.1f}%)")
        log_event(f"  PostgreSQL load: {format_duration(sql_load_duration)} ({sql_load_duration/total_duration*100:.1f}%)")
        log_event(f"  Total records processed: {len(df)}")
        log_event(f"  Overall throughput: {len(df)/total_duration:.1f} records/second")
        log_event("=" * 50)
        
        return tables
        
    except Exception as e:
        session.rollback()
        error_duration = time.time() - total_start_time
        log_event(f"ETL failed after {format_duration(error_duration)}: {str(e)}", level="error")
        send_alert("ETL Failure", f"Pipeline failed after {format_duration(error_duration)}\nError: {str(e)}", "admin@example.com")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    configure_logging()
    run_etl()