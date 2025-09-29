import logging
import time
from datetime import datetime
import pandas as pd
import numpy as np
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.extract import extract_movies
from scripts.transform import transform_movies
from scripts.validate import validate_movies
from scripts.monitor import log_event

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

def clean_data_for_mongodb(df):
    """
    Clean DataFrame for MongoDB insertion
    Handles NaT, NaN, and other problematic values
    """
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Handle datetime columns - convert NaT to None
    datetime_cols = df_clean.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        df_clean[col] = df_clean[col].apply(lambda x: None if pd.isna(x) else x.isoformat())
    
    # Handle object columns that might contain datetime objects
    object_cols = df_clean.select_dtypes(include=['object']).columns
    for col in object_cols:
        df_clean[col] = df_clean[col].apply(lambda x: None if pd.isna(x) else x)
    
    # Handle numeric columns - convert NaN to None
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df_clean[col] = df_clean[col].replace([np.inf, -np.inf, np.nan], None)
    
    # Convert to dict and then to JSON and back to ensure serialization
    records = df_clean.to_dict('records')
    
    # Clean each record
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value) or value is pd.NaT:
                cleaned_record[key] = None
            elif isinstance(value, (np.int64, np.int32)):
                cleaned_record[key] = int(value)
            elif isinstance(value, (np.float64, np.float32)):
                cleaned_record[key] = float(value) if not np.isnan(value) else None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return pd.DataFrame(cleaned_records)

def run_mongo_etl():
    """
    Execute ETL pipeline specifically for MongoDB with timing metrics
    """
    total_start = time.time()
    
    try:
        # MongoDB Connection
        print("\n" + "="*70)
        print("MONGODB ETL PIPELINE STARTED")
        print("="*70)
        print(f"Process initiated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        connect_start = time.time()
        print("\nConnecting to MongoDB...")
        
        MONGO_URL = "mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin"
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        
        # Verify connection
        client.admin.command('ping')
        db = client.movies_db
        movies_collection = db.movies
        
        connect_time = time.time() - connect_start
        print(f"  MongoDB connected in: {format_duration(connect_time)}")
        
        # === EXTRACT PHASE ===
        print("\n" + "-"*50)
        print("Phase 1: DATA EXTRACTION")
        print("-"*50)
        extract_start = time.time()
        
        df = extract_movies("data/imdb_movies_final.csv")
        
        extract_time = time.time() - extract_start
        print(f"  Extracted: {len(df)} records")
        print(f"  Time: {format_duration(extract_time)}")
        print(f"  Throughput: {len(df)/extract_time:.0f} records/second")
        
        # === TRANSFORM PHASE ==
        print("\n" + "-"*50)
        print("Phase 2: DATA TRANSFORMATION")
        print("-"*50)
        transform_start = time.time()
        
        tables = transform_movies(df)
        
        transform_time = time.time() - transform_start
        print(f"  Transformation completed")
        print(f"  Time: {format_duration(transform_time)}")
        print(f"  Records processed: {len(tables['full'])}")
        
        # === VALIDATE PHASE ===
        print("\n" + "-"*50)
        print("Phase 3: DATA VALIDATION")
        print("-"*50)
        validate_start = time.time()
        
        errors = validate_movies(tables["full"])
        
        validate_time = time.time() - validate_start
        
        if errors:
            print(f"  Validation FAILED")
            print(f"  Errors found: {len(errors)}")
            print(f"  Time: {format_duration(validate_time)}")
            for i, error in enumerate(errors[:5], 1):
                print(f"    Error {i}: {error}")
            return {
                'status': 'failed',
                'errors': errors[:10],
                'records_loaded': 0,
                'execution_time': time.time() - total_start
            }
        
        print(f"  Validation PASSED")
        print(f"  Time: {format_duration(validate_time)}")
        
        # === LOAD PHASE - MongoDB ===
        print("\n" + "-"*50)
        print("Phase 4: MONGODB LOADING")
        print("-"*50)
        load_start = time.time()
        
        # Convert DataFrame to documents
        print("  Converting data to MongoDB format...")
        conversion_start = time.time()
        
        # CLEAN DATA BEFORE CONVERSION
        cleaned_df = clean_data_for_mongodb(tables["full"])
        movies_data = cleaned_df.to_dict('records')
        
        conversion_time = time.time() - conversion_start
        print(f"    Conversion time: {format_duration(conversion_time)}")
        
        # Clear existing data
        print("  Clearing existing data...")
        delete_start = time.time()
        delete_result = movies_collection.delete_many({})
        delete_time = time.time() - delete_start
        print(f"    Deleted {delete_result.deleted_count} documents")
        print(f"    Time: {format_duration(delete_time)}")
        
        # Insert new data
        print("  Inserting new data...")
        insert_start = time.time()
        if movies_data:
            insert_result = movies_collection.insert_many(movies_data)
            insert_time = time.time() - insert_start
            print(f"    Inserted {len(insert_result.inserted_ids)} documents")
            print(f"    Time: {format_duration(insert_time)}")
            print(f"    Throughput: {len(movies_data)/insert_time:.0f} documents/second")
        else:
            insert_time = 0
            print(f"    No data to insert")
        
        # Create indexes
        print("  Creating indexes...")
        index_start = time.time()
        movies_collection.create_index("title")
        movies_collection.create_index("release_year")
        movies_collection.create_index("imdb_rating")
        movies_collection.create_index([("genre", 1), ("release_year", -1)])
        index_time = time.time() - index_start
        print(f"    Indexes created: 4")
        print(f"    Time: {format_duration(index_time)}")
        
        load_time = time.time() - load_start
        print(f"  Total load time: {format_duration(load_time)}")
        
        # === VERIFICATION ===
        print("\n" + "-"*50)
        print("Phase 5: DATA VERIFICATION")
        print("-"*50)
        verify_start = time.time()
        
        mongo_count = movies_collection.count_documents({})
        expected_count = len(cleaned_df)
        
        verify_time = time.time() - verify_start
        
        if mongo_count == expected_count:
            print(f"  Status: SUCCESS")
            print(f"  Documents in MongoDB: {mongo_count}")
            print(f"  Expected: {expected_count}")
        else:
            print(f"  Status: WARNING - Count mismatch")
            print(f"  Documents in MongoDB: {mongo_count}")
            print(f"  Expected: {expected_count}")
        
        print(f"  Time: {format_duration(verify_time)}")
        
        # === PERFORMANCE TESTING ===
        print("\n" + "-"*50)
        print("Phase 6: PERFORMANCE TESTING")
        print("-"*50)
        perf_start = time.time()
        
        # Test query 1
        query1_start = time.time()
        top_movies = list(movies_collection.find(
            {"imdb_rating": {"$gte": 8.0}}, 
            {"title": 1, "imdb_rating": 1}
        ).limit(10))
        query1_time = time.time() - query1_start
        print(f"  Query 1 (Top rated): {len(top_movies)} results in {format_duration(query1_time)}")
        
        # Test query 2
        query2_start = time.time()
        recent_count = movies_collection.count_documents({"release_year": {"$gte": 2020}})
        query2_time = time.time() - query2_start
        print(f"  Query 2 (Recent movies): {recent_count} results in {format_duration(query2_time)}")
        
        perf_time = time.time() - perf_start
        print(f"  Total test time: {format_duration(perf_time)}")
        
        # === FINAL SUMMARY ===
        total_time = time.time() - total_start
        
        print("\n" + "="*70)
        print("MONGODB ETL SUMMARY")
        print("="*70)
        print(f"Status: COMPLETED SUCCESSFULLY")
        print(f"Total execution time: {format_duration(total_time)}")
        print("\nPhase Breakdown:")
        print(f"  1. Connection:    {format_duration(connect_time)} ({connect_time/total_time*100:.1f}%)")
        print(f"  2. Extract:       {format_duration(extract_time)} ({extract_time/total_time*100:.1f}%)")
        print(f"  3. Transform:     {format_duration(transform_time)} ({transform_time/total_time*100:.1f}%)")
        print(f"  4. Validate:      {format_duration(validate_time)} ({validate_time/total_time*100:.1f}%)")
        print(f"  5. Load:          {format_duration(load_time)} ({load_time/total_time*100:.1f}%)")
        print(f"  6. Verify:        {format_duration(verify_time)} ({verify_time/total_time*100:.1f}%)")
        print(f"  7. Performance:   {format_duration(perf_time)} ({perf_time/total_time*100:.1f}%)")
        print(f"\nRecords processed: {len(df)}")
        print(f"Overall throughput: {len(df)/total_time:.1f} records/second")
        print("="*70 + "\n")
        
        client.close()
        
        return {
            'status': 'success',
            'records_loaded': mongo_count,
            'execution_time': total_time
        }
        
    except ConnectionFailure as e:
        error_time = time.time() - total_start
        print(f"\nCONNECTION ERROR after {format_duration(error_time)}: {str(e)}")
        return {'status': 'error', 'error': str(e), 'records_loaded': 0, 'execution_time': error_time}
        
    except Exception as e:
        error_time = time.time() - total_start
        print(f"\nERROR after {format_duration(error_time)}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'error': str(e), 'records_loaded': 0, 'execution_time': error_time}

if __name__ == "__main__":
    run_mongo_etl()