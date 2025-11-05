from sqlalchemy import create_engine
import os
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from models import Movie_Info, EtlMetadata, Production_Info, Rating_Info
import pandas as pd

load_dotenv()

# Build connection string
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def load_tables(tables: dict):
    """Load transformed DataFrames into PostgreSQL."""
    for name, df in tables.items():
        df.to_sql(name, engine, if_exists="append", index=False)
        print(f"✅ Loaded {len(df)} rows into {name}")

def load_incremental(tables: dict, session: Session):

    df_full = tables["full"]
    # Leer último año cargado
    last_loaded = session.query(EtlMetadata).filter_by(key="last_date").first()
    last_date = pd.to_datetime(last_loaded.value) if last_loaded else pd.Timestamp.min

    # Filtrar solo nuevos
    new_df = df_full[df_full["date_published"] > last_date]

    if new_df.empty:
        print("⚠️ No new data to load.")
        return

    # Prepare mappings for batch insert
    movies_data = new_df[["imdb_title_id", "title", "year", "duration", "description"]].to_dict(orient="records")
    prod_data = new_df[["imdb_title_id", "director", "writer", "production_company", "actors", "country", "language"]].to_dict(orient="records")
    rating_data = new_df[["imdb_title_id", "avg_vote", "votes", "reviews_from_users", "reviews_from_critics"]].to_dict(orient="records")

    # Bulk insert
    session.bulk_insert_mappings(Movie_Info, movies_data)
    session.bulk_insert_mappings(Production_Info, prod_data)
    session.bulk_insert_mappings(Rating_Info, rating_data)
    
    # Update metadata
    if not new_df.empty:
        # Get the most recent date from the new batch
        max_date = new_df["date_published"].max()
        if last_loaded:
            last_loaded.value = str(max_date.date())
        else:
            session.add(EtlMetadata(key="last_date", value=str(max_date.date())))