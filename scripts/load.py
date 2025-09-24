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
    last_loaded = session.query(EtlMetadata).filter_by(key="last_year").first()
    last_year = int(last_loaded.value) if last_loaded else 0

    # Filtrar solo nuevos
    new_df = df_full[df_full["year"] > last_year]

    if new_df.empty:
        print("⚠️ No new data to load.")
        return

    for _, row in new_df.iterrows():
        movie = Movie_Info(
            imdb_title_id=row["imdb_title_id"],
            title=row["title"],
            year=row["year"],
            duration=row["duration"],
            description=row["description"]
        )
        session.add(movie)

        prod = Production_Info(
            imdb_title_id=row["imdb_title_id"],
            director=row["director"],
            writer=row["writer"],
            production_company=row["production_company"],
            actors=row["actors"],
            country=row["country"],
            language=row["language"]
        )
        session.add(prod)


        rating = Rating_Info(
            imdb_title_id=row["imdb_title_id"],
            avg_vote=row["avg_vote"],
            votes=row["votes"],
            reviews_from_users=row["reviews_from_users"],
            reviews_from_critics=row["reviews_from_critics"]
        )
        session.add(rating)

    # Actualizar metadatos
    if not new_df.empty:
        if last_loaded:
            last_loaded.value = str(new_df["year"].max())
        else:
            session.add(EtlMetadata(key="last_year", value=str(new_df["year"].max())))