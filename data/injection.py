import pandas as pd
import json
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get variables
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# Build connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

#Load the CSV
df = pd.read_csv("data/imdb_movies_final.csv", encoding="utf-8")

#Transform columns to match our database design
df["date_published"] = pd.to_datetime(df["date_published"], errors="coerce")
df["reviews_from_users"] = df["reviews_from_users"].fillna(0).astype(int)
df["reviews_from_critics"] = df["reviews_from_critics"].fillna(0).astype(int)
df["votes"] = df["votes"].fillna(0).astype(int)
df["avg_vote"] = df["avg_vote"].fillna(0).astype(float)
df["language"] = df["language"].fillna("Unknown").astype(str)
df["country"] = df["country"].fillna("Unknown").astype(str)
df["director"] = df["director"].fillna("Unknown").astype(str)
df["writer"] = df["writer"].fillna("Unknown").astype(str)
df["actors"] = df["actors"].fillna("Unknown").astype(str)
df["production_company"] = df["production_company"].fillna("Unknown").astype(str)
df["description"] = df["description"].fillna("No description given").astype(str)
df["language"] = df["language"].apply(json.dumps)
df["actors"] = df["actors"].apply(json.dumps)
df["director"] = df["director"].apply(json.dumps)
df["writer"] = df["writer"].apply(json.dumps)
df["country"] = df["country"].apply(json.dumps)
df.loc[df["year"].isna() & df["date_published"].notna(), "year"] = df["date_published"].dt.year
df["year"] = df["year"].astype(int)

# Create the dataframes for the tables
movie_info = df[["imdb_title_id", "title", "year", "duration", "description"]].copy()

production_info = df[["imdb_title_id", "director", "writer", "production_company", "actors", "country", "language"]].copy()

rating_info = df[["imdb_title_id", "avg_vote", "votes", "reviews_from_users", "reviews_from_critics"]].copy()

# Insert into tables
movie_info.to_sql("movie_info", engine, if_exists="append", index=False)
production_info.to_sql("production_info", engine, if_exists="append", index=False)
rating_info.to_sql("rating_info", engine, if_exists="append", index=False)