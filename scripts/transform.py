import pandas as pd
import json
import os

def transform_movies(df: pd.DataFrame) -> dict:
    """Transform raw dataframe into multiple cleaned tables."""

    df = df.copy()

    # Standardize columns
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

    # Convert list-like fields to JSON strings
    for col in ["language", "actors", "director", "writer", "country"]:
        df[col] = df[col].apply(json.dumps)

    # Fix missing year with date_published
    df.loc[df["year"].isna() & df["date_published"].notna(), "year"] = df["date_published"].dt.year
    df["year"] = df["year"].astype(int)

    # Split into target tables
    movie_info = df[["imdb_title_id", "title", "year", "duration", "description"]].copy()

    production_info = df[
        ["imdb_title_id", "director", "writer", "production_company", "actors", "country", "language"]
    ].copy()

    rating_info = df[
        ["imdb_title_id", "avg_vote", "votes", "reviews_from_users", "reviews_from_critics"]
    ].copy()

    # Save the DataFrame to a CSV file
    os.makedirs(os.path.dirname("data/processed/"), exist_ok=True)
    df.to_csv("data/processed/processed.csv", index=False)

    return {
        "full": df,
        "movie_info": movie_info,
        "production_info": production_info,
        "rating_info": rating_info,
    }

def csv_to_json(csv_path: str, json_path: str):
    df = pd.read_csv(csv_path, encoding="utf-8")
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    df.to_json(json_path, orient="records", lines=True, force_ascii=False)

def json_to_csv(json_path: str, csv_path: str):
    df = pd.read_json(json_path, lines=True)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8")