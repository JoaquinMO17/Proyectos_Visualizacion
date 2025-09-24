import pandas as pd

def validate_movies(df: pd.DataFrame):
    errors = []

    # Example rules
    if df["year"].isnull().any():
        errors.append("Missing values in 'year' column")

    if (df["avg_vote"] < 0).any() or (df["avg_vote"] > 10).any():
        errors.append("Invalid vote range")

    if df["imdb_title_id"].duplicated().any():
        errors.append("Duplicate imdb_title_id detected")

    return errors
