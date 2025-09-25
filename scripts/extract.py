import pandas as pd
from pathlib import Path

def extract_movies(path: str = "data/imdb_movies_final.csv") -> pd.DataFrame:
    """Extract movies dataset from CSV."""
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    return pd.read_csv(csv_path, encoding="utf-8")
