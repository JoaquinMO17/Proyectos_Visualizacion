from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any


class MovieService:
    def __init__(self, db: Session):
        self.db = db

    def get_top_words_by_decade(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the most frequent words in movie descriptions grouped by decade"""
        query = text("""
            WITH decade_descriptions AS (
                SELECT 
                    (year / 10) * 10 AS decade,
                    description
                FROM movie_info
                WHERE year IS NOT NULL AND description IS NOT NULL
            ),
            tokenized AS (
                SELECT 
                    decade,
                    LOWER(unnest(string_to_array(regexp_replace(description, '[^a-zA-Z ]', '', 'g'), ' '))) AS word
                FROM decade_descriptions
            )
            SELECT 
                decade,
                word,
                COUNT(*) AS frequency
            FROM tokenized
            WHERE LENGTH(word) > 3
            GROUP BY decade, word
            ORDER BY decade DESC, frequency DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, {"limit": limit})

        return [
            {
                "decade": row[0],
                "word": row[1],
                "frequency": row[2]
            }
            for row in result
        ]

    def get_movie_count_by_year(self) -> List[Dict[str, Any]]:
        """Get the number of movies grouped by year"""
        query = text("""
            SELECT 
                year,
                COUNT(*) AS total_movies
            FROM movie_info
            WHERE year IS NOT NULL
            GROUP BY year
            ORDER BY year
        """)

        result = self.db.execute(query)

        return [
            {
                "year": row[0],
                "total_movies": row[1]
            }
            for row in result
        ]