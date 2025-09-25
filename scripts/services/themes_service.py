from sqlalchemy.orm import Session
from sqlalchemy import text
from collections import Counter, defaultdict
import re

# Stopwords básicas en español e inglés (puedes ampliarlas)
STOPWORDS = set([
    "de", "la", "el", "en", "y", "a", "del", "los", "las",
    "una", "un", "con", "por", "para", "se", "su", "al",
    "the", "of", "and", "to", "in", "on", "at", "is", "it"
])

class ThemesService:
    def __init__(self, db: Session):
        self.db = db

    def get_themes_by_decade(self, top_n: int = 20):
        """
        Retorna las palabras más frecuentes en las descripciones de películas agrupadas por década.
        """
        query = text("""
            SELECT year, description
            FROM movie_info
            WHERE description IS NOT NULL
              AND year IS NOT NULL
              AND year >= 1900
        """)
        rows = self.db.execute(query).fetchall()

        # Diccionario {decada: lista_palabras}
        decade_words = defaultdict(list)

        for row in rows:
            decade = (row.year // 10) * 10  # ejemplo: 1994 -> 1990
            words = re.findall(r'\b\w+\b', row.description.lower())  # tokenización simple
            filtered_words = [w for w in words if w not in STOPWORDS and len(w) > 2]
            decade_words[decade].extend(filtered_words)

        # Contar palabras más comunes por década
        result = {}
        for decade, words in decade_words.items():
            counter = Counter(words).most_common(top_n)
            result[decade] = [{"word": w, "count": c} for w, c in counter]

        # Metadatos
        metadata = {
            "total_decades": len(result),
            "total_movies_analyzed": len(rows),
            "words_per_decade": top_n
        }

        return {"status": "success", "data": result, "metadata": metadata}
