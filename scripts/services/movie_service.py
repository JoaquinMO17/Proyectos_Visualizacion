# services/movie_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any

class MoviesService:
    def __init__(self, db: Session):
        self.db = db

    def get_distribution_by_year(self, start_year: int = None, end_year: int = None, group_by: str = "year") -> Dict[str, Any]:
        """Obtiene la cantidad de películas por año o década"""

        conditions = []
        params = {}

        if start_year:
            conditions.append("year >= :start_year")
            params["start_year"] = start_year
        if end_year:
            conditions.append("year <= :end_year")
            params["end_year"] = end_year

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        if group_by == "decade":
            query = text(f"""
                SELECT (year/10)*10 AS period, COUNT(*) AS count
                FROM movie_info
                {where_clause}
                GROUP BY period
                ORDER BY period
            """)
        else:  # por año
            query = text(f"""
                SELECT year AS period, COUNT(*) AS count
                FROM movie_info
                {where_clause}
                GROUP BY year
                ORDER BY year
            """)

        result = self.db.execute(query, params).fetchall()
        data = [{"period": row[0], "count": row[1]} for row in result]

        # Metadatos
        metadata = {
            "total_movies": sum([d["count"] for d in data]),
            "total_periods": len(data),
            "average_per_period": round(sum([d["count"] for d in data]) / len(data), 2) if data else 0,
            "peak_production": max(data, key=lambda x: x["count"]) if data else None
        }

        return {"status": "success", "data": data, "metadata": metadata} 
