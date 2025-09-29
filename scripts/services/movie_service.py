from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any
import json

class MovieService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_top_movies_by_rating(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene las películas mejor calificadas con información completa"""
        query = text("""
            SELECT 
                m.imdb_title_id,
                m.title,
                m.year,
                m.duration,
                m.description,
                r.avg_vote,
                r.votes,
                r.reviews_from_users,
                p.director,
                p.actors,
                p.production_company
            FROM movie_info m
            JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
            JOIN production_info p ON m.imdb_title_id = p.imdb_title_id
            WHERE r.avg_vote IS NOT NULL
            ORDER BY r.avg_vote DESC, r.votes DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {"limit": limit})
        movies = []
        
        for row in result:
            movies.append({
                "imdb_title_id": row[0],
                "title": row[1],
                "year": row[2],
                "duration": row[3],
                "description": row[4],
                "avg_vote": float(row[5]) if row[5] else 0,
                "votes": row[6],
                "reviews_from_users": row[7],
                "director": json.loads(row[8]) if row[8] else [],
                "actors": json.loads(row[9]) if row[9] else [],
                "production_company": row[10]
            })
        
        return movies
    
    def get_movies_by_year_range(self, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """Obtiene películas dentro de un rango de años"""
        query = text("""
            SELECT 
                m.imdb_title_id,
                m.title,
                m.year,
                m.duration,
                r.avg_vote,
                r.votes
            FROM movie_info m
            LEFT JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
            WHERE m.year BETWEEN :start_year AND :end_year
            ORDER BY m.year DESC, r.avg_vote DESC
            LIMIT 100
        """)
        
        result = self.db.execute(query, {"start_year": start_year, "end_year": end_year})
        
        return [
            {
                "imdb_title_id": row[0],
                "title": row[1],
                "year": row[2],
                "duration": row[3],
                "avg_vote": float(row[4]) if row[4] else None,
                "votes": row[5]
            }
            for row in result
        ]
    
    def get_movie_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas generales de la base de datos"""
        stats_query = text("""
            SELECT 
                COUNT(DISTINCT m.imdb_title_id) as total_movies,
                AVG(r.avg_vote) as average_rating,
                MAX(r.avg_vote) as max_rating,
                MIN(r.avg_vote) as min_rating,
                AVG(m.duration) as avg_duration,
                COUNT(DISTINCT m.year) as unique_years,
                MIN(m.year) as oldest_year,
                MAX(m.year) as newest_year
            FROM movie_info m
            LEFT JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
        """)
        
        result = self.db.execute(stats_query).fetchone()
        
        top_directors_query = text("""
            SELECT 
                p.director,
                COUNT(*) as movie_count,
                AVG(r.avg_vote) as avg_rating
            FROM production_info p
            JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.director IS NOT NULL AND p.director != '"Unknown"'
            GROUP BY p.director
            ORDER BY movie_count DESC
            LIMIT 5
        """)
        
        directors = self.db.execute(top_directors_query).fetchall()
        
        return {
            "total_movies": result[0] or 0,
            "average_rating": round(float(result[1]), 2) if result[1] else 0,
            "max_rating": float(result[2]) if result[2] else 0,
            "min_rating": float(result[3]) if result[3] else 0,
            "average_duration_minutes": round(float(result[4]), 1) if result[4] else 0,
            "unique_years": result[5] or 0,
            "oldest_year": result[6],
            "newest_year": result[7],
            "top_directors": [
                {
                    "director": json.loads(director[0]) if director[0] else "Unknown",
                    "movie_count": director[1],
                    "avg_rating": round(float(director[2]), 2) if director[2] else 0
                }
                for director in directors
            ]
        }
    
    def search_movies_advanced(self, 
                              title: str = None,
                              min_year: int = None,
                              max_year: int = None,
                              min_rating: float = None) -> List[Dict[str, Any]]:
        """Búsqueda avanzada de películas con múltiples filtros"""
        conditions = []
        params = {}
        
        if title:
            conditions.append("LOWER(m.title) LIKE LOWER(:title)")
            params["title"] = f"%{title}%"
        
        if min_year:
            conditions.append("m.year >= :min_year")
            params["min_year"] = min_year
        
        if max_year:
            conditions.append("m.year <= :max_year")
            params["max_year"] = max_year
        
        if min_rating:
            conditions.append("r.avg_vote >= :min_rating")
            params["min_rating"] = min_rating
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = text(f"""
            SELECT 
                m.imdb_title_id,
                m.title,
                m.year,
                m.duration,
                m.description,
                r.avg_vote,
                r.votes,
                p.director,
                p.production_company
            FROM movie_info m
            LEFT JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
            LEFT JOIN production_info p ON m.imdb_title_id = p.imdb_title_id
            WHERE {where_clause}
            ORDER BY r.avg_vote DESC NULLS LAST
            LIMIT 50
        """)
        
        result = self.db.execute(query, params)
        
        return [
            {
                "imdb_title_id": row[0],
                "title": row[1],
                "year": row[2],
                "duration": row[3],
                "description": row[4],
                "avg_vote": float(row[5]) if row[5] else None,
                "votes": row[6],
                "director": json.loads(row[7]) if row[7] else None,
                "production_company": row[8]
            }
            for row in result
        ]