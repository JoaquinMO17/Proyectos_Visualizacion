from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc, and_
from models import Production_Info, Movie_Info, Rating_Info
from typing import List, Dict, Any, Optional
import json

class ProductionService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_top_production_companies(
        self, 
        limit: int = 10,
        min_movies: int = 0
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                p.production_company,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                SUM(r.votes) as total_votes,
                MIN(m.year) as first_movie_year,
                MAX(m.year) as last_movie_year,
                AVG(m.duration) as avg_duration
            FROM production_info p
            INNER JOIN movie_info m ON p.imdb_title_id = m.imdb_title_id
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.production_company IS NOT NULL 
                AND p.production_company != 'Unknown'
                AND p.production_company != ''
            GROUP BY p.production_company
            HAVING COUNT(DISTINCT p.imdb_title_id) >= :min_movies
            ORDER BY movie_count DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "min_movies": min_movies
        }).mappings().all()
        
        return [
            {
                "production_company": row["production_company"],
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "total_votes": row["total_votes"] or 0,
                "years_active": f"{row['first_movie_year']}-{row['last_movie_year']}" if row['first_movie_year'] else None,
                "avg_duration": round(float(row["avg_duration"]), 0) if row["avg_duration"] else None
            }
            for row in result
        ]
    
    def get_movies_by_country(
        self,
        top_n: int = 20,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        year_filter = ""
        params = {"limit": top_n}
        
        if year_from or year_to:
            year_filter = "AND m.year BETWEEN :year_from AND :year_to"
            params["year_from"] = year_from or 1900
            params["year_to"] = year_to or 2030
        
        query = text(f"""
            SELECT 
                p.country as country,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                AVG(m.duration) as avg_duration,
                COUNT(DISTINCT 
                    CASE 
                        WHEN r.avg_vote >= 7.0 THEN p.imdb_title_id 
                    END
                ) as high_rated_count
            FROM production_info p
            INNER JOIN movie_info m ON p.imdb_title_id = m.imdb_title_id
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.country IS NOT NULL
                AND p.country::text != 'Unknown'
                AND p.country::text != '""'
                AND p.country::text != '"Unknown"'
                {year_filter}
            GROUP BY p.country
            ORDER BY movie_count DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, params).mappings().all()
        
        output = []
        for row in result:
            country_raw = str(row["country"]) if row["country"] else "Unknown"
            
            if country_raw.startswith('[') or country_raw.startswith('"['):
                try:
                    if country_raw.startswith('"') and country_raw.endswith('"'):
                        country_raw = country_raw[1:-1]
                    countries_list = json.loads(country_raw)
                    country_clean = countries_list[0] if isinstance(countries_list, list) and countries_list else country_raw
                except:
                    country_clean = country_raw.strip('"[]')
            else:
                country_clean = country_raw.strip('"')
            
            output.append({
                "country": country_clean,
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "avg_duration": round(float(row["avg_duration"]), 0) if row["avg_duration"] else None,
                "high_rated_count": row["high_rated_count"],
                "high_rated_percentage": round(row["high_rated_count"] / row["movie_count"] * 100, 1) if row["movie_count"] > 0 else 0
            })
        
        return output
    
    def get_top_directors(
        self,
        limit: int = 15,
        min_movies: int = 2,
        sort_by: str = "movie_count"
    ) -> List[Dict[str, Any]]:
        sort_column = {
            "movie_count": "movie_count DESC",
            "avg_rating": "avg_rating DESC NULLS LAST",
            "total_votes": "total_votes DESC NULLS LAST"
        }.get(sort_by, "movie_count DESC")
        
        query = text(f"""
            SELECT 
                p.director as director_data,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                SUM(r.votes) as total_votes,
                MAX(r.avg_vote) as best_rating
            FROM production_info p
            INNER JOIN movie_info m ON p.imdb_title_id = m.imdb_title_id
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.director IS NOT NULL
                AND p.director::text != 'Unknown'
                AND p.director::text != '""'
                AND p.director::text != '"Unknown"'
            GROUP BY p.director
            HAVING COUNT(DISTINCT p.imdb_title_id) >= :min_movies
            ORDER BY {sort_column}
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "min_movies": min_movies
        }).mappings().all()
        
        output = []
        for row in result:
            director_raw = str(row["director_data"]) if row["director_data"] else "Unknown"
            
            if director_raw.startswith('[') or director_raw.startswith('"['):
                try:
                    if director_raw.startswith('"') and director_raw.endswith('"'):
                        director_raw = director_raw[1:-1]
                    directors_list = json.loads(director_raw)
                    director_clean = directors_list[0] if isinstance(directors_list, list) and directors_list else director_raw
                except:
                    director_clean = director_raw.strip('"[]')
            else:
                director_clean = director_raw.strip('"')
            
            output.append({
                "director": director_clean,
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "total_votes": row["total_votes"] or 0,
                "best_rating": round(float(row["best_rating"]), 2) if row["best_rating"] else None
            })
        
        return output
    
    def get_language_distribution(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                p.language as language_data,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                COUNT(DISTINCT p.production_company) as production_companies
            FROM production_info p
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.language IS NOT NULL
                AND p.language::text != 'Unknown'
                AND p.language::text != '""'
            GROUP BY p.language
            ORDER BY movie_count DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {"limit": limit}).mappings().all()
        
        output = []
        for row in result:
            language_raw = str(row["language_data"]) if row["language_data"] else "Unknown"
            
            if language_raw.startswith('[') or language_raw.startswith('"['):
                try:
                    if language_raw.startswith('"') and language_raw.endswith('"'):
                        language_raw = language_raw[1:-1]
                    languages_list = json.loads(language_raw)
                    language_clean = languages_list[0] if isinstance(languages_list, list) and languages_list else language_raw
                except:
                    language_clean = language_raw.strip('"[]')
            else:
                language_clean = language_raw.strip('"')
            
            output.append({
                "language": language_clean,
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "production_companies": row["production_companies"]
            })
        
        return output
    
    def get_top_actors(
        self,
        limit: int = 20,
        min_movies: int = 3
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                p.actors as actors_data,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                SUM(r.votes) as total_votes,
                MIN(m.year) as first_movie,
                MAX(m.year) as last_movie,
                COUNT(DISTINCT p.production_company) as companies_worked_with
            FROM production_info p
            INNER JOIN movie_info m ON p.imdb_title_id = m.imdb_title_id
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.actors IS NOT NULL
                AND p.actors::text != 'Unknown'
                AND p.actors::text != '""'
            GROUP BY p.actors
            HAVING COUNT(DISTINCT p.imdb_title_id) >= :min_movies
            ORDER BY movie_count DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "min_movies": min_movies
        }).mappings().all()
        
        output = []
        for row in result:
            actors_raw = str(row["actors_data"]) if row["actors_data"] else "Unknown"
            
            if actors_raw.startswith('[') or actors_raw.startswith('"['):
                try:
                    if actors_raw.startswith('"') and actors_raw.endswith('"'):
                        actors_raw = actors_raw[1:-1]
                    actors_list = json.loads(actors_raw)
                    actor_clean = actors_list[0] if isinstance(actors_list, list) and actors_list else actors_raw
                except:
                    actor_clean = actors_raw.strip('"[]')
            else:
                actor_clean = actors_raw.strip('"')
            
            output.append({
                "actor": actor_clean,
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "total_votes": row["total_votes"] or 0,
                "career_span": f"{row['first_movie']}-{row['last_movie']}" if row['first_movie'] else None,
                "companies_worked_with": row["companies_worked_with"]
            })
        
        return output
    
    def get_top_writers(
        self,
        limit: int = 15
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                p.writer as writer_data,
                COUNT(DISTINCT p.imdb_title_id) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                AVG(m.duration) as avg_duration
            FROM production_info p
            INNER JOIN movie_info m ON p.imdb_title_id = m.imdb_title_id
            LEFT JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
            WHERE p.writer IS NOT NULL
                AND p.writer::text != 'Unknown'
                AND p.writer::text != '""'
            GROUP BY p.writer
            HAVING COUNT(DISTINCT p.imdb_title_id) >= 2
            ORDER BY avg_rating DESC NULLS LAST
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {"limit": limit}).mappings().all()
        
        output = []
        for row in result:
            writer_raw = str(row["writer_data"]) if row["writer_data"] else "Unknown"
            
            if writer_raw.startswith('[') or writer_raw.startswith('"['):
                try:
                    if writer_raw.startswith('"') and writer_raw.endswith('"'):
                        writer_raw = writer_raw[1:-1]
                    writers_list = json.loads(writer_raw)
                    writer_clean = writers_list[0] if isinstance(writers_list, list) and writers_list else writer_raw
                except:
                    writer_clean = writer_raw.strip('"[]')
            else:
                writer_clean = writer_raw.strip('"')
            
            output.append({
                "writer": writer_clean,
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "avg_duration": round(float(row["avg_duration"]), 0) if row["avg_duration"] else None
            })
        
        return output