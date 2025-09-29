from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc, and_
from models import Rating_Info, Movie_Info, Production_Info
from typing import List, Dict, Any, Optional

class RatingService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_rating_distribution(
        self,
        bins: int = 10
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                FLOOR(avg_vote) as rating_floor,
                COUNT(*) as movie_count,
                AVG(votes) as avg_votes,
                SUM(votes) as total_votes,
                AVG(reviews_from_users) as avg_user_reviews,
                AVG(reviews_from_critics) as avg_critic_reviews
            FROM rating_info
            WHERE avg_vote IS NOT NULL
            GROUP BY FLOOR(avg_vote)
            ORDER BY rating_floor
        """)
        
        result = self.db.execute(query).mappings().all()
        
        return [
            {
                "rating_range": f"{int(row['rating_floor'])}-{int(row['rating_floor'])+1}",
                "movie_count": row["movie_count"],
                "avg_votes": round(float(row["avg_votes"]), 0) if row["avg_votes"] else 0,
                "total_votes": row["total_votes"] or 0,
                "avg_user_reviews": round(float(row["avg_user_reviews"]), 0) if row["avg_user_reviews"] else 0,
                "avg_critic_reviews": round(float(row["avg_critic_reviews"]), 0) if row["avg_critic_reviews"] else 0
            }
            for row in result
        ]
    
    def get_top_rated_movies(
        self,
        limit: int = 20,
        min_votes: int = 1000
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                r.imdb_title_id,
                m.title,
                m.year,
                m.duration,
                r.avg_vote,
                r.votes,
                r.reviews_from_users,
                r.reviews_from_critics,
                p.director,
                p.production_company,
                (r.avg_vote * LOG(r.votes + 1)) as weighted_score
            FROM rating_info r
            INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
            LEFT JOIN production_info p ON r.imdb_title_id = p.imdb_title_id
            WHERE r.votes >= :min_votes
            ORDER BY r.avg_vote DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "min_votes": min_votes
        }).mappings().all()
        
        return [
            {
                "imdb_title_id": row["imdb_title_id"],
                "title": row["title"],
                "year": row["year"],
                "duration": row["duration"],
                "avg_vote": round(float(row["avg_vote"]), 2) if row["avg_vote"] else None,
                "votes": row["votes"],
                "reviews_from_users": row["reviews_from_users"],
                "reviews_from_critics": row["reviews_from_critics"],
                "director": str(row["director"]).strip('"[]') if row["director"] else None,
                "production_company": row["production_company"],
                "weighted_score": round(float(row["weighted_score"]), 2) if row["weighted_score"] else None
            }
            for row in result
        ]
    
    def get_most_voted_movies(
        self,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                r.imdb_title_id,
                m.title,
                m.year,
                r.avg_vote,
                r.votes,
                r.reviews_from_users,
                r.reviews_from_critics,
                (r.reviews_from_users + r.reviews_from_critics) as total_reviews
            FROM rating_info r
            INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
            ORDER BY r.votes DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {"limit": limit}).mappings().all()
        
        return [
            {
                "imdb_title_id": row["imdb_title_id"],
                "title": row["title"],
                "year": row["year"],
                "avg_vote": round(float(row["avg_vote"]), 2) if row["avg_vote"] else None,
                "votes": row["votes"],
                "reviews_from_users": row["reviews_from_users"],
                "reviews_from_critics": row["reviews_from_critics"],
                "total_reviews": row["total_reviews"]
            }
            for row in result
        ]
    
    def get_rating_trends_by_year(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        year_filter = ""
        params = {}
        
        if start_year or end_year:
            year_filter = "WHERE m.year BETWEEN :start_year AND :end_year"
            params["start_year"] = start_year or 1900
            params["end_year"] = end_year or 2030
        
        query = text(f"""
            SELECT 
                m.year,
                COUNT(*) as movie_count,
                AVG(r.avg_vote) as avg_rating,
                AVG(r.votes) as avg_votes,
                SUM(r.votes) as total_votes,
                MAX(r.avg_vote) as best_rating,
                MIN(r.avg_vote) as worst_rating,
                STDDEV(r.avg_vote) as rating_std_dev,
                AVG(r.reviews_from_users) as avg_user_reviews,
                AVG(r.reviews_from_critics) as avg_critic_reviews
            FROM rating_info r
            INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
            {year_filter}
            GROUP BY m.year
            HAVING m.year IS NOT NULL
            ORDER BY m.year DESC
            LIMIT 50
        """)
        
        result = self.db.execute(query, params).mappings().all()
        
        return [
            {
                "year": row["year"],
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "avg_votes": round(float(row["avg_votes"]), 0) if row["avg_votes"] else None,
                "total_votes": row["total_votes"],
                "best_rating": round(float(row["best_rating"]), 2) if row["best_rating"] else None,
                "worst_rating": round(float(row["worst_rating"]), 2) if row["worst_rating"] else None,
                "rating_std_dev": round(float(row["rating_std_dev"]), 2) if row["rating_std_dev"] else None,
                "avg_user_reviews": round(float(row["avg_user_reviews"]), 0) if row["avg_user_reviews"] else None,
                "avg_critic_reviews": round(float(row["avg_critic_reviews"]), 0) if row["avg_critic_reviews"] else None
            }
            for row in result
        ]
    
    def get_controversial_movies(
        self,
        limit: int = 20,
        min_votes: int = 500
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                r.imdb_title_id,
                m.title,
                m.year,
                r.avg_vote,
                r.votes,
                r.reviews_from_users,
                r.reviews_from_critics,
                ABS(r.reviews_from_critics - r.reviews_from_users) as review_gap,
                CASE 
                    WHEN r.reviews_from_critics > r.reviews_from_users 
                    THEN 'Critics preferred'
                    ELSE 'Users preferred'
                END as preference
            FROM rating_info r
            INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
            WHERE r.votes >= :min_votes 
                AND r.reviews_from_critics > 0
                AND r.reviews_from_users > 0
            ORDER BY ABS(r.reviews_from_critics - r.reviews_from_users) DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "min_votes": min_votes
        }).mappings().all()
        
        return [
            {
                "imdb_title_id": row["imdb_title_id"],
                "title": row["title"],
                "year": row["year"],
                "avg_vote": round(float(row["avg_vote"]), 2) if row["avg_vote"] else None,
                "votes": row["votes"],
                "reviews_from_users": row["reviews_from_users"],
                "reviews_from_critics": row["reviews_from_critics"],
                "review_gap": row["review_gap"],
                "preference": row["preference"]
            }
            for row in result
        ]
    
    def get_rating_vs_duration_analysis(self) -> List[Dict[str, Any]]:
        query = text("""
            WITH duration_categories AS (
                SELECT 
                    r.imdb_title_id,
                    r.avg_vote,
                    r.votes,
                    m.duration,
                    CASE 
                        WHEN m.duration < 60 THEN 'Short (<60 min)'
                        WHEN m.duration BETWEEN 60 AND 90 THEN 'Medium (60-90 min)'
                        WHEN m.duration BETWEEN 91 AND 120 THEN 'Standard (91-120 min)'
                        WHEN m.duration BETWEEN 121 AND 150 THEN 'Long (121-150 min)'
                        WHEN m.duration > 150 THEN 'Very Long (>150 min)'
                        ELSE 'Unknown'
                    END as duration_category,
                    CASE 
                        WHEN m.duration < 60 THEN 1
                        WHEN m.duration BETWEEN 60 AND 90 THEN 2
                        WHEN m.duration BETWEEN 91 AND 120 THEN 3
                        WHEN m.duration BETWEEN 121 AND 150 THEN 4
                        WHEN m.duration > 150 THEN 5
                        ELSE 6
                    END as sort_order
                FROM rating_info r
                INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
                WHERE m.duration IS NOT NULL
            )
            SELECT 
                duration_category,
                COUNT(*) as movie_count,
                AVG(avg_vote) as avg_rating,
                AVG(votes) as avg_votes,
                AVG(duration) as avg_duration
            FROM duration_categories
            GROUP BY duration_category, sort_order
            ORDER BY sort_order
        """)
        
        result = self.db.execute(query).mappings().all()
        
        return [
            {
                "duration_category": row["duration_category"],
                "movie_count": row["movie_count"],
                "avg_rating": round(float(row["avg_rating"]), 2) if row["avg_rating"] else None,
                "avg_votes": round(float(row["avg_votes"]), 0) if row["avg_votes"] else None,
                "avg_duration": round(float(row["avg_duration"]), 0) if row["avg_duration"] else None
            }
            for row in result
        ]
    
    def get_underrated_movies(
        self,
        limit: int = 20,
        max_votes: int = 10000,
        min_rating: float = 7.0
    ) -> List[Dict[str, Any]]:
        query = text("""
            SELECT 
                r.imdb_title_id,
                m.title,
                m.year,
                m.duration,
                r.avg_vote,
                r.votes,
                r.reviews_from_users,
                r.reviews_from_critics,
                p.director,
                p.country
            FROM rating_info r
            INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
            LEFT JOIN production_info p ON r.imdb_title_id = p.imdb_title_id
            WHERE r.avg_vote >= :min_rating
                AND r.votes <= :max_votes
                AND r.votes > 100
            ORDER BY r.avg_vote DESC, r.votes DESC
            LIMIT :limit
        """)
        
        result = self.db.execute(query, {
            "limit": limit,
            "max_votes": max_votes,
            "min_rating": min_rating
        }).mappings().all()
        
        return [
            {
                "imdb_title_id": row["imdb_title_id"],
                "title": row["title"],
                "year": row["year"],
                "duration": row["duration"],
                "avg_vote": round(float(row["avg_vote"]), 2) if row["avg_vote"] else None,
                "votes": row["votes"],
                "reviews_from_users": row["reviews_from_users"],
                "reviews_from_critics": row["reviews_from_critics"],
                "director": str(row["director"]).strip('"[]') if row["director"] else None,
                "country": str(row["country"]).strip('"[]') if row["country"] else None
            }
            for row in result
        ]
    
    def get_rating_statistics(self) -> Dict[str, Any]:
        query = text("""
            SELECT 
                COUNT(*) as total_movies,
                AVG(avg_vote) as overall_avg_rating,
                MAX(avg_vote) as highest_rating,
                MIN(avg_vote) as lowest_rating,
                STDDEV(avg_vote) as rating_std_dev,
                SUM(votes) as total_votes,
                AVG(votes) as avg_votes_per_movie,
                MAX(votes) as max_votes,
                MIN(votes) as min_votes,
                SUM(reviews_from_users) as total_user_reviews,
                SUM(reviews_from_critics) as total_critic_reviews,
                AVG(reviews_from_users) as avg_user_reviews,
                AVG(reviews_from_critics) as avg_critic_reviews,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_vote) as median_rating,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_vote) as q1_rating,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_vote) as q3_rating
            FROM rating_info
            WHERE avg_vote IS NOT NULL
        """)
        
        result = self.db.execute(query).mappings().first()
        
        if result:
            return {
                "total_movies": result["total_movies"],
                "overall_avg_rating": round(float(result["overall_avg_rating"]), 2) if result["overall_avg_rating"] else None,
                "highest_rating": round(float(result["highest_rating"]), 2) if result["highest_rating"] else None,
                "lowest_rating": round(float(result["lowest_rating"]), 2) if result["lowest_rating"] else None,
                "rating_std_dev": round(float(result["rating_std_dev"]), 2) if result["rating_std_dev"] else None,
                "total_votes": result["total_votes"],
                "avg_votes_per_movie": round(float(result["avg_votes_per_movie"]), 0) if result["avg_votes_per_movie"] else None,
                "max_votes": result["max_votes"],
                "min_votes": result["min_votes"],
                "total_user_reviews": result["total_user_reviews"],
                "total_critic_reviews": result["total_critic_reviews"],
                "avg_user_reviews": round(float(result["avg_user_reviews"]), 0) if result["avg_user_reviews"] else None,
                "avg_critic_reviews": round(float(result["avg_critic_reviews"]), 0) if result["avg_critic_reviews"] else None,
                "median_rating": round(float(result["median_rating"]), 2) if result["median_rating"] else None,
                "q1_rating": round(float(result["q1_rating"]), 2) if result["q1_rating"] else None,
                "q3_rating": round(float(result["q3_rating"]), 2) if result["q3_rating"] else None
            }
        return {}