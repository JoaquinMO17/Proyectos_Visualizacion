# services/rating_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from scripts.services.querys.rating_queries import (
    TOP_VOTED_MOVIES_QUERY,
    TOP_RATED_MOVIES_QUERY,
    TREND_RATINGS_BY_YEAR_QUERY
)

class RatingService:
    """
    Service layer for handling rating-related queries and business logic.
    """

    def _init_(self, db: Session):
        self.db = db

    def get_top_voted_movies(self, limit: int = 10):
        """
        Return the most voted movies (popularity).
        Default limit is 10, but can be customized.
        """
        query = TOP_VOTED_MOVIES_QUERY.replace("LIMIT 10", f"LIMIT {limit}")
        result = self.db.execute(text(query)).fetchall()
        return [dict(row) for row in result]

    def get_top_rated_movies(self, limit: int = 10):
        """
        Return the top-rated movies (acclaimed).
        Only considers movies with more than 1000 votes.
        """
        query = TOP_RATED_MOVIES_QUERY.replace("LIMIT 10", f"LIMIT {limit}")
        result = self.db.execute(text(query)).fetchall()
        return [dict(row) for row in result]

    def get_trend_ratings_by_year(self):
        """
        Return the average rating and voting trends by year.
        Includes average rating, average votes, and number of movies.
        """
        result = self.db.execute(text(TREND_RATINGS_BY_YEAR_QUERY)).fetchall()
        return [dict(row) for row in result]
