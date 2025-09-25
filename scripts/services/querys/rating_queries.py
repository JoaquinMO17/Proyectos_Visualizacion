"""
SQL queries for rating_info analysis.
Separated by purpose for better organization and reuse.
"""

# 1. Most voted movies (popularity)
TOP_VOTED_MOVIES_QUERY = """
    SELECT 
        m.title,
        m.year,
        r.votes,
        r.avg_vote
    FROM movie_info m
    INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    ORDER BY r.votes DESC
    LIMIT 10;
"""

# 2. Top-rated movies (acclaim)
TOP_RATED_MOVIES_QUERY = """
    SELECT 
        m.title,
        m.year,
        r.avg_vote,
        r.votes
    FROM movie_info m
    INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    WHERE r.votes > 1000 -- avoids movies with too few votes
    ORDER BY r.avg_vote DESC
    LIMIT 10;
"""

# 3. Rating trends over time (yearly average rating and votes)
TREND_RATINGS_BY_YEAR_QUERY = """
    SELECT 
        m.year,
        AVG(r.avg_vote) AS avg_rating,
        AVG(r.votes) AS avg_votes,
        COUNT(*) AS movies_count
    FROM movie_info m
    INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    WHERE m.year IS NOT NULL
    GROUP BY m.year
    ORDER BY m.year;
"""