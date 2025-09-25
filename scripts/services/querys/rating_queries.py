"""
SQL queries for rating_info analysis.
Separated by purpose for better organization and reuse.
"""

# 1. Películas más votadas (popularidad)
TOP_VOTED_MOVIES_QUERY = """
    SELECT 
        r.imdb_title_id,
        m.year,
        r.votes,
        r.avg_vote,
        m.title,
        m.year
    FROM rating_info r
    INNER JOIN movie_info m ON r.imdb_title_id = m.imdb_title_id
    WHERE r.avg_vote IS NOT NULL
    ORDER BY r.avg_vote DESC, r.votes DESC
    LIMIT :limit
"""