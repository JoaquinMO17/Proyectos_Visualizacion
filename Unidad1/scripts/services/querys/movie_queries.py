# Queries SQL separadas para mejor mantenibilidad

TOP_MOVIES_QUERY = """
    SELECT 
        m.imdb_title_id,
        m.title,
        m.year,
        m.duration,
        m.description,
        r.avg_vote,
        r.votes,
        r.reviews_from_users,
        r.reviews_from_critics,
        p.director,
        p.actors,
        p.writer,
        p.production_company,
        p.country,
        p.language
    FROM movie_info m
    INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    INNER JOIN production_info p ON m.imdb_title_id = p.imdb_title_id
    WHERE r.avg_vote IS NOT NULL AND r.avg_vote > 0
    ORDER BY r.avg_vote DESC, r.votes DESC
    LIMIT :limit
"""

MOVIES_BY_YEAR_QUERY = """
    SELECT 
        m.imdb_title_id,
        m.title,
        m.year,
        m.duration,
        r.avg_vote,
        r.votes
    FROM movie_info m
    LEFT JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    WHERE m.year = :year
    ORDER BY r.avg_vote DESC NULLS LAST
    LIMIT :limit
"""

STATISTICS_QUERY = """
    WITH movie_stats AS (
        SELECT 
            COUNT(DISTINCT m.imdb_title_id) as total_movies,
            AVG(r.avg_vote) as average_rating,
            MAX(r.avg_vote) as max_rating,
            MIN(r.avg_vote) as min_rating,
            AVG(m.duration) as avg_duration,
            COUNT(DISTINCT m.year) as unique_years,
            MIN(m.year) as oldest_year,
            MAX(m.year) as newest_year,
            SUM(r.votes) as total_votes,
            SUM(r.reviews_from_users) as total_user_reviews,
            SUM(r.reviews_from_critics) as total_critic_reviews
        FROM movie_info m
        LEFT JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    )
    SELECT * FROM movie_stats
"""

TOP_RATED_BY_DECADE_QUERY = """
    WITH decades AS (
        SELECT 
            m.imdb_title_id,
            m.title,
            m.year,
            (m.year / 10) * 10 as decade,
            r.avg_vote,
            r.votes,
            ROW_NUMBER() OVER (PARTITION BY (m.year / 10) * 10 ORDER BY r.avg_vote DESC) as rank
        FROM movie_info m
        INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
        WHERE m.year IS NOT NULL AND r.avg_vote IS NOT NULL
    )
    SELECT 
        decade,
        imdb_title_id,
        title,
        year,
        avg_vote,
        votes
    FROM decades
    WHERE rank <= :top_n
    ORDER BY decade DESC, rank
"""

MOST_REVIEWED_MOVIES_QUERY = """
    SELECT 
        m.imdb_title_id,
        m.title,
        m.year,
        r.avg_vote,
        r.votes,
        r.reviews_from_users,
        r.reviews_from_critics,
        (r.reviews_from_users + r.reviews_from_critics) as total_reviews
    FROM movie_info m
    INNER JOIN rating_info r ON m.imdb_title_id = r.imdb_title_id
    ORDER BY total_reviews DESC
    LIMIT :limit
"""

PRODUCTION_COMPANY_STATS_QUERY = """
    SELECT 
        p.production_company,
        COUNT(*) as movie_count,
        AVG(r.avg_vote) as avg_rating,
        SUM(r.votes) as total_votes
    FROM production_info p
    INNER JOIN rating_info r ON p.imdb_title_id = r.imdb_title_id
    WHERE p.production_company IS NOT NULL 
        AND p.production_company != 'Unknown'
    GROUP BY p.production_company
    HAVING COUNT(*) >= :min_movies
    ORDER BY avg_rating DESC, movie_count DESC
    LIMIT :limit
"""