# Queries SQL separadas para mejor mantenibilidad

TOP_WORDS_BY_DECADE_QUERY = """
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
"""

MOVIE_COUNT_BY_YEAR_QUERY = """
    SELECT 
        year,
        COUNT(*) AS total_movies
    FROM movie_info
    WHERE year IS NOT NULL
    GROUP BY year
    ORDER BY year
"""
