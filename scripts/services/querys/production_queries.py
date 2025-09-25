# ======================================
# 1) TOP 10 PRODUCTORAS DE CINE
# ======================================
TOP_PRODUCTION_COMPANIES_QUERY = """
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
ORDER BY movie_count DESC
LIMIT 10
"""
# ======================================
# 2) COLABORACIONES CLAVE DE LA INDUSTRIA
# ======================================

# 2a) Edges para network graph
COLLABORATIONS_EDGES_QUERY = """
WITH expanded AS (
    SELECT 
        p.imdb_title_id,
        jsonb_array_elements_text(p.director) AS person
    FROM production_info p
    UNION ALL
    SELECT 
        p.imdb_title_id,
        jsonb_array_elements_text(p.writer) AS person
    FROM production_info p
    UNION ALL
    SELECT 
        p.imdb_title_id,
        jsonb_array_elements_text(p.actors) AS person
    FROM production_info p
),
pairs AS (
    SELECT 
        e1.person AS source,
        e2.person AS target,
        e1.imdb_title_id
    FROM expanded e1
    JOIN expanded e2 
        ON e1.imdb_title_id = e2.imdb_title_id
       AND e1.person < e2.person
)
SELECT 
    source,
    target,
    COUNT(*) AS collaborations
FROM pairs
GROUP BY source, target
ORDER BY collaborations DESC
LIMIT 100
"""

# 2b) Nodes para network graph
COLLABORATIONS_NODES_QUERY = """
SELECT DISTINCT 
    person,
    role
FROM (
    SELECT 
        jsonb_array_elements_text(p.director) AS person,
        'Director' AS role
    FROM production_info p
    UNION ALL
    SELECT 
        jsonb_array_elements_text(p.writer) AS person,
        'Writer' AS role
    FROM production_info p
    UNION ALL
    SELECT 
        jsonb_array_elements_text(p.actors) AS person,
        'Actor' AS role
    FROM production_info p
) roles
WHERE person IS NOT NULL
ORDER BY person
"""