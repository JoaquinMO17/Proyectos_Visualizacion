#services 
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any
import json

class VisualizationService:
    def _init_(self, db):
        self.db = db

    # ================================
    # 1) Top 10 Production Companies
    # ================================
    def get_top_production_companies(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = text("""
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
            LIMIT :limit
        """)
        result = self.db.execute(query, {"limit": limit})
        return [
            {
                "production_company": row["production_company"],
                "movie_count": row["movie_count"],
                "avg_rating": float(row["avg_rating"]) if row["avg_rating"] else None,
                "total_votes": row["total_votes"]
            }
            for row in result
        ]

    # ========================================
    # 2) Collaborations Network (Directors, Writers, Actors)
    # ========================================
    def get_collaborations_network(self, max_edges: int = 100) -> Dict[str, List[Dict[str, Any]]]:
        # Nodos
        nodes_query = text("""
            SELECT DISTINCT 
                person,
                role
            FROM (
                SELECT jsonb_array_elements_text(p.director) AS person, 'Director' AS role FROM production_info p
                UNION ALL
                SELECT jsonb_array_elements_text(p.writer) AS person, 'Writer' AS role FROM production_info p
                UNION ALL
                SELECT jsonb_array_elements_text(p.actors) AS person, 'Actor' AS role FROM production_info p
            ) roles
            WHERE person IS NOT NULL
        """)
        nodes_result = self.db.execute(nodes_query).fetchall()
        nodes = [{"id": row["person"], "role": row["role"]} for row in nodes_result]

        # Aristas / conexiones
        edges_query = text("""
            WITH expanded AS (
                SELECT p.imdb_title_id, jsonb_array_elements_text(p.director) AS person FROM production_info p
                UNION ALL
                SELECT p.imdb_title_id, jsonb_array_elements_text(p.writer) AS person FROM production_info p
                UNION ALL
                SELECT p.imdb_title_id, jsonb_array_elements_text(p.actors) AS person FROM production_info p
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
            SELECT source, target, COUNT(*) AS collaborations
            FROM pairs
            GROUP BY source, target
            ORDER BY collaborations DESC
            LIMIT :max_edges
        """)
        edges_result = self.db.execute(edges_query, {"max_edges": max_edges}).fetchall()
        edges = [
            {"source": row["source"], "target": row["target"], "collaborations": row["collaborations"]}
            for row in edges_result
        ]

        return {"nodes": nodes, "edges": edges}