from fastapi import FastAPI, BackgroundTasks, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from models import Base, Movie_Info, Production_Info, Rating_Info, EtlMetadata
from database import engine, get_db
from mongodb_database import connect_to_mongo, close_mongo_connection, get_mongo_database
from scripts.etl import run_etl
from scripts.services.movie_service import MovieService
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from scripts.services.production_service import ProductionService
from scripts.services.rating_service import RatingService

import json

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    await connect_to_mongo()
    yield
    # Shutdown
    await close_mongo_connection()

app = FastAPI(
    title="Movie Database API - PostgreSQL + MongoDB", 
    version="3.0.0",
    lifespan=lifespan
)

# Add CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins like ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def root():
    return {
        "message": "API is running with PostgreSQL and MongoDB",
        "databases": {
            "postgresql": "Primary relational database",
            "mongodb": "Document database for JSON data"
        },
        "endpoints": {
            "postgresql": {
                "test_db": "/test-db",
                "run_etl": "POST /run-etl",
                "run_etl_async": "POST /run-etl-async",
                "movies": "/movies",
                "top_movies": "/api/movies/top-rated",
                "movies_by_year": "/api/movies/by-year/{start_year}/{end_year}",
                "statistics": "/api/movies/statistics",
                "search": "/api/movies/search"
            },
            "mongodb": {
                "test_mongodb": "/test-mongodb",
                "mongo_movies": "/mongo/movies",
                "mongo_search": "/mongo/search",
                "mongo_stats": "/mongo/stats",
                "mongo_aggregations": "/mongo/aggregations",
                "sync_to_mongo": "POST /mongo/sync"
            },
            "documentation": "/docs"
        }
    }

# ============= PostgreSQL Endpoints (Original) =============

@app.get('/test-db')
def test_database(db: Session = Depends(get_db)):
    try:
        return {
            'database': 'PostgreSQL',
            'connected': True,
            'movie_info': db.query(Movie_Info).count(),
            'production_info': db.query(Production_Info).count(),
            'rating_info': db.query(Rating_Info).count(),
            'etl_metadata': db.query(EtlMetadata).count()
        }
    except Exception as e:
        return {'database': 'PostgreSQL', 'connected': False, 'error': str(e)}

@app.post('/run-etl')
def execute_etl():
    try:
        run_etl()
        return {'status': 'success', 'message': 'ETL process completed'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@app.post('/run-etl-async')
def execute_etl_async(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(run_etl)
        return {'status': 'success', 'message': 'ETL process started in background'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@app.get('/movies')
def get_movies(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    movies = db.query(Movie_Info).offset(skip).limit(limit).all()
    return movies

@app.get("/api/movies/top-rated")
def get_top_rated_movies(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Get top rated movies from PostgreSQL"""
    service = MovieService(db)
    return service.get_top_movies_by_rating(limit)

@app.get("/api/movies/by-year/{start_year}/{end_year}")
def get_movies_by_year_range(
    start_year: int,
    end_year: int,
    db: Session = Depends(get_db)
):
    """Get movies by year range from PostgreSQL"""
    service = MovieService(db)
    return service.get_movies_by_year_range(start_year, end_year)

@app.get("/api/movies/statistics")
def get_movie_statistics(db: Session = Depends(get_db)):
    """Get statistics from PostgreSQL"""
    service = MovieService(db)
    return service.get_movie_statistics()

@app.get("/api/movies/search")
def search_movies(
    title: Optional[str] = Query(None, description="Movie title"),
    min_year: Optional[int] = Query(None, description="Minimum year"),
    max_year: Optional[int] = Query(None, description="Maximum year"),
    min_rating: Optional[float] = Query(None, ge=0, le=10, description="Minimum rating"),
    db: Session = Depends(get_db)
):
    """Advanced search in PostgreSQL"""
    service = MovieService(db)
    return service.search_movies_advanced(
        title=title,
        min_year=min_year,
        max_year=max_year,
        min_rating=min_rating
    )

# ============= MongoDB Endpoints (New) =============

@app.get('/test-mongodb')
async def test_mongodb():
    """Test MongoDB connection"""
    try:
        db = get_mongo_database()
        if db is None:
            return {'database': 'MongoDB', 'connected': False, 'error': 'Database not initialized'}
        
        count = await db.movies.count_documents({})
        collections = await db.list_collection_names()
        
        return {
            'database': 'MongoDB',
            'connected': True,
            'collections': collections,
            'movies_count': count
        }
    except Exception as e:
        return {'database': 'MongoDB', 'connected': False, 'error': str(e)}

@app.get('/mongo/movies')
async def get_mongo_movies(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    sort_by: str = Query("year", description="Field to sort by"),
    order: str = Query("desc", description="asc or desc")
):
    """Get movies from MongoDB"""
    db = get_mongo_database()
    
    sort_order = -1 if order == "desc" else 1
    
    cursor = db.movies.find().sort(sort_by, sort_order).skip(skip).limit(limit)
    movies = []
    
    async for movie in cursor:
        movie["_id"] = str(movie["_id"])
        movies.append(movie)
    
    total = await db.movies.count_documents({})
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": movies
    }

@app.get('/mongo/search')
async def search_mongo_movies(
    q: str = Query(None, description="Search query"),
    title: Optional[str] = Query(None, description="Title filter"),
    director: Optional[str] = Query(None, description="Director filter"),
    country: Optional[str] = Query(None, description="Country filter"),
    min_year: Optional[int] = Query(None, description="Minimum year"),
    max_year: Optional[int] = Query(None, description="Maximum year"),
    min_rating: Optional[float] = Query(None, ge=0, le=10),
    limit: int = Query(10, ge=1, le=100)
):
    """Search movies in MongoDB with multiple filters"""
    db = get_mongo_database()
    
    # Build query
    query_filter = {}
    
    if q:
        # General text search
        query_filter["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"description": {"$regex": q, "$options": "i"}},
            {"director": {"$regex": q, "$options": "i"}}
        ]
    
    if title:
        query_filter["title"] = {"$regex": title, "$options": "i"}
    
    if director:
        query_filter["director"] = {"$regex": director, "$options": "i"}
    
    if country:
        query_filter["country"] = {"$regex": country, "$options": "i"}
    
    if min_year or max_year:
        year_filter = {}
        if min_year:
            year_filter["$gte"] = min_year
        if max_year:
            year_filter["$lte"] = max_year
        query_filter["year"] = year_filter
    
    if min_rating:
        query_filter["avg_vote"] = {"$gte": min_rating}
    
    cursor = db.movies.find(query_filter).limit(limit)
    results = []
    
    async for movie in cursor:
        movie["_id"] = str(movie["_id"])
        results.append(movie)
    
    return {
        "query": query_filter,
        "count": len(results),
        "results": results
    }

@app.get('/mongo/stats')
async def get_mongo_stats():
    """Get aggregated statistics from MongoDB"""
    db = get_mongo_database()
    
    # Total movies
    total_movies = await db.movies.count_documents({})
    
    # Movies by year
    year_pipeline = [
        {"$match": {"year": {"$ne": None}}},
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
        {"$limit": 10}
    ]
    
    year_stats = []
    async for doc in db.movies.aggregate(year_pipeline):
        year_stats.append({"year": doc["_id"], "count": doc["count"]})
    
    # Top rated movies
    top_rated_pipeline = [
        {"$match": {"avg_vote": {"$ne": None}}},
        {"$sort": {"avg_vote": -1}},
        {"$limit": 10},
        {"$project": {"title": 1, "year": 1, "avg_vote": 1, "_id": 0}}
    ]
    
    top_rated = []
    async for doc in db.movies.aggregate(top_rated_pipeline):
        top_rated.append(doc)
    
    # Average rating
    avg_rating_pipeline = [
        {"$match": {"avg_vote": {"$ne": None}}},
        {"$group": {"_id": None, "avg_rating": {"$avg": "$avg_vote"}}}
    ]
    
    avg_rating_cursor = db.movies.aggregate(avg_rating_pipeline)
    avg_rating_result = await avg_rating_cursor.next()
    avg_rating = avg_rating_result["avg_rating"] if avg_rating_result else 0
    
    return {
        "total_movies": total_movies,
        "average_rating": round(avg_rating, 2),
        "movies_by_year": year_stats,
        "top_rated_movies": top_rated
    }

@app.get('/mongo/aggregations')
async def get_mongo_aggregations():
    """Complex aggregations from MongoDB"""
    db = get_mongo_database()
    
    # Directors with most movies
    directors_pipeline = [
        {"$match": {"director": {"$ne": None}}},
        {"$group": {"_id": "$director", "movie_count": {"$sum": 1}, "avg_rating": {"$avg": "$avg_vote"}}},
        {"$sort": {"movie_count": -1}},
        {"$limit": 10}
    ]
    
    directors = []
    async for doc in db.movies.aggregate(directors_pipeline):
        director_name = doc["_id"]
        if director_name.startswith('"') or director_name.startswith('['):
            try:
                director_name = json.loads(director_name)
                if isinstance(director_name, list):
                    director_name = ", ".join(director_name)
            except:
                pass
        
        directors.append({
            "director": director_name,
            "movie_count": doc["movie_count"],
            "avg_rating": round(doc["avg_rating"], 2) if doc["avg_rating"] else None
        })
    
    # Movies by duration range
    duration_pipeline = [
        {"$match": {"duration": {"$ne": None}}},
        {"$bucket": {
            "groupBy": "$duration",
            "boundaries": [0, 60, 90, 120, 150, 180, 300],
            "default": "300+",
            "output": {
                "count": {"$sum": 1},
                "titles": {"$push": "$title"}
            }
        }}
    ]
    
    duration_ranges = []
    async for doc in db.movies.aggregate(duration_pipeline):
        range_label = f"{doc['_id']}-{doc['_id']+30} min" if doc['_id'] != "300+" else "300+ min"
        duration_ranges.append({
            "range": range_label,
            "count": doc["count"],
            "sample_titles": doc["titles"][:3]
        })
    
    return {
        "top_directors": directors,
        "movies_by_duration": duration_ranges
    }

@app.post('/mongo/sync')
async def sync_postgres_to_mongo(db: Session = Depends(get_db)):
    """Sync data from PostgreSQL to MongoDB"""
    try:
        mongo_db = get_mongo_database()
        
        # Get all movies from PostgreSQL
        movies = db.query(Movie_Info).all()
        productions = db.query(Production_Info).all()
        ratings = db.query(Rating_Info).all()
        
        # Create lookup dictionaries
        prod_dict = {p.imdb_title_id: p for p in productions}
        rating_dict = {r.imdb_title_id: r for r in ratings}
        
        # Prepare documents for MongoDB
        documents = []
        for movie in movies:
            doc = {
                "imdb_title_id": movie.imdb_title_id,
                "title": movie.title,
                "year": movie.year,
                "duration": movie.duration,
                "description": movie.description
            }
            
            # Add production info
            if movie.imdb_title_id in prod_dict:
                prod = prod_dict[movie.imdb_title_id]
                doc.update({
                    "director": prod.director,
                    "writer": prod.writer,
                    "production_company": prod.production_company,
                    "actors": prod.actors,
                    "country": prod.country,
                    "language": prod.language
                })
            
            # Add rating info
            if movie.imdb_title_id in rating_dict:
                rating = rating_dict[movie.imdb_title_id]
                doc.update({
                    "avg_vote": rating.avg_vote,
                    "votes": rating.votes,
                    "reviews_from_users": rating.reviews_from_users,
                    "reviews_from_critics": rating.reviews_from_critics
                })
            
            documents.append(doc)
        
        # Clear existing data and insert new
        await mongo_db.movies.delete_many({})
        if documents:
            await mongo_db.movies.insert_many(documents)
        
        return {
            "status": "success",
            "message": f"Synced {len(documents)} movies from PostgreSQL to MongoDB"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================
# PRODUCTION TABLE DASHBOARD ENDPOINTS
# =====================================

@app.get("/api/production/companies")
def get_production_companies(
    limit: int = Query(default=10, ge=1, le=100),
    min_movies: int = Query(default=0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get top production companies with statistics.
    Perfect for: Bar charts, company rankings
    """
    service = ProductionService(db)
    return service.get_top_production_companies(limit=limit, min_movies=min_movies)

@app.get("/api/production/countries")
def get_movies_by_country(
    top_n: int = Query(default=20, ge=1, le=100),
    year_from: Optional[int] = Query(None, ge=1900, le=2030),
    year_to: Optional[int] = Query(None, ge=1900, le=2030),
    db: Session = Depends(get_db)
):
    """
    Get movie distribution by country with filtering.
    Perfect for: World maps, country comparisons
    """
    service = ProductionService(db)
    return service.get_movies_by_country(
        top_n=top_n,
        year_from=year_from,
        year_to=year_to
    )

@app.get("/api/production/directors")
def get_top_directors(
    limit: int = Query(default=15, ge=1, le=100),
    min_movies: int = Query(default=2, ge=1),
    sort_by: str = Query(default="movie_count", regex="^(movie_count|avg_rating|total_votes)$"),
    db: Session = Depends(get_db)
):
    """
    Get top directors with their statistics.
    Sort options: movie_count, avg_rating, total_votes
    Perfect for: Director rankings, talent analysis
    """
    service = ProductionService(db)
    return service.get_top_directors(
        limit=limit,
        min_movies=min_movies,
        sort_by=sort_by
    )

@app.get("/api/production/languages")
def get_language_distribution(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Get movie distribution by language.
    Perfect for: Language diversity charts, pie charts
    """
    service = ProductionService(db)
    return service.get_language_distribution(limit=limit)

@app.get("/api/production/actors")
def get_top_actors(
    limit: int = Query(default=20, ge=1, le=100),
    min_movies: int = Query(default=3, ge=1),
    db: Session = Depends(get_db)
):
    """
    Get top actors by appearances and ratings.
    Perfect for: Star power analysis, actor rankings
    """
    service = ProductionService(db)
    return service.get_top_actors(
        limit=limit,
        min_movies=min_movies
    )

@app.get("/api/production/writers")
def get_top_writers(
    limit: int = Query(default=15, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get top writers with their statistics.
    Perfect for: Writer rankings, screenplay quality analysis
    """
    service = ProductionService(db)
    return service.get_top_writers(limit=limit)


# =====================================
# RATING TABLE DASHBOARD ENDPOINTS
# =====================================

@app.get("/api/ratings/distribution")
def get_rating_distribution(
    bins: int = Query(default=10, ge=5, le=20),
    db: Session = Depends(get_db)
):
    """
    Get rating distribution across all movies.
    Perfect for: Histogram charts, rating analysis
    """
    service = RatingService(db)
    return service.get_rating_distribution(bins=bins)

@app.get("/api/ratings/top-rated")
def get_top_rated_movies_endpoint(
    limit: int = Query(default=20, ge=1, le=100),
    min_votes: int = Query(default=1000, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get top rated movies with minimum vote threshold.
    Perfect for: Top movie lists, quality rankings
    """
    service = RatingService(db)
    return service.get_top_rated_movies(limit=limit, min_votes=min_votes)

@app.get("/api/ratings/most-voted")
def get_most_voted_movies_endpoint(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get most voted movies (popularity).
    Perfect for: Popular movie lists, engagement metrics
    """
    service = RatingService(db)
    return service.get_most_voted_movies(limit=limit)

@app.get("/api/ratings/trends")
def get_rating_trends(
    start_year: Optional[int] = Query(None, ge=1900, le=2030),
    end_year: Optional[int] = Query(None, ge=1900, le=2030),
    db: Session = Depends(get_db)
):
    """
    Get rating trends by year.
    Perfect for: Time series charts, trend analysis
    """
    service = RatingService(db)
    return service.get_rating_trends_by_year(start_year=start_year, end_year=end_year)

@app.get("/api/ratings/controversial")
def get_controversial_movies_endpoint(
    limit: int = Query(default=20, ge=1, le=100),
    min_votes: int = Query(default=500, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get movies with biggest gap between user and critic reviews.
    Perfect for: Controversy analysis, critic vs audience comparison
    """
    service = RatingService(db)
    return service.get_controversial_movies(limit=limit, min_votes=min_votes)

@app.get("/api/ratings/duration-analysis")
def get_rating_duration_analysis(db: Session = Depends(get_db)):
    """
    Analyze ratings by movie duration categories.
    Perfect for: Duration impact analysis, optimal length insights
    """
    service = RatingService(db)
    return service.get_rating_vs_duration_analysis()

@app.get("/api/ratings/underrated")
def get_underrated_movies_endpoint(
    limit: int = Query(default=20, ge=1, le=100),
    max_votes: int = Query(default=10000, ge=100),
    min_rating: float = Query(default=7.0, ge=0, le=10),
    db: Session = Depends(get_db)
):
    """
    Get hidden gems - high rated movies with few votes.
    Perfect for: Discovery features, hidden gem recommendations
    """
    service = RatingService(db)
    return service.get_underrated_movies(
        limit=limit,
        max_votes=max_votes,
        min_rating=min_rating
    )

@app.get("/api/ratings/statistics")
def get_rating_statistics_endpoint(db: Session = Depends(get_db)):
    """
    Get comprehensive rating statistics.
    Perfect for: Dashboard KPIs, overall metrics
    """
    service = RatingService(db)
    return service.get_rating_statistics()








