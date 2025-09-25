from fastapi import FastAPI, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session
from models import Base, Movie_Info, Production_Info, Rating_Info, EtlMetadata
from database import engine, get_db, SessionLocal
from scripts.etl import run_etl
from scripts.services.movie_service import MovieService
from scripts.services.production_service import VisualizationService
from scripts.services.rating_service import RatingService
from typing import Optional

app = FastAPI(title="Movie Database API", version="2.0.0")

# Crear las tablas al iniciar
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

@app.get('/')
def root():
    return {
        "message": "API is running",
        "endpoints": {
            "test_db": "/test-db",
            "run_etl": "POST /run-etl",
            "run_etl_async": "POST /run-etl-async",
            "movies": "/movies",
            "top_movies": "/api/movies/top-rated",
            "movies_by_year": "/api/movies/by-year/{start_year}/{end_year}",
            "statistics": "/api/movies/statistics",
            "search": "/api/movies/search",
            "documentation": "/docs"
        }
    }

# Testing database connection and querying some data
@app.get('/test-db')
def test_database(db: Session = Depends(get_db)):
    try:
        return {
            'connected': True,
            'movie_info': db.query(Movie_Info).count(),
            'production_info': db.query(Production_Info).count(),
            'rating_info': db.query(Rating_Info).count(),
            'etl_metadata': db.query(EtlMetadata).count()
        }
    except Exception as e:
        return {'connected': False, 'error': str(e)}

# Data ingestion
@app.post('/run-etl')
def execute_etl():
    try:
        run_etl()
        return {'status': 'success', 'message': 'ETL process completed'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

# Mode asynchronous
@app.post('/run-etl-async')
def execute_etl_async(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(run_etl)
        return {'status': 'success', 'message': 'ETL process started in background'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
    
# ----------------------------
# MOVIES ENDPOINTS
# ----------------------------

@app.get("/api/movies/by-year/{year}")
def get_movies_by_year(year: int, db: Session = Depends(get_db)):
    """Get all movies released in a specific year."""
    service = MovieService(db)
    return service.get_movies_by_year(year=year)

@app.get("/api/movies/longest")
def get_longest_movies(limit: int = Query(default=10, ge=1, le=100), db: Session = Depends(get_db)):
    """Get the longest movies by duration."""
    service = MovieService(db)
    return service.get_longest_movies(limit=limit)

@app.get("/api/movies/with-keyword/{keyword}")
def get_movies_with_keyword(keyword: str, db: Session = Depends(get_db)):
    """Get all movies that contain a specific keyword in the description."""
    service = MovieService(db)
    return service.get_movies_with_keyword(keyword=keyword)


# ----------------------------
# PRODUCTION ENDPOINTS
# ----------------------------

@app.get("/api/top-production-companies")
def top_production_companies(limit: int = 10):
    """Get the top production companies by number of movies."""
    db: Session = SessionLocal()
    service = VisualizationService(db)
    try:
        return service.get_top_production_companies(limit=limit)
    finally:
        db.close()

@app.get("/api/collaborations")
def collaborations(max_edges: int = 100):
    """Get the collaborations network between production companies and movies."""
    db: Session = SessionLocal()
    service = VisualizationService(db)
    try:
        return service.get_collaborations_network(max_edges=max_edges)
    finally:
        db.close()


# ----------------------------
# RATINGS ENDPOINTS
# ----------------------------

@app.get("/api/ratings")
def get_ratings(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    """Get all ratings with pagination."""
    service = RatingService(db)
    return service.get_all_ratings(skip, limit)

@app.get("/api/ratings/top-voted")
def get_top_voted_movies(
    limit: int = Query(default=10, ge=1, le=100, description="Number of movies to return"),
    db: Session = Depends(get_db)
):
    """Get the most voted movies (popularity)."""
    service = RatingService(db)
    return service.get_top_voted_movies(limit=limit)

@app.get("/api/ratings/top-rated")
def get_top_rated_movies(
    limit: int = Query(default=10, ge=1, le=100, description="Number of movies to return"),
    db: Session = Depends(get_db)
):
    """Get the top-rated movies (acclaim, only with more than 1000 votes)."""
    service = RatingService(db)
    return service.get_top_rated_movies(limit=limit)

@app.get("/api/ratings/trends")
def get_rating_trends_by_year(db: Session = Depends(get_db)):
    """Get rating trends over time (average rating and votes per year)."""
    service = RatingService(db)
    return service.get_rating_trends_by_year()
