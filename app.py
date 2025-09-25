from fastapi import FastAPI, BackgroundTasks, Depends, Query
from sqlalchemy.orm import Session
from models import Base, Movie_Info, Production_Info, Rating_Info, EtlMetadata
from database import engine, get_db
from scripts.etl import run_etl
from scripts.services.movie_service import MovieService
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

@app.get('/movies')
def get_movies(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    movies = db.query(Movie_Info).offset(skip).limit(limit).all()
    return movies

# NUEVOS ENDPOINTS USANDO EL SERVICIO

@app.get("/api/movies/top-rated")
def get_top_rated_movies(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Obtiene las películas mejor calificadas con toda su información"""
    service = MovieService(db)
    return service.get_top_movies_by_rating(limit)

@app.get("/api/movies/by-year/{start_year}/{end_year}")
def get_movies_by_year_range(
    start_year: int,
    end_year: int,
    db: Session = Depends(get_db)
):
    """Obtiene películas dentro de un rango de años"""
    service = MovieService(db)
    return service.get_movies_by_year_range(start_year, end_year)

@app.get("/api/movies/statistics")
def get_movie_statistics(db: Session = Depends(get_db)):
    """Obtiene estadísticas completas de la base de datos"""
    service = MovieService(db)
    return service.get_movie_statistics()

@app.get("/api/movies/search")
def search_movies(
    title: Optional[str] = Query(None, description="Título de la película"),
    min_year: Optional[int] = Query(None, description="Año mínimo"),
    max_year: Optional[int] = Query(None, description="Año máximo"),
    min_rating: Optional[float] = Query(None, ge=0, le=10, description="Rating mínimo"),
    db: Session = Depends(get_db)
):
    """Búsqueda avanzada de películas con múltiples filtros"""
    service = MovieService(db)
    return service.search_movies_advanced(
        title=title,
        min_year=min_year,
        max_year=max_year,
        min_rating=min_rating
    )








