from fastapi import FastAPI, BackgroundTasks
from sqlalchemy.orm import Session
from models import Base, Movie_Info, Production_Info, Rating_Info, EtlMetadata
from fastapi import Depends
from database import engine, get_db
from scripts.etl import run_etl


app = FastAPI()

# Crear las tablas al iniciar
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

@app.get('/')
def root(): 
    return {'message": "API is running'}

# Testing database connection and querying some data
@app.get('/test-db')
def test_database(db:Session=Depends(get_db)):
    try:
        return{
            'conected': True,
            'movie_info': db.query(Movie_Info).first(),
            'production_info': db.query(Production_Info).count(),
            'rating_info': db.query(Rating_Info).count(),
            'etl_metadata': db.query(EtlMetadata).count(),
        }
    except Exception as e:
        return {'conected': False, 'error': str(e)}
    
# Data ingestion  
@app.post('/run-etl')
def excecute_etl(background_tasks: BackgroundTasks):
    try:
        run_etl()
        return {'status': 'success', 'message': 'ETL process started in background'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

# Mode asynchronous    
@app.post('/run-etl-async')
def excecute_etl_async(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(run_etl)
        return {'status': 'success', 'message': 'ETL process started in background'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
    
@app.get('/movies')
def get_movies(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    movies = db.query(Movie_Info).offset(skip).limit(limit).all()
    return movies
    








