from fastapi import FastAPI
from models import Base
from database import engine

app = FastAPI()

# Crear las tablas al iniciar
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


