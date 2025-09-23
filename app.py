from fastapi import FastAPI
from scripts.models import Base
from scripts.database import engine

app = FastAPI()

# Crear las tablas al iniciar
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


