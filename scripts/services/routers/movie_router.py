from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from scripts.services.themes_service import ThemesService
from scripts.services.movie_service import MoviesService

router = APIRouter(prefix="/api/movies", tags=["Movies"])

@router.get("/themes-by-decade")
def themes_by_decade(db: Session = Depends(get_db)):
    service = ThemesService(db)
    return service.get_themes_by_decade()

@router.get("/distribution-by-year")
def distribution_by_year(start_year: int = None, end_year: int = None, group_by: str = "year", db: Session = Depends(get_db)):
    service = MoviesService(db)
    return service.get_distribution_by_year(start_year=start_year, end_year=end_year, group_by=group_by)
