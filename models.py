from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, func, DateTime
from database import Base
from typing import Optional
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime


class Movie_Info(Base):
    __tablename__ = "movie_info"
    __table_args__ = {'extend_existing': True}

    imdb_title_id = Column(String(10), primary_key=True, index=True)
    title = Column(String(250), index=True, nullable=False)
    year = Column(Integer, index=True, nullable=True)
    duration = Column(Integer, index=True, nullable=True)
    description = Column(String(500), index=True, nullable=True)

    # Relationships
    production_info = relationship("Production_Info", back_populates="movie_info")
    rating_info = relationship("Rating_Info", back_populates="movie_info")

class Production_Info(Base):
    __tablename__ = "production_info"
    __table_args__ = {'extend_existing': True}

    imdb_title_id = Column(String(10), ForeignKey("movie_info.imdb_title_id"), primary_key=True, index=True)
    director = Column(JSONB,  index=True, nullable=False)
    writer = Column(JSONB, index=True, nullable=False)
    production_company = Column(String(150), index=True, nullable=False)
    actors = Column(JSONB, index=True, nullable=False)
    country = Column(JSONB, nullable=True)
    language = Column(JSONB, index=True, nullable=True)

    # Relationship
    movie_info = relationship("Movie_Info", back_populates="production_info")
    

class Rating_Info(Base):
    __tablename__ = "rating_info"
    __table_args__ = {'extend_existing': True}

    imdb_title_id = Column(String(10), ForeignKey("movie_info.imdb_title_id"), primary_key=True, index=True)
    avg_vote = Column(Float, index=True, nullable=False)
    votes = Column(Integer, index=True, nullable=False)
    reviews_from_users = Column(Integer, index=True, nullable=False)
    reviews_from_critics = Column(Integer, index=True, nullable=False)
    
    

    # Relationship
    movie_info = relationship("Movie_Info", back_populates="rating_info")

class EtlMetadata(Base):
    __tablename__ = "etl_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, unique=True, nullable=False) 
    value = Column(String, nullable=False)             
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
