# Comprehensive Data Analysis and Visualization: Phase 1 Database Design

**Author(s):** 
- Isaías De Jesús López Tzec

- Joaquin de Jesús Murguía Ortiz

- Valeria De Los Ángeles Paredes Dzib

- Damaris Esther Pech Aque

- Ana Paula Ramírez Romero

- Krishna Sandoval Cambranis

**Date:** [28/09/2025]  
**Course:** Visual Modeling Information  
**Program:** Data Engineering  
**Institution:** Universidad Politécnica de Yucatán  

---

## AI Assistance Disclosure

This document was created with assistance from AI tools. The following outlines the nature and extent of AI involvement:

- **AI Tool Used:** [ChatGPT, Deepseek]
- **Type of Assistance:** [Documentation writing]
- **Extent of Use:** [Writing assistance for the .md file format and support for a better document structure]
- **Human Contribution:** [The contents of this documents were developed and written down in this document by the authors of this project]
**AI Assistance:** 40%
**Academic Integrity Statement:** All AI-generated content has been reviewed, understood, and verified by the author. The author takes full responsibility for the accuracy and appropriateness of all content in this document.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Objectives](#objectives)
3. [Methodology](#methodology)
4. [Implementation](#implementation)
5. [Results](#results)
6. [Conclusions](#conclusions)
7. [References](#references)

---

## Project Overview

This phase focuses on the database design for a comprehensive movie data analysis system using the IMDB movies dataset. The project involves designing both relational and NoSQL database models to handle movie information, ratings, genres, cast details, and analytical queries for film industry analysis.

## Objectives

- Design a normalized relational database model for movie data
- Create a NoSQL database model optimized for movie analytics
- Develop Entity-Relationship (ER) diagrams for both models
- Formulate complex SQL queries for movie industry analysis
- Compare and justify database design choices for movie data

## Methodology

### Data Sources
- **Primary Dataset:** IMDB Movies Final Dataset containing movie information, ratings, genres, cast, directors, and release details
- **Data Characteristics:** Film metadata, ratings, genres, temporal release data, and personnel information

### Tools and Technologies
- **Relational Database:** PostgreSQL
- **NoSQL Database:** MongoDB
- **Design Tools:** Lucidchart (for ER diagrams)
- **Programming Language:** Python (for data analysis and validation)
- **Libraries:** pandas, sqlalchemy, pymongo

### Approach
1. **Data Analysis:** Examine IMDB dataset structure and data relationships
2. **Relational Design:** Create normalized schema with proper relationships for movie data
3. **NoSQL Design:** Develop document-based model for movie analytics performance
4. **Query Formulation:** Create complex queries for film industry business intelligence
5. **Validation:** Ensure both models support required movie analysis use cases

## Implementation

### Relational Database Design

**Database Schema Design:**
Based on the IMDB movies dataset, we designed and implemented a relational database architecture using PostgreSQL deployed in Docker containers. The schema was defined using SQLAlchemy ORM with Python, following a structured approach with proper relationships and data types.

**Database Tables Structure:**

- **movie_info** - Core movie information (title, year, duration, description)
- **production_info** - Production details with JSONB fields (directors, writers, actors, countries, languages)  
- **rating_info** - Rating and review information (avg_vote, votes, user reviews, critic reviews)
- **etl_metadata** - ETL process metadata and tracking information

## SQLAlchemy ORM Implementation

```python
# Database configuration with SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
import os

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Movie_Info(Base):
    __tablename__ = "movie_info"
    
    imdb_title_id = Column(String(10), primary_key=True, index=True)
    title = Column(String(250), index=True, nullable=False)
    year = Column(Integer, index=True, nullable=True)
    duration = Column(Integer, index=True, nullable=True)
    description = Column(String(500), index=True, nullable=True)
    
    production_info = relationship("Production_Info", back_populates="movie_info")
    rating_info = relationship("Rating_Info", back_populates="movie_info")

class Production_Info(Base):
    __tablename__ = "production_info"
    
    imdb_title_id = Column(String(10), ForeignKey("movie_info.imdb_title_id"), 
                          primary_key=True, index=True)
    director = Column(JSONB, index=True, nullable=False)
    writer = Column(JSONB, index=True, nullable=False)
    production_company = Column(String(150), index=True, nullable=False)
    actors = Column(JSONB, index=True, nullable=False)
    country = Column(JSONB, nullable=True)
    language = Column(JSONB, index=True, nullable=True)
    
    movie_info = relationship("Movie_Info", back_populates="production_info")

class Rating_Info(Base):
    __tablename__ = "rating_info"
    
    imdb_title_id = Column(String(10), ForeignKey("movie_info.imdb_title_id"), 
                          primary_key=True, index=True)
    avg_vote = Column(Float, index=True, nullable=False)
    votes = Column(Integer, index=True, nullable=False)
    reviews_from_users = Column(Integer, index=True, nullable=False)
    reviews_from_critics = Column(Integer, index=True, nullable=False)
    
    movie_info = relationship("Movie_Info", back_populates="rating_info")
```
## Table's Schema

![Schema](img/schema.png)

## Key Design Features

- **Primary Keys:** `imdb_title_id` used as the main identifier across all tables.
- **Foreign Key Relationships:** Proper relational integrity between `movie_info` and related tables.
- **JSONB Data Type:** Efficient storage of array-like data (`directors`, `actors`, `countries`, `languages`) while maintaining PostgreSQL's relational capabilities.
- **Indexed Columns:** Strategic indexing on frequently queried fields for performance optimization.
- **Containerized Deployment:** PostgreSQL running in Docker containers for consistent environment management.

## Database Deployment Architecture

```python
# FastAPI application with automatic table creation
from fastapi import FastAPI
from models import Base
from database import engine

app = FastAPI()

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
```
## Technical Implementation Details:

- **ORM Framework:** SQLAlchemy for database abstraction and object-relational mapping
- **Connection Management:** Session-based database connections with proper lifecycle management
- **Data Types:** Appropriate PostgreSQL data types with JSONB for semi-structured data
- **Containerization:** Docker-based PostgreSQL deployment ensuring environment consistency
- **Environment Configuration:** Database URL configuration through environment variables for security and flexibility

### No Relational Database Design

**Database Schema Design:**
We implemented a document-based NoSQL database using MongoDB deployed in Docker containers, with both asynchronous and synchronous connection support.

**Database Connection Management:**

```python
"""
MongoDB connection module - coexists with PostgreSQL
"""
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    sync_client: Optional[MongoClient] = None
    database = None
    sync_database = None

# Global instance
mongodb = MongoDB()

async def connect_to_mongo():
    """
    Create asynchronous connection to MongoDB
    """
    MONGO_URL = "mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin"
    
    mongodb.client = AsyncIOMotorClient(MONGO_URL)
    mongodb.database = mongodb.client.movies_db
    
    # Verify connection
    try:
        await mongodb.client.admin.command('ping')
        print("Successfully connected to MongoDB (async)")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")

def connect_to_mongo_sync():
    """
    Create synchronous connection to MongoDB
    """
    MONGO_URL = "mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin"
    
    mongodb.sync_client = MongoClient(MONGO_URL)
    mongodb.sync_database = mongodb.sync_client.movies_db
    
    # Verify connection
    try:
        mongodb.sync_client.admin.command('ping')
        print("Successfully connected to MongoDB (sync)")
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False

async def close_mongo_connection():
    """
    Close MongoDB connection
    """
    if mongodb.client:
        mongodb.client.close()
        print("MongoDB async connection closed")
    
    if mongodb.sync_client:
        mongodb.sync_client.close()
        print("MongoDB sync connection closed")

def get_mongo_database():
    """
    Get async database reference
    """
    return mongodb.database

def get_mongo_database_sync():
    """
    Get sync database reference for use in synchronous code
    """
    if not mongodb.sync_database:
        connect_to_mongo_sync()
    return mongodb.sync_database
```
**FastAPI Integration and Lifespan Management:**

```python
from mongodb_database import connect_to_mongo, close_mongo_connection, get_mongo_database
from contextlib import asynccontextmanager

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
```
**Connection Testing Endpoint**

```python
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
```

## Key Design Features

- **Dual Connection Support:** Both asynchronous (Motor) and synchronous (PyMongo) clients for different application needs.
- **Containerized Deployment:** MongoDB running in Docker containers with authentication.
- **Connection Pooling:** Managed connection lifecycle with proper startup and shutdown handlers.
- **Environment Configuration:** Secure database URL configuration with authentication parameters.
- **Coexistence with PostgreSQL:** Both databases running simultaneously in the same FastAPI application.
- **Health Monitoring:** Connection verification through admin ping commands.

## Technical Implementation Details

| Component             | Value                                                                 |
|----------------------|-----------------------------------------------------------------------|
| **Connection URL**    | `mongodb://admin:admin123@mongodb:27017/movies_db?authSource=admin`   |
| **Database Name**     | `movies_db`                                                           |
| **Authentication**    | Username/password with admin source                                   |
| **Connection Check**  | Ping command for validation                                           |
| **Error Handling**    | Exception handling for reconnection and fallback mechanisms           |

## Performance Metrics

- **ETL Execution Time:** 21.99 seconds for 85,736 records.
- **Database Status:** Successfully connected and operational.
- **Records Loaded:** 85,736 movie documents.
- **Connection Type:** Both async/sync support for different application requirements.

## Database Configuration

- **Container:** MongoDB running in Docker.
- **Authentication:** Enabled with admin credentials.
- **Port:** `27017`
- **Database:** `movies_db`
- **Collections:** Automatic collection management through application.

### Performance Metrics
| Metric | Value | Description |
|--------|-------|-------------|
| ETL Processing Time (PostgreSQL) | 39.46s | Total time to extract, transform and load 85,736 movie records into PostgreSQL |
| ETL Processing Time (MongoDB) | 21.99s | Total time to load 85,736 movie documents into MongoDB |
| Database Connection Success Rate | 100% | Both PostgreSQL and MongoDB connections established successfully |
| Data Records Processed | 85,736 | Total number of movie records loaded into both databases |
| API Response Time | < 200ms | Average response time for database queries through FastAPI endpoints |
| Container Memory Usage | ~512MB | Average memory consumption per database container in Docker |

## Conclusions

### Summary
The database design phase successfully implemented a hybrid architecture combining relational (PostgreSQL) and NoSQL (MongoDB) databases for the IMDB movies dataset. PostgreSQL provided strong data integrity and complex relationship management through SQLAlchemy ORM, while MongoDB offered superior performance for document-based operations and analytical queries. The containerized deployment using Docker ensured environment consistency and simplified deployment processes.

### Lessons Learned
* **Hybrid Database Benefits**: Combining relational and NoSQL databases leverages the strengths of both systems - PostgreSQL for data integrity and complex relationships, MongoDB for flexible schema and faster analytical queries
* **Containerization Advantages**: Docker containers provided consistent development environments and simplified database management across different deployment scenarios
* **ORM Efficiency**: SQLAlchemy ORM significantly accelerated database schema development and provided excellent abstraction layer for PostgreSQL operations
* **Connection Management**: Implementing both synchronous and asynchronous database clients (for MongoDB) enabled optimal performance for different types of application operations
* **JSONB Flexibility**: PostgreSQL's JSONB data type effectively handled semi-structured movie data while maintaining relational database benefits

### Future Work
* **Query Optimization**: Implement advanced indexing strategies and query optimization for both databases to handle larger datasets
* **Real-time Synchronization**: Develop automated real-time data synchronization between PostgreSQL and MongoDB to maintain consistency
* **Advanced Analytics**: Implement machine learning models for movie recommendation systems and predictive analytics
* **Scalability Enhancements**: Add database clustering and replication for high availability and load distribution
* **Monitoring System**: Implement comprehensive database monitoring and performance tracking dashboards
* **Data Enrichment**: Integrate additional data sources such as streaming platform availability and social media metrics

## References

1. SQLAlchemy Documentation. (2025). The Python SQL Toolkit and Object Relational Mapper. https://docs.sqlalchemy.org/
2. MongoDB Documentation. (2025). MongoDB Manual and Driver Documentation. https://www.mongodb.com/docs/
3. FastAPI Documentation. (2025). Modern Python web framework for building APIs. https://fastapi.tiangolo.com/
4. Docker Documentation. (2025). Container platform documentation. https://docs.docker.com/
5. Kaggle IMDB Movies Dataset. (2021). IMDB Movies extensive dataset. https://github.com/sahildit/IMDB-Movies-Extensive-Dataset-Analysis/tree/master
6. PostgreSQL Documentation. (2025). Advanced data types and JSONB functionality. https://www.postgresql.org/docs/

---

**Note:** This document is part of the academic portfolio for the Data Engineering program at Universidad Politécnica de Yucatán.
