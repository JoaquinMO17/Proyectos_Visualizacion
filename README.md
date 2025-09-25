# Movie Database API - PostgreSQL + MongoDB

Sistema de análisis y gestión de películas con arquitectura dual de bases de datos: **PostgreSQL** para datos relacionales estructurados y **MongoDB** para almacenamiento de documentos JSON.

---

## Arquitectura del Sistema

- **PostgreSQL**: Base de datos relacional para datos estructurados con integridad referencial
- **MongoDB**: Base de datos NoSQL para documentos JSON flexibles
- **FastAPI**: Framework web moderno para crear APIs RESTful
- **Docker**: Orquestación de servicios mediante contenedores

---

## Prerequisites

Antes de comenzar, asegúrese de tener instalado:

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Visual Studio Code](https://code.visualstudio.com/) *(recomendado)*
- Git

---

## Installation & Setup

### Step 1: Clone the repository
```bash
git clone https://github.com/JoaquinMO17/Proyectos_Visualizacion
cd Proyectos_Visualizacion
```

### Step 2: Configure environment variables
```bash
# Crear archivo .env con las siguientes variables:
POSTGRES_USER=postgres
POSTGRES_PASSWORD=AbraKada99$
POSTGRES_DB=videoanalysisdb
POSTGRES_HOST=db
POSTGRES_PORT=5432
DATABASE_URL=postgresql://postgres:AbraKada99$@db:5432/videoanalysisdb
```

### Step 3: Start Docker containers
```bash
# Asegurarse de que Docker Desktop esté ejecutándose
docker compose up --build
```

### Step 4: Verify services are running
```bash
docker ps
# Deberías ver 3 contenedores:
# - fastapi-app (aplicación)
# - fastapi-db (PostgreSQL)
# - fastapi-mongodb (MongoDB)
```

---

## API Endpoints Documentation

### Base URL
```
http://localhost:8000
```

### 📊 General Information

#### `GET /`
**Descripción**: Muestra información general de la API y todos los endpoints disponibles  
**Respuesta**: JSON con estructura de endpoints organizados por base de datos

**Ejemplo**:
```bash
curl http://localhost:8000/
```

#### `GET /docs`
**Descripción**: Documentación interactiva de la API (Swagger UI)  
**Acceso**: Abrir en navegador `http://localhost:8000/docs`

---

### 🐘 PostgreSQL Endpoints

#### `GET /test-db`
**Descripción**: Verifica la conexión con PostgreSQL y muestra conteos de tablas  
**Respuesta**: Estado de conexión y cantidad de registros por tabla

**Ejemplo**:
```bash
curl http://localhost:8000/test-db
```

#### `POST /run-etl`
**Descripción**: Ejecuta el proceso ETL completo (Extract, Transform, Load)  
**Función**: Extrae datos del CSV, los transforma y los carga en PostgreSQL  
**Modo**: Síncrono (espera hasta completar)

**Ejemplo**:
```bash
curl -X POST http://localhost:8000/run-etl
```

#### `POST /run-etl-async`
**Descripción**: Ejecuta el proceso ETL en segundo plano  
**Función**: Mismo que `/run-etl` pero no bloquea la respuesta  
**Modo**: Asíncrono (retorna inmediatamente)

**Ejemplo**:
```bash
curl -X POST http://localhost:8000/run-etl-async
```

#### `GET /movies`
**Descripción**: Lista películas con paginación  
**Parámetros**:
- `skip` (int): Número de registros a saltar (default: 0)
- `limit` (int): Cantidad de registros a retornar (default: 10)

**Ejemplo**:
```bash
# Obtener primeras 10 películas
curl http://localhost:8000/movies

# Obtener 20 películas saltando las primeras 10
curl "http://localhost:8000/movies?skip=10&limit=20"
```

#### `GET /api/movies/top-rated`
**Descripción**: Obtiene las películas mejor calificadas  
**Parámetros**:
- `limit` (int): Cantidad de películas (1-100, default: 10)

**Ejemplo**:
```bash
# Top 10 películas
curl http://localhost:8000/api/movies/top-rated

# Top 25 películas
curl "http://localhost:8000/api/movies/top-rated?limit=25"
```

#### `GET /api/movies/by-year/{start_year}/{end_year}`
**Descripción**: Busca películas en un rango de años  
**Parámetros de ruta**:
- `start_year` (int): Año inicial
- `end_year` (int): Año final

**Ejemplo**:
```bash
# Películas entre 2010 y 2020
curl http://localhost:8000/api/movies/by-year/2010/2020
```

#### `GET /api/movies/statistics`
**Descripción**: Obtiene estadísticas completas de la base de datos  
**Respuesta**: Totales, promedios, distribuciones por año, país, etc.

**Ejemplo**:
```bash
curl http://localhost:8000/api/movies/statistics
```

#### `GET /api/movies/search`
**Descripción**: Búsqueda avanzada con múltiples filtros  
**Parámetros**:
- `title` (str): Título de película (búsqueda parcial)
- `min_year` (int): Año mínimo
- `max_year` (int): Año máximo
- `min_rating` (float): Rating mínimo (0-10)

**Ejemplo**:
```bash
# Buscar por título
curl "http://localhost:8000/api/movies/search?title=Matrix"

# Búsqueda combinada
curl "http://localhost:8000/api/movies/search?min_year=2000&max_year=2010&min_rating=7.5"
```

---

### 🍃 MongoDB Endpoints

#### `GET /test-mongodb`
**Descripción**: Verifica conexión con MongoDB  
**Respuesta**: Estado, colecciones disponibles y conteo de documentos

**Ejemplo**:
```bash
curl http://localhost:8000/test-mongodb
```

#### `GET /mongo/movies`
**Descripción**: Lista películas desde MongoDB con ordenamiento  
**Parámetros**:
- `skip` (int): Documentos a saltar (default: 0)
- `limit` (int): Límite de documentos (1-100, default: 10)
- `sort_by` (str): Campo para ordenar (default: "year")
- `order` (str): "asc" o "desc" (default: "desc")

**Ejemplo**:
```bash
# Últimas películas por año
curl http://localhost:8000/mongo/movies

# Ordenar por rating descendente
curl "http://localhost:8000/mongo/movies?sort_by=avg_vote&order=desc&limit=20"
```

#### `GET /mongo/search`
**Descripción**: Búsqueda flexible en MongoDB  
**Parámetros**:
- `q` (str): Búsqueda general (título, descripción, director)
- `title` (str): Filtro por título
- `director` (str): Filtro por director
- `country` (str): Filtro por país
- `min_year` (int): Año mínimo
- `max_year` (int): Año máximo
- `min_rating` (float): Rating mínimo
- `limit` (int): Límite de resultados

**Ejemplo**:
```bash
# Búsqueda general
curl "http://localhost:8000/mongo/search?q=spielberg"

# Búsqueda específica
curl "http://localhost:8000/mongo/search?director=Nolan&min_year=2010&min_rating=8"
```

#### `GET /mongo/stats`
**Descripción**: Estadísticas agregadas desde MongoDB  
**Respuesta**:
- Total de películas
- Rating promedio
- Distribución por año
- Top 10 películas mejor calificadas

**Ejemplo**:
```bash
curl http://localhost:8000/mongo/stats
```

#### `GET /mongo/aggregations`
**Descripción**: Agregaciones complejas de MongoDB  
**Respuesta**:
- Top directores por cantidad de películas
- Distribución de películas por duración
- Promedios de rating por director

**Ejemplo**:
```bash
curl http://localhost:8000/mongo/aggregations
```

#### `POST /mongo/sync`
**Descripción**: Sincroniza todos los datos de PostgreSQL a MongoDB  
**Función**: Copia completa de datos relacionales a formato documento

**Ejemplo**:
```bash
curl -X POST http://localhost:8000/mongo/sync
```

---

## Database Access

### PostgreSQL Access
```bash
# Acceder a PostgreSQL
docker exec -it fastapi-db psql -U postgres -d videoanalysisdb

# Comandos útiles en PostgreSQL:
\dt              # Listar tablas
\d movie_info    # Describir tabla
SELECT COUNT(*) FROM movie_info;  # Contar registros
```

### MongoDB Access
```bash
# Acceder a MongoDB
docker exec -it fastapi-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin

# Comandos útiles en MongoDB:
use movies_db
show collections
db.movies.countDocuments()
db.movies.findOne()
db.movies.find({year: 2020}).limit(5)
```

---

## Data Flow

### ETL Pipeline Process
1. **Extract**: Lee archivo CSV (`data/imdb_movies_final.csv`)
2. **Transform**: 
   - Limpia y valida datos
   - Convierte CSV ↔ JSON
   - Normaliza para PostgreSQL
3. **Load**:
   - PostgreSQL: Carga incremental con tracking
   - MongoDB: Carga desde JSON generado

### Workflow típico
```bash
# 1. Ejecutar ETL para cargar datos en PostgreSQL
curl -X POST http://localhost:8000/run-etl

# 2. Sincronizar a MongoDB
curl -X POST http://localhost:8000/mongo/sync

# 3. Verificar datos
curl http://localhost:8000/test-db        # PostgreSQL
curl http://localhost:8000/test-mongodb   # MongoDB

# 4. Consultar datos
curl http://localhost:8000/api/movies/top-rated
curl http://localhost:8000/mongo/stats
```

---

## Project Structure
```
.
├── app.py                    # API principal FastAPI
├── database.py               # Conexión PostgreSQL
├── mongodb_database.py       # Conexión MongoDB
├── models.py                 # Modelos SQLAlchemy
├── requirements.txt          # Dependencias Python
├── docker-compose.yml        # Configuración Docker
├── Dockerfile               # Imagen Docker
├── .env                     # Variables de entorno
├── data/
│   ├── imdb_movies_final.csv    # Datos origen
│   ├── raw/                      # JSONs generados
│   └── processed/                # CSVs procesados
└── scripts/
    ├── etl.py               # Pipeline ETL principal
    ├── extract.py           # Extracción de datos
    ├── transform.py         # Transformación
    ├── validate.py          # Validación
    ├── load.py              # Carga PostgreSQL
    ├── init-mongo.js        # Script inicialización MongoDB
    └── services/
        └── movie_service.py  # Servicio PostgreSQL
```

---

## Common Commands

### Docker Management
```bash
# Iniciar servicios
docker compose up -d

# Ver logs
docker compose logs -f

# Detener servicios
docker compose down

# Limpiar todo (incluye volúmenes)
docker compose down -v

# Reconstruir contenedores
docker compose up --build
```

### Testing Endpoints
```bash
# Usar curl para pruebas rápidas
curl http://localhost:8000/endpoint

# O usar herramientas como:
# - Postman
# - Thunder Client (VSCode extension)
# - HTTPie: http localhost:8000/endpoint
```

---

## Troubleshooting

### Port already in use
**Problema**: Puerto 5432 o 27017 ya está en uso  
**Solución**: Detener servicios locales o cambiar puertos en `docker-compose.yml`

### MongoDB empty after start
**Problema**: MongoDB no tiene datos  
**Solución**: 
1. Ejecutar ETL primero: `curl -X POST http://localhost:8000/run-etl`
2. Sincronizar a MongoDB: `curl -X POST http://localhost:8000/mongo/sync`

### Connection refused
**Problema**: No se puede conectar a la API  
**Solución**: Verificar que todos los contenedores estén ejecutándose con `docker ps`

### ETL fails
**Problema**: El proceso ETL falla  
**Solución**: Verificar que el archivo CSV esté en `data/imdb_movies_final.csv`

---

## Authors
- Isaías De Jesús López Tzec [@Iasisa](https://github.com/Iasisa)
- Joaquin de Jesús Murguía Ortiz [@JoaquinMO17](https://github.com/JoaquinMO17)
- Valeria De Los Ángeles Paredes Dzib [@valinyourarea](https://github.com/valinyourarea)
- Damaris Esther Pech Aque [@damapech1](https://github.com/damapech1)
- Ana Paula Ramírez Romero [@AnaPauR](https://github.com/AnaPauR)
- Krishna Sandoval Cambranis [@Playmaker3334](https://github.com/Playmaker3334)