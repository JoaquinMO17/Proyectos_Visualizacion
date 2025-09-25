# 🚀 Guía de Inicio - ETL de Películas IMDB

Esta guía te ayudará a inicializar y ejecutar el pipeline ETL de películas en Windows para tu tarea escolar.

## 📋 Prerrequisitos

### 1. Software Requerido
- **Docker Desktop** (versión más reciente)
  - Descarga desde: https://www.docker.com/products/docker-desktop
  - Asegúrate de que esté ejecutándose antes de continuar

### 2. Verificar Instalación
Abre PowerShell o CMD y verifica:

```powershell
# Verificar Docker
docker --version
docker-compose --version

# Verificar que Docker esté ejecutándose
docker ps
```

## 🏗️ Configuración Inicial

### 1. Navegue al directorio del proyecto
```cmd
cd "C:\Users\titis\Desktop\Pedrozo proyecto 1111\Proyectos_Visualizacion"
```

### 2. Verificar archivos necesarios
Asegúrate de que estos archivos existen:
- ✅ `.env` (configuración de base de datos)
- ✅ `data/imdb_movies_final.csv` (datos fuente)
- ✅ `docker-compose.yml` (configuración de servicios)

### 3. Crear directorios necesarios (si no existen)
```cmd
mkdir data\processed 2>nul
mkdir data\raw 2>nul
mkdir logs 2>nul
```

## 🚀 Métodos de Ejecución

### Método 1: Script PowerShell (Recomendado)

```powershell
# En PowerShell, ejecutar:
.\run_etl.ps1
```

**Si hay error de ejecución de scripts, ejecutar primero:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Método 2: Script Batch (CMD)

```cmd
# En Command Prompt:
run_etl.bat
```

### Método 3: Comandos Docker Manuales

```cmd
# 1. Iniciar la base de datos
docker-compose up -d db

# 2. Esperar unos segundos para que la DB inicie
timeout /t 10

# 3. Ejecutar el ETL
docker-compose run --rm web python -m scripts.etl

# 4. Detener servicios (opcional)
docker-compose down
```

### Método 4: Servicios Completos

```cmd
# Iniciar todos los servicios (FastAPI + PostgreSQL)
docker-compose up -d

# Ver logs en tiempo real
docker-compose logs -f

# Ejecutar ETL mientras los servicios corren
docker-compose exec web python -m scripts.etl
```

## 📊 Lo que hace el ETL

### Proceso Automatizado Simple
1. **Extracción**: Lee `data/imdb_movies_final.csv`
2. **Transformación**:
   - Convierte CSV → JSON (guardado en `data/raw/`)
   - Limpia y normaliza datos
   - Divide en 3 tablas: movie_info, production_info, rating_info
   - Convierte JSON → CSV de respaldo
3. **Validación**: Verifica calidad de datos básica
4. **Carga**: Inserta datos incrementalmente en PostgreSQL (solo registros nuevos)

### Características del ETL
- ✅ **Carga incremental**: Solo procesa datos nuevos
- ✅ **Procesamiento por lotes**: 5,000 registros por lote
- ✅ **Manejo de errores**: Rollback automático en fallos
- ✅ **Logging**: Registra actividad en `logs/etl.log`
- ✅ **Conversión bidireccional**: CSV ↔ JSON

## 🔍 Verificación de Resultados

### 1. Conectarse a la Base de Datos
```cmd
# Obtener el ID del contenedor de PostgreSQL
docker ps

# Conectarse (reemplaza <container_id> con el ID real)
docker exec -it <container_id> psql -U postgres -d videoanalysisdb
```

### 2. Consultas SQL de Verificación
```sql
-- Contar registros en cada tabla
SELECT 'movie_info' as tabla, COUNT(*) as registros FROM movie_info
UNION ALL
SELECT 'production_info', COUNT(*) FROM production_info
UNION ALL
SELECT 'rating_info', COUNT(*) FROM rating_info;

-- Ver metadatos del ETL
SELECT * FROM etl_metadata;

-- Ver algunas películas
SELECT title, year, avg_vote FROM movie_info
ORDER BY avg_vote DESC LIMIT 10;
```

### 3. Verificar Archivos Generados
Después de la ejecución, deberías ver:
- `data/raw/imdb_movies_final.json` - Datos originales en JSON
- `data/raw/imdb_movies_back.csv` - Conversión de vuelta a CSV
- `data/processed/processed.csv` - Datos procesados y limpios
- `logs/etl.log` - Log de ejecución

## 🚨 Solución de Problemas

### Error: "Docker is not running"
- Abrir Docker Desktop
- Esperar a que aparezca el ícono verde
- Intentar de nuevo

### Error: "Port 5432 already in use"
```cmd
# Detener PostgreSQL local si está ejecutándose
net stop postgresql-x64-13

# O cambiar puerto en docker-compose.yml
```

### Error: "Permission denied" en PowerShell
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Error: "Container not found"
```cmd
# Reconstruir contenedores
docker-compose down
docker-compose up --build -d
```

### Error: "Database connection failed"
```cmd
# Verificar variables de entorno en .env
# Esperar más tiempo para que la DB inicie
timeout /t 30
```

## 📈 Programación Automática (Opcional)

### Usando Task Scheduler de Windows
1. Abrir "Programador de tareas"
2. Crear tarea básica
3. Configurar para ejecutar diariamente
4. Acción: Iniciar programa
5. Programa: `powershell.exe`
6. Argumentos: `-File "C:\ruta\completa\run_etl.ps1"`

### Usando Airflow (Avanzado)
```cmd
# Iniciar Airflow (incluye Scheduler y Web UI)
docker-compose -f docker-compose-airflow.yml up -d

# Acceder a la interfaz web en: http://localhost:8080
# Usuario: admin, Contraseña: admin
```

## 🔄 Ejecuciones Subsecuentes

Para ejecuciones posteriores:
- El ETL es **incremental** - solo procesará datos con año mayor al último cargado
- Para recargar todos los datos, ejecuta:
  ```sql
  -- En PostgreSQL, resetear metadatos
  DELETE FROM etl_metadata WHERE key = 'last_year';
  ```

## 📞 Contactos de Soporte

Si encuentras problemas:
1. Revisar `logs/etl.log` para detalles de errores
2. Verificar que Docker Desktop esté ejecutándose
3. Confirmar que el archivo `data/imdb_movies_final.csv` existe
4. Contactar al equipo de desarrollo con los logs de error

---

**¡El ETL está listo para la tarea escolar! 🎬📊**