# 🎯 ETL Performance Benchmarking Guide

Esta guía explica cómo usar el sistema de benchmarking para analizar el rendimiento del pipeline ETL de películas IMDB.

## 📋 Resumen del Sistema

El sistema de benchmarking mide:
- ⏱️ **Tiempo de procesamiento** (total y por fase)
- 💾 **Uso de memoria** (RAM, picos, eficiencia)
- 🖥️ **Utilización de CPU** (promedio, máximo)
- 📁 **Generación de archivos** (tamaños, throughput)
- 🚀 **Throughput** (registros/segundo, MB/segundo)

## 🚀 Inicio Rápido

### 1. Benchmark Básico (Dataset Completo)
```powershell
# Ejecutar benchmark del ETL completo (85,736 películas)
.\run_benchmark_etl.ps1
```

**Salida esperada:**
- `benchmark_results.json` - Métricas detalladas
- `BENCHMARK_REPORT.md` - Reporte completo
- `benchmark_charts.png` - Gráficas de rendimiento

### 2. Benchmark Avanzado (Monitoreo en Tiempo Real)
```powershell
# Para análisis más detallado con monitoreo continuo
docker-compose run --rm web python advanced_benchmark.py
```

**Salida adicional:**
- `advanced_benchmark_results.json` - Métricas avanzadas
- `monitoring_data.csv` - Datos de monitoreo segundo a segundo

### 3. Benchmark de Escalabilidad
```powershell
# Crear datasets de prueba pequeños
docker-compose run --rm web python create_test_datasets.py

# Ejecutar benchmarks en múltiples tamaños
.\run_scalability_benchmark.ps1
```

## 📊 Interpretación de Resultados

### Métricas Clave

| Métrica | Descripción | Valor Típico |
|---------|-------------|--------------|
| **Records/Second** | Velocidad de procesamiento | 100-200 rps |
| **Peak Memory (MB)** | Máximo uso de memoria | 200-500 MB |
| **Memory Delta (MB)** | Memoria adicional usada | 50-200 MB |
| **Memory Efficiency** | MB por 1,000 registros | 1-5 MB/1K |
| **Processing Time** | Tiempo total de ETL | 5-15 minutos |

### Interpretación de Performance

#### ✅ **Excelente Performance**
- Records/Second: >150
- Memory Efficiency: <3 MB/1K records
- Processing Time: <10 minutos

#### ⚠️ **Performance Aceptable**
- Records/Second: 50-150
- Memory Efficiency: 3-7 MB/1K records
- Processing Time: 10-20 minutos

#### ❌ **Performance Necesita Optimización**
- Records/Second: <50
- Memory Efficiency: >7 MB/1K records
- Processing Time: >20 minutos

## 📁 Archivos del Sistema

### Scripts de Ejecución
- `run_benchmark_etl.ps1` - Benchmark principal
- `run_scalability_benchmark.ps1` - Pruebas de escalabilidad

### Scripts de Python
- `benchmark_etl.py` - Benchmark básico
- `advanced_benchmark.py` - Benchmark con monitoreo en tiempo real
- `benchmark_utils.py` - Generación de reportes y gráficas
- `create_test_datasets.py` - Generador de datasets de prueba

### Archivos de Resultados
- `benchmark_results.json` - Resultados básicos
- `advanced_benchmark_results.json` - Resultados avanzados
- `BENCHMARK_REPORT.md` - Reporte detallado
- `benchmark_charts.png` - Visualizaciones
- `monitoring_data.csv` - Datos de monitoreo continuo

## 🔧 Configuración de Pruebas

### Tamaños de Dataset Disponibles
- **Small**: 1,000 registros (~1 MB)
- **Medium**: 5,000 registros (~5 MB)
- **Large**: 10,000 registros (~10 MB)
- **XLarge**: 25,000 registros (~25 MB)
- **Full**: 85,736 registros (~42 MB)

### Personalizar Benchmark

Para crear un benchmark personalizado:

```python
from benchmark_etl import ETLBenchmark

# Crear instancia de benchmark
benchmark = ETLBenchmark()

# Ejecutar con monitoreo personalizado
benchmark.run_full_benchmark()

# Acceder a resultados
results = benchmark.results
print(f"Procesados: {results['total_records']} registros")
print(f"Tiempo: {results['total_duration_minutes']:.2f} minutos")
```

## 📊 Análisis de Bottlenecks

### Identificar Problemas de Performance

#### 1. **Memory Bottlenecks**
- **Síntoma**: Memory Delta >500 MB
- **Causa**: Cargar todo el dataset en memoria
- **Solución**: Implementar procesamiento por chunks

#### 2. **CPU Bottlenecks**
- **Síntoma**: CPU >90% por períodos largos
- **Causa**: Transformaciones complejas
- **Solución**: Optimizar funciones de transformación

#### 3. **I/O Bottlenecks**
- **Síntoma**: Records/Second <50
- **Causa**: Muchas operaciones de disco
- **Solución**: Batch processing, escritura en memoria

#### 4. **Database Bottlenecks**
- **Síntoma**: Fase de Load muy lenta
- **Causa**: Inserciones individuales
- **Solución**: Bulk inserts, transacciones por lotes

## 🎯 Mejores Prácticas

### Para Obtener Resultados Consistentes
1. **Cerrar aplicaciones innecesarias** antes del benchmark
2. **Ejecutar múltiples veces** y promediar resultados
3. **Usar el mismo hardware** para comparaciones
4. **Reiniciar servicios Docker** entre pruebas

### Para Presentations del Equipo
1. **Usar gráficas** (`benchmark_charts.png`)
2. **Mostrar métricas clave** (Records/Second, Memory Efficiency)
3. **Comparar con benchmarks** de la industria
4. **Explicar optimizaciones** implementadas

## 🔍 Troubleshooting

### Problemas Comunes

#### Error: "psutil not found"
```bash
# Instalar dependencias
pip install psutil matplotlib
```

#### Error: "Docker not running"
```powershell
# Verificar Docker
docker info
# Iniciar servicios si es necesario
docker-compose up -d
```

#### Error: "Benchmark results not found"
- Verificar que el ETL completó exitosamente
- Revisar logs de Docker para errores
- Verificar permisos de escritura de archivos

#### Performance Inesperadamente Baja
- Verificar recursos disponibles del sistema
- Cerrar aplicaciones que consumen CPU/memoria
- Verificar que no hay otros containers ejecutándose

## 📈 Benchmarks de la Industria

### Comparación con Estándares ETL

| Tipo de ETL | Records/Second | Memory/1K Records |
|-------------|----------------|-------------------|
| **ETL Simple** | 500-1000 | 0.5-2 MB |
| **ETL Complejo** | 100-500 | 2-10 MB |
| **ETL Enterprise** | 50-200 | 5-20 MB |
| **Nuestro ETL** | **100-200** | **2-5 MB** |

### Factores que Afectan Performance
- **Complejidad de transformaciones**
- **Tamaño del dataset**
- **Tipo de base de datos**
- **Recursos de hardware**
- **Configuración de red**

## 💡 Optimizaciones Recomendadas

### Para Mejorar Throughput
1. **Incrementar batch size** en cargas
2. **Usar conexiones paralelas** a DB
3. **Optimizar queries SQL**
4. **Implementar caching** de resultados

### Para Reducir Memoria
1. **Procesamiento por chunks**
2. **Liberar DataFrames** después de uso
3. **Usar generadores** en lugar de listas
4. **Optimizar tipos de datos** en pandas

### Para Reducir Tiempo
1. **Procesamiento paralelo** de archivos
2. **Eliminar validaciones** innecesarias
3. **Usar formato binario** (parquet vs CSV)
4. **Optimizar I/O** con SSD

## 📞 Soporte

Para dudas sobre el sistema de benchmarking:
1. Revisar logs en `logs/etl.log`
2. Verificar `benchmark_results.json` para detalles
3. Consultar este documento para troubleshooting
4. Revisar configuración de Docker y base de datos

---

**Sistema desarrollado para proyecto de Visualización de Datos**
*Última actualización: Septiembre 2025*