import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import json
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

print("Iniciando procesamiento...")

# PASO 1: Cargar datos
print("1. Cargando datos de crímenes...")
df = pd.read_csv('./data/Crimes_-_2001_to_Present_20251023.csv')
df = df.dropna(subset=['Latitude', 'Longitude'])

df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
df = df.dropna(subset=['Date'])
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['YearMonth'] = df['Date'].dt.to_period('M').astype(str)

gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df['Longitude'], df['Latitude']),
    crs="EPSG:4326"
)

print(f"   ✓ {len(gdf)} registros cargados")

# PASO 2: Cargar boundaries
print("2. Cargando boundaries...")
communities = gpd.read_file("./data/Boundaries_-_Community_Areas_20251023.geojson")
communities = communities.to_crs("EPSG:4326")
print(f"   ✓ {len(communities)} áreas cargadas")

# PASO 3: Spatial join
print("3. Haciendo spatial join...")
gdf_joined = gpd.sjoin(gdf, communities, how="inner", predicate="within")
print(f"   ✓ {len(gdf_joined)} registros con ubicación")

# PASO 4: Contar crímenes totales por comunidad
print("4. Contando crímenes totales por comunidad...")
crime_counts = (
    gdf_joined.groupby('community')
    .size()
    .reset_index(name='crime_count')
)
print(f"   ✓ {len(crime_counts)} áreas con datos")

# PASO 5: CLUSTERING - Crear matriz de características
print("5. Creando clusters de seguridad...")

# Crear matriz de tipos de crimen por área
crime_matrix = pd.crosstab(
    gdf_joined['community'],
    gdf_joined['Primary Type']
)

# Normalizar
crime_matrix_norm = crime_matrix.div(crime_matrix.sum(axis=1), axis=0)

# Clustering con K-means
scaler = StandardScaler()
crime_matrix_scaled = scaler.fit_transform(crime_matrix_norm)

kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
clusters = kmeans.fit_predict(crime_matrix_scaled)

# Crear DataFrame con clusters
cluster_df = pd.DataFrame({
    'community': crime_matrix.index,
    'cluster': clusters
})

# Merge con crime counts para calcular severidad
cluster_severity = cluster_df.merge(crime_counts, on='community')
cluster_avg = cluster_severity.groupby('cluster')['crime_count'].mean().sort_values(ascending=False)

# Mapear a niveles de riesgo
cluster_to_risk = {cluster: idx for idx, cluster in enumerate(cluster_avg.index)}
cluster_df['risk_level'] = cluster_df['cluster'].map(cluster_to_risk)

# Nombres de riesgo
risk_names = {
    0: 'Very High Risk',
    1: 'High Risk',
    2: 'Moderate Risk',
    3: 'Low Risk',
    4: 'Very Low Risk'
}

cluster_df['risk_name'] = cluster_df['risk_level'].map(risk_names)

print(f"   ✓ Clusters creados")
print("\n   Distribución de riesgo:")
for risk_level in range(5):
    count = (cluster_df['risk_level'] == risk_level).sum()
    name = risk_names[risk_level]
    print(f"      {name}: {count} áreas")

# PASO 6: Exportar datos mensuales (2020-2025)
print("\n6. Exportando datos mensuales (2020-2025)...")
df_filtered = gdf_joined[gdf_joined['Year'].between(2020, 2025)]

monthly_totals = (
    df_filtered.groupby('YearMonth')
    .size()
    .reset_index(name='crime_count')
    .sort_values('YearMonth')
)

monthly_data_export = {
    'dates': monthly_totals['YearMonth'].tolist(),
    'totalCrimes': monthly_totals['crime_count'].tolist()
}

# Top 5 áreas
top5_areas = crime_counts.nlargest(5, 'crime_count')['community'].tolist()

colors = ['#FFB5C0', '#A8D8EA', '#B5EAD7', '#C7CEEA', '#FFD5C2']
top5_monthly = {}
for idx, area in enumerate(top5_areas):
    area_monthly = df_filtered[df_filtered['community'] == area].groupby('YearMonth').size()
    area_monthly = area_monthly.reindex(monthly_totals['YearMonth'], fill_value=0)
    top5_monthly[area] = {
        'color': colors[idx],
        'data': area_monthly.tolist()
    }

with open('./data/monthly_data.json', 'w') as f:
    json.dump({
        'monthlyData': monthly_data_export,
        'top5Communities': top5_monthly
    }, f, indent=2)

print(f"   ✓ monthly_data.json exportado")

# PASO 7: Merge CORRECTO con geometrías
print("7. Haciendo merge con clusters...")
merged = communities.merge(crime_counts, on='community', how='left')
merged['crime_count'] = merged['crime_count'].fillna(0)

# MERGE CORRECTO CON CLUSTERS
merged = merged.merge(cluster_df[['community', 'risk_level', 'risk_name']], on='community', how='left')

# Rellenar valores faltantes
merged['risk_level'] = merged['risk_level'].fillna(4).astype(int)
merged['risk_name'] = merged['risk_name'].fillna('Very Low Risk')

print(f"   ✓ Merged completado")
print(f"   Áreas con cluster asignado: {merged['risk_level'].notna().sum()}")

# Convert datetime columns
for col in merged.select_dtypes(include=['datetime64']).columns:
    merged[col] = merged[col].astype(str)

# PASO 8: Exportar GeoJSON
print("8. Exportando GeoJSON...")
merged_export = merged.copy()
merged_export['geometry'] = merged_export['geometry'].simplify(0.001)

# Asegurar que risk_level es int
merged_export['risk_level'] = merged_export['risk_level'].astype(int)

merged_export.to_file('./data/chicago_crimes_data.geojson', driver='GeoJSON')
print(f"   ✓ chicago_crimes_data.geojson exportado")

# Verificar contenido
print("\n   Verificando GeoJSON:")
sample = merged_export[['community', 'crime_count', 'risk_level', 'risk_name']].head()
print(sample)

# PASO 9: Exportar configuración de clusters
print("\n9. Exportando configuración de clusters...")

cluster_config = {
    'riskLevels': {
        '0': 'Very High Risk',
        '1': 'High Risk',
        '2': 'Moderate Risk',
        '3': 'Low Risk',
        '4': 'Very Low Risk'
    },
    'riskColors': {
        '0': '#8B0000',
        '1': '#DC143C',
        '2': '#FF8C00',
        '3': '#4682B4',
        '4': '#000080'
    }
}

with open('./data/cluster_config.json', 'w') as f:
    json.dump(cluster_config, f, indent=2)

print(f"   ✓ cluster_config.json exportado")

# PASO 10: Exportar datos para sunburst chart
print("10. Exportando datos para sunburst chart...")

# Crear jerarquía: Primary Type -> Arrest -> Domestic
sunburst_data = gdf_joined.groupby(['Primary Type', 'Arrest', 'Domestic']).size().reset_index(name='count')

# Preparar datos en formato para Plotly sunburst
labels = []
parents = []
values = []

# Nivel 1: Primary Types (Top 10)
crime_types = sunburst_data.groupby('Primary Type')['count'].sum().sort_values(ascending=False).head(10)

for crime_type in crime_types.index:
    labels.append(crime_type)
    parents.append("")
    values.append(int(crime_types[crime_type]))
    
    # Nivel 2: Arrest status
    crime_subset = sunburst_data[sunburst_data['Primary Type'] == crime_type]
    
    for arrest_status in [True, False]:
        arrest_label = f"{crime_type}-{'Arrested' if arrest_status else 'Not Arrested'}"
        arrest_data = crime_subset[crime_subset['Arrest'] == arrest_status]
        arrest_count = int(arrest_data['count'].sum())
        
        if arrest_count > 0:
            labels.append(arrest_label)
            parents.append(crime_type)
            values.append(arrest_count)
            
            # Nivel 3: Domestic status
            for domestic_status in [True, False]:
                domestic_label = f"{arrest_label}-{'Domestic' if domestic_status else 'Not Domestic'}"
                domestic_count = int(arrest_data[arrest_data['Domestic'] == domestic_status]['count'].sum())
                
                if domestic_count > 0:
                    labels.append(domestic_label)
                    parents.append(arrest_label)
                    values.append(domestic_count)

sunburst_export = {
    'labels': labels,
    'parents': parents,
    'values': values
}

with open('./data/sunburst_data.json', 'w') as f:
    json.dump(sunburst_export, f, indent=2)

print(f"   ✓ sunburst_data.json exportado ({len(labels)} nodos)")

# PASO 11: Exportar datos horarios por tipo de crimen
print("11. Exportando datos horarios por tipo de crimen...")

# Extraer hora del crimen
gdf_joined['Hour'] = gdf_joined['Date'].dt.hour

# Top 10 tipos de crimen
top10_crime_types = gdf_joined['Primary Type'].value_counts().head(10).index.tolist()

# Filtrar solo top 10
hourly_data = gdf_joined[gdf_joined['Primary Type'].isin(top10_crime_types)]

# Agrupar por tipo y hora
hourly_counts = hourly_data.groupby(['Primary Type', 'Hour']).size().reset_index(name='count')

# Estructura para cada tipo de crimen
hourly_export = {}

for crime_type in top10_crime_types:
    crime_hourly = hourly_counts[hourly_counts['Primary Type'] == crime_type]
    
    # Asegurar que tenemos todas las horas (0-23)
    hours = list(range(24))
    counts = []
    
    for hour in hours:
        hour_count = crime_hourly[crime_hourly['Hour'] == hour]['count']
        counts.append(int(hour_count.values[0]) if len(hour_count) > 0 else 0)
    
    # Identificar hora pico
    peak_hour = counts.index(max(counts))
    
    hourly_export[crime_type] = {
        'data': counts,
        'peak_hour': peak_hour,
        'peak_count': max(counts)
    }

with open('./data/hourly_data.json', 'w') as f:
    json.dump(hourly_export, f, indent=2)

print(f"   ✓ hourly_data.json exportado ({len(top10_crime_types)} tipos de crimen)")

# PASO 12: Exportar estadísticas clave (Crime Type + Location)
print("12. Exportando estadísticas clave (Crime Type + Location)...")

# Top 10 tipos de crimen
top10_types = gdf_joined['Primary Type'].value_counts().head(10)

# Ubicaciones más comunes
top_locations = gdf_joined['Location Description'].value_counts().head(5)

# Calcular estadísticas por ubicación específica
street_crimes = int(gdf_joined[gdf_joined['Location Description'] == 'STREET'].shape[0])
residence_crimes = int(gdf_joined[gdf_joined['Location Description'] == 'RESIDENCE'].shape[0])
apartment_crimes = int(gdf_joined[gdf_joined['Location Description'] == 'APARTMENT'].shape[0])
sidewalk_crimes = int(gdf_joined[gdf_joined['Location Description'] == 'SIDEWALK'].shape[0])

# Calcular arrest rate
total_arrests = int(gdf_joined['Arrest'].sum())
arrest_rate = (total_arrests / len(gdf_joined) * 100)

stats_export = {
    'total_crimes': int(len(gdf_joined)),
    'top_crime_type': top10_types.index[0],
    'top_crime_count': int(top10_types.values[0]),
    'top_crime_percentage': round((top10_types.values[0] / len(gdf_joined) * 100), 1),
    'top_location': top_locations.index[0],
    'top_location_count': int(top_locations.values[0]),
    'top_location_percentage': round((top_locations.values[0] / len(gdf_joined) * 100), 1),
    'street_crimes': street_crimes,
    'residence_crimes': residence_crimes,
    'apartment_crimes': apartment_crimes,
    'sidewalk_crimes': sidewalk_crimes,
    'total_arrests': total_arrests,
    'arrest_rate': round(arrest_rate, 1)
}

with open('./data/crime_stats.json', 'w') as f:
    json.dump(stats_export, f, indent=2)

print(f"   ✓ crime_stats.json exportado")

print("\n✓ COMPLETADO")
print(f"\nArchivos generados:")
print(f"  - ./data/chicago_crimes_data.geojson")
print(f"  - ./data/monthly_data.json")
print(f"  - ./data/cluster_config.json")
print(f"  - ./data/sunburst_data.json")
print(f"  - ./data/hourly_data.json")
print(f"  - ./data/crime_stats.json")
print(f"\nTop 5 áreas con más crímenes:")
for idx, area in enumerate(top5_areas, 1):
    count = crime_counts[crime_counts['community'] == area]['crime_count'].values[0]
    risk = merged_export[merged_export['community'] == area]['risk_name'].values[0]
    print(f"  {idx}. {area}: {count:,} crímenes - {risk}")