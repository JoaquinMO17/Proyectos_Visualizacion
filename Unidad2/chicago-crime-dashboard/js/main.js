let monthlyData = null;
let top5Communities = null;
let statsData = null;

document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
});

async function initializeDashboard() {
    try {
        // Cargar datos mensuales
        const response = await fetch('./data/monthly_data.json');
        const data = await response.json();
        
        monthlyData = data.monthlyData;
        top5Communities = data.top5Communities;
        
        console.log('✓ Monthly data loaded');
        console.log('  Date range:', monthlyData.dates[0], 'to', monthlyData.dates[monthlyData.dates.length - 1]);
        console.log('  Top 5 areas:', Object.keys(top5Communities));
        
        // Cargar y mostrar estadísticas
        await loadStatsCards();
        
        // Crear las 6 gráficas
        createOverallTrendChart();
        createTop5CommunitiesChart();
        createSunburstChart();
        createHourlyPatternsChart();
        createCrimeMap();
        createClusterMap();
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert('Error loading data. Make sure all JSON files exist in /data/');
    }
}

// ===================================
// Load and Display Stats Cards
// ===================================
async function loadStatsCards() {
    try {
        const response = await fetch('./data/crime_stats.json');
        statsData = await response.json();
        
        console.log('✓ Stats loaded');
        
        // Poblar tarjetas
        document.getElementById('topCrimeType').textContent = statsData.top_crime_type;
        document.getElementById('topCrimeSubtitle').textContent = 
            `${statsData.top_crime_count.toLocaleString()} crimes (${statsData.top_crime_percentage}%)`;
        
        document.getElementById('topLocation').textContent = statsData.top_location;
        document.getElementById('topLocationSubtitle').textContent = 
            `${statsData.top_location_count.toLocaleString()} incidents (${statsData.top_location_percentage}%)`;
        
        document.getElementById('streetCrimes').textContent = statsData.street_crimes.toLocaleString();
        
        const totalResidence = statsData.residence_crimes + statsData.apartment_crimes;
        document.getElementById('residenceCrimes').textContent = totalResidence.toLocaleString();
        
        document.getElementById('arrestRate').textContent = `${statsData.arrest_rate}%`;
        document.getElementById('arrestSubtitle').textContent = 
            `${statsData.total_arrests.toLocaleString()} total arrests`;
        
        document.getElementById('totalCrimes').textContent = statsData.total_crimes.toLocaleString();
        
        // Actualizar textos con fechas reales
        const dateMin = new Date(statsData.date_min);
        const dateMax = new Date(statsData.date_max);
        
        const yearMin = dateMin.getFullYear();
        const monthMax = dateMax.toLocaleString('en-US', { month: 'short' });
        const yearMax = dateMax.getFullYear();
        
        // Actualizar header subtitle
        document.querySelector('.subtitle').textContent = 
            `Análisis Espacial y Temporal (${yearMin} - ${monthMax} ${yearMax})`;
        
        // Actualizar footer
        document.querySelector('.dashboard-footer p').textContent = 
            `Data Source: Chicago Police Department | Analysis Period: ${yearMin} - ${monthMax} ${yearMax} | Total Records: ${statsData.total_records.toLocaleString()}`;
        
    } catch (error) {
        console.error('ERROR loading stats:', error);
    }
}

// ===================================
// Funciones auxiliares
// ===================================
function calculateMovingAverage(data, windowSize = 3) {
    const result = [];
    for (let i = 0; i < data.length; i++) {
        if (i < windowSize - 1) {
            result.push(null);
        } else {
            let sum = 0;
            for (let j = 0; j < windowSize; j++) {
                sum += data[i - j];
            }
            result.push(sum / windowSize);
        }
    }
    return result;
}

function calculateTrend(data) {
    const validData = data.filter(d => d !== null);
    const n = validData.length;
    
    let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
    
    validData.forEach((y, x) => {
        sumX += x;
        sumY += y;
        sumXY += x * y;
        sumXX += x * x;
    });
    
    const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;
    
    return data.map((_, i) => slope * i + intercept);
}

// Función para obtener color según el cambio porcentual
function getChangeColor(changePercent) {
    if (changePercent <= -20) return '#2ECC71';
    if (changePercent <= -5) return '#A8D8EA';
    if (changePercent < 5) return '#FFF4A3';
    if (changePercent < 20) return '#FFB5C0';
    return '#DC143C';
}

// ===================================
// Chart 1: Overall Monthly Trend
// ===================================
function createOverallTrendChart() {
    const ctx = document.getElementById('overallTrendChart').getContext('2d');
    
    const smoothedData = calculateMovingAverage(monthlyData.totalCrimes, 3);
    const trendData = calculateTrend(smoothedData.map(d => d || 0));
    
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: monthlyData.dates,
            datasets: [
                {
                    label: 'Original Data',
                    data: monthlyData.totalCrimes,
                    borderColor: 'rgba(168, 216, 234, 0.3)',
                    backgroundColor: 'rgba(168, 216, 234, 0.1)',
                    borderWidth: 1,
                    pointRadius: 0,
                    tension: 0.4
                },
                {
                    label: 'Smoothed (3-month Moving Average)',
                    data: smoothedData,
                    borderColor: '#2E86AB',
                    backgroundColor: 'rgba(46, 134, 171, 0.1)',
                    borderWidth: 3,
                    pointRadius: 0,
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Trend Line',
                    data: trendData,
                    borderColor: '#D32F2F',
                    borderWidth: 2,
                    borderDash: [10, 5],
                    pointRadius: 0,
                    fill: false
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 15,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(255, 255, 255, 0.9)',
                    titleColor: '#2C3E50',
                    bodyColor: '#5F6368',
                    borderColor: '#E0E0E0',
                    borderWidth: 1,
                    padding: 12
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Date',
                        font: { size: 13, weight: 'bold' }
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        autoSkip: true,
                        maxTicksLimit: 20
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Number of Crimes',
                        font: { size: 13, weight: 'bold' }
                    },
                    beginAtZero: false
                }
            }
        }
    });
    
    console.log('✓ Chart 1 (Overall Trend) created');
}

// ===================================
// Chart 2: Top 5 Community Areas CON LINEAS DE TENDENCIA
// ===================================
function createTop5CommunitiesChart() {
    const ctx = document.getElementById('top5CommunitiesChart').getContext('2d');
    
    const datasets = [];
    
    Object.keys(top5Communities).forEach(areaName => {
        const areaData = top5Communities[areaName].data;
        const areaColor = top5Communities[areaName].color;
        
        // Calcular smoothed data y trend
        const smoothedData = calculateMovingAverage(areaData, 3);
        const trendData = calculateTrend(smoothedData.map(d => d || 0));
        
        // Dataset principal (smoothed)
        datasets.push({
            label: `${areaName} (Smoothed)`,
            data: smoothedData,
            borderColor: areaColor,
            backgroundColor: areaColor + '20',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.4,
            fill: false
        });
        
        // Dataset de tendencia
        datasets.push({
            label: `Trend_${areaName}`,
            data: trendData,
            borderColor: areaColor,
            backgroundColor: 'transparent',
            borderWidth: 1.5,
            borderDash: [8, 4],
            pointRadius: 0,
            tension: 0,
            fill: false
        });
    });
    
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: monthlyData.dates,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'right',
                    labels: {
                        usePointStyle: true,
                        padding: 10,
                        font: { size: 11 }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    titleColor: '#2C3E50',
                    bodyColor: '#5F6368',
                    borderColor: '#E0E0E0',
                    borderWidth: 1,
                    padding: 12
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Date',
                        font: { size: 13, weight: 'bold' }
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        autoSkip: true,
                        maxTicksLimit: 20
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Number of Crimes (Moving Average)',
                        font: { size: 13, weight: 'bold' }
                    },
                    beginAtZero: true
                }
            }
        }
    });
    
    console.log('✓ Chart 2 (Top 5 Communities) created');
}

// ===================================
// Chart 3: Sunburst Chart
// ===================================
async function createSunburstChart() {
    try {
        const response = await fetch('./data/sunburst_data.json');
        const sunburstData = await response.json();
        
        console.log('✓ Sunburst data loaded');
        
        const data = [{
            type: 'sunburst',
            labels: sunburstData.labels,
            parents: sunburstData.parents,
            values: sunburstData.values,
            branchvalues: 'total',
            marker: {
                line: { width: 2, color: 'white' }
            },
            hovertemplate: '<b>%{label}</b><br>Count: %{value:,}<br>%{percentParent}<extra></extra>',
            textfont: { size: 12 }
        }];
        
        const layout = {
            margin: { t: 0, l: 0, r: 0, b: 0 },
            sunburstcolorway: [
                '#667eea', '#764ba2', '#f093fb', '#f5576c',
                '#4facfe', '#00f2fe', '#43e97b', '#38f9d7',
                '#fa709a', '#fee140', '#30cfd0', '#330867'
            ],
            extendsunburstcolors: true
        };
        
        const config = {
            responsive: true,
            displayModeBar: false
        };
        
        Plotly.newPlot('sunburstChart', data, layout, config);
        
        // Agregar insights
        const insightsList = document.getElementById('sunburstInsights');
        insightsList.innerHTML = `
            <li>The chart shows <strong>hierarchical relationships</strong> between crime types, arrest outcomes, and domestic incidents</li>
            <li>Click on any segment to <strong>zoom in</strong> and explore details</li>
            <li>The size of each segment represents the <strong>relative proportion</strong> of crimes</li>
            <li>Three levels: <strong>Crime Type → Arrest Status → Domestic Status</strong></li>
        `;
        
        console.log('✓ Chart 3 (Sunburst) created');
        
    } catch (error) {
        console.error('ERROR loading sunburst data:', error);
    }
}

// ===================================
// Chart 4: Hourly Patterns
// ===================================
async function createHourlyPatternsChart() {
    try {
        const response = await fetch('./data/hourly_data.json');
        const hourlyData = await response.json();
        
        console.log('✓ Hourly data loaded');
        
        const ctx = document.getElementById('hourlyPatternsChart').getContext('2d');
        
        const colors = [
            '#667eea', '#f5576c', '#4facfe', '#43e97b',
            '#fa709a', '#764ba2', '#00f2fe', '#38f9d7',
            '#fee140', '#330867'
        ];
        
        const datasets = Object.keys(hourlyData).map((crimeType, index) => {
            const data = hourlyData[crimeType].data;
            const peakHour = hourlyData[crimeType].peak_hour;
            
            // Marcar la hora pico
            const pointBackgroundColors = data.map((_, hour) => 
                hour === peakHour ? colors[index] : 'transparent'
            );
            const pointRadius = data.map((_, hour) => 
                hour === peakHour ? 6 : 0
            );
            
            return {
                label: crimeType,
                data: data,
                borderColor: colors[index],
                backgroundColor: colors[index] + '20',
                borderWidth: 2,
                pointBackgroundColor: pointBackgroundColors,
                pointRadius: pointRadius,
                pointBorderColor: colors[index],
                pointBorderWidth: 2,
                tension: 0.4,
                fill: false
            };
        });
        
        const hours = Array.from({ length: 24 }, (_, i) => 
            i === 0 ? '12 AM' : i < 12 ? `${i} AM` : i === 12 ? '12 PM' : `${i - 12} PM`
        );
        
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: hours,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            padding: 10,
                            font: { size: 11 }
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(255, 255, 255, 0.95)',
                        titleColor: '#2C3E50',
                        bodyColor: '#5F6368',
                        borderColor: '#E0E0E0',
                        borderWidth: 1,
                        padding: 12,
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                label += context.parsed.y.toLocaleString() + ' crimes';
                                
                                const crimeType = context.dataset.label;
                                const peakHour = hourlyData[crimeType].peak_hour;
                                if (context.dataIndex === peakHour) {
                                    label += ' ⭐ PEAK';
                                }
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Hour of Day',
                            font: { size: 13, weight: 'bold' }
                        },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: 'Number of Crimes',
                            font: { size: 13, weight: 'bold' }
                        },
                        beginAtZero: true
                    }
                },
                interaction: {
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                }
            }
        });
        
        console.log('✓ Chart 4 (Hourly Patterns) created');
        
    } catch (error) {
        console.error('ERROR loading hourly data:', error);
    }
}

// ===================================
// Chart 5: Crime Distribution Map
// ===================================
function createCrimeMap() {
    const map = L.map('crimeMap').setView([41.8781, -87.6298], 10);
    
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors © CARTO',
        subdomains: 'abcd',
        maxZoom: 19
    }).addTo(map);
    
    fetch('./data/chicago_crimes_data.geojson')
        .then(response => response.json())
        .then(geojsonData => {
            
            const counts = geojsonData.features
                .map(f => f.properties.crime_count)
                .filter(c => c > 0);
            const minCount = Math.min(...counts);
            const maxCount = Math.max(...counts);
            
            console.log('✓ Chart 5 created - Crime range:', minCount.toLocaleString(), '-', maxCount.toLocaleString());
            
            function getColor(crimeCount) {
                if (crimeCount === 0) return '#CCCCCC';
                if (crimeCount < 5000) return '#FFFFCC';
                if (crimeCount < 10000) return '#FFEDA0';
                if (crimeCount < 20000) return '#FED976';
                if (crimeCount < 30000) return '#FEB24C';
                if (crimeCount < 40000) return '#FD8D3C';
                if (crimeCount < 50000) return '#FC4E2A';
                return '#E31A1C';
            }
            
            function style(feature) {
                return {
                    fillColor: getColor(feature.properties.crime_count),
                    weight: 2,
                    opacity: 1,
                    color: '#3388ff',
                    fillOpacity: 0.7
                };
            }
            
            function highlightFeature(e) {
                const layer = e.target;
                layer.setStyle({
                    weight: 3,
                    color: '#666',
                    fillOpacity: 0.9
                });
                layer.bringToFront();
            }
            
            function resetHighlight(e) {
                geojsonLayer.resetStyle(e.target);
            }
            
            function onEachFeature(feature, layer) {
                const props = feature.properties;
                
                const changeColor = getChangeColor(props.change_percent);
                const changeSign = props.change_percent > 0 ? '+' : '';
                
                layer.bindPopup(`
                    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-width: 280px;">
                        <h3 style="margin: 0 0 12px 0; font-size: 1.15rem; font-weight: 700; color: #2C3E50; border-bottom: 2px solid #667eea; padding-bottom: 8px;">
                            ${props.community}
                        </h3>
                        
                        <div style="background: linear-gradient(135deg, #f6f8fb 0%, #e9ecef 100%); padding: 12px; border-radius: 8px; margin-bottom: 12px;">
                            <p style="margin: 0 0 6px 0; font-size: 0.85rem; color: #7F8C8D; font-weight: 600; text-transform: uppercase;">
                                Change 2020 → 2024
                            </p>
                            <p style="margin: 0; font-size: 1.3rem;">
                                <span style="color: ${changeColor}; font-weight: 700;">
                                    ${changeSign}${props.change_percent}%
                                </span>
                            </p>
                            <p style="margin: 5px 0 0 0; color: #5F6368; font-size: 0.85rem;">
                                ${props.change_category}
                            </p>
                        </div>
                        
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px;">
                            <div style="background: #E8F5E9; padding: 10px; border-radius: 6px; text-align: center;">
                                <p style="margin: 0; font-size: 0.75rem; color: #2E7D32; font-weight: 600;">2020 CRIMES</p>
                                <p style="margin: 5px 0 0 0; font-size: 1.2rem; font-weight: 700; color: #1B5E20;">
                                    ${props.crimes_2020.toLocaleString()}
                                </p>
                            </div>
                            <div style="background: #E3F2FD; padding: 10px; border-radius: 6px; text-align: center;">
                                <p style="margin: 0; font-size: 0.75rem; color: #1565C0; font-weight: 600;">2024 CRIMES</p>
                                <p style="margin: 5px 0 0 0; font-size: 1.2rem; font-weight: 700; color: #0D47A1;">
                                    ${props.crimes_2024.toLocaleString()}
                                </p>
                            </div>
                        </div>
                        
                        <hr style="border: none; border-top: 1px solid #E0E0E0; margin: 12px 0;">
                        
                        <p style="margin: 6px 0; color: #2C3E50; font-size: 0.9rem;">
                            <strong>Total (2020-2025):</strong> 
                            <span style="font-weight: 700; color: #667eea;">${props.crime_count.toLocaleString()}</span>
                        </p>
                        <p style="margin: 6px 0; color: #2C3E50; font-size: 0.9rem;">
                            <strong>Risk Level:</strong> 
                            <span style="font-weight: 700; color: #DC143C;">${props.risk_name}</span>
                        </p>
                    </div>
                `, {
                    maxWidth: 320
                });
                
                layer.on({
                    mouseover: highlightFeature,
                    mouseout: resetHighlight
                });
            }
            
            const geojsonLayer = L.geoJSON(geojsonData, {
                style: style,
                onEachFeature: onEachFeature
            }).addTo(map);
            
            map.fitBounds(geojsonLayer.getBounds());
            
            const legend = L.control({position: 'topright'});
            legend.onAdd = function(map) {
                const div = L.DomUtil.create('div', 'info legend');
                div.innerHTML = '<div style="background: white; padding: 10px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">' +
                    '<div style="background: linear-gradient(to right, #FFFFCC, #E31A1C); height: 20px; width: 200px; border: 1px solid #999;"></div>' +
                    '<div style="display: flex; justify-content: space-between; font-size: 11px; margin-top: 5px;">' +
                    '<span>' + minCount.toLocaleString() + '</span>' +
                    '<span>' + maxCount.toLocaleString() + '</span>' +
                    '</div>' +
                    '<div style="text-align: center; font-size: 12px; margin-top: 5px; font-weight: bold;">Total Crimes per Community Area</div>' +
                    '</div>';
                return div;
            };
            legend.addTo(map);
        })
        .catch(error => {
            console.error('ERROR loading GeoJSON:', error);
        });
}

// ===================================
// Chart 6: Safety Risk Clustering Map
// ===================================
async function createClusterMap() {
    const map = L.map('clusterMap').setView([41.8781, -87.6298], 10);
    
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors © CARTO',
        subdomains: 'abcd',
        maxZoom: 19
    }).addTo(map);
    
    try {
        const configResponse = await fetch('./data/cluster_config.json');
        const config = await configResponse.json();
        
        const geoResponse = await fetch('./data/chicago_crimes_data.geojson');
        const geojsonData = await geoResponse.json();
        
        console.log('✓ Chart 6 (Clustering) data loaded');
        
        function style(feature) {
            const riskLevel = feature.properties.risk_level;
            const color = config.riskColors[String(riskLevel)] || '#CCCCCC';
            
            return {
                fillColor: color,
                weight: 1,
                opacity: 1,
                color: 'black',
                fillOpacity: 0.6
            };
        }
        
        function highlightFeature(e) {
            const layer = e.target;
            layer.setStyle({
                weight: 3,
                color: '#666',
                fillOpacity: 0.9
            });
            layer.bringToFront();
        }
        
        function resetHighlight(e) {
            geojsonLayer.resetStyle(e.target);
        }
        
        function onEachFeature(feature, layer) {
            const props = feature.properties;
            
            const changeColor = getChangeColor(props.change_percent);
            const changeSign = props.change_percent > 0 ? '+' : '';
            const riskColor = config.riskColors[props.risk_level.toString()];
            
            layer.bindPopup(`
                <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; min-width: 280px;">
                    <h3 style="margin: 0 0 12px 0; font-size: 1.15rem; font-weight: 700; color: #2C3E50; border-bottom: 2px solid ${riskColor}; padding-bottom: 8px;">
                        ${props.community}
                    </h3>
                    
                    <div style="background: ${riskColor}; color: white; padding: 10px; border-radius: 8px; margin-bottom: 12px; text-align: center;">
                        <p style="margin: 0; font-size: 0.85rem; font-weight: 600; text-transform: uppercase; opacity: 0.9;">
                            Risk Classification
                        </p>
                        <p style="margin: 5px 0 0 0; font-size: 1.2rem; font-weight: 700;">
                            ${props.risk_name}
                        </p>
                    </div>
                    
                    <div style="background: linear-gradient(135deg, #f6f8fb 0%, #e9ecef 100%); padding: 12px; border-radius: 8px; margin-bottom: 12px;">
                        <p style="margin: 0 0 6px 0; font-size: 0.85rem; color: #7F8C8D; font-weight: 600; text-transform: uppercase;">
                            Change 2020 → 2024
                        </p>
                        <p style="margin: 0; font-size: 1.3rem;">
                            <span style="color: ${changeColor}; font-weight: 700;">
                                ${changeSign}${props.change_percent}%
                            </span>
                        </p>
                        <p style="margin: 5px 0 0 0; color: #5F6368; font-size: 0.85rem;">
                            ${props.change_category}
                        </p>
                    </div>
                    
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px;">
                        <div style="background: #E8F5E9; padding: 10px; border-radius: 6px; text-align: center;">
                            <p style="margin: 0; font-size: 0.75rem; color: #2E7D32; font-weight: 600;">2020 CRIMES</p>
                            <p style="margin: 5px 0 0 0; font-size: 1.2rem; font-weight: 700; color: #1B5E20;">
                                ${props.crimes_2020.toLocaleString()}
                            </p>
                        </div>
                        <div style="background: #E3F2FD; padding: 10px; border-radius: 6px; text-align: center;">
                            <p style="margin: 0; font-size: 0.75rem; color: #1565C0; font-weight: 600;">2024 CRIMES</p>
                            <p style="margin: 5px 0 0 0; font-size: 1.2rem; font-weight: 700; color: #0D47A1;">
                                ${props.crimes_2024.toLocaleString()}
                            </p>
                        </div>
                    </div>
                    
                    <hr style="border: none; border-top: 1px solid #E0E0E0; margin: 12px 0;">
                    
                    <p style="margin: 6px 0; color: #2C3E50; font-size: 0.9rem;">
                        <strong>Total (2020-2025):</strong> 
                        <span style="font-weight: 700; color: #667eea;">${props.crime_count.toLocaleString()}</span>
                    </p>
                </div>
            `, {
                maxWidth: 320
            });
            
            layer.on({
                mouseover: highlightFeature,
                mouseout: resetHighlight
            });
        }
        
        const geojsonLayer = L.geoJSON(geojsonData, {
            style: style,
            onEachFeature: onEachFeature
        }).addTo(map);
        
        map.fitBounds(geojsonLayer.getBounds());
        
        console.log('✓ Chart 6 created');
        
    } catch (error) {
        console.error('ERROR loading cluster data:', error);
    }
}

console.log('Chicago Crime Dashboard initialized');