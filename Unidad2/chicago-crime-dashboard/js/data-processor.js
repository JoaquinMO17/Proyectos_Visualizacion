// ===================================
// Data Processor - Chicago Crime Dashboard
// DATOS REALES del análisis de Chicago
// ===================================

// Monthly crime data (2020-2025) - Simulated based on analysis
const monthlyData = {
    dates: [
        '2020-01', '2020-02', '2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12',
        '2021-01', '2021-02', '2021-03', '2021-04', '2021-05', '2021-06', '2021-07', '2021-08', '2021-09', '2021-10', '2021-11', '2021-12',
        '2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10', '2022-11', '2022-12',
        '2023-01', '2023-02', '2023-03', '2023-04', '2023-05', '2023-06', '2023-07', '2023-08', '2023-09', '2023-10', '2023-11', '2023-12',
        '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07', '2024-08', '2024-09', '2024-10', '2024-11', '2024-12',
        '2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07'
    ],
    totalCrimes: [
        19500, 17800, 18500, 13200, 15800, 18200, 19800, 19200, 18500, 19800, 18200, 17500,
        16000, 15800, 19200, 18800, 19200, 18900, 19500, 19000, 18700, 19400, 18800, 18200,
        15000, 15200, 18600, 18200, 18800, 19000, 19200, 18800, 18800, 19000, 18500, 17800,
        16200, 16400, 18900, 19200, 19000, 19200, 23000, 22800, 20000, 20200, 19800, 19200,
        20200, 20000, 22800, 23200, 23800, 23200, 22400, 23200, 23000, 23500, 23200, 22800,
        18200, 18800, 20200, 20800, 21200, 20800, 21000
    ]
};

// Top 5 Community Areas data
const top5Communities = {
    'AUSTIN': {
        color: '#FFB5C0',
        data: [1050, 980, 1000, 950, 1100, 980, 1000, 950, 850, 880, 920, 1000,
               860, 830, 1000, 980, 1000, 1020, 1080, 1050, 980, 1010, 990, 1020,
               800, 850, 980, 950, 1000, 1020, 1030, 1000, 1010, 1080, 1200, 1050,
               920, 920, 980, 1000, 1020, 1040, 1080, 1020, 1020, 1100, 1050, 980,
               950, 960, 1020, 1040, 1050, 1080, 1140, 1100, 980, 1000, 1020, 1050,
               1020, 1040, 1050, 1060, 1070, 1050, 1040]
    },
    'NEAR NORTH SIDE': {
        color: '#A8D8EA',
        data: [650, 580, 620, 450, 550, 620, 680, 670, 640, 690, 650, 620,
               550, 520, 640, 620, 640, 650, 680, 670, 650, 680, 660, 640,
               560, 580, 650, 640, 660, 680, 710, 690, 680, 700, 880, 750,
               640, 650, 680, 700, 720, 750, 840, 820, 740, 780, 760, 720,
               720, 730, 800, 850, 880, 900, 1020, 1030, 980, 1000, 1020, 1050,
               800, 840, 900, 950, 1000, 980, 990]
    },
    'NEAR WEST SIDE': {
        color: '#B5EAD7',
        data: [600, 550, 580, 420, 500, 570, 640, 630, 610, 650, 600, 580,
               520, 490, 600, 580, 600, 610, 640, 630, 610, 640, 620, 600,
               540, 560, 610, 600, 620, 640, 660, 650, 640, 660, 830, 700,
               600, 610, 640, 660, 680, 710, 790, 770, 700, 740, 720, 680,
               680, 690, 760, 810, 840, 860, 980, 990, 940, 960, 980, 1010,
               760, 800, 860, 910, 960, 940, 950]
    },
    'SOUTH SHORE': {
        color: '#C7CEEA',
        data: [620, 580, 600, 450, 520, 590, 650, 640, 620, 660, 610, 590,
               540, 510, 610, 590, 610, 620, 650, 640, 620, 650, 630, 610,
               560, 580, 620, 610, 630, 650, 670, 660, 650, 670, 810, 720,
               610, 620, 650, 670, 690, 720, 800, 780, 710, 750, 730, 690,
               690, 700, 770, 820, 850, 870, 990, 1000, 950, 970, 990, 1020,
               770, 810, 870, 920, 970, 950, 960]
    },
    'LOOP': {
        color: '#FFD5C2',
        data: [580, 520, 560, 380, 460, 540, 610, 600, 580, 620, 560, 540,
               480, 420, 340, 280, 440, 480, 580, 600, 640, 660, 580, 500,
               480, 500, 560, 560, 580, 600, 620, 610, 600, 620, 700, 680,
               480, 500, 550, 570, 660, 680, 770, 880, 760, 720, 690, 720,
               620, 640, 700, 750, 780, 800, 880, 810, 620, 600, 700, 800,
               540, 580, 640, 690, 730, 710, 720]
    }
};

// Community crime counts - DATOS REALES
const communityCrimeCounts = {
    'AUSTIN': 8431,
    'NEAR NORTH SIDE': 8039,
    'NEAR WEST SIDE': 7508,
    'LOOP': 6300,
    'SOUTH SHORE': 5751,
    'WEST TOWN': 4950,
    'NORTH LAWNDALE': 4628,
    'HUMBOLDT PARK': 4558,
    'GREATER GRAND CROSSING': 4378,
    'AUBURN GRESHAM': 4349,
    'LAKE VIEW': 4220,
    'ROSELAND': 3825,
    'CHATHAM': 3789,
    'ENGLEWOOD': 3716,
    'CHICAGO LAWN': 3393,
    'LOGAN SQUARE': 3255,
    'WEST ENGLEWOOD': 3081,
    'UPTOWN': 3081,
    'SOUTH LAWNDALE': 2816,
    'BELMONT CRAGIN': 2805,
    'EAST GARFIELD PARK': 2793,
    'WOODLAWN': 2767,
    'SOUTH CHICAGO': 2701,
    'LINCOLN PARK': 2699,
    'ROGERS PARK': 2607,
    'NEW CITY': 2534,
    'WEST GARFIELD PARK': 2399,
    'WEST PULLMAN': 2294,
    'GRAND BOULEVARD': 2293,
    'EDGEWATER': 2203,
    'WEST RIDGE': 2183,
    'PORTAGE PARK': 2172,
    'DOUGLAS': 2117,
    'WASHINGTON HEIGHTS': 2020,
    'WASHINGTON PARK': 1820,
    'LOWER WEST SIDE': 1809,
    'HYDE PARK': 1802,
    'NEAR SOUTH SIDE': 1779,
    'IRVING PARK': 1659,
    'ALBANY PARK': 1637,
    'GAGE PARK': 1586,
    'LINCOLN SQUARE': 1496,
    'KENWOOD': 1492,
    'AVONDALE': 1474,
    'GARFIELD RIDGE': 1457,
    'BRIGHTON PARK': 1447,
    'ASHBURN': 1411,
    'SOUTH DEERING': 1350,
    'MORGAN PARK': 1309,
    'BRIDGEPORT': 1243,
    'WEST LAWN': 1196,
    'OHARE': 1171,
    'CALUMET HEIGHTS': 1043,
    'EAST SIDE': 1003,
    'DUNNING': 969,
    'NORTH CENTER': 854,
    'NORWOOD PARK': 841,
    'ARMOUR SQUARE': 827,
    'ARCHER HEIGHTS': 807,
    'RIVERDALE': 791,
    'CLEARING': 745,
    'AVALON PARK': 737,
    'PULLMAN': 735,
    'NORTH PARK': 705,
    'HERMOSA': 701,
    'BEVERLY': 697,
    'MCKINLEY PARK': 636,
    'JEFFERSON PARK': 626,
    'WEST ELSDON': 617,
    'OAKLAND': 587,
    'HEGEWISCH': 549,
    'MONTCLARE': 470,
    'FULLER PARK': 424,
    'MOUNT GREENWOOD': 333,
    'FOREST GLEN': 288,
    'BURNSIDE': 221,
    'EDISON PARK': 162
};

// Function to calculate 3-month moving average
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

// Function to calculate linear trend
function calculateTrend(data) {
    const n = data.filter(d => d !== null).length;
    const validData = data.filter(d => d !== null);
    
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

// Function to get color based on crime count
function getColor(crimeCount) {
    if (crimeCount === 0) return '#CCCCCC'; // Gray for no data
    
    // Define thresholds based on real data
    if (crimeCount < 500) return '#FFFFCC'; // Very light yellow
    if (crimeCount < 1000) return '#FFEDA0'; // Light yellow
    if (crimeCount < 2000) return '#FED976'; // Yellow-orange
    if (crimeCount < 3000) return '#FEB24C'; // Orange
    if (crimeCount < 4000) return '#FD8D3C'; // Dark orange
    if (crimeCount < 6000) return '#FC4E2A'; // Red-orange
    return '#E31A1C'; // Dark red
}

// Export functions and data
window.ChicagoCrimeData = {
    monthlyData,
    top5Communities,
    communityCrimeCounts,
    calculateMovingAverage,
    calculateTrend,
    getColor
};