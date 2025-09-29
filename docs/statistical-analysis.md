# Comprehensive Data Analysis and Visualization Project : Phase 2 Data Analysis 

**Author(s):** [Isaias Lopez, Joaquin Murguia, Valeria Paredes, Damaris Pech and Krishna Sandoval]
**Date:** [28/09/2025]  
**Course:** Visual Modeling Information  
**Program:** Data Engineering  
**Institution:** Universidad Politécnica de Yucatán  

---

## AI Assistance Disclosure

This document was created with assistance from AI tools. The following outlines the nature and extent of AI involvement:

- **AI Tool Used:** ChatGPT
- **Type of Assistance:** Code generation and Data analysis.
- **Extent of Use:** Complete code generation with human review.
- **Human Contribution:** The code generated entirely by artificial intelligence, the student reviewed each answer, as sometimes it was not correct or the result obtained was incomplete.

-  **Prompt link:** The link does not find because my classmate delete the prompt.

- **AI Assistance:** 100%

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

This project focuses on the exploratory and comparative analysis of a massive movie dataset (IMDb) using two distinct data storage and processing paradigms: Relational (CSV file loaded into a DataFrame) and Non-Relational (JSON file). The primary goal is to evaluate the ease of implementation, data preparation requirements, and extraction of key descriptive statistics in each environment to draw conclusions about their respective suitability for structured data analysis.



## Objectives

- [ ]  Load and structure the movie dataset using a Relational data approach (CSV file).
- [ ] Transform and load the same dataset using a Non-Relational data approach (JSON file).
- [ ] Perform key descriptive statistics and exploratory data analysis (EDA) in both environments to extract findings and compare the efficiency and process complexity of each method.

## Methodology

### Data Sources
- **Dataset 1:** Internet Movie Database (`IMDb_movies_final.csv`, `IMDb_movies_final.csv`)
According to Britannica IMDb provides information about millions of films and television programs as well as their cast and crew. 
This database is from [sahildit](https://github.com/sahildit/IMDB-Movies-Extensive-Dataset-Analysis/tree/master) Github.

- **Dataset 2:**  IMDb Movies Final JSON (Derived JSON file, movies.json). This is the same dataset exported from CSV to JSON format using the orient='records' parameter to facilitate the Non-Relational analysis.

### Tools and Technologies
- **Database:** 
- PostgreSQL: It an open-source descendant of this original Berkeley code. It supports a large part of the SQL standard and offers many modern features: complex queries, foreign keys, triggers, updatable views, transactional integrity and multiversion concurrency control.

- MongoDB: It stores data in flexible JSON-like documents, so fields can vary between documents and the data structure can change over time. The document model is mapped to objects in your application code to make it easier to work with the data.

- **Programming Language:** 

- Python: It is an interpreted, object-oriented, high-level programming language with dynamic semantics. Its high-level built in data structures, combined with dynamic typing and dynamic binding, make it very attractive for Rapid Application Development, as well as for use as a scripting or glue language to connect existing 
components together.

- **Libraries:** 
- pandas:  It is a powerful and widely-used open-source Python library designed for data manipulation, analysis, and exploration. It provides flexible and efficient tools to work with structured data, such as tables or time series.

- matplotlib:  It is a powerful and widely-used library in Python for data visualization. It allows users to create a variety of static, animated, and interactive plots to effectively represent data.

- scikit-learn: It is a popular open-source Python library for machine learning. It provides simple, efficient tools for data analysis and modeling.

- **Visualization:**

- seaborn: It is a powerful and user-friendly Python library designed for creating statistical graphics. It is built on top of Matplotlib and integrates seamlessly with Pandas data structures, making it an excellent tool for data visualization and analysis.

- scipy: It is an open-source library for scientific and technical computing in Python. It is built on top of NumPy and provides additional functionality for tasks such as optimization, integration, interpolation, eigenvalue problems, signal processing, and more. SciPy is widely used in scientific research, engineering, and data analysis due to its efficiency and ease of use.

### Approach
The methodology involved a two-phase data processing pipeline. 

- The primary CSV source was loaded directly using the efficient pd.read_csv method to simulate a Relational (structured, tabular) environment.

- The data was converted to a JSON format, loaded using custom Python functions and the json library, and then converted back into a pandas DataFrame (pd.DataFrame(data)) for the Non-Relational analysis, focusing on the loading and parsing complexity. 

- Finally, descriptive statistics were calculated on both resulting DataFrames to ensure data consistency and to compare the processing steps required for each data type.

## Implementation

### Phase 1: Relational Data Analysis (CSV)

The CSV file was loaded directly. Initial exploration confirmed the dataset shape to be (85736, 16). The initial data types required attention, specifically the `year` column which was initially a float64 and was found to have one null value, suggesting a necessary data type conversion (to integer) and minor missing value handling before advanced analysis.

**Code Example:**
```python
df = pd.read_csv('imdb_movies_final.csv')
print(df.describe())
```

### Phase 2: Non-Relational Data Analysis (JSON)

The non-relational approach involved two steps: 
1.  converting the CSV data into JSON format df.to_json(..., orient=`records`)

2. implementing a custom load_json_data function using the json library to handle the data loading before converting it to a DataFrame. This confirmed that while the final data structure was tabular, the loading phase was more involved in the JSON environment, requiring explicit handling of the list of records structure. The final data overview mirrored the relational result, showing (85736, 16).

**Code Example:**
```python
def load_json_data(file_path):
    """Load data from JSON file and convert to DataFrame"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # If JSON is a list of objects, convert directly to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        # If JSON has nested structure, you might need to normalize it
        elif isinstance(data, dict):
            df = pd.json_normalize(data)
        else:
            raise ValueError("Unsupported JSON structure")
        
        return df
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return None
```

### Phase 3:  Data Cleaning and Feature Analysis

Across both phases, standard data cleaning involved:

- Identifying and noting columns with null values (e.g., year (1 missing), `language`, `reviews_from_critics`, `reviews_from_users`, etc.).

- Converting non-null numerical columns to appropriate types (e.g., `year` from float to int for time series analysis).

- Analyzing the distributions of key numerical features such as `duration`, `avg_vote`, and `votes`.

**Code Example:**
```python
# Check available numeric columns
numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
print(f"Numeric columns available: {numeric_columns}")

# Check available categorical columns
categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
print(f"Categorical columns available: {categorical_columns}")


```

## Results

### Key Findings
1. **Finding 1** Dataset Size and Span: The dataset contains a total of 85,736 movies. The cinematic history covered ranges from 1894 (earliest entry) up to 2020 (latest entry).

2. **Finding 2** Central Tendency for Duration: The movies exhibit a mean duration of 100.35 minutes and a median duration of 96.00 minutes. This difference suggests a slight skew in the distribution, with a few longer movies pulling the mean up.

3. **Finding 3** Duration Distribution (Quartiles): 50% of all movies in the dataset have a duration between 88 minutes (25th percentile) and 108 minutes (75th percentile).

4. **Finding 4** Rating Statistics: The overall average IMDb vote is 5.90. The distribution shows a wide range, with a minimum rating of 1.0 and a maximum of 9.9.

5. **Finding 5** Global Production Focus: The most common language featured in the movies is English, and the country with the highest production volume is the USA.

### Visualizations

## Descriptive Statistics
![alt text](/img/image16.png)

# Movie Rating 
![alt text](/img/image17.png)

## Correlation 

![alt text](/img/image18.png)

## Hypotesis Tesis 

![alt text](/img/image19.png)

## Clustering analysis 

![alt text](/img/image20.png)

## Preticte Modeling 

![alt text](/img/image22.png)

### Performance Metrics

![alt text](/img/image21.png)

## ROC Curves 

![alt text](/img/image23.png)

## Conclusions

### Summary

The project successfully demonstrated the equivalent statistical results obtained from handling the same structured dataset in both Relational (CSV) and Non-Relational (JSON) formats. Key statistics, such as the mean movie duration (100.35 minutes) and median duration (96.00 minutes), were validated across both approaches, confirming data integrity. The major difference was observed in the initial data loading and preparation complexity, with the JSON approach requiring more explicit parsing logic.


### Lessons Learned
- Relational Simplicity: For highly structured data with a fixed schema (like this IMDb dataset), the Relational (CSV) approach using `pandas.read_csv` is significantly more straightforward and requires less custom code for initial ingestion.
- Non-Relational Flexibility vs. Complexity: While JSON offers greater flexibility for deeply nested or schema-less data (not fully leveraged here), loading this particular "records" oriented JSON required a specialized function using the json library, adding an intermediate step compared to CSV.

- Data Quality: The data quality checks revealed minor issues ( a single missing `year` value and floating-point data types for years), which must be addressed regardless of the initial file format.

### Future Work
- Performance Benchmarking: Implement rigorous time benchmarking of loading and complex query execution (e.g., filtering, aggregation) using actual database systems (e.g., SQLite for Relational vs. MongoDB for Non-Relational) to provide a quantitative efficiency comparison.

- Advanced Modeling: Leverage the imported `sklearn` libraries to perform advanced analysis, such as K-Means clustering to identify groups of movies based on features like `duration`, `avg_vote`, and `votes`, or a Random Forest Classifier to predict high-rated movies.

- Deep Feature Engineering: Conduct detailed feature engineering on text-based columns `description`, `actors`, `writer` using Natural Language Processing (NLP) techniques to identify correlations between textual content and high average ratings.

## References

1. [scikit-learn: machine learning in Python, A. (2025). scikit-learn 1.7.2 documentation.]
2. [https://scikit-learn.org/stable/index.html]
3. [https://www.britannica.com/topic/IMDb]
4. [What is PostgreSQL?, A. (2025). “PostgreSQL Documentation,”]
5. [https://www.postgresql.org/docs/current/intro-whatis.html]
6. [MongoDB, A. (2025). “¿Qué es MongoDB?,” MongoDB.]
7. [https://www.mongodb.com/es/company/what-is-mongodb?msockid=36904fd7b86269e2111b59a7b9ec68fb]
8. [What is Python? Executive Summary, A. (2025).  Python.org.]
9. [https://www.python.org/doc/essays/blurb/]
10. [pandas, A. (2025). Python Data Analysis Library.]
11. [https://pandas.pydata.org/]
12. [Matplotlib, A. (2025). Visualization with Python.]
13. [https://matplotlib.org/]
14. [seaborn, A. (2024). PyPI.]
15. [https://pypi.org/project/seaborn/]
16. [imdb_movies final.csv]

---

**Note:** This document is part of the academic portfolio for the Data Engineering program at Universidad Politécnica de Yucatán.
