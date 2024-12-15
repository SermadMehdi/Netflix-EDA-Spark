# Netflix EDA with Spark

This project performs Exploratory Data Analysis (EDA) on the Netflix dataset using Apache Spark and Python. The dataset contains information about TV shows and movies available on Netflix.

## Dataset
The dataset used is [Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows), available on Kaggle.

- **File**: `data/netflix_titles.csv`

## Steps

1. **Setup Environment**:
   - Run Apache Spark in Docker using the `bitnami/spark` image.
   - Install necessary Python libraries (`pyspark`, `pandas`, `matplotlib`, `seaborn`).

2. **Run the Code**:
   - Use `spark-submit` to execute the script:
     ```bash
     spark-submit netflix_eda.py
     ```

3. **Features**:
   - Analyze missing data.
   - Compare TV shows and movies.
   - Study ratings distribution and trends over the years.
   - Explore genres and content-producing countries.
   - Generate visualizations and export results to CSV.

4. **Results**:
   - Outputs are saved in the `output/` folder:
     - Bar charts for type, ratings, and countries.
     - Line chart for content addition over years.
     - CSV files for further analysis.

## How to Use
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/Netflix-EDA-Spark.git
