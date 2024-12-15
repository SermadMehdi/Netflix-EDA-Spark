from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, year, to_date, split, explode, desc

# Step 1: Initialize SparkSession
spark = SparkSession.builder.appName("Netflix EDA Enhanced").getOrCreate()

# Step 2: Load Netflix Dataset
print("Loading the Netflix dataset...")
df = spark.read.csv("netflix_titles.csv", header=True, inferSchema=True)

# Step 3: Display Dataset Information
print("Schema of the Dataset:")
df.printSchema()
print("First 5 Rows of the Dataset:")
df.show(5)

# Step 4: Count Total Rows and Columns
total_rows = df.count()
total_columns = len(df.columns)
print(f"Total Rows: {total_rows}, Total Columns: {total_columns}")

# Step 5: Check for Missing Values
print("Checking for Missing Values...")
missing_values = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()

# Step 6: Distribution of Content by Type (Movies vs TV Shows)
print("Movies vs TV Shows Distribution:")
df.groupBy("type").count().orderBy(desc("count")).show()

# Step 7: Analyze Ratings
print("Content Distribution by Ratings:")
df.groupBy("rating").count().orderBy(desc("count")).show()

# Step 8: Analyze Year-wise Content Addition
print("Analyzing Content Added Over Time...")
df = df.withColumn("release_year", year(to_date("date_added", "MMMM d, yyyy")))
df.groupBy("release_year").count().orderBy("release_year").show()

# Step 9: Analyze Genres (Explode Genre Column)
print("Analyzing Content by Genres...")
df = df.withColumn("genres", explode(split(col("listed_in"), ", ")))
genre_distribution = df.groupBy("genres").count().orderBy(desc("count"))
genre_distribution.show(10)

# Step 10: Top Countries Producing Content
print("Top 10 Countries by Content Count:")
country_distribution = df.groupBy("country").count().orderBy(desc("count"))
country_distribution.show(10)

# Step 11: Analyze Content Duration
if "duration" in df.columns:
    print("Analyzing Content Duration...")
    df_movies = df.filter(df.type == "Movie")
    df_movies.select("title", "duration").orderBy(desc("duration")).show(10)

# Step 12: Save Results to CSV (Optional)
print("Saving results to CSV for visualization...")
df.groupBy("type").count().write.csv("output/type_distribution.csv", header=True)
genre_distribution.write.csv("output/genre_distribution.csv", header=True)
country_distribution.write.csv("output/country_distribution.csv", header=True)

# Step 13: Final Dataset Summary
print(f"Summary of the Dataset (Total Rows: {total_rows}, Total Columns: {total_columns}):")
missing_values.show()
print("EDA Completed!")

# Stop SparkSession
spark.stop()
