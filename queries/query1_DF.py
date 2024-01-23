from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, count, dense_rank, desc
from pyspark.sql.window import Window
import time, datetime

spark = SparkSession \
    .builder \
    .appName("DF query 1 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate() 

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)

start_time = time.time()

d = data_df.withColumn("year", year(col("DATE OCC"))).withColumn("month", month(col("DATE OCC"))) 
crime_counts = d.groupBy("year", "month").agg(count("*").alias("crime_count"))
window_spec = Window.partitionBy("year").orderBy(desc("crime_count"))
ranked_crimes = crime_counts.withColumn("rank", dense_rank().over(window_spec))

# Filter only the top-3 ranked months for each year
top3_months_per_year = ranked_crimes.filter(col("rank") <= 3)

# Show the result
result = top3_months_per_year.orderBy("year", desc("crime_count"))

end_time = time.time()

result.show(result.count(), truncate=False)

# Save into csv
result.write.csv("/user/user/csv_files/Query1_DF", header=True, mode="overwrite")

print('Total time for DF: ', end_time - start_time, 'sec')

