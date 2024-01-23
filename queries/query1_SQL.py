from pyspark.sql import SparkSession
import time, datetime
spark = SparkSession \
    .builder \
    .appName("SQL query 1 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)

start_time = time.time()
# To utilize as SQL tables
data_df.createOrReplaceTempView("data")

query = "WITH ranked_crimes AS (\
    SELECT \
        YEAR(`DATE OCC`) AS year, \
        MONTH(`DATE OCC`) AS month, \
        COUNT(*) AS crime_total, \
        DENSE_RANK() OVER (PARTITION BY YEAR(`DATE OCC`) ORDER BY COUNT(*) DESC) AS crime_rank \
    FROM data \
    GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)) \
SELECT year, month, crime_total, crime_rank \
FROM ranked_crimes \
WHERE crime_rank <= 3 \
ORDER BY year ASC, crime_total DESC "

result = spark.sql(query)
end_time = time.time()

result.show(result.count(), truncate=False)
result.write.csv("/user/user/csv_files/Query1_SQL", header=True, mode="overwrite")
print('Total time for SQL: ', end_time - start_time, 'sec')

