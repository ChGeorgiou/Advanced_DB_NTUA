from pyspark.sql import SparkSession
import time, datetime
spark = SparkSession \
    .builder \
    .appName("SQL query 2 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)

# To utilize as SQL tables
data_df.createOrReplaceTempView("data")

start_time = time.time()

query = "SELECT COUNT(*) AS crime_count, \
    CASE \
        WHEN `TIME OCC` BETWEEN 500 AND 1159 THEN 'Morning' \
        WHEN `TIME OCC` BETWEEN 1200 AND 1659 THEN 'Afternoon' \
        WHEN `TIME OCC` BETWEEN 1700 AND 2059 THEN 'Evening' \
        ELSE 'Night' \
    END AS time_of_day \
FROM data \
WHERE `Premis Desc` = 'STREET' \
GROUP BY time_of_day \
ORDER BY crime_count DESC "

result = spark.sql(query)
end_time = time.time()
result.show(result.count(), truncate=False)
result.write.csv("/user/user/csv_files/Query2_SQL", header=True, mode="overwrite")
print('Total time for SQL: ', end_time - start_time, 'sec')

