from pyspark.sql import SparkSession
import time, datetime
from io import StringIO
from contextlib import redirect_stdout

spark = SparkSession \
    .builder \
    .appName("SQL query 3 execution") \
    .config("spark.executor.instances", 2) \
    .getOrCreate()

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)
revgecoding_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/revgecoding.csv", header=True, inferSchema=True)
LA_income_2015_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/income/LA_income_2015.csv", header=True, inferSchema=True)

start_time = time.time()

# To utilize as SQL tables
data_df.createOrReplaceTempView("data")
revgecoding_df.createOrReplaceTempView("revgecoding")
LA_income_2015_df.createOrReplaceTempView("LA_income_2015")

# Write the main SQL query to count victims for the specified ZIP codes
query = """
    SELECT CASE
        WHEN d.`Vict Descent` = 'W' THEN 'White'
        WHEN d.`Vict Descent` = 'B' THEN 'Black'
        WHEN d.`Vict Descent` = 'A' THEN 'Other Asian'
		WHEN d.`Vict Descent` = 'C' THEN 'Chinese'
		WHEN d.`Vict Descent` = 'D' THEN 'Cambodian'
		WHEN d.`Vict Descent` = 'F' THEN 'Filipino'
		WHEN d.`Vict Descent` = 'G' THEN 'Guamanian'
		WHEN d.`Vict Descent` = 'H' THEN 'Hispanic/Latin/Mexican'
		WHEN d.`Vict Descent` = 'I' THEN 'American Indian/Alaskan Native'
		WHEN d.`Vict Descent` = 'J' THEN 'Japanese'
		WHEN d.`Vict Descent` = 'K' THEN 'Korean'
		WHEN d.`Vict Descent` = 'L' THEN 'Laotian'
		WHEN d.`Vict Descent` = 'O' THEN 'Other'
		WHEN d.`Vict Descent` = 'P' THEN 'Pacific Islander'
		WHEN d.`Vict Descent` = 'S' THEN 'Samoan'
		WHEN d.`Vict Descent` = 'U' THEN 'Hawaiian'
		WHEN d.`Vict Descent` = 'V' THEN 'Vietnamese'
		WHEN d.`Vict Descent` = 'Z' THEN 'Asian Indian'
		WHEN d.`Vict Descent` = 'X' THEN 'Unknown'
        ELSE d.`Vict Descent`  
    END AS Race,
	COUNT(*) AS victim_count
FROM data as d
INNER JOIN revgecoding as r
ON d.LAT=r.LAT AND d.LON=r.LON 
WHERE d.`Vict Descent` IS NOT NULL
AND YEAR(d.`DATE OCC`) = 2015
AND r.ZIPCode IN (
		(SELECT `Zip Code`
		FROM LA_income_2015 
		WHERE `Zip Code` IN (SELECT DISTINCT ZIPCode FROM revgecoding)
		ORDER BY CAST(regexp_replace(`Estimated Median Income`, '[^0-9.]', '') AS DECIMAL(18,2)) DESC
		LIMIT 3)
	UNION ALL
		(SELECT `Zip Code`
		FROM LA_income_2015 
		WHERE `Zip Code` IN (SELECT DISTINCT ZIPCode FROM revgecoding)
		ORDER BY CAST(regexp_replace(`Estimated Median Income`, '[^0-9.]', '') AS DECIMAL(18,2)) ASC
		LIMIT 3)
	)
GROUP BY d.`Vict Descent`
ORDER BY victim_count DESC
"""

query_broadcast = query.replace("SELECT CASE", "SELECT /*+ BROADCAST(r) */ CASE")
query_merge = query.replace("SELECT CASE", "SELECT /*+ MERGE(r) */ CASE")
query_shuffle_hash = query.replace("SELECT CASE", "SELECT /*+ SHUFFLE_HASH(r) */ CASE")
query_shuffle_replicate_nl = query.replace("SELECT CASE", "SELECT /*+ SHUFFLE_REPLICATE_NL(r) */ CASE")


# Execute the main SQL query
result = spark.sql(query)

with StringIO() as buffer:
    with redirect_stdout(buffer):
        result.explain()
    physical_plan_str = buffer.getvalue()

with open("./explain/query3/explain_normal.txt", "w") as explain_file:
    explain_file.write(physical_plan_str)
#result.explain()

end_time = time.time()
#result.show(result.count(), truncate=False)
result.write.csv("/user/user/csv_files/Query3_SQL", header=True,
        mode="overwrite")

print('Total time for SQL: ', end_time - start_time, 'sec')

