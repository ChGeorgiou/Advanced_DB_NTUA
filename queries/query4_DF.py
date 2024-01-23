from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, row_number, month, dayofmonth, count, dense_rank, desc, avg, asc, min, monotonically_increasing_id
from pyspark.sql.window import Window
import time, datetime
import geopy.distance
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from io import StringIO
from contextlib import redirect_stdout

@udf(FloatType())
def get_distance(lat1 , long1 , lat2 , long2):
    return geopy.distance.geodesic((lat1 , long1), (lat2 , long2)).km

spark = SparkSession \
    .builder \
    .appName("DF query 4 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)
LAPD_Police_Stations_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/LAPD_Police_Stations.csv", header=True, inferSchema=True)

start_time = time.time()

filtered_data = data_df.filter((col("LAT") != 0.0) & (col("LON") != 0.0) & col("Weapon Used Cd").isNotNull() & (col("Weapon Used Cd")<=199.9) & (col("Weapon Used Cd")>=100.0))

# 4.1a
data_a = filtered_data.withColumn("year", year(col("DATE OCC")))
data_1a = data_a.select(["year", "LAT", "LON", "AREA"])
lapd_a = LAPD_Police_Stations_df.select(["X", "Y", "PREC"])
joined1 = data_1a.join(lapd_a, data_1a["AREA"] == lapd_a["PREC"], "inner")
#joined1 = data_1a.join(lapd_a.hint("BROADCAST"), data_1a["AREA"] == lapd_a["PREC"], "inner")
#joined1 = data_1a.join(lapd_a.hint("MERGE"), data_1a["AREA"] == lapd_a["PREC"], "inner")
#joined1 = data_1a.join(lapd_a.hint("SHUFFLE_HASH"), data_1a["AREA"] == lapd_a["PREC"], "inner")
#joined1 = data_1a.join(lapd_a.hint("SHUFFLE_REPLICATE_NL"), data_1a["AREA"] == lapd_a["PREC"], "inner")

result1 = joined1.withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))\
    .groupBy("year").agg(avg("dist").alias("average_distance"), count("*").alias("crime_count"))\
    .orderBy(asc("year"))

# 4.1b
data_b = filtered_data.select(["LAT", "LON", "AREA"]).withColumnRenamed("AREA", "PREC")
lapd_b = LAPD_Police_Stations_df.select(["X", "Y", "PREC", "DIVISION"])
joined2 = data_b.join(lapd_b, on="PREC", how="inner").withColumnRenamed("DIVISION", "division")
#joined2 = data_b.join(lapd_b.hint("BROADCAST"), on="PREC", how="inner").withColumnRenamed("DIVISION", "division")
#joined2 = data_b.join(lapd_b.hint("MERGE"), on="PREC", how="inner").withColumnRenamed("DIVISION", "division")
#joined2 = data_b.join(lapd_b.hint("SHUFFLE_HASH"), on="PREC", how="inner").withColumnRenamed("DIVISION", "division")
#joined2 = data_b.join(lapd_b.hint("SHUFFLE_REPLICATE_NL"), on="PREC", how="inner").withColumnRenamed("DIVISION", "division")

result2 = joined2.withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))\
    .groupBy("division").agg(avg("dist").alias("average_distance"), count("*").alias("crime_count"))\
    .orderBy(desc("crime_count"))

#4.2a
data_2a = data_a.select(["year", "LAT", "LON"]).withColumn("ID", monotonically_increasing_id())
joined3 = data_2a.crossJoin(lapd_a)
#joined3 = data_2a.crossJoin(lapd_a.hint("BROADCAST"))
#joined3 = data_2a.crossJoin(lapd_a.hint("MERGE"))
#joined3 = data_2a.crossJoin(lapd_a.hint("SHUFFLE_HASH"))
#joined3 = data_2a.crossJoin(lapd_a.hint("SHUFFLE_REPLICATE_NL"))


result3 = joined3.withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))\
        .groupBy("ID", "year").agg(min("dist").alias("min_distance"))\
        .groupBy("year").agg(avg("min_distance").alias("average_min_distance"), count("*").alias("crime_count"))\
        .orderBy(asc("year"))

#4.2b
data_2b = data_b.select(["LAT", "LON"]).withColumn("ID", monotonically_increasing_id())
joined4 = data_2b.crossJoin(lapd_b).withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
#joined4 = data_2b.crossJoin(lapd_b.hint("BROADCAST")).withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
#joined4 = data_2b.crossJoin(lapd_b.hint("MERGE")).withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
#joined4 = data_2b.crossJoin(lapd_b.hint("SHUFFLE_HASH")).withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
#joined4 = data_2b.crossJoin(lapd_b.hint("SHUFFLE_REPLICATE_NL")).withColumn("dist", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

window_spec = Window.partitionBy("ID").orderBy("dist")
ranked_df = joined4.withColumn("rank", row_number().over(window_spec))
closest_div = ranked_df.filter(col("rank") == 1).drop("rank")
result4 = closest_div.groupBy("DIVISION").agg(avg("dist").alias("average_min_distance"), count("*").alias("crime_count"))\
           .orderBy(desc("crime_count"))

# Show the result
end_time = time.time()

result1.show(result1.count(), truncate=False)
result2.show(result2.count(), truncate=False)
result3.show(result3.count(), truncate=False)
result4.show(result4.count(), truncate=False)
#print(result3.count())

with StringIO() as buffer:
    with redirect_stdout(buffer):
        result1.explain()
        result2.explain()
        result3.explain()
        result4.explain()
    physical_plan_str = buffer.getvalue()

with open("./explain/query4/explain_normal.txt", "w") as explain_file:
    explain_file.write(physical_plan_str)
#result.explain()

# Save into csv
result1.write.csv("/user/user/csv_files/Query4-1a_DF", header=True, mode="overwrite")
result2.write.csv("/user/user/csv_files/Query4-1b_DF", header=True, mode="overwrite")
result3.write.csv("/user/user/csv_files/Query4-2a_DF", header=True, mode="overwrite")
result4.write.csv("/user/user/csv_files/Query4-2b_DF", header=True, mode="overwrite")

print('Total time for DF: ', end_time - start_time, 'sec')

