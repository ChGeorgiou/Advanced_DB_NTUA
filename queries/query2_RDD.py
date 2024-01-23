from pyspark.sql import SparkSession
import time, datetime
spark = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate() 

#data = spark.textFile("hdfs://okeanos-master:54310/user/user/csv_files/data.csv") \
#                .map(lambda x: (x.split(",(?![ ])")))

data = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)
data_rdd = data.rdd

start_time = time.time()

street = data_rdd.filter(lambda x: x["Premis Desc"] == "STREET")

time_of_day = street.map(lambda x: "Morning" if (int(x[3]) >= 500 and int(x[3]) <= 1159) \
else ("Afternoon" if (int(x[3]) >= 1200 and int(x[3]) <= 1659) \
else ("Evening" if (int(x[3]) >= 1700 and int(x[3]) <= 2059) \
else "Night"))).map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

end_time = time.time()

output_path = "hdfs://okeanos-master:54310/user/user/csv_files/Query2_RDD"
time_of_day.coalesce(1).saveAsTextFile(output_path)

print(time_of_day.collect())

print('Total time for RDD: ', end_time - start_time, 'sec')


