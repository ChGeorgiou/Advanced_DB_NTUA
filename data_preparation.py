import pandas as pd

d1 = pd.read_csv('Crime_Data_from_2010_to_2019.csv')
d2 = pd.read_csv('Crime_Data_from_2020_to_Present.csv')

d1.rename(columns={"AREA ": "AREA"}, inplace=True)
d2.rename(columns={"AREA ": "AREA"}, inplace=True)

data = pd.concat([d1, d2], ignore_index=True, sort=False).drop_duplicates()
data = data.astype({"Date Rptd": 'datetime64[ns]', "DATE OCC": 'datetime64[ns]', "Vict Age": int, "LAT": float, "LON": float})

print("Total number of rows: " + str(len(data)))
print()
print("Data types of all columns:")
print(data.dtypes)

data.to_csv('data.csv', index=False)

import subprocess
hdfs_command = f'hdfs dfs -copyFromLocal -f data.csv /user/user/csv_files/'


subprocess.run(hdfs_command, shell=True)

from pyspark.sql import SparkSession
import time, datetime
spark = SparkSession \
    .builder \
    .appName("SQL query 3 execution") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

data_df = spark.read.csv("hdfs://okeanos-master:54310/user/user/csv_files/data.csv", header=True, inferSchema=True)
data_df.printSchema()

