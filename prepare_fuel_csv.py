# Carolyn Mason
# 12/11/18
# CSCIE-63 Big Data Analytics

# Get the fuel.csv up and going
from pyspark.sql.functions import regexp_replace, col
import Pandas

# Pair down the fuel data frame  
df_fuel = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("fuel.csv")
df_fuel = df_fuel.withColumn('trany', regexp_replace('trany', 'Automatic.*', 'automatic'))
df_fuel = df_fuel.withColumn('trany', regexp_replace('trany', 'Manual.*', 'manual'))

# Remove duplicates:
df_fuel.createOrReplaceTempView("fuel")
ans = spark.sql("SELECT * FROM (select cylinders,displ,drive,fuelType AS fueltype,make,model,UCity AS ucity,UHighway AS uhighway,trany AS transmission,VClass as vclass,year, row_number() OVER (PARTITION BY make,model,year,trany,cylinders ORDER BY displ DESC) AS rn FROM fuel) AS dt WHERE rn =1 ORDER BY make,model,year")

# Create csv to read
ans.toPandas().to_csv('fuel_simple.csv', encoding='utf-8', index=False)

# MUST MAKE ALL ITEMS IN CSV LOWERCASE-- currently doing this by hand

