# Carolyn Mason
# 12/12/18
# Big Data Analytics CSCIE-63

# Join the data sets
from pyspark.sql.types import *
from pyspark.sql.functions import expr, desc, col
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType

# Custom schemas
#cylinders,displ,drive,fueltype,make,model,ucity,uhighway,transmission,vclass,year,rn
fields = [StructField("cylinders",FloatType(),True), StructField("displ",FloatType(),True), StructField("drive",StringType(),True),StructField("fueltype", StringType(), True),StructField("make", StringType(),True), StructField("model",StringType(),True), StructField("ucity",FloatType(),True), StructField("uhighway",FloatType(),True), StructField("transmission",StringType(),True), StructField("vclass",StringType(),True), StructField("year",IntegerType(),True), StructField("rn",IntegerType(),True)]
fuelSchema = StructType(fields)
#title,url,price,address,vin,odometer,condition,cylinders,drive,fuel,paint_color,size,title_status,transmission,type,year,make,model,description
fields2 = [StructField("title",StringType(),True), StructField("url",StringType(),True), StructField("price",FloatType(),True),StructField("address", StringType(), True),StructField("vin", StringType(),True), StructField("odometer",FloatType(),True), StructField("condition",StringType(),True), StructField("cylinders",FloatType(),True), StructField("drive",StringType(),True), StructField("fuel",StringType(),True), StructField("paint_color",StringType(),True), StructField("size",StringType(),True), StructField("title_status",StringType(),True), StructField("transmission",StringType(),True),StructField("type",StringType(),True), StructField("year",IntegerType(),True),StructField("make",StringType(),True), StructField("model",StringType(),True), StructField("description",StringType(),True)]
dataSchema = StructType(fields2)

# Loads csv's to data frames
#df_fuel = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("fuel_simple.csv")
#df_craigslist = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("vans.csv")
df_fuel = spark.read.format("csv").option("header", "true").schema(fuelSchema).load("fuel_simple.csv")
df_craigslist = spark.read.format("csv").option("header", "true").schema(dataSchema).load("vans.csv")


# Create tables to query using SQL
df_fuel.createOrReplaceTempView("fuel")
df_craigslist.createOrReplaceTempView("data")

# Joins
#cylinders,displ,drive,fueltype,make,model,ucity,uhighway,transmission,vclass,year,rn
#title,url,price,address,vin,odometer,condition,cylinders,drive,fuel,paint_color,size,title_status,transmission,type,year,make,model,description
df_combined = spark.sql("SELECT data.title,data.make,data.model,data.year,data.transmission,fuel.ucity,fuel.uhighway,fuel.vclass,data.size,fuel.drive,fuel.fueltype,data.cylinders,data.fuel,fuel.displ,data.url,data.price,data.odometer,data.condition,data.address,data.vin,data.paint_color,data.title_status,data.type,data.description FROM data LEFT OUTER JOIN fuel ON fuel.make=data.make AND fuel.model=data.model AND fuel.transmission=data.transmission AND fuel.year=data.year AND fuel.cylinders=data.cylinders")

# Create table for sql queries
df_combined.createOrReplaceTempView("query")

# Return useful information
df_ans = spark.sql("SELECT make,model,year,price,ucity,uhighway,vclass,condition FROM query WHERE uhighway IS NOT NULL AND (vclass LIKE '%van%' OR description LIKE '%sprinter%') AND condition!='fair' AND vclass NOT LIKE '%minivan%' AND description NOT LIKE '%mini%' ORDER BY uhighway DESC, price")

# Print results
df_ans.show(100,False)

# result for plotting
df_ans = spark.sql("SELECT * FROM query WHERE uhighway IS NOT NULL AND price < 50000 AND vin NOT LIKE '%0%'")

# More visuals!!
# Create visuals of the data
import matplotlib
import matplotlib.pyplot as plt


# Add information
df_ans = df_ans.toPandas()
df_ans = df_ans.dropna()
df_ans = df_ans.sort_values(by=['displ'])

plt.subplot(221)
x = df_ans['displ']
y = df_ans['uhighway']
plt.title("Engine Displacement vs Mpg Highway")
plt.xlabel("engine displacement")
plt.ylabel("mpg highway")
l1 = plt.scatter(x,y)

plt.subplot(222)
x = df_ans['year']
y = df_ans['price']
plt.title("Price vs Year")
plt.xlabel("year")
plt.ylabel("price")
l1 = plt.scatter(x,y)

plt.subplot(223)
x = df_ans['condition']
y = df_ans['price']
plt.title("Price vs Condition")
plt.xlabel("condition")
plt.ylabel("price")
l1 = plt.scatter(x,y)

plt.subplot(224)
x = df_ans['uhighway']
y = df_ans['price']
plt.title("Mpg highway vs Price")
plt.xlabel("mpg highway")
plt.ylabel("price")
l1 = plt.scatter(x,y)

l1 = plt.tight_layout()
plt.show()



