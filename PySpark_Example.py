# -*- coding: utf-8 -*-
"""
Erickson, Holly

"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Week4").getOrCreate()

#%%
"""
1.Load both files into Spark and print the schemas
Data located at https://github.com/bellevue-university/dsc650/tree/master/data
"""
df_flights = spark.read.parquet("C:/Master/Semester 5/data/domestic-flights/flights.parquet")
df_airport_codes = spark.read.load(
  "C:/Master/Semester 5/data/airport-codes/airport-codes.csv",
  format="csv",
  sep=",",
  inferSchema=True,
  header=True
)

df_flights.printSchema()
df_airport_codes.printSchema()

"""
Output:

root
 |-- origin_airport_code: string (nullable = true)
 |-- destination_airport_code: string (nullable = true)
 |-- origin_city: string (nullable = true)
 |-- destination_city: string (nullable = true)
 |-- passengers: long (nullable = true)
 |-- seats: long (nullable = true)
 |-- flights: long (nullable = true)
 |-- distance: double (nullable = true)
 |-- origin_population: long (nullable = true)
 |-- destination_population: long (nullable = true)
 |-- flight_year: long (nullable = true)
 |-- flight_month: long (nullable = true)
 |-- __index_level_0__: long (nullable = true)

root
 |-- ident: string (nullable = true)
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- elevation_ft: double (nullable = true)
 |-- continent: string (nullable = true)
 |-- iso_country: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- municipality: string (nullable = true)
 |-- gps_code: string (nullable = true)
 |-- iata_code: string (nullable = true)
 |-- local_code: string (nullable = true)
 |-- coordinates: string (nullable = true)

"""

#%%
"""
a. Join the Data
Left join the data keeping all data in df_flights
Print the schema of the joined dataframe.
"""
joinType = "outer"
df_join = df_flights.join(df_airport_codes, df_flights.origin_airport_code == df_airport_codes.iata_code, how = "left")
df_join.printSchema()

"""
Output:
    
root
 |-- origin_airport_code: string (nullable = true)
 |-- destination_airport_code: string (nullable = true)
 |-- origin_city: string (nullable = true)
 |-- destination_city: string (nullable = true)
 |-- passengers: long (nullable = true)
 |-- seats: long (nullable = true)
 |-- flights: long (nullable = true)
 |-- distance: double (nullable = true)
 |-- origin_population: long (nullable = true)
 |-- destination_population: long (nullable = true)
 |-- flight_year: long (nullable = true)
 |-- flight_month: long (nullable = true)
 |-- __index_level_0__: long (nullable = true)
 |-- ident: string (nullable = true)
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- elevation_ft: double (nullable = true)
 |-- continent: string (nullable = true)
 |-- iso_country: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- municipality: string (nullable = true)
 |-- gps_code: string (nullable = true)
 |-- iata_code: string (nullable = true)
 |-- local_code: string (nullable = true)
 |-- coordinates: string (nullable = true)

"""

#%%
"""
b. Rename and Remove Columns
"""
remove = ["__index_level_0__", "ident", "local_code", "continent", "iso_country", "iata_code"]
df_remove = df_join.select([column for column in df_join.columns if column not in remove])
#print(df_join.columns)
#print(df_remove.columns)

df_renamed = df_remove.withColumnRenamed("type","origin_airport_type") \
    .withColumnRenamed("name", "origin_airport_name").withColumnRenamed( "elevation_ft", "origin_airport_elevation_ft") \
        .withColumnRenamed("iso_region", "origin_airport_region").withColumnRenamed("municipality", "origin_airport_municipality") \
        .withColumnRenamed("gps_code", "origin_airport_gps_code").withColumnRenamed( "coordinates", "origin_airport_coordinates")
df_renamed.printSchema()

"""
Output:
    
root
 |-- origin_airport_code: string (nullable = true)
 |-- destination_airport_code: string (nullable = true)
 |-- origin_city: string (nullable = true)
 |-- destination_city: string (nullable = true)
 |-- passengers: long (nullable = true)
 |-- seats: long (nullable = true)
 |-- flights: long (nullable = true)
 |-- distance: double (nullable = true)
 |-- origin_population: long (nullable = true)
 |-- destination_population: long (nullable = true)
 |-- flight_year: long (nullable = true)
 |-- flight_month: long (nullable = true)
 |-- origin_airport_type: string (nullable = true)
 |-- origin_airport_name: string (nullable = true)
 |-- origin_airport_elevation_ft: double (nullable = true)
 |-- origin_airport_region: string (nullable = true)
 |-- origin_airport_municipality: string (nullable = true)
 |-- origin_airport_gps_code: string (nullable = true)
 |-- origin_airport_coordinates: string (nullable = true)



"""

#%%
"""
c. Join to Destination Airport
Join the airport codes file to the destination airport
Drop the same columns and rename the same columns using the prefix destination_airport_ instead of origin_airport_
The final schema and dataframe should contain the added information for the destination and origin airports.
"""

df_join2 = df_renamed.join(df_airport_codes, df_flights.destination_airport_code == df_airport_codes.iata_code, how = "left")
#df_join2.printSchema()
df_remove2 = df_join2.select([column for column in df_join2.columns if column not in remove])
#print(df_join2.columns)

df_renamed2 = df_remove2.withColumnRenamed("type","destination_airport_type") \
    .withColumnRenamed("name", "destination_airport_name").withColumnRenamed( "elevation_ft", "destination_airport_elevation_ft") \
        .withColumnRenamed("iso_region", "destination_airport_region").withColumnRenamed("municipality", "destination_airport_municipality") \
        .withColumnRenamed("gps_code", "destination_airport_gps_code").withColumnRenamed( "coordinates", "destination_airport_coordinates")

#print(df_flights.count())
#print(df_renamed2.count())
df_renamed2.printSchema()

"""
Output:
    
root
 |-- origin_airport_code: string (nullable = true)
 |-- destination_airport_code: string (nullable = true)
 |-- origin_city: string (nullable = true)
 |-- destination_city: string (nullable = true)
 |-- passengers: long (nullable = true)
 |-- seats: long (nullable = true)
 |-- flights: long (nullable = true)
 |-- distance: double (nullable = true)
 |-- origin_population: long (nullable = true)
 |-- destination_population: long (nullable = true)
 |-- flight_year: long (nullable = true)
 |-- flight_month: long (nullable = true)
 |-- origin_airport_type: string (nullable = true)
 |-- origin_airport_name: string (nullable = true)
 |-- origin_airport_elevation_ft: double (nullable = true)
 |-- origin_airport_region: string (nullable = true)
 |-- origin_airport_municipality: string (nullable = true)
 |-- origin_airport_gps_code: string (nullable = true)
 |-- origin_airport_coordinates: string (nullable = true)
 |-- id: long (nullable = false)
 |-- destination_airport_type: string (nullable = true)
 |-- destination_airport_name: string (nullable = true)
 |-- destination_airport_elevation_ft: double (nullable = true)
 |-- destination_airport_region: string (nullable = true)
 |-- destination_airport_municipality: string (nullable = true)
 |-- destination_airport_gps_code: string (nullable = true)
 |-- destination_airport_coordinates: string (nullable = true)

"""

#%%
"""
d.Create a dataframe using only data from 2008. 
This dataframe will be a report of the top ten airports by the number of inbound passengers (destination)
This dataframe should contain specific fields
Show the results of this dataframe using the show method.
"""
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Get top 10 destination airports for 2008
df_2008 = df_renamed2.filter(df_renamed2.flight_year.contains('2008')) 
df_grouped = df_2008.groupBy("destination_airport_code").sum("passengers")
df_sorted = df_grouped.sort("sum(passengers)", ascending=False)
df_top10 = df_sorted.limit(10)
df_top10_alldata = df_top10.join(df_2008,"destination_airport_code", how = "left")

# Calculate requested data and join to top 10 dataframe 
df_top10 = df_top10.select('destination_airport_code','sum(passengers)', row_number().over(Window.partitionBy().orderBy(col('sum(passengers)').desc()) ).alias("Rank (1-10)"))
df_total_flights = df_top10_alldata.groupBy("destination_airport_code").count()
df_avg_flights = df_top10_alldata.groupBy("destination_airport_code").agg({'flights': 'mean'}) 
df_top10 = df_top10.select('*', (df_top10['sum(passengers)'] / 365).alias("Average Daily Passengers"))       

df_top10 = df_top10.join(df_avg_flights,"destination_airport_code", how = "left")
df_top10 = df_top10.join(df_total_flights ,"destination_airport_code", how = "left")                     
df_top10_Name = df_top10.join(df_airport_codes, df_top10.destination_airport_code == df_airport_codes.iata_code, how = "left")

# Rename columns and show final data
df_top10 = df_top10_Name.withColumnRenamed("sum(passengers)","Total Inbound Passengers") \
    .withColumnRenamed("avg(flights)","Average Inbound Flights").withColumnRenamed("destination_airport_code", "IATA code") \
        .withColumnRenamed("count","Total Inbound Flights").withColumnRenamed("name", "Name")
df_final = df_top10.select("Rank (1-10)","Name", "IATA code","Total Inbound Passengers","Total Inbound Flights", "Average Daily Passengers", "Average Inbound Flights")
df_final.show()

"""
Output:

+-----------+--------------------+---------+------------------------+---------------------+------------------------+-----------------------+
|Rank (1-10)|                Name|IATA code|Total Inbound Passengers|Total Inbound Flights|Average Daily Passengers|Average Inbound Flights|
+-----------+--------------------+---------+------------------------+---------------------+------------------------+-----------------------+
|          8|George Bush Inter...|      IAH|                14870717|                 5811|        40741.6904109589|      36.86886938564791|
|          5|McCarran Internat...|      LAS|                18262263|                 3887|       50033.59726027397|      42.22356573192694|
|          7|Charlotte Douglas...|      CLT|                15038489|                 6152|        41201.3397260274|      33.32899869960988|
|          6|Phoenix Sky Harbo...|      PHX|                17305718|                 4423|       47412.92602739726|     40.981008365362875|
|          3|Dallas Fort Worth...|      DFW|                22883558|                 5109|       62694.67945205479|      52.89547856723429|
|          1|Hartsfield Jackso...|      ATL|                35561795|                 8775|       97429.57534246576|      45.03612535612535|
|          2|Chicago O'Hare In...|      ORD|                26398793|                 9479|        72325.4602739726|      37.61683721911594|
|         10|Detroit Metropoli...|      DTW|                14228887|                 5638|      38983.252054794524|      34.03866619368571|
|          4|Los Angeles Inter...|      LAX|                19741782|                 4634|       54087.07397260274|      46.39620198532585|
|          9|Orlando Internati...|      MCO|                14581086|                 3584|      39948.180821917806|     36.749441964285715|
+-----------+--------------------+---------+------------------------+---------------------+------------------------+-----------------------+
"""

#%%
"""
e. User Defined Functions
Add new columns for destination_airport_longitude, destination_airport_latitude, origin_airport_longitude, and origin_airport_latitude.
"""
from pyspark.sql.functions import udf

@udf('double')
def get_latitude(coordinates):
    split_coords = coordinates.split(',')
    if len(split_coords) != 2:
        return None

    return float(split_coords[0].strip())


@udf('double')
def get_longitude(coordinates):
    split_coords = coordinates.split(',')
    if len(split_coords) != 2:
        return None

    return float(split_coords[1].strip())

df_renamed2 = df_renamed2.withColumn(
  'destination_airport_longitude',
  get_longitude(df_renamed2['destination_airport_coordinates'])
)

df_renamed2  = df_renamed2.withColumn(
  'destination_airport_latitude',
  get_latitude(df_renamed2['destination_airport_coordinates'])
)

df_renamed2 = df_renamed2.withColumn(
  'origin_airport_longitude',
  get_longitude(df_renamed2['origin_airport_coordinates'])
)

df_renamed2 = df_renamed2.withColumn(
  'origin_airport_latitude',
  get_latitude(df_renamed2['origin_airport_coordinates'])
)

df_renamed2.printSchema()

"""
Output:
    
root
 |-- origin_airport_code: string (nullable = true)
 |-- destination_airport_code: string (nullable = true)
 |-- origin_city: string (nullable = true)
 |-- destination_city: string (nullable = true)
 |-- passengers: long (nullable = true)
 |-- seats: long (nullable = true)
 |-- flights: long (nullable = true)
 |-- distance: double (nullable = true)
 |-- origin_population: long (nullable = true)
 |-- destination_population: long (nullable = true)
 |-- flight_year: long (nullable = true)
 |-- flight_month: long (nullable = true)
 |-- origin_airport_type: string (nullable = true)
 |-- origin_airport_name: string (nullable = true)
 |-- origin_airport_elevation_ft: double (nullable = true)
 |-- origin_airport_region: string (nullable = true)
 |-- origin_airport_municipality: string (nullable = true)
 |-- origin_airport_gps_code: string (nullable = true)
 |-- origin_airport_coordinates: string (nullable = true)
 |-- destination_airport_type: string (nullable = true)
 |-- destination_airport_name: string (nullable = true)
 |-- destination_airport_elevation_ft: double (nullable = true)
 |-- destination_airport_region: string (nullable = true)
 |-- destination_airport_municipality: string (nullable = true)
 |-- destination_airport_gps_code: string (nullable = true)
 |-- destination_airport_coordinates: string (nullable = true)
 |-- destination_airport_longitude: double (nullable = true)
 |-- destination_airport_latitude: double (nullable = true)
 |-- origin_airport_longitude: double (nullable = true)
 |-- origin_airport_latitude: double (nullable = true)

"""