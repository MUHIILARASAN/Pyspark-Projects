# Databricks notebook source
from pyspark.sql.functions import *



## load the data file and create spark dataframe

raw_fire_df = spark.read\
    .format('csv') \
    .option("header","true") \
    .option("inferschema","true") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")


## columns name standardization
renamed_fire_df = raw_fire_df \
.withColumnRenamed("Call Number", "CallNumber") \
.withColumnRenamed("Unit ID", "UnitID") \
.withColumnRenamed("Incident Number", "IncidentNumber") \
.withColumnRenamed("Call Date", "CallDate") \
.withColumnRenamed("Watch Date", "WatchDate") \
.withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
.withColumnRenamed("Available DtTm", "AvailableDtTm") \
.withColumnRenamed("Zipcode of Incident", "Zipcode") \
.withColumnRenamed("Station Area", "StationArea") \
.withColumnRenamed("Final Priority", "FinalPriority") \
.withColumnRenamed("ALS Unit", "ALSUnit") \
.withColumnRenamed("Call Type Group", "CallTypeGroup") \
.withColumnRenamed("Unit sequence in call dispatch", "Unitsequenceincalldispatch") \
.withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
.withColumnRenamed("Supervisor District", "SupervisorDistrict")


renamed_fire_df.display()


## converting some data fields from string to timestamp
## rounding delay column
fire_df = renamed_fire_df \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yy hh:mm:ss a"))\
    .withColumn("Delay", round("Delay",2))


## Qestion 1 - How many distinct type of calls were made to fire department


## 1 SQL approach -- convert your dataframe into a temporary view, -- run a sql on the view

fire_df.createOrReplaceTempView("fire_service_calls_view")

q1_spark_df = spark.sql("""
                        Select count(distinct CallType) as no_of_distinct_calls
                        from fire_service_calls_view
                        """)
q1_spark_df.show()


##  Dataframe transformation approach

q1_df = fire_df.where("CallType is not null") \
    .select("CallType") \
    .distinct()
display(q1_df.count())


## Q2 - What are distinct type of calls were made to the fire department??


q2_df = fire_df.where("CallType Is not null")\
    .select('CallType')\
    .distinct()
display(q2_df)


## Q3 - Find out all response for delayed times greater than 5 minutes?

q3_df = fire_df.where("Delay > 5")\
    .select("CallNumber", "Delay")
q3_df.show()


## what were the most common call types?

q4_df = fire_df.select("CallType")\
    .groupBy("CallType")\
    .count()\
    .orderBy("count", ascending=False)
q4_df.show(1)


## Q5 Which zip code accounted for most common calls

q5_df = fire_df.select("CallType","Zipcode")\
    .groupBy("CallType","Zipcode")\
    .count()\
    .orderBy("count",ascending=False)
q5_df.show(5)


## Q6 What San Fransico neighborhoods are in zip codes 94102 and 94103

fire_df.select("Neighborhood", "Zipcode")\
    .where("Zipcode in (94102, 94103)")\
    .distinct()\
    .show()


## Q7 What was the sum of all call alarms, average, min, and max of the response times for calls?

fire_df.select(
    sum("NumAlarms").alias("total_call_alarms"),
    avg("Delay").alias("avg_call_response_time"),
    min("Delay").alias("min_call_response_time"),
    max("Delay").alias("max_call_response_time")
).show()


## Q8 How many distinct years of data is in the dataset

fire_df.select(year("CallDate").alias("call_year")) \
    .distinct()\
    .orderBy("call_year")\
    .show()


## Q9 What week of the year in 2018 had most of the calls?


fire_df.withColumn('week_of_year',weekofyear("CallDate")).select("week_of_year")\
    .filter(year("CallDate")==2018)\
    .groupBy("week_of_year")\
    .count()\
    .orderBy("count",ascending=False)\
    .show(1)


## Q10 -- What neighborhood in San Francisco had the worst reponse time in 2018

fire_df.select("Neighborhood","Delay")\
    .filter(year("CallDate")=='2018')\
    .orderBy("Delay", ascending = False)\
    .show(1)



