from __future__ import print_function
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# Exception Handling and removing incorrect lines
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Data cleaning function
def correctRows(p):
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]) and isfloat(p[4]) and isfloat(p[16]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return True
    return False

# Filter functions
def filter_by_trip_time(row):
    try:
        trip_time = float(row[4])
        return trip_time >= 60
    except ValueError:
        return False

def filter_by_fare(row):
    try:
        fare_amount = float(row[16])
        return fare_amount > 0
    except ValueError:
        return False
    
def filter_by_distance(row):   
    try:
        trip_distance = float(row[5])
        return trip_distance > 0
    except ValueError:
        return False

# Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output_task1> <output_task2>", file=sys.stderr)
        exit(-1)
    
    input_file = sys.argv[1]
    output_task1 = sys.argv[2]
    output_task2 = sys.argv[3]
    
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Assignment-1") \
        .getOrCreate()
    
    # Access SparkContext from SparkSession
    sc = spark.sparkContext
    
    # Load data from CSV file
    rdd = sc.textFile(input_file)
    
    
    # Clean data: split by comma, and apply correctRows function to filter invalid rows
    clean_rdd = rdd.map(lambda x: x.split(',')) \
                   .filter(correctRows)
    
    # Apply additional filters by trip time (no trip less than 60 seconds),
    # trip fare (no trip with $0)
    # trip distance (no trip with distance = 0)
    clean_rdd = clean_rdd.filter(filter_by_trip_time).filter(filter_by_fare).filter(filter_by_distance)
    
                   
    # Task 1
    
    # Extract medallion id and driver ID by creating an RDD with tuple (medallion id, driver_id)
    medallion_driver_rdd = clean_rdd.map(lambda x: (x[0], x[1]))
    
    # Removes any duplicate (medallion_id, driver_id) pairs from the RDD 
    # Only unique combination of taxi and driver tuple remains
    medallion_counts = medallion_driver_rdd.distinct()
    
    # Create a key-value pair where key is medallion_id and value is 1
    medallion_counts = medallion_counts.map(lambda x: (x[0], 1))
    
    # Sum up the counts for each medallion_id using reduceByKey, which is the mediallion id
    medallion_counts = medallion_counts.reduceByKey(lambda x, y: x + y)
    
    # Sort and get top 10
    top_10_taxis = medallion_counts.takeOrdered(10, key=lambda x: -x[1])
    
    # Convert to RDD
    results_1 = sc.parallelize(top_10_taxis)

    
    # Save results of Task 1
    results_1.coalesce(1).saveAsTextFile(output_task1)
    

    # Task 2
    
    # create tuple (hack license, (total_amount, trip_duration))
    driver_earnings_rdd = clean_rdd.map(lambda x: (x[1], (float(x[16]), float(x[4]))))  

    # Calculate total amount and total trip duration for each driver using reduceByKey (hack license)
    # a[0] + b[0] will calculate the trip amount for every row with the corresponding hack license
    # a[1] + b[1] will calculate the trip duration for every with the corresponding hack license
    driver_totals = driver_earnings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Calculate earnings per minute for each driver / hack license
    # (hack license, money per minute) -- (x[1][1] / 60) convert trip duration from seconds to minutes 
    # (x[0], x[1][0] / (x[1][1] / 60) divides the total amount earned by the trip durations in minutes
    driver_earnings_per_minute = driver_totals.map(lambda x: (x[0], x[1][0] / (x[1][1] / 60)))  

    # Sort by earnings per minute and get top 10
    top_10_drivers = driver_earnings_per_minute.takeOrdered(10, key=lambda x: -x[1])

    # Convert to RDD
    results_2 = sc.parallelize(top_10_drivers)
    
    # Save results of Task 2
    results_2.coalesce(1).saveAsTextFile(output_task2)

    # Stop the SparkContext
    sc.stop()
    
    
    
    