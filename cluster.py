import os
import re

# import matplotlib.pyplot as plt
import numpy as np
# import pandas as pd
import pyspark
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import (avg, col, count, date_format, hour, lit,
                                   minute, sum, to_timestamp, udf, when)
from pyspark.sql.types import (ArrayType, DoubleType, FloatType, IntegerType,
                               StringType)

# May need to change this depending on where you are running the code
DATA = [
    './content/taxi_data_2023/yellow_tripdata_2023-01.parquet', 
    './content/taxi_data_2023/yellow_tripdata_2023-02.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-03.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-04.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-05.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-06.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-07.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-08.parquet',
    './content/taxi_data_2023/yellow_tripdata_2023-09.parquet',
]
ZONES = './content/taxi_zones.csv'
SEED = 42

def get_spark_context():
    """
    Get spark context

    Returns
    -------
    pyspark.context.SparkContext
        Spark context
    pyspark.sql.session.SparkSession
        Spark session
    """
    conf = pyspark.SparkConf()
    conf.set('spark.driver.memory','4g')

    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.SQLContext.getOrCreate(sc)
    return sc, spark

def get_data(spark, paths: list):
    """
    Get data from parquet files

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Spark session
    paths : list
        List of paths to parquet files

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Dataframe containing data from all parquet files
    """

    # For each path, read in the parquet files and union them together
    df = None
    for path in paths:
        if df is None:
            df = spark.read.parquet(path)
        else:
            df = df.union(spark.read.parquet(path))
    return df

def extract_coords(input_string):
    """
    Given a Multipolygon object as a string, extract the list of coordinates
    """
    pattern = r'(-?\d+\.\d+\s-?\d+\.\d+)'

    # Find all matches in the string
    matches = re.findall(pattern, input_string)

    # Extract coordinates from the matched pairs
    coordinates = [tuple(map(float, match.split())) for match in matches]

    return coordinates

def calculate_center(vertices):
    """
    Given a list of vertices of a Multipolygon, calculate the center as a coordinate
    """
    return np.mean(vertices, axis=0).tolist()

def get_hotspots(time: str):
    """
    Given a time, determine hotspots using KMeans clustering

    Parameters
    ----------
    time : str
        Time to use for clustering (example: '15:00:00')

    Returns
    -------
    list
        List of hotspots (tuples of lat, lon, average revenue, average tip)
    """
    # Get spark context
    sc, spark = get_spark_context()

    # Get data
    df = get_data(spark, DATA)

    # Clean data
    df = df.dropna()
    df = df.withColumn("Passenger_count", df["Passenger_count"].cast(IntegerType()))
    df = df.withColumn("Trip_distance", df["Trip_distance"].cast(FloatType()))

    # Extracting the hour from the pickoff/dropoff columns
    df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    df = df.withColumn("dropoff_hour", hour("tpep_dropoff_datetime"))

    # Get zones data
    zones = spark.read.csv(ZONES, header=True)

    # Creating dataframe for pickup
    pickup_zone = zones.selectExpr("LocationID as LocationID_PU", "Zone as Zone_PU", "the_geom as Geometry")

    # Modify pickup_zone to contain center coordinates for each zone 
    coords_udf = udf(extract_coords, ArrayType(ArrayType(DoubleType())))
    center_udf = udf(calculate_center, ArrayType(DoubleType()))

    zone_with_centers = pickup_zone.withColumn('vertices', coords_udf(col('Geometry'))) \
                                .withColumn('centroids', center_udf(col('vertices'))) \
                                .withColumn('long', col('centroids')[0]) \
                                .withColumn('lat', col('centroids')[1])

    df_zones = df.join(zone_with_centers, df.PULocationID == zone_with_centers.LocationID_PU, how='left')

    windowed_df = df_zones.withColumn("hour", hour("tpep_pickup_datetime")) \
                        .withColumn("minute", minute("tpep_pickup_datetime")) \
                        .withColumn("user_hour", hour(lit(time))) \
                        .withColumn("user_minute", minute(lit(time))) \
                        .withColumn("time_diff", (col("hour") * 60 + col("minute")) - (col("user_hour") * 60 + col("user_minute"))) \
                        .filter((col("time_diff") >= -30) & (col("time_diff") <= 30)) \
                        .select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'time_diff', 'Trip_distance', 'Passenger_count', 'PULocationID','DOLocationID', 'Zone_PU','long', 'lat', 'tip_amount', 'total_amount')

    # Run KMeans clustering to determine pickup hotspots
    assembler = VectorAssembler(inputCols = ['long','lat'], outputCol='features', handleInvalid="skip")

    assembled_df = assembler.transform(windowed_df)

    kmeans = KMeans(k=5, seed=SEED)
    model = kmeans.fit(assembled_df.select('features'))

    transformed = model.transform(assembled_df)

    # Determine the average revenue for each cluster
    avg_revenue = transformed.groupBy('Prediction') \
                        .agg({'total_amount': 'avg'}) \
                        .withColumnRenamed('avg(total_amount)', 'Average Revenue') \
                        .withColumnRenamed('Prediction', 'Cluster') \
                        .select('Cluster', 'Average Revenue')

    # Determine the average tip for each cluster
    avg_tip = transformed.groupBy('Prediction') \
                        .agg({'tip_amount': 'avg'}) \
                        .withColumnRenamed('avg(tip_amount)', 'Average Tip') \
                        .withColumnRenamed('Prediction', 'Cluster') \
                        .select('Cluster', 'Average Tip')

    # Determine cluster centers
    cluster_centers = transformed.groupBy('Prediction') \
                                .agg({'long': 'avg', 'lat': 'avg'}) \
                                .withColumnRenamed('avg(long)', 'Centroid Longitude') \
                                .withColumnRenamed('avg(lat)', 'Centroid Latitude') \
                                .withColumnRenamed('Prediction', 'Cluster') \
                                .select('Cluster', 'Centroid Longitude', 'Centroid Latitude')

    # Join cluster centers with average revenue and average tip
    cluster_centers = cluster_centers.join(avg_revenue, on='Cluster', how='left')
    cluster_centers = cluster_centers.join(avg_tip, on='Cluster', how='left')

    # Get cluster centers, average revenue, and average tip as a list of tuples
    cluster_centers = cluster_centers.collect()
    cluster_centers = [(x['Centroid Longitude'], x['Centroid Latitude'], x['Average Revenue'], x['Average Tip']) for x in cluster_centers]

    sc.stop()

    return cluster_centers