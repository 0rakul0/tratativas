"""
pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

posts = spark.read.format("mongo").option("uri", "mongodb://localhost/teste_mongo.posts").load()
posts.show()

posts.write.format("mongo").option("uri", "mongodb://localhost/teste_mongo2.posts").save()
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mongo_teste").getOrCreate()

posts = spark.read.format("mongo").option("uri", "mongodb://localhost/teste_mongo.posts").load()
posts.show()