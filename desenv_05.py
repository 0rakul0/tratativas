from pyspark.sql import SparkSession, Row, functions

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Mongobdu").getOrCreate()
    lines = spark.read.format("json").load("./data/posts.json")
    lines.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://localhost:27017/teste_mongo2.posts")\
        .mode("append")\
        .save()