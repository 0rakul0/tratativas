"""
spark-submit --jars D:\github\base_teste\tratativas\jars\postgresql-42.7.1.jar .\desenv_03.py
"""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("exemplo").getOrCreate()

    arqSchema = "nome STRING, postagem STRING, data STRING"

    df = spark.readStream.json("D:/github/base_teste/tratativas/data/", schema=arqSchema)

    diretorio = "D:/github/base_teste/tratativas/temp/"

    def atualizaPostgres(dataf, batchId):
        dataf.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/posts").option("dbtable", "posts").option("user", "postgres").option("password", "self.python").option("driver", "org.postgresql.Driver").mode("append").save()

    Stcal = df.writeStream.foreachBatch(atualizaPostgres).outputMode('append').trigger(processingTime="5 second").option("checkpointLocation", diretorio).start()