"""
importando arquivos de entrada
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("importando_arquivos").getOrCreate()

schema = "nome STRING, endereco STRING, email STRING, telefone STRING, data_nascimento STRING, profissao STRING"

# por parq
parq = spark.read.format("parquet").schema(schema).load("./data/parquet/*.parquet")
parq.show()

# por json
json_data = spark.read.format("json").load("./data/posts.json")
json_data.show()

arqjson_lista = spark.read.option("multiline", "true").json("./data/json/*.json")
arqjson_lista.show()
arqjson_lista.write.format("mongo").option("uri", "mongodb://localhost/teste_mongo3.posts").save()
arqjson_lista.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/teste_vendas").option("dbtable", "relacional.teste_json").option("user","postgres").option("password","self.python").option("driver","org.postgresql.Driver").save()


# por csv
dfcsv = spark.read.format("csv").schema(schema).load("./data/csv/*.csv")
dfcsv.show()