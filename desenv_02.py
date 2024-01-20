"""
conexão com o banco
"""

"""
pyspark --jars ./jars/postgresql-42.7.1.jar

exemplo de leitura
resumo = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/teste_vendas").option("dbtable","relacional.vendas").option("user","postgres").option("password","self.python").option("driver","org.postgresql.Driver").load()

exemplo de escrita
vendadata = resumo.select("data", "total")
vendadata.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/teste_vendas").option("dbtable", "relacional.vendadata").option("user","postgres").option("password","self.python").option("driver","org.postgresql.Driver").save()

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("teste_banco").config("spark.jars", "./jars/postgresql-42.7.1.jar").getOrCreate()

url = "jdbc:postgresql://localhost:5432/teste_vendas"

propriedades = {
    "user": "postgres",
    "password": "self.python",
    "driver": "org.postgresql.Driver"
}

# Leia os dados da tabela "vendas"
df_vendas = spark.read.format("jdbc").option("url", url).option("dbtable", "relacional.vendas").options(**propriedades).load()

# Mostre os dados
df_vendas.show()