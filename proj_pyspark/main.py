from pyspark.sql import SparkSession

# Cria uma sessão do Spark
spark = SparkSession.builder \
    .appName("Leitura de Arquivo com PySpark") \
    .getOrCreate()

# Caminho do arquivo (ajuste conforme a localização do seu arquivo)
arquivo = "/home/estudo/projetos/proj-pyspark/data/product.csv"

# Lê o arquivo CSV com PySpark
df = spark.read.csv(arquivo, header=True, inferSchema=True)

# Exibe as primeiras linhas do arquivo lido
df.show()

# Fechar a sessão Spark
spark.stop()
