from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("TourismDataBatchProcessing") \
    .getOrCreate()

# URL del archivo CSV
csv_url = 'https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv'

# Descargar el archivo CSV
import urllib.request
csv_path = '/tmp/tourism_data.csv'
urllib.request.urlretrieve(csv_url, csv_path)

# Leer el archivo CSV en un DataFrame de Spark
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Mostrar las primeras 5 filas del DataFrame
df.show(5)

# Eliminar los espacios en blanco y comillas adicionales de los nombres de las columnas
df = df.select([col(c).alias(c.strip().replace('"', '')) for c in df.columns])

# Mostrar el esquema del DataFrame
df.printSchema()

# Realizar algunas operaciones de procesamiento en lote
# Por ejemplo, calcular la altura promedio en pulgadas
df.agg(avg("Height(Inches)").alias("Average_Height")).show()

# Finalizar la sesión de Spark
spark.stop()
