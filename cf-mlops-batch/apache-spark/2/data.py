from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("EjemploBatchPySpark") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Mostrar el contenido del DataFrame
print("Contenido del DataFrame original:")
df.show()

# Transformaciones:
# 1. Filtrar personas mayores de 30 años
# 2. Seleccionar solo las columnas 'name' y 'age'
df_filtered = df.filter(col("age") > 30).select("name", "age")

# Mostrar el contenido del DataFrame transformado
print("Contenido del DataFrame transformado:")
df_filtered.show()

# Guardar los resultados en un nuevo archivo CSV
df_filtered.write.csv("resultados.csv", header=True)

# Finalizar la sesión de Spark
spark.stop()
