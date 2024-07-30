from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("BatchProcessingExample") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame
df = spark.read.csv("large_data.csv", header=True, inferSchema=True)

# Mostrar el contenido del DataFrame original
print("Contenido del DataFrame original:")
df.show()

# Transformaciones:
# 1. Filtrar empleados con salario mayor a 50,000
df_filtered = df.filter(col("salary") > 50000)

# 2. Seleccionar columnas 'name' y 'salary'
df_selected = df_filtered.select("name", "salary")

# Mostrar el contenido del DataFrame transformado
print("Contenido del DataFrame transformado:")
df_selected.show()

# Acciones adicionales:
# 1. Calcular el salario promedio
avg_salary = df_filtered.agg(avg("salary")).collect()[0][0]
print("Salario promedio:", avg_salary)

# 2. Calcular el salario máximo
max_salary = df_filtered.agg(max("salary")).collect()[0][0]
print("Salario máximo:", max_salary)

# 3. Calcular el salario mínimo
min_salary = df_filtered.agg(min("salary")).collect()[0][0]
print("Salario mínimo:", min_salary)

# Guardar los resultados en un nuevo archivo CSV
df_selected.write.csv("filtered_data.csv", header=True)

# Finalizar la sesión de Spark
spark.stop()
