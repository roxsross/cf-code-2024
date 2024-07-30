from pyspark.sql import SparkSession

# Guardar el CSV localmente
csv_content = """Date,Product,Category,Quantity,Price
2024-07-01,Widget A,Widgets,10,19.99
2024-07-01,Widget B,Widgets,5,24.99
2024-07-02,Widget A,Widgets,7,19.99
2024-07-02,Gadget A,Gadgets,3,39.99
2024-07-03,Widget C,Widgets,4,29.99
2024-07-03,Gadget B,Gadgets,2,49.99
2024-07-04,Widget A,Widgets,8,19.99
2024-07-04,Gadget A,Gadgets,6,39.99
2024-07-05,Widget B,Widgets,10,24.99
2024-07-05,Gadget B,Gadgets,5,49.99
"""

with open('sales_data.csv', 'w') as file:
    file.write(csv_content)

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("CSVProcessingExample") \
    .getOrCreate()

# Especificar el path del archivo CSV
csv_path = 'sales_data.csv'

# Leer el archivo CSV en un DataFrame de Spark
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Mostrar las primeras 5 filas del DataFrame
df.show(5)

# Realizar alguna transformación, por ejemplo, seleccionar ciertas columnas
selected_df = df.select('Date', 'Product', 'Quantity', 'Price')

# Mostrar el esquema del DataFrame
df.printSchema()

# Guardar el DataFrame en formato Parquet
output_path = 'output_sales_data'
selected_df.write.parquet(output_path)

# Finalizar la sesión de Spark
spark.stop()
