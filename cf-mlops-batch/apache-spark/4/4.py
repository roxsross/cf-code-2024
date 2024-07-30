from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("MLBatchProcessingExample") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame
df = spark.read.csv("iris.csv", header=True, inferSchema=True)

# Mostrar el contenido del DataFrame original
print("Contenido del DataFrame original:")
df.show()

# Preprocesamiento de datos:
# 1. Convertir la columna de etiquetas (species) a índices numéricos
indexer = StringIndexer(inputCol="species", outputCol="label")
df = indexer.fit(df).transform(df)

# 2. Combinar las características en un solo vector
assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")
df = assembler.transform(df)

# Seleccionar solo las columnas necesarias
df = df.select("features", "label")

# Mostrar el contenido del DataFrame preprocesado
print("Contenido del DataFrame preprocesado:")
df.show()

# Dividir los datos en conjuntos de entrenamiento y prueba
train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

# Crear y entrenar el modelo de regresión logística
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(train_data)

# Evaluar el modelo en el conjunto de prueba
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Precisión del modelo:", accuracy)

# Guardar el modelo entrenado
model.save("logistic_regression_model")

# Finalizar la sesión de Spark
spark.stop()
