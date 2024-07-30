from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("TitanicMLExample") \
    .getOrCreate()

# URL del archivo CSV
csv_url = 'https://github.com/datasciencedojo/datasets/raw/master/titanic.csv'

# Descargar el archivo CSV
import urllib.request
csv_path = '/tmp/titanic.csv'
urllib.request.urlretrieve(csv_url, csv_path)

# Leer el archivo CSV en un DataFrame de Spark
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Mostrar las primeras 5 filas del DataFrame
df.show(5)

# Seleccionar las columnas de interés y realizar limpieza de datos
df = df.select("Survived", "Pclass", "Sex", "Age", "SibSp", "Parch", "Fare")

# Reemplazar valores nulos en la columna "Age" con la media de la columna
mean_age = df.select("Age").dropna().agg({"Age": "avg"}).collect()[0][0]
df = df.withColumn("Age", when(col("Age").isNull(), mean_age).otherwise(col("Age")))

# Codificar la columna categórica "Sex" en valores numéricos
indexer = StringIndexer(inputCol="Sex", outputCol="SexIndex")
df = indexer.fit(df).transform(df)

# Crear una columna de características para el modelo de ML
assembler = VectorAssembler(
    inputCols=["Pclass", "SexIndex", "Age", "SibSp", "Parch", "Fare"],
    outputCol="features")

# Transformar los datos para incluir la columna de características
feature_df = assembler.transform(df)

# Seleccionar las columnas de características y etiqueta
feature_df = feature_df.select("features", col("Survived").alias("label"))

# Dividir los datos en conjuntos de entrenamiento y prueba
train_df, test_df = feature_df.randomSplit([0.7, 0.3], seed=42)

# Crear un modelo de regresión logística
lr = LogisticRegression(featuresCol='features', labelCol='label')

# Entrenar el modelo
lr_model = lr.fit(train_df)

# Hacer predicciones en el conjunto de prueba
predictions = lr_model.transform(test_df)

# Mostrar las primeras 5 predicciones
predictions.select("features", "label", "prediction").show(5)

# Evaluar el modelo utilizando AUC (Área Bajo la Curva ROC)
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"Área bajo la curva ROC: {auc}")

# Finalizar la sesión de Spark
spark.stop()
