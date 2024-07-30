from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("EjemploPySpark") \
    .getOrCreate()

data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)

rdd2 = rdd.map(lambda x: x * 2)

result = rdd2.collect()
print("Resultado de la transformación (multiplicar por 2):", result)

sum_result = rdd.reduce(lambda a, b: a + b)
print("Suma de los elementos:", sum_result)

spark.stop()
