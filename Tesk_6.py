from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .appName("Transactions Example") \
    .get0rCreate()

schema = StructType([
    StructField(name: "user_id", IntegerType(), nullable: True),
    StructField(name: "transaction_id", IntegerType(), nullable: True),
    StructField(name: "amount", FloatType(), nullable: True),
    StructField(name: "date", StringType(), nullable: True),
 ])

data_path = "transactions.txt"

df = spark.read.csv(data_path, schema=schema, sep=",", header=False)
print(df.show)

# Общее количество транзакций
total_transactions = df.count()
print(total_transactions)

# Общая сумма всех транзакций
total_amount = df.agg({"amount": "sum"}).collect()[0][0]
print(total_amount)

# Средняя сумма транзакции
total_amount = df.agg({"amount": "avg"}).collect()[0][0]
print(total_amount)

# Максимальная и минимальная суммы транзакций
max = df.agg({"amount": "max"}).collect()[0][0]
print(max)
min = df.agg({"amount": "min"}).collect()[0][0]
print(min)

# Общее количество уникальных пользователей, совершивших транзакции
uniq = df.select("user_id").distinct().count()
print(uniq)

spark.stop()
