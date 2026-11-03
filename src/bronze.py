import sys
from pyspark.sql.functions import col
from common import get_spark, BRONZE, CHECKPOINTS

spark = get_spark('bronze')

raw = (spark.readStream.format('kafka')
       .option('kafka.bootstrap.servers', 'kafka:9092')
       .option('subscribe', 'cdc.shop.customers,cdc.shop.products,cdc.shop.orders')
       .option('startingOffsets', 'earliest')
       .load())

bronze = raw.select(
    col('topic'),
    col('partition'),
    col('offset'),
    col('timestamp').alias('kafka_ts'),
    col('key').cast('string').alias('key'),
    col('value').cast('string').alias('value'),
)

trigger = {'availableNow': True} if '--once' in sys.argv else {'processingTime': '10 seconds'}

query = (bronze.writeStream.format('delta').outputMode('append')
         .option('checkpointLocation', CHECKPOINTS + '/bronze')
         .trigger(**trigger)
         .start(BRONZE))
query.awaitTermination()