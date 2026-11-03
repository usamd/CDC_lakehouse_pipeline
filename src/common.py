from pyspark.sql import SparkSession

BRONZE = '/lakehouse/bronze/cdc_events'
SILVER = '/lakehouse/silver'
GOLD = '/lakehouse/gold'
CHECKPOINTS = '/lakehouse/_checkpoints'


def get_spark(app_name):
    return (SparkSession.builder.appName(app_name)
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog',
                    'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.shuffle.partitions', '4')
            .getOrCreate())