from common import get_spark, SILVER

spark = get_spark('check')
for t in ['customers', 'products', 'orders']:
    print(t, spark.read.format('delta').load(SILVER + '/' + t).count())