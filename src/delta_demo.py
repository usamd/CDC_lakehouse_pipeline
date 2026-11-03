import time
from common import get_spark, BRONZE, SILVER

spark = get_spark('demo')

print('--- 1. Table history ---')
spark.sql('DESCRIBE HISTORY delta.`' + SILVER + '/orders`').select('version', 'timestamp', 'operation').show(10, False)

print('--- 2. Time travel ---')
old = spark.read.format('delta').option('versionAsOf', 0).load(SILVER + '/orders').count()
now = spark.read.format('delta').load(SILVER + '/orders').count()
print('version 0 rows:', old, '| current rows:', now)

print('--- 3. OPTIMIZE benchmark on bronze ---')


def timed(label):
    start = time.time()
    spark.read.format('delta').load(BRONZE).groupBy('topic').count().collect()
    files = spark.sql('DESCRIBE DETAIL delta.`' + BRONZE + '`').first()['numFiles']
    print(label, '| files:', files, '| seconds:', round(time.time() - start, 2))


timed('before OPTIMIZE')
spark.sql('OPTIMIZE delta.`' + BRONZE + '`')
timed('after OPTIMIZE ')

print('--- 4. VACUUM dry run ---')
spark.sql('VACUUM delta.`' + BRONZE + '` DRY RUN').show(5, False)