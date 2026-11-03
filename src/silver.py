import sys
from pyspark.sql.functions import col, from_json, coalesce, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from common import get_spark, BRONZE, SILVER, CHECKPOINTS

TABLES = {
    'customers': {'id': 'INT', 'name': 'STRING', 'email': 'STRING', 'city': 'STRING',
                  'created_at': 'STRING', 'updated_at': 'STRING'},
    'products': {'id': 'INT', 'name': 'STRING', 'category': 'STRING', 'price': 'DOUBLE',
                 'updated_at': 'STRING'},
    'orders': {'id': 'INT', 'customer_id': 'INT', 'product_id': 'INT', 'quantity': 'INT',
               'status': 'STRING', 'order_total': 'DOUBLE', 'created_at': 'STRING',
               'updated_at': 'STRING'},
}


def struct_ddl(cols):
    return 'STRUCT<' + ', '.join(c + ': ' + t for c, t in cols.items()) + '>'


def process_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    for table, cols in TABLES.items():
        envelope = ('before ' + struct_ddl(cols) + ', after ' + struct_ddl(cols) +
                    ', op STRING, ts_ms LONG')
        rows = (batch_df.filter(col('topic') == 'cdc.shop.' + table)
                .select(from_json(col('value'), envelope).alias('e'), col('offset')))
        if rows.isEmpty():
            continue

        # id comes from 'after' normally, and from 'before' for deletes
        select_cols = [col('e.op').alias('op'), col('offset'),
                       coalesce(col('e.after.id'), col('e.before.id')).alias('id')]
        for c in cols:
            if c == 'id':
                continue
            expr = col('e.after.' + c)
            select_cols.append(expr.cast('timestamp').alias(c) if c.endswith('_at')
                               else expr.alias(c))
        flat = rows.select(*select_cols)

        w = Window.partitionBy('id').orderBy(col('offset').desc())
        latest = flat.withColumn('rn', row_number().over(w)).filter('rn = 1').drop('rn')

        path = SILVER + '/' + table
        if not DeltaTable.isDeltaTable(spark, path):
            latest.drop('op', 'offset').limit(0).write.format('delta').save(path)

        target = DeltaTable.forPath(spark, path)
        (target.alias('t')
         .merge(latest.alias('s'), 't.id = s.id')
         .whenMatchedDelete(condition=col('s.op') == 'd')
         .whenMatchedUpdate(condition=col('s.op') != 'd',
                            set={c: 's.' + c for c in cols})
         .whenNotMatchedInsert(condition=col('s.op') != 'd',
                               values={c: 's.' + c for c in cols})
         .execute())


spark = get_spark('silver')
stream = spark.readStream.format('delta').load(BRONZE)
trigger = {'availableNow': True} if '--once' in sys.argv else {'processingTime': '10 seconds'}

query = (stream.writeStream.foreachBatch(process_batch)
         .option('checkpointLocation', CHECKPOINTS + '/silver')
         .trigger(**trigger)
         .start())
query.awaitTermination()