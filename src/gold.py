from pyspark.sql.functions import col, to_date, count, max as max_, round as round_, sum as sum_
from common import get_spark, SILVER, GOLD

spark = get_spark('gold')
orders = spark.read.format('delta').load(SILVER + '/orders')
customers = spark.read.format('delta').load(SILVER + '/customers')
valid = orders.filter(col('status') != 'cancelled')

daily_sales = (valid.groupBy(to_date('created_at').alias('order_date'))
               .agg(count('id').alias('orders'), round_(sum_('order_total'), 2).alias('revenue')))
daily_sales.write.format('delta').mode('overwrite').save(GOLD + '/daily_sales')

ltv = (valid.groupBy('customer_id')
       .agg(count('id').alias('orders'), round_(sum_('order_total'), 2).alias('lifetime_value'),
            max_('created_at').alias('last_order'))
       .join(customers.select(col('id').alias('customer_id'), 'name', 'city'), 'customer_id'))
ltv.write.format('delta').mode('overwrite').save(GOLD + '/customer_ltv')

spark.read.format('delta').load(GOLD + '/daily_sales').orderBy('order_date').show()
spark.read.format('delta').load(GOLD + '/customer_ltv').orderBy(col('lifetime_value').desc()).show(10)