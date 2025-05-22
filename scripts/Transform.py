# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast,col, array_contains, size
class Transformer:
    def __init__(self):
        pass
    def transform(self):
        pass

class AirpodsAfterIphone(Transformer):
    def transform(self,inputDFs):
        """
         Customers who have bought Airpods after buying the iphone
        """

        transactionInputDF = inputDFs.get('transactionInputDF')

        customerDF=inputDFs.get('customerDF')
        #print('transactionInputDF in transform')

        windowSpec= Window.partitionBy('customer_id').orderBy('transaction_date')
        transformedInputDF = transactionInputDF.withColumn('next_product',lead('product_name').over(windowSpec))
        print('Airpods after buying')
        #transformedInputDF.orderBy('customer_id','transaction_date','product_name').show()

        filteredDF=transformedInputDF.filter((col('product_name') == 'iPhone') & (col('next_product')=='AirPods'))
        #fitleredDF.orderBy('customer_id','transaction_date','product_name').show()

        joinDF = filteredDF.join(broadcast(customerDF), filteredDF.customer_id == customerDF.customer_id, "inner") \
                   .drop(customerDF.customer_id)
        finalDf=joinDF.orderBy('customer_id', 'transaction_date', 'product_name')
        return finalDf

class IphoneandAirpodsonly(Transformer):
    def transform(self,inputDFs):
        """
         Customers who have bought Airpods after buying the iphone
        """

        transactionInputDF = inputDFs.get('transactionInputDF')

        customerDF=inputDFs.get('customerDF')

        groupedDF= transactionInputDF.groupBy('customer_id').agg(collect_set('product_name').alias('Products'))
        groupedDF.show()

        filteredDF = groupedDF.filter(
            array_contains(col('Products'), 'iPhone') &
            array_contains(col('Products'), 'AirPods') &
            (size(col('Products')) == 2)
            )

        filteredDF.show()
        #fitleredDF.orderBy('customer_id','transaction_date','product_name').show()

        joinDF = filteredDF.join(broadcast(customerDF),
                                 'customer_id',"inner")
        joinDF.show()

        finalDf=joinDF.select('customer_id', 'customer_name', 'location')
        return finalDf





# COMMAND ----------

