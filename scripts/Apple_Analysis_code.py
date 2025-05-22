# Databricks notebook source
# MAGIC %run "./Reader_Factory"

# COMMAND ----------

# MAGIC %run "./Transform"

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

class Workflow1:
    """
    ETL pipeline for customers who bought airpods after iphone
    """
    def __init__(self):
        pass
    def runner(self):
        #Step 1 : Extract all different data from different source
        inputDFs=AirpodsAfterIphoneExtractor().extract()



        # Step 2: Implement te trasformation logic
        firstTransformer = AirpodsAfterIphone().transform(inputDFs)
        print(firstTransformer)
      

        #Load all reuquired data to different
        AirPodsAfterIphoneLoader(firstTransformer).sink()


# COMMAND ----------

class Workflow2:
    """
    ETL pipeline for customers who bought only iphones and airpods
    """
    def __init__(self):
        pass
    def runner(self):
        #Step 1 : Extract all different data from different source
        inputDFs=AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement te trasformation logic
        Iphoneonly = IphoneandAirpodsonly().transform(inputDFs)
        print(Iphoneonly)
      

        #Load all reuquired data to different
        IphoneAirpodsOnlyLoader(Iphoneonly).sink()


# COMMAND ----------

class WorkFlowRunner:
    def __init__(self,name):
        self.name=name
    def runner(self):
        if self.name=="Workflow1":
            return Workflow1().runner()
        elif self.name=='Workflow2':
            return Workflow2().runner()


name="Workflow2"
WorkFlowRunner(name).runner()


# COMMAND ----------

from pyspark.sql import SparkSession

spark= SparkSession.builder.appName("Apple Data Analysis").getOrCreate()

input_df= spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/Customer_Updated.csv')

# COMMAND ----------

input_df.show()

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/output", recurse=True)


# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/output")


# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/IphoneOnly")

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/IphoneOnly', recurse=True)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/IphoneAirpodsOnly")

# COMMAND ----------

