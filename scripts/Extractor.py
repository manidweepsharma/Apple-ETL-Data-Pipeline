# Databricks notebook source
class Extractor:
    """Abstract Class
    """
    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor:
    def extract(self):
        """
        Implement the steps  for extracting or reading the data
        """
        transactionInputDF = get_data_source(
            data_type='csv',
            file_path='dbfs:/FileStore/tables/Transaction_Updated.csv'
        ).get_data_frame()

        customerDF=get_data_source(
            data_type='delta',
            file_path='default.customer_updated_csv'
        ).get_data_frame()

        transactionInputDF.orderBy('customer_id','transaction_date').show()

        inputDFs ={
            'transactionInputDF': transactionInputDF,
            'customerDF':customerDF

        }
        return  inputDFs


# COMMAND ----------

