# Databricks notebook source
class DataSink:
    """
    Abstract Class
    """
    def __init__(self,df,path,method,params):
        self.path = path
        self.df = df
        self.method=method
        self.params=params

    def load_data_frame(self):
        """
        Abstract method, should be defined in sub-classes
        """
        raise ValueError("Not Implemented")


class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.format("parquet").mode(self.method).save(self.path)

class LoadToDBFSwithPartition(DataSink):
    def load_data_frame(self):
        partitionByColumns=self.params.get("partitionByColumns")
        self.df.write.format("parquet").mode(self.method).partitionBy(*partitionByColumns).save(self.path)

class LoadToDBFSwithDelta(DataSink):
    def load_data_frame(self):
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)


def get_sink_source(sink_type,df,path,method,params=None):
    if sink_type=="dbfs":
        return LoadToDBFS(df,path,method,params)
    elif sink_type=="dbfs_with_partition":
        return  LoadToDBFSwithPartition(df,path,method,params)
    elif sink_type=="delta":
        return  LoadToDBFSwithDelta(df,path,method,params)
    else:
        raise ValueError(f'Not Implemented for sink Type:{sink_type}')
    


# COMMAND ----------

