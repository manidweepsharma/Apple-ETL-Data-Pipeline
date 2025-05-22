# Databricks notebook source
class DataSource:
    """
    Abstract Class
    """
    def __init__(self, path):
        self.path = path

    def get_data_frame(self):
        """
        Abstract method, should be defined in sub-classes
        """
        raise NotImplementedError("Not Implemented")


class CSVDataSource(DataSource):
    def __init__(self, path):
        super().__init__(path)

    def get_data_frame(self):
        table_name=self.path
        return spark.read.format('csv').option('header', True).load(table_name)


class ParquetDataSource(DataSource):
    def __init__(self, path):
        super().__init__(path)

    def get_data_frame(self):
        table_name=self.path
        return spark.read.format('parquet').load(table_name)


class DeltaDataSource(DataSource):
    def __init__(self, path):
        super().__init__(path)

    def get_data_frame(self):
        # Check if the path is an absolute path (file/folder) or a table name
        if (
            self.path.startswith("/") or
            self.path.startswith("dbfs:") or
            self.path.startswith("s3://") or
            self.path.startswith("wasbs://")
        ):
            return spark.read.format('delta').load(self.path)
        else:
            # Treat as table name
            return spark.table(self.path)


def get_data_source(data_type, file_path):
    if data_type == 'csv':
        return CSVDataSource(file_path)
    elif data_type == 'parquet':
        return ParquetDataSource(file_path)
    elif data_type == 'delta':
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f'Not Implemented for Data Type: {data_type}')


# COMMAND ----------

