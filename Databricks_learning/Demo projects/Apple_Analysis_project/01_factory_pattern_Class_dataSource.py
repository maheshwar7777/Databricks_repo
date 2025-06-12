# Databricks notebook source
class DataSource:

    """ abstract class"""
    def __init__(self,path):
        self.path=path

    def get_data_frame(self):
        """ Abstract method"""
        raise ValueError('Not implemented method')

# inherate the abstract class

class CsvDataSource(DataSource):
    
    def get_data_frame(self):
        return spark.read.format('CSV').option('header',True).option('inferSchema',True).option('sep',',').load(self.path)
    

class ParquetDataSource(DataSource):
    
    def get_data_frame(self):
        return spark.read.format('parquet').load(self.path)
    
class DeltaDataSource(DataSource):
    
    def get_data_frame(self):
        return spark.read.format('delta').load(self.path)
    

#create a function to call the class to load the dataframe

def call_data_fram_class(data_type,path):
    try:
        if  data_type =='CSV':
            return CsvDataSource(path).get_data_frame()

        elif  data_type =='parquet':
            return ParquetDataSource(path).get_data_frame()
        
        elif  data_type =='delta':
            return DeltaDataSource(path).get_data_frame()
        
        else:
            raise ValueError(f"provide correct file type (CSV/parquet/delta)")
        
    except Exception as e:
        print(f"unknown error in call_data_fram_class function")

  
