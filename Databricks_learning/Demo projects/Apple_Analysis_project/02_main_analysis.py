# Databricks notebook source
# MAGIC %run ./01_factory_pattern_Class_dataSource

# COMMAND ----------

# create a workflow class

class Workflow:
    def __init__(self,data_type, path):
        self.data_type=data_type
        self.path=path

    def input_file_load(self):

        return call_data_fram_class(self.data_type,self.path)
    
# invoke the dataSrouce class

# f = Workflow('CSV', "dbfs:/input/Apple_analysis/Customer_Updated.csv").input_file_load()
# f= Workflow('CSV', "dbfs:/input/Apple_analysis/Customer_Updated.csv").input_file_load()
# print(f.data_type)
# print(f.path)
# display(f)


# COMMAND ----------

# create customer dataframe 

# f = Workflow('CSV', "dbfs:/input/Apple_analysis/Customer_Updated.csv").input_file_load()

customer_df= Workflow('CSV', "dbfs:/input/Apple_analysis/Customer_Updated.csv").input_file_load()
display(customer_df)

transcation_df= Workflow('CSV', "dbfs:/input/Apple_analysis/Transaction_Updated.csv").input_file_load()
display(transcation_df)

product_df= Workflow('ORC', "dbfs:/input/Apple_analysis/Products_Updated.csv").input_file_load()
display(product_df)
