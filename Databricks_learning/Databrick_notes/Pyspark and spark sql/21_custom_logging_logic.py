# Databricks notebook source
# from datetime import *
# import pytz

# print(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S'))
# print(datetime.now(pytz.timezone('Asia/Kolkata')))
# print(datetime.now())

# COMMAND ----------

# DBTITLE 1,File name creation
# importing time libraries

from datetime import datetime
import pytz

current_dt = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
print(current_dt)

fileName = 'Demo_logging_'+current_dt+'.log'
print(fileName)

directory = '/tmp/'

finalFileName = directory+fileName
print(finalFileName)

partitions = current_dt # to creation parttions folders in dbfs location



# COMMAND ----------

# DBTITLE 1,File handler creation
# importing logging library
import logging

ct_logger = logging.getLogger('Demo_logging')
ct_logger.setLevel(logging.INFO)

file_Handler = logging.FileHandler(finalFileName, mode = 'a')
file_formatter = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(message)s",datefmt = '%m/%d/%Y%I:%M:%S%p')
file_Handler.setFormatter(file_formatter)

ct_logger.addHandler(file_Handler)

ct_logger.debug('debug_message')
ct_logger.info('info_message')
ct_logger.warning('warning_message')
ct_logger.error('error_message')
ct_logger.critical('critical_message')



