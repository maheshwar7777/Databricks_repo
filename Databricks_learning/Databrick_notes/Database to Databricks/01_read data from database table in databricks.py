# Databricks notebook source
# Create database connection using below commands ;

# Approch 1 :

jdbcHostname = '<Place the host name>'
jdbcPort = 1433  #it default value
jdbcDatabase = '<Place the database name>'
jdbcUsername = '<Please the user name>'
jdbcPassword = '<Place the password to connec the database>'
jdbcDriver = 'com.mircosoft.sqlserver.jdbc.SQLServerDriver'  #it's default value for sql server database

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName = {jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"



# COMMAND ----------

#  now read the table into dataframe

df1= spark.read.format('jdbc').option('url',jdbcUrl).option('dbtable','<Place the table name which we want to read >').load()

display(df1)

# COMMAND ----------

# Approach 2 :

connectionString = '<get the connection string from azure sql database (go to overview table and copy the connection string), then replace the password>'

# COMMAND ----------

# Raed the table inot dataframe

df2= spark.read.jdbc(connectionString,'<place the table name which we want to read>')

display(df2)
