# Databricks notebook source
'''
What is transformations and action :

  Transformation : We use to tranform laoded the data from files using commands like filter and union. Here task is not executes and it just created logical gragh (DAG)
  
  Two types of transformations : 
    1->Narrow transformation (here we don't shuffle the data and no performance is impacted (commands like Filter))  
    2->Wide transformation (here we shuffle the data and  performance is impacted (commands like groupby )) 


  Actions : Here the tranformations gets executed. It go to the DAG plan and execute steps by the executor and return the data to driver program. (commads like collect(), display() etc)





'''
