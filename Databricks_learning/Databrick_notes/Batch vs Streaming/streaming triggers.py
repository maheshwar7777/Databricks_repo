# Databricks notebook source
"""
In streaming trigger , we have 3 types. those are:

1. Default : Executes micro batch as soon as previous batch finishes

2. Fixed interval mirco-batches : Specifies the interval when th emicro batch will execute.  For exaample 1 hrs, 20 mins or 1 hr

3. One-time micro batch : Executes one micro batch to process all available data and then stops.

"""
