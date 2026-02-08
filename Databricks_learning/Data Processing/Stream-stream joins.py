# Databricks notebook source
# MAGIC %md 
# MAGIC #Stream-Stream Joins using Structured Streaming (Python)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A stream-stream join is when you join two continuously updating data streams in real-time. For example, imagine:
# MAGIC
# MAGIC One stream contains **user clicks**.
# MAGIC Another stream contains **ad impressions**. You might want to join them on a common user_id to analyze which ads users clicked.

# COMMAND ----------

# MAGIC %md
# MAGIC # Requirements for Stream-Stream Joins

# COMMAND ----------

# MAGIC %md
# MAGIC **Watermarking**: To handle late data and avoid infinite state buildup.
# MAGIC
# MAGIC **Time Constraints**: You must specify time bounds for the join condition.
# MAGIC
# MAGIC **Event Time Columns**: Both streams should have a timestamp column.

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook illustrates different ways of joining streams.
# MAGIC
# MAGIC We are going to use the the canonical example of ad monetization, where we want to find out which ad impressions led to user clicks. 
# MAGIC Typically, in such scenarios, there are two streams of data from different sources - ad impressions and ad clicks. 
# MAGIC Both type of events have a common ad identifier (say, `adId`), and we want to match clicks with impressions based on the `adId`. 
# MAGIC In addition, each event also has a timestamp, which we will use to specify additional conditions in the query to limit the streaming state.

# COMMAND ----------

# MAGIC %md In absence of actual data streams, we are going to generate fake data streams using our built-in "rate stream", that generates data at a given fixed rate.

# COMMAND ----------

from pyspark.sql.functions import rand

spark.conf.set("spark.sql.shuffle.partitions", "1")

impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)

clicks = (
  spark
  .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
  .where((rand() * 100).cast("integer") < 10)      # 10 out of every 100 impressions result in a click
  .selectExpr("(value - 50) AS adId ", "timestamp AS clickTime")      # -50 so that a click with same id as impression is generated later (i.e. delayed data).
  .where("adId > 0")
)    
  

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's see what data these two streaming DataFrames generate.
# MAGIC

# COMMAND ----------

display(impressions.limit(10))

# COMMAND ----------

display(clicks)

# COMMAND ----------

# MAGIC %md Note: 
# MAGIC - If you get an error saying the join is not supported, the problem may be that you are running this notebook in an older version of Spark. 
# MAGIC - If you are running on Community Edition, click Cancel above to stop the streams, as you do not have enough cores to run many streams simultaneously.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join
# MAGIC
# MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by their common key `adId`.

# COMMAND ----------

display(impressions.join(clicks, "adId"))

# COMMAND ----------

# MAGIC %md 
# MAGIC After you start this query, within a minute, you will start getting joined impressions and clicks. The delays of a minute is due to the fact that clicks are being generated with delay over the corresponding impressions.
# MAGIC
# MAGIC In addition, if you expand the details of the query above, you will find a few timelines of query metrics - the processing rates, the micro-batch durations, and the size of the state. 
# MAGIC If you keep running this query, you will notice that the state will keep growing in an unbounded manner. This is because the query must buffer all past input as any new input can match with any input from the past. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Inner Join with Watermarking
# MAGIC
# MAGIC To avoid unbounded state, you have to define additional join conditions such that indefinitely old inputs cannot match with future inputs and therefore can be cleared from the state. In other words, you will have to do the following additional steps in the join.
# MAGIC
# MAGIC 1. Define watermark delays on both inputs such that the engine knows how delayed the input can be. 
# MAGIC
# MAGIC 1. Define a constraint on event-time across the two inputs such that the engine can figure out when old rows of one input is not going to be required (i.e. will not satisfy the time constraint) for matches with the other input. This constraint can be defined in one of the two ways.
# MAGIC
# MAGIC   a. Time range join conditions (e.g. `...JOIN ON leftTime BETWEN rightTime AND rightTime + INTERVAL 1 HOUR`),
# MAGIC   
# MAGIC   b. Join on event-time windows (e.g. `...JOIN ON leftTimeWindow = rightTimeWindow`).
# MAGIC
# MAGIC Let's apply these steps to our use case. 
# MAGIC
# MAGIC 1. Watermark delays: Say, the impressions and the corresponding clicks can be delayed/late in event-time by at most "10 seconds" and "20 seconds", respectively. This is specified in the query as watermarks delays using `withWatermark`.
# MAGIC
# MAGIC 1. Event-time range condition: Say, a click can occur within a time range of 0 seconds to 1 minute after the corresponding impression. This is specified in the query as a join condition between `impressionTime` and `clickTime`.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC KeyPoints:
# MAGIC
# MAGIC withWatermark("timestampColumn", "delay"): tells Spark how long to wait for late data.
# MAGIC
# MAGIC The join condition must include a bounded time range (e.g., BETWEEN impressionTime AND impressionTime + interval 1 minute).
# MAGIC
# MAGIC Spark will only keep state for the duration of the watermark.

# COMMAND ----------

from pyspark.sql.functions import expr

# Define watermarks
impressionsWithWatermark = impressions \
  .selectExpr("adId AS impressionAdId", "impressionTime") \
  .withWatermark("impressionTime", "10 seconds ")
clicksWithWatermark = clicks \
  .selectExpr("adId AS clickAdId", "clickTime") \
  .withWatermark("clickTime", "20 seconds")        # max 20 seconds late


# Inner join with time range conditions
display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes    
      """
    )
  )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC We are getting the similar results as the previous simple join query. However, if you look at the query metrics now, you will find that after about a couple of minutes of running the query, the size of the state will stabilize as the old buffered events will start getting cleared up.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Joins with Watermarking 
# MAGIC
# MAGIC Let's extend this use case to illustrate outer joins. Not all ad impressions will lead to clicks and you may want to keep track of impressions that did not produce clicks. This can be done by applying a left outer join on the impressions and clicks. The joined output will not have the matched clicks, but also the unmatched ones (with clicks data being NULL).
# MAGIC
# MAGIC While the watermark + event-time constraints is optional for inner joins, for left and right outer joins they must be specified. This is because for generating the NULL results in outer join, the engine must know when an input row is not going to match with anything in future. Hence, the watermark + event-time constraints must be specified for generating correct results. 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr

# Inner join with time range conditions
display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes    
      """
    ),
    "leftOuter"
  )
)

# COMMAND ----------

# MAGIC %md After starting this query, you will start getting the inner results within a minute. But after a couple of minutes, you will also start getting the outer NULL results.