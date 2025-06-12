# Databricks notebook source
"""
1-> Difference between Deep and shallow clone in databricks
    ans :
        ** Shallow Clone
            --> Metadata Duplication: Only duplicates the metadata of the table being cloned. The actual data files are not copied.
            --> Cost-Effective: Cheaper to create since it doesn't involve copying data files.
            --> Dependency on Source: Relies on the source table's data files. If the source files are removed (e.g., via VACUUM), the shallow clone may become unusable.
            --> Use Cases: Typically used for short-lived scenarios like testing and experimentation.
            --> Syntax : CREATE TABLE target_table SHALLOW CLONE source_table_name;
                            or
                         CREATE TABLE target_table SHALLOW CLONE source_table_name VERSION AS OF version;
        ** Deep Clone
            --> Full Copy: Makes a complete copy of both the metadata and data files of the table being cloned.
            --> Self-Contained: Independent of the source table, meaning it won't be affected if the source files are removed.
            --> Use Cases: Suitable for long-term use cases such as data archiving, reproducing machine learning datasets, and data sharing.
            --> syntax : CREATE TABLE target_table DEEP CLONE source_table_name;
                            or
                         CREATE TABLE target_table DEEP CLONE source_table_name TIMESTAMP AS OF timestamp_expression;

2-> How to restore the delta table based on timestamp in databricks
    ans :
        syntax :
        --> for timestamp : RESTORE TABLE table_name TO TIMESTAMP AS OF 'yyyy-MM-dd HH:mm:ss';
        --> for version : RESTORE TABLE table_name TO VERSION AS OF version_number; 

3-> Write a sql statement with dense rank in databricks ?
    ans : 
        syntax :
            SELECT
                    employee_id,
                    sales_amount,
                    sales_date,
                    DENSE_RANK() OVER (PARTITION BY sales_date ORDER BY sales_amount DESC) AS rank
                FROM
                    sales;

4-> What are indexs in sql ( i mean in spark sql) in databricks ?
    ans:
        1. Bloom Filter Indexes
            --> Purpose: Used for data skipping by creating a space-efficient data structure that helps determine if a column value is present in a file.
            --> Usage: Particularly useful for fields containing arbitrary text and can significantly reduce the amount of data read during queries1.
            --> Syntax:
            CREATE BLOOMFILTER INDEX ON TABLE table_name FOR COLUMNS(column_name OPTIONS (fpp=0.1, numItems=5000));
        2. Partitioning
            --> Purpose: While not a traditional index, partitioning organizes data into subdirectories based on column values, allowing for efficient data retrieval.
            --> Usage: Helps in querying only the relevant partitions, thus skipping unnecessary data.
        3. Data Skipping
            --> Purpose: Uses metadata to skip reading files that do not match query predicates.
            --> Usage: Automatically applied in Delta Lake tables to improve query performance.
        4. Z-Ordering
            --> Purpose: A multi-dimensional clustering technique that co-locates related information in the same set of files.
            --> Usage: Optimizes the layout of data to improve the performance of queries that filter on multiple columns

5-> What are the reasons of slow query performance and how can we increase the query speed in databricks ?
    ans :
    1-> Common Reasons for Slow Query Performance
            --> Data Skew: Uneven distribution of data across partitions can cause some tasks to process large amounts of data while others remain idle.
            --> Small File Problem: Having many small files can overwhelm Spark's metadata handling, slowing down reads.
            --> Inefficient Query Logic: Poorly written SQL or DataFrame operations can force unnecessary shuffles or full scans.
            --> Suboptimal Spark Configurations: Default settings may not match your workload, leading to underutilized resources or out-of-memory errors.
            --> Lack of Caching: Not using caching effectively can result in repeated data reads and processing.
            --> Resource Bottlenecks: Insufficient cluster resources (CPU, memory) can slow down query execution.
    2-> Ways to Improve Query Speed
            --> Optimize Data Distribution: Use techniques like salting to redistribute skewed keys and enable Adaptive Query Execution (AQE) to auto-optimize skewed joins.
            --> Compact Small Files: Use the OPTIMIZE command in Delta Lake to compact small files and improve read performance.
            --> Improve Query Logic: Avoid SELECT * and use predicate pushdown with partition filters. Replace Cartesian joins with broadcast joins for small tables.
            --> Tune Spark Configurations: Adjust settings like spark.sql.shuffle.partitions to better match your workload.
            --> Leverage Caching: Use Delta caching to store frequently accessed data in memory.
            --> Increase Cluster Resources: Provision larger clusters to handle workloads more efficiently.
            --> Use Photon: Databricks's Photon engine can significantly speed up query execution by leveraging native code and better memory management.

6-> What is Data profiling in databricks ?
    ans :
        1-> Data profiling in Databricks involves analyzing and summarizing data to understand its structure, quality, and characteristics. This process is essential for data discovery, quality assessment, and preparation for further analysis or machine learning tasks. Here are some key aspects of data profiling in Databricks:

        Key Features of Data Profiling in Databricks:
            --> Summary Statistics: Provides basic statistics such as row count, null count, mean, standard deviation, and unique value counts for each column.

            --> Data Distribution: Visualizes the distribution of values in each column using histograms and other charts.

            --> Data Quality Metrics: Identifies issues like missing values, duplicates, and outliers to assess data quality.

            --> Column Profiling: Analyzes individual columns to understand their data types, value ranges, and common patterns

        How to Perform Data Profiling in Databricks
            --> Using the Databricks Notebook: 
                    a-> When viewing a DataFrame or SQL query result, you can switch to the "Data Profile" tab to automatically generate a profile of the data.
                    b-> This profile includes summary statistics and visualizations for numeric, string, and date columns.
            
            --> Using the dbutils Library: You can generate data profiles programmatically using the dbutils.data.summarize(df) command in Python, Scala, or R

7-> What is Unity catalog in databricks ? 
    ans :
        Unity Catalog in Databricks is a unified governance solution for managing data and AI assets across Databricks workspaces. It provides centralized access control, auditing, lineage, and data discovery capabilities. Here are some key features:

        Key Features of Unity Catalog
            --> Centralized Access Control: Unity Catalog offers a single place to administer data access policies that apply across all workspaces.
            --> Standards-Compliant Security Model: Based on ANSI SQL, it allows administrators to grant permissions using familiar syntax at the level of catalogs, schemas, tables, and views.
            --> Built-in Auditing and Lineage: Automatically captures user-level audit logs and lineage data, tracking how data assets are created and used.
            --> Data Discovery: Enables tagging and documenting data assets, and provides a search interface to help data consumers find relevant data.
            --> System Tables: Provides access to operational data, including audit logs, billable usage, and lineage.
        
        Object Hierarchy in Unity Catalog
            --> Metastore: The top-level container for metadata, registering data and AI assets and permissions.
            --> Catalogs: Organize data assets and often mirror organizational units or development lifecycle scopes.
            --> Schemas: Contain tables, views, volumes, AI models, and functions.

8-> What is Data govorance in unity cataog in databricks ?
9-> Difference between Delta lake and data lake.
    ans :
            Data Lake
                --> Definition: A Data Lake is a centralized repository that allows you to store all your structured and unstructured data at any scale. You can store your data as-is, without having to first structure the data, and run different types of analytics—from dashboards and visualizations to big data processing, real-time analytics, and machine learning.
                --> Flexibility: Data Lakes offer high flexibility, allowing you to store data in its raw format, which can be in various forms such as JSON, CSV, Parquet, etc.
                --> Challenges: Without proper management, Data Lakes can become data swamps, where the data is hard to manage, query, and analyze due to lack of schema enforcement and data quality checks.
            
            Delta Lake
                --> Definition: Delta Lake is an open-source storage layer that brings reliability to Data Lakes. It provides ACID (Atomicity, Consistency, Isolation, Durability) transactions, scalable metadata handling, and unifies streaming and batch data processing.
                --> Enhanced Features: Delta Lake adds features like schema enforcement, data versioning, and time travel (the ability to query past versions of the data), which help in maintaining data quality and reliability.
                --> Performance: Delta Lake optimizes query performance by indexing data and providing efficient data layout and storage.
            
            Key Differences
                --> Data Management: Delta Lake provides ACID transactions and schema enforcement, which are not inherently available in traditional Data Lakes.
                --> Data Quality: Delta Lake ensures higher data quality with features like data versioning and time travel, whereas Data Lakes might struggle with data consistency and quality.
                --> Use Cases: Data Lakes are suitable for storing large volumes of raw data, while Delta Lake is ideal for scenarios requiring reliable data processing and analytics.
                            
10-> What is Pedicate pushdown ? ("maybe we can call this "data skipping")
    Ans : Predicate push down is another feature of Spark and Parquet that can improve query performance by reducing the amount of data read from Parquet files. Predicate push down works by evaluating filtering predicates in the query against metadata stored in the Parquet files.

        ->Predicate pushdown is a query optimization technique used in database systems to improve performance by filtering data at the data source before it is transferred and processed. This method reduces the amount of data that needs to be handled during query execution, making the process more efficient.

        ->When you run a query with filtering conditions (like WHERE clauses), predicate pushdown ensures that these filters are applied as close to the data source as possible. This means only the necessary data is retrieved, minimizing the amount of data read from disk, transferred over networks, or loaded into memory.

        ->For example, in a SQL query, if you have a condition to filter rows based on a specific date range, predicate pushdown will apply this filter at the storage level, so only the rows that meet the condition are fetched.

        ->This technique is beneficial for handling large datasets, as it significantly reduces the computational burden and speeds up query execution

11-> What are the transformations used in databricks pipelines development.
    ans : Filtering, aggregating, joining, handing missing values, writing data into data lake/ delta tables , adding new columns and performing calculation in columns (using withColumns) 

12-> How do we handle memory out issue when job failure happened in databricks.
        Ans : 
            Handling out-of-memory (OOM) issues in Databricks can be challenging, but there are several strategies you can employ to mitigate and resolve these errors:

            Optimize Data Partitioning:

                1-> Smaller Partitions: Ensure that your data is partitioned into smaller chunks to avoid overloading memory. You can adjust the number of partitions using repartition() or coalesce() methods.
                2-> Shuffle Partitions: Increase the number of shuffle partitions by setting spark.sql.shuffle.partitions to a higher value.
            
            Memory Configuration:

                1-> Increase Executor Memory: Allocate more memory to executors by adjusting the spark.executor.memory and spark.executor.memoryOverhead settings.
                2-> Driver Memory: Similarly, increase the memory allocated to the driver by setting spark.driver.memory.
            
            Avoid Collecting Large Datasets:

                1-> Avoid collect(): Instead of collecting large datasets to the driver, write results to cloud storage (e.g., S3, ADLS) or use show() with a limited number of rows.
            
            Optimize Joins and Aggregations:

                1-> Broadcast Joins: Use broadcast joins for smaller tables to reduce memory usage. Set spark.sql.autoBroadcastJoinThreshold to an appropriate value.
                2-> Skewed Data Handling: Address data skew by using techniques like salting or repartitioning skewed keys.
            
            Garbage Collection and Memory Management:

                1-> GC Tuning: Enable and tune garbage collection logging to identify and address frequent GC cycles. Use JVM options like -XX:+PrintGCDetails and -XX:+PrintGCTimeStamps.
                2-> Heap Dumps: Generate heap dumps for in-depth analysis of memory usage and potential leaks.
            
            Monitoring and Debugging:

                1-> Spark UI: Use the Spark UI to monitor memory usage, identify stages with high memory consumption, and analyze task execution.
                2-> Logs: Check driver and executor logs for OutOfMemoryError messages and other relevant information.
            
            By implementing these strategies, you can effectively manage and resolve memory-related issues in your Databricks jobs

13-> How do we optimize the job performance when dealing with large data in databricks ?
    ans : 
        --> Use Larger Clusters: Increasing the size of your clusters can significantly speed up your workloads without necessarily increasing costs. Larger clusters can process data faster, reducing the overall runtime.

        --> Photon Engine: Utilize Databricks' Photon engine, which is a high-performance query engine written in C++. It leverages modern CPU architectures and improves query performance.

        --> Delta Lake: Use Delta Lake for your data storage. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. It also supports efficient data layout and indexing.

        --> Optimize File Sizes: Ensure that your data files are of optimal size (between 16MB and 1GB). This helps in reducing read latency and improves query performance.

        --> Delta Caching: Enable Delta caching to speed up data reads by caching data in memory. This can significantly reduce the time taken to read frequently accessed data.

        --> Clean Configurations: Regularly review and clean up your Spark configurations. Outdated or unnecessary configurations can cause performance issues1.

        --> Monitor Job Performance: Use Databricks' monitoring tools to track job performance. This helps in identifying bottlenecks and optimizing resource utilization3.

        --> Lazy Evaluation: Be aware of Spark's lazy evaluation model. This means transformations are not executed until an action is called. Understanding this can help in optimizing the execution plan.

14-> How do we read csv file and write into parque files.
    ans : 
        --> Reading csv file :
            df = spark.read.format('csv').option('header',True).option('inferSchema, True).schema(schema).load('<file path'>)

        --> Writing into parque file :

            df.write.format('parquet').mode('overwrite').save('<file path>')

15-> How do we handle duplicates in dataframes.
    ans : 
        syntax : df.dropDuplicate()  or df.distinct()

16-> What are the issue and error faced in your project development.
    ans :
            Databricks pipelines can encounter a range of issues and errors. Here are some common ones:

        1. Cluster Configuration Problems:
                Insufficient resources: Jobs fail due to lack of memory or CPU.
                Wrong cluster type: Using a standard cluster instead of a job cluster can lead to cost inefficiencies or performance issues.
        
        2. Dependency Management:
                Missing libraries: Pipelines fail if required Python/Scala libraries are not installed.
                Version conflicts: Different libraries may require incompatible versions of dependencies.
        
        3. Data Quality and Schema Issues:
                Schema drift: Unexpected changes in source data schema can break pipelines.
                Null or corrupt data: Can cause transformations to fail or produce incorrect results.

        4. Job Failures and Timeouts:
                Long-running transformations: Poorly optimized Spark jobs can exceed time limits.
                Retries not configured: Transient failures can cause job termination if retries aren’t set.
        
        5. Code and Logic Errors:
                Incorrect joins: Can lead to data duplication or loss.
                Improper handling of nulls: Can cause runtime errors or misleading results.

        6. Delta Lake Specific Issues:
                Concurrent writes: Can lead to transaction conflicts.
                Vacuum misconfiguration: Can delete data still in use if retention period is too short.

        7. Permissions and Access Control:
                Missing permissions: Users or jobs may not have access to required data or resources.
                Unity Catalog misconfigurations: Can block access to tables or views.

        8. Monitoring and Debugging Challenges:
                Lack of logging: Makes it hard to trace issues.
                Limited visibility into Spark UI: Especially for short-lived jobs.

        9. Pipeline Scheduling and Orchestration:
                Incorrect triggers: Pipelines may not run as expected.
                Dependency mismanagement: Downstream tasks may start before upstream ones finish.

        10. Environment Differences:
                Dev vs Prod mismatch: Pipelines that work in dev may fail in prod due to data volume or config differences

17-> what are the way to perform data validation in databricks pipelines development
        ans : 
        Performing data validation in Databricks pipelines is crucial to ensure data quality and reliability. Here are some effective methods:

        Delta Live Tables (DLT) Expectations:
            1-> Define Expectations: Use SQL Boolean expressions to set constraints on data. For example, you can ensure that a column value falls within a specific range or matches a certain pattern.
            2-> Monitor Data Quality: Track and monitor data quality metrics through the Delta Live Tables event log.
        
        Data Quality Management Framework:

            1-> Six Dimensions Model: Implement checks for consistency, accuracy, validity, completeness, timeliness, and uniqueness.
            2-> Medallion Architecture: Use a structured approach to clean and transform data as it moves through different stages (bronze, silver, gold) in the lakehouse.
        
        Custom Validation Rules:

            1-> Batch Processing: Apply custom validation rules during batch processing to clean and validate data before it is ingested into the pipeline.
            2-> Stream Processing: Implement real-time validation rules to ensure data quality as it flows through the pipeline.
        
        Automated Testing:

            1-> Unit Tests: Write unit tests for your data transformation logic to catch errors early in the development process.
            2-> Integration Tests: Validate the entire pipeline by running integration tests that simulate real-world data scenarios.
            
        Data Profiling:
            1-> Statistical Analysis: Perform statistical analysis to identify anomalies and outliers in the data.
            2-> Schema Validation: Ensure that the data conforms to the expected schema and structure.

18-> Do u know  CI_CD process and code checkin github.
19-> How to invoke a notebook from another notebook ?
    Ans : we can use mgic commadn to invoke notebook from another notebook.
        syntax : %run '<notebook name>' 
        or
        syntax : dbutils.notebook.run('<notebook name>')

20-> Difference between map and mapfunction in databricks and it uses ?
21-> What is scehma on write and schema on read in databricks ?
    ans :
        --> Schema-on-Write: Suitable for scenarios where data quality and consistency are paramount, and the data structure is well-defined and stable.
        --> Schema-on-Read: Ideal for environments where data formats are diverse and evolving, and flexibility is needed to handle raw and semi-structured data.

22-> what is use of vaccum command in databricks
    ans : 
        ->The VACUUM command in Databricks is used to remove unused data files from a table directory, which helps in managing storage costs and maintaining data compliance. Here are some key points about its usage:

        ->Delta Tables: For Delta tables, VACUUM removes all files that are no longer in the latest state of the transaction log and are older than a specified retention threshold (default is 7 days).
        ->Non-Delta Tables: It can also be used on non-Delta tables to clean up unused files.
        ->Retention Period: The retention period can be adjusted to retain data for longer durations if needed, which is useful for supporting time travel queries.
        ->Cost and Compliance: Regularly running VACUUM helps reduce cloud storage costs and ensures that deleted or modified records are permanently removed.

23-> What does an index mean in the context of Delta Lake?
    Ans :
        The context of Delta Lake, an index refers to a mechanism that helps improve the performance of data retrieval operations. Here are some key points about indexing in Delta Lake:

        Automatic Indexing:
            Delta Lake automatically indexes the columns in the files you write. This indexing is added to the internal table metadata, which helps speed up query performance.
        
        Data Skipping:
            Delta Lake uses data skipping to enhance query performance. It maintains statistics about the data in each file, such as min and max values for each column. When a query is executed, Delta Lake can skip reading files that do not match the query criteria.

        Z-Ordering:
            Z-ordering is a technique used to optimize the layout of data in storage. By clustering related information together, Z-ordering improves the efficiency of data skipping and reduces the amount of data read during queries.
        
        Compaction:
            Delta Lake supports compaction, which coalesces small files into larger ones. This reduces the overhead of managing many small files and improves read performance.
        
    These indexing and optimization techniques help Delta Lake provide fast and efficient data retrieval, making it suitable for large-scale data processing and analytics.

24-> what are optimized joins in databricks
    ans : 
        1. Broadcast Joins
            Description: Small tables are broadcasted to all worker nodes, allowing for efficient joins with larger tables.
            Use Case: Ideal when one of the tables is small enough to fit into the memory of each worker node.
                    Example:
                    small_df = spark.read.parquet("path/to/small_table")
                    large_df = spark.read.parquet("path/to/large_table")
                    joined_df = large_df.join(broadcast(small_df), "key")

        2. Shuffle Hash Joins
            Description: Data is partitioned and shuffled across nodes based on the join key, allowing for parallel processing.
            Use Case: Suitable for large tables where broadcast joins are not feasible.
                    Example:
                    joined_df = large_df.join(another_large_df, "key", "shuffle_hash")


                    🔄 How it works (Step-by-Step):
                        Shuffle Phase:
                        Spark partitions both datasets by the join key and shuffles them across the network so that matching keys end up on the same executor.

                        Hash Build Phase:
                        On each executor, Spark builds a hash table from one side of the join (usually the smaller partition).
                        
                        Probe Phase:
                        Spark scans the other side and probes the hash table to find matching rows.

        3. Sort Merge Joins
            Description: Both tables are sorted by the join key, and then merged. This is efficient for sorted data.
            Use Case: Effective when both datasets are already sorted or can be sorted efficiently.
                    Example:
                    sorted_df1 = large_df.sort("key")
                    sorted_df2 = another_large_df.sort("key")
                    joined_df = sorted_df1.join(sorted_df2, "key", "merge")

        4. Range Joins
            Description: Joins based on a range condition, such as a value falling within a specific interval.
            Use Case: Useful for time-series data or spatial data where range conditions are common.
                    Example:
                    range_joined_df = points_df.join(ranges_df, points_df["p"].between(ranges_df["start"], ranges_df["end"]))

        5. Join Hints
            Description: Explicitly suggest the join strategy to the optimizer using hints.
            Use Case: Helps the optimizer choose the most efficient join strategy based on the data characteristics.
                    Example:
                    joined_df = large_df.join(another_large_df.hint("broadcast"), "key")

        6. Delta Lake Optimizations
            Description: Using Delta Lake's features like Z-order indexing and data skipping to optimize join performance.
            Use Case: Enhances performance by reducing the amount of data scanned during joins.
                    Example:
                    delta_df = spark.read.format("delta").load("path/to/delta_table")
                    delta_df.optimize().zorderBy("key")

25-> What is bucketing in databricks?
    ans:
        Bucketing involves distributing data into a specified number of buckets based on values from one or more columns. This process helps in organizing data more efficiently.

        ** Benefits of Bucketing
            --> Improved Performance: By pre-sorting and shuffling data, bucketing reduces the need for these operations during downstream processes, such as joins.
            --> Efficient Joins: When joining large tables, bucketing can significantly speed up the process by minimizing shuffling.
        ** How to Implement Bucketing
            --> In Databricks, you can use the BUCKET BY clause in SQL to create bucketed tables. Here's a simple example:
                syntax :
                        CREATE TABLE bucketed_table
                        USING parquet
                        BUCKET BY (column_name) INTO 10 BUCKETS;
        ** Considerations
            --> Initial Overhead: Bucketing involves an initial overhead due to sorting and shuffling, but it pays off in subsequent operations.
            --> Delta Tables: Bucketing is not supported for Delta tables in Databricks

26--> what are the action function in databricks
    ans:
        In Databricks, action functions are operations that trigger the execution of computations and return results to the driver program. These functions are essential for interacting with data and performing transformations. Here are some common action functions in Databricks:
            Example are :
                --> collect(): Retrieves the entire dataset as an array to the driver.
                --> count(): Returns the number of elements in the dataset.
                --> take(n): Returns the first n elements of the dataset.
                --> show(): Displays the top rows of the dataset in a tabular format.
                --> first(): Returns the first element of the dataset.
                --> last(): Returns the last element of the dataset.
                --> reduce(func): Aggregates the elements of the dataset using the specified function.
                --> fold(zeroValue)(func): Similar to reduce, but with a specified initial value.
                --> aggregate(zeroValue)(seqOp, combOp): Aggregates the elements of the dataset using the specified sequence and combination operations.
                --> foreach(func): Applies a function to each element of the dataset.
                --> foreachPartition(func): Applies a function to each partition of the dataset.
                --> saveAsTextFile(path): Saves the dataset as a text file at the specified path.
                --> saveAsTable(tableName): Saves the dataset as a table in the Databricks metastore.
                --> write.format("format").save(path): Saves the dataset in the specified format (e.g., Parquet, JSON) at the given path.
                --> countByValue(): Returns the count of each unique value in the dataset.
                --> takeSample(withReplacement, num, [seed]): Returns a sample of the dataset.


27--> What is the full list of narrow and wide transformation functions in databricks
    ans:
        In Databricks, transformations are categorized into narrow and wide transformations based on how data is shuffled across partitions. Here's a comprehensive list of both types:

        1--> Narrow Transformations:
            Narrow transformations do not require shuffling of data across partitions. Each output partition depends on a single input partition. 
            
                Examples include:
                    --> map(): Applies a function to each element in the dataset.
                    --> filter(): Selects elements that satisfy a predicate.
                    --> flatMap(): Similar to map, but can return multiple elements for each input element.
                    --> mapPartitions(): Applies a function to each partition.
                    --> union(): Combines two datasets into one.
                    --> sample(): Returns a sampled subset of the dataset.
                    --> distinct(): Removes duplicate elements.
                    --> coalesce(): Reduces the number of partitions without shuffling.
        
        2--> Wide Transformations
            Wide transformations require shuffling of data across partitions, which can be more computationally expensive. 
        
                Examples include:
                    --> groupByKey(): Groups elements by key.
                    --> reduceByKey(): Reduces elements by key using a specified function.
                    --> aggregateByKey(): Aggregates elements by key using specified functions.
                    --> sortByKey(): Sorts elements by key.
                    --> join(): Joins two datasets based on keys.
                    --> cogroup(): Groups datasets by key and applies a function.
                    --> repartition(): Changes the number of partitions, causing a shuffle.
                    --> combineByKey(): Combines elements by key using specified functions.

28--> What is SCD type 1 and 2 . implement SCD type 2 ?
29-->  What are the file present in delta log folder ? And what data represents in .json and.crc files in delta log folder ?
    ANS : 
            In Delta Lake, the _delta_log/ folder is a critical component that stores the transaction log for a Delta table. This log enables ACID transactions, schema enforcement, time travel, and data versioning.

            📁 Files in the _delta_log/ Folder
            1--> .json files : 
            --> These are transaction log files.
            --> Each file represents a single atomic commit to the Delta table.
            --> Named as <version>.json, e.g., 00000000000000000010.json.
            
            2--> .crc files:
            --> These are checksum files for the corresponding .json files.
            --> Used to verify data integrity and detect corruption.
            --> Named as <version>.json.crc.
            
            3--> _last_checkpoint:            
            --> A small JSON file that points to the latest checkpoint.
            --> Helps Delta Lake quickly load the latest state of the table.
            
            4-->Checkpoint Parquet files (optional but common):
            --> Periodically, Delta creates checkpoint files in Parquet format.
            --> These summarize the state of the table up to a certain version.
            --> Named like 00000000000000000100.checkpoint.parquet.

30 --> what is control and data plane in databricks
    ans :

    In Databricks, the control plane and data plane are two key architectural components that separate the management of the platform from the processing of data. Here's a breakdown of each:

        🔧 Control Plane
            The control plane is responsible for managing and orchestrating the Databricks environment. It includes:

            --> User interface (UI): Notebooks, jobs, clusters, and workspace management.
            --> APIs: For interacting with Databricks programmatically.
            --> Job scheduling: Managing job execution and workflows.
            --> Cluster management: Creating, configuring, and terminating clusters.
            --> Security and access control: Authentication, authorization, and audit logging.
            --> The control plane is hosted and managed by Databricks in their cloud environment (e.g., AWS, Azure, or GCP).

        💾 Data Plane
            The data plane is where your actual data processing happens. It includes:

            --> Compute resources: Virtual machines (VMs) or containers that run Spark jobs.
            --> Data access: Reading from and writing to data sources like S3, ADLS, or GCS.
            --> Execution of code: Python, SQL, Scala, or R code runs here.
            --> The data plane can be in your own cloud account (in a customer-managed VPC), which means Databricks never accesses your data --> directly—this is known as the "customer-managed" or "data plane isolation" model.



----------###########################------------------------
Impetus interiew question :

1-> Explain about yourself
2-> Explain about your current project
3-> What is pyspark and spark architecture ?
    Ans : 
    🐍 What is PySpark?
            PySpark is the Python API for Apache Spark. It allows you to write Spark applications using Python instead of Scala or Java.

    🔑 Key Features:
           --> Enables distributed data processing using Python.
           --> Supports DataFrame API, RDD API, SQL, MLlib (machine learning), and GraphX (graph processing).
           --> Integrates with Pandas, NumPy, and other Python libraries.
    
    ⚙️ Apache Spark Architecture
        Apache Spark follows a master-slave architecture with the following components:

        1. Driver Program
           --> The main application that runs your Spark code.
            Responsible for:
                -->Converting user code into tasks
                -->Scheduling tasks on executors
                -->Tracking job progress
        
        2. Cluster Manager
           --> Allocates resources across applications.
            Can be:
                -->Standalone
                -->YARN (Hadoop)
                --> Mesos
                --> Kubernetes
        
        3. Executors
            -->Run on worker nodes.
            -->Execute tasks assigned by the driver.
            -->Store data in memory or disk (for caching and shuffling).
        
        4. Tasks and Jobs
            -->A job is triggered by an action (e.g., .collect(), .count()).
            -->A job is split into stages, and each stage into tasks.
            -->Tasks are distributed across executors.


4-> How do we establish connection between ADLS and Databricks? 
    a-> How do we connect ADLS and databricks other than access connector?
        Ans : 
            --> We can use below ways to perform connection between ADLS and datarbicks
            a. Databricks access connector
            b. Service principle
            c. SAS token
            d. storage account key 

5-> Difference between Azure synapsys and Azure databricks
6-> What is Delta table
    ans :
        A Delta table is a type of table in Databricks that leverages Delta Lake, an open-source storage layer that brings reliability to data lakes. Delta tables provide ACID (Atomicity, Consistency, Isolation, Durability) transactions, scalable metadata handling, and unifies streaming and batch data processing.


7-> Explain your project end -end and what is the final output 
8-> How do you import csv files and how did u cleaning the data and what transfromation did u applied in ur project
9-> What is unity catalog and it's benefits
10-> Suppose we dont have unity catalog and how do u connect ADLS storage to databricks
11-> What is Delta live table
12-> Difference between Delta live table and delta table
    ANS :
        Delta Tables
            --> Storage Format: Delta Tables are a storage format built on top of Delta Lake, which provides ACID transactions, scalable metadata handling, and data versioning.
            --> Usage: They are typically used for storing large-scale structured data in data lakes, supporting both batch and streaming data ingestion.
            --> Management: Delta Tables can be managed using SQL and the DataFrame API.
        
        Delta Live Tables (DLT)
            --> Pipeline Management: DLT is a framework for building reliable data pipelines. It allows you to define data transformations and manage the flow of data between Delta Tables.
            --> Declarative Approach: DLT uses a declarative approach to define ETL (Extract, Transform, Load) processes, simplifying the development and management of data pipelines.
            --> Enhanced Features: DLT provides additional features like task orchestration, cluster management, monitoring, data quality checks, and error handling1. It also supports both batch and streaming operations.

13-> What is autoloader
14-> What is SCD type 2 and write a pesudo code for it 
15-> Difference between Exceptall and left anti
16-> Difference between Shallow and deep clone
17-> What are the optimzation techniques
18-> What is the use of optimize command
19-> What is expections in DLT
20-> How do you inject json files (when header columns keep on changing the positions)
21-> What is cluster and types of cluster
22-> What are the steps u taken to handle data cleaning and bad data (bad records) ( like solving null values/ missing values) ?
23-> Can we have delta sharing at catalog level in unity catalog ? 
    ans : --> No , we do not have delta sharing at catalog level


"""
