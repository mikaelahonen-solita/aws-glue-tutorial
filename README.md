# AWS Glue tutorial
AWS Glue tutorial for data developers.

This is a complementary repository for this [AWS Glue tutorial with Spark and Python for data developers](https://data.solita.fi/aws-glue-tutorial-with-spark-and-python-for-data-developers).

## DynamicFrame vs DataFrame in AWS Glue
Note the difference between [DynamicFrame](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html) and DataFrame. DataFrame is Spark native table like structure. DynamicFrame class is an attempt from AWS to address limitations of the DataFrame.

DynamicFrames might be handy to read and write data. Often the data processing is more efficient with standard [PySpark functions](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html).
