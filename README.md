# AWS Glue tutorial
AWS Glue tutorial for data developers.

This is a complementary repository for this [AWS Glue tutorial with spark and python for data developers](https://data.solita.fi/aws-glue-tutorial-with-spark-and-python-for-data-developers). 

## Usage in Glue dev endpoint
To make the script work in dev endpoint, copy it to the notebook. Then comment out these lines in the beginning:

```py
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
````

and in the end:

```py
job.commit()
```

## DynamicFrame vs DataFrame in AWS Glue
Note the difference between [DynamicFrame](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html) and DataFrame. DataFrame is spark native table like structure. DynamicFrame class is an attempt from AWS to address limitations of the DataFrame.

In my opinion Dynamic Frames are handy to read and write data. I feel that data processing is more convenient with standard [pyspark functions](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html).
