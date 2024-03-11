import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
import pyspark.sql.types as T


if __name__ == "__main__":

    ## @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv, 
        [
            'JOB_NAME', 
            'DATE', 
            'SOURCE_PATH', 
            'DEST_PATH',
            'CATALOG_DATABASE',
            'CATALOG_TABLE'
        ]
    )

    spark = (SparkSession.builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .getOrCreate())

    sc = spark.sparkContext
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    read_filepath = args.source_files_pattern
    write_filepath = args.destination_files_pattern
    read_filepath=read_filepath.format(date)
    print(f"date received: {date}")

    df = spark.read.json(read_filepath)

    allowed_events = [
        "PushEvent",
        "ForkEvent",
        "PublicEvent",
        "WatchEvent",
        "PullRequestEvent",
    ]

    main_df = df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp( F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'" ).alias("created_at"),
        F.col("repo.id").alias("repository_id"),
        F.col("repo.name").alias("repository_name"),
        F.col("repo.url").alias("repository_url"),
        F.col("actor.id").alias("user_id"),
        F.col("actor.login").alias("user_name"),
        F.col("actor.url").alias("user_url"),
        F.col("actor.avatar_url").alias("user_avatar_url"),
        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_name"),
        F.col("org.url").alias("org_url"),
        F.col("org.avatar_url").alias("org_avatar_url"),
        F.col("payload.push_id").alias("push_id"),
        F.col("payload.distinct_size").alias("number_of_commits"),
        F.col("payload.pull_request.base.repo.language").alias("language"),
    ).filter(
        F.col("type").isin(allowed_events)
    )

    main_df = main_df.withColumn("year", F.year("created_at")) \
        .withColumn("month", F.month("created_at")) \
        .withColumn("day", F.dayofmonth("created_at")) \
        .withColumn("hour", F.hour("created_at")) \
        .withColumn("minute", F.minute("created_at")) \
        .withColumn("second", F.second("created_at")) \
    
    # add timestamp field
    main_df = main_df.withColumn("ts", F.unix_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # add update date field 
    main_df = main_df.withColumn("updated_at", F.current_timestamp())

    # For custom configuration visit 
    # https://hudi.apache.org/docs/configurations/
    hudi_options = {
        "hoodie.table.name": "gh_archive",
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "event_id",
        "hoodie.datasource.write.precombine.field": "updated_at",
        "hoodie.datasource.write.partitionpath.field": "year, month, day, hour",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": CATALOG_DATABASE,
        "hoodie.datasource.hive_sync.table": CATALOG_TABLE,
        "hoodie.datasource.hive_sync.partition_fields": "year, month, day, hour",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }


    main_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save("s3a://gh-archive-data-curated/hudi/gh_archive/")

    job.commit()
