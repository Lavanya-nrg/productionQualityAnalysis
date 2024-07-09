import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import from_json, col, expr, isnan, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
import re

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_bucket', 'output_bucket', 'redshift_url', 'redshift_user', 'redshift_password', 'report1_table', 'report2_table', 'report3_table', 'sales_table', 'complaints_table', 'production_table','redshift_schema','redshift_database'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read sales data from S3
sales_data = glueContext.create_dynamic_frame.from_catalog(database="ln-redshiftdb", table_name=args['sales_table'])
sales_df = sales_data.toDF()

# Read complaints data from S3
complaints_data = glueContext.create_dynamic_frame.from_catalog(database="ln-redshiftdb", table_name=args['complaints_table'])
complaints_df = complaints_data.toDF()

# Read production data from S3
production_df = spark.read.option("delimiter", "\t").csv(f"s3://{args['input_bucket']}/Production_logs.tsv", header=True, inferSchema=True)

# Define schema for JSON columns
json_schema = StructType([
    StructField("scissor", IntegerType(), True),
    StructField("paper", IntegerType(), True),
    StructField("rock", IntegerType(), True)
])

# Parse JSON columns and flatten them
production_flat = production_df.withColumn("Items_Produced_json", from_json(col("Items_Produced"), json_schema)) \
            .withColumn("Defective_Items_json", from_json(col("Defective_Items"), json_schema)) \
            .select(
                col("Production_Unit_Id"),
                col("Items_Produced_json.scissor").alias("Items_Produced_Scissor"),
                col("Items_Produced_json.paper").alias("Items_Produced_Paper"),
                col("Items_Produced_json.rock").alias("Items_Produced_Rock"),
                col("Defective_Items_json.scissor").alias("Defective_Items_Scissor"),
                col("Defective_Items_json.paper").alias("Defective_Items_Paper"),
                col("Defective_Items_json.rock").alias("Defective_Items_Rock")
            )

# Data quality checks
# Check that all IDs are unique
duplicate_ids = production_flat.groupBy("Production_Unit_Id").count().filter(col("count") > 1).select("Production_Unit_Id").collect()
if duplicate_ids:
    duplicate_ids_list = [row["Production_Unit_Id"] for row in duplicate_ids]
    print(f"Duplicate Production_Unit_Id found: {duplicate_ids_list}")
    production_flat = production_flat.dropDuplicates(["Production_Unit_Id"])

# Check that no other product other than rock, paper, scissor is there
allowed_products = ["Items_Produced_Scissor", "Items_Produced_Paper", "Items_Produced_Rock",
                    "Defective_Items_Scissor", "Defective_Items_Paper", "Defective_Items_Rock"]
unexpected_columns = set(production_flat.columns) - set(allowed_products + ["Production_Unit_Id"])
if unexpected_columns:
    raise ValueError(f"Unexpected products found: {unexpected_columns}")

# Check that numeric values are > 0
for col_name in allowed_products:
    if production_flat.filter((col(col_name) < 0) | isnan(col(col_name))).count() > 0:
        raise ValueError(f"Negative or NaN values found in column {col_name}")

# Check that IDs are alphanumeric
alphanumeric_regex = "^[a-zA-Z0-9]*$"
if production_flat.filter(~col("Production_Unit_Id").rlike(alphanumeric_regex)).count() > 0:
    raise ValueError("Production_Unit_Id contains non-alphanumeric characters")

# Calculate total items produced and defective items per unit
production_aggregated = production_flat.withColumn("Total_Items_Produced",
                                   col("Items_Produced_Scissor") +
                                   col("Items_Produced_Paper") +
                                   col("Items_Produced_Rock")) \
                       .withColumn("Total_Defective_Items",
                                   col("Defective_Items_Scissor") +
                                   col("Defective_Items_Paper") +
                                   col("Defective_Items_Rock"))

# Aggregate total items produced and defective items per unit
total_items_sum = production_aggregated.groupBy("Production_Unit_Id").agg(
    F.sum("Total_Items_Produced").alias("Total_Items_Sum"),
    F.sum("Total_Defective_Items").alias("Total_Defective_Items_Sum")
)

# Calculate defective percentages per unit and overall
production_percentage = production_aggregated.join(total_items_sum, "Production_Unit_Id") \
    .withColumn("Defective_Percentage_Scissor",
                (col("Defective_Items_Scissor") / col("Items_Produced_Scissor")) * 100) \
    .withColumn("Defective_Percentage_Paper",
                (col("Defective_Items_Paper") / col("Items_Produced_Paper")) * 100) \
    .withColumn("Defective_Percentage_Rock",
                (col("Defective_Items_Rock") / col("Items_Produced_Rock")) * 100) \
    .withColumn("Overall_Percentage_Defective",
                (col("Total_Defective_Items") / col("Total_Items_Sum")) * 100) \
    .select(
        col("Production_Unit_Id").alias("production_unit_id"),
        col("Defective_Percentage_Scissor"),
        col("Defective_Percentage_Paper"),
        col("Defective_Percentage_Rock"),
        col("Overall_Percentage_Defective")
    )
# production_percentage_selected = production_percentage.select(
#     "production_unit_id",
#     "Defective_Percentage_Scissor",
#     "Defective_Percentage_Paper",
#     "Defective_Percentage_Rock",
#     "Overall_Percentage_Defective"
# )
# Convert DataFrame to DynamicFrame for Glue
dynamic_frame1 = DynamicFrame.fromDF(production_percentage, glueContext, "production_percentage")

# Write Report 1 to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame1,
    connection_type="redshift",
    connection_options={
        "url": args['redshift_url'],
        "user": args['redshift_user'],
        "password": args['redshift_password'],
        "dbtable": f"{args['redshift_schema']}.{args['report1_table']}",
        "database": args['redshift_database'],
        "schema": args['redshift_schema'],
        "redshiftTmpDir": f"s3://{args['output_bucket']}temp/"
    }
)

# Filter units with defective percentages > 20%
production_above_20 = production_percentage.filter(
    col("Overall_Percentage_Defective") > 20
).select("Production_Unit_Id", "Overall_Percentage_Defective")

# Convert DataFrame to DynamicFrame for Glue
dynamic_frame2 = DynamicFrame.fromDF(production_above_20, glueContext, "production_above_20")

# Write Report 2 to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame2,
    connection_type="redshift",
    connection_options={
        "url": args['redshift_url'],
        "user": args['redshift_user'],
        "password": args['redshift_password'],
        "dbtable": f"{args['redshift_schema']}.{args['report2_table']}",
        "database": args['redshift_database'],
        "schema": args['redshift_schema'],
        "redshiftTmpDir": f"s3://{args['output_bucket']}temp/"
    }
)

# Calculate total items produced, discarded, and percentage discarded per unit
production_tsv = production_df.withColumn("items_produced", from_json(col("Items_Produced"), json_schema)) \
               .withColumn("defective_items", from_json(col("Defective_Items"), json_schema)) \
               .select(
                   col("Production_Unit_Id").alias("production_unit_id"),
                   col("items_produced.scissor").alias("items_produced_scissor"),
                   col("items_produced.paper").alias("items_produced_paper"),
                   col("items_produced.rock").alias("items_produced_rock"),
                   col("defective_items.scissor").alias("items_discarded_scissor"),
                   col("defective_items.paper").alias("items_discarded_paper"),
                   col("defective_items.rock").alias("items_discarded_rock")
               ).withColumn("total_items_produced",
                            col("items_produced_scissor") + col("items_produced_paper") + col("items_produced_rock")) \
               .withColumn("total_items_discarded",
                           col("items_discarded_scissor") + col("items_discarded_paper") + col("items_discarded_rock")) \
               .withColumn("percentage_items_discarded",
                           expr("total_items_discarded / total_items_produced * 100")) \
               .select("production_unit_id", "percentage_items_discarded")

# Convert DataFrame to DynamicFrame for Glue
dynamic_frame3 = DynamicFrame.fromDF(production_tsv, glueContext, "production_tsv")

# Write Report 3 to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame3,
    connection_type="redshift",
    connection_options={
        "url": args['redshift_url'],
        "user": args['redshift_user'],
        "password": args['redshift_password'],
        "dbtable": f"{args['redshift_schema']}.{args['report3_table']}",
        "database": args['redshift_database'],
        "schema": args['redshift_schema'],
        "redshiftTmpDir": f"s3://{args['output_bucket']}temp/"
    }
)
# Commit the job
job.commit()
