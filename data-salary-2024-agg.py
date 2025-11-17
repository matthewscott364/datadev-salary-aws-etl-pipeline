import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node s3-dataset-salary-2024
s3datasetsalary2024_node1763337196941 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://amzn-s3-dataset-salary/csv/dataset_salary_2024.csv"], "recurse": True}, transformation_ctx="s3datasetsalary2024_node1763337196941")

# Script generated for node Change Schema
ChangeSchema_node1763337372939 = ApplyMapping.apply(frame=s3datasetsalary2024_node1763337196941, mappings=[("work_year", "string", "work_year", "int"), ("experience_level", "string", "experience_level", "string"), ("employment_type", "string", "employment_type", "string"), ("job_title", "string", "job_title", "string"), ("salary", "string", "salary", "int"), ("salary_currency", "string", "salary_currency", "string"), ("salary_in_usd", "string", "salary_in_usd", "int"), ("employee_residence", "string", "employee_residence", "string"), ("remote_ratio", "string", "remote_ratio", "int"), ("company_location", "string", "company_location", "string"), ("company_size", "string", "company_size", "string")], transformation_ctx="ChangeSchema_node1763337372939")

# Script generated for node dataset-salary-maxbyjobtitle
datasetsalarymaxbyjobtitle_node1763337294380 = sparkAggregate(glueContext, parentFrame = ChangeSchema_node1763337372939, groups = ["job_title"], aggs = [["salary_in_usd", "max"]], transformation_ctx = "datasetsalarymaxbyjobtitle_node1763337294380")

# Script generated for node dataset-salary-maxbylevel
datasetsalarymaxbylevel_node1763337548077 = sparkAggregate(glueContext, parentFrame = ChangeSchema_node1763337372939, groups = ["experience_level"], aggs = [["salary_in_usd", "max"]], transformation_ctx = "datasetsalarymaxbylevel_node1763337548077")

# Script generated for node dataset-sal-product1
EvaluateDataQuality().process_rows(frame=datasetsalarymaxbyjobtitle_node1763337294380, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1763336751841", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (datasetsalarymaxbyjobtitle_node1763337294380.count() >= 1):
   datasetsalarymaxbyjobtitle_node1763337294380 = datasetsalarymaxbyjobtitle_node1763337294380.coalesce(1)
datasetsalproduct1_node1763337720903 = glueContext.getSink(path="s3://amzn-s3-dataset-salary/product_1/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="datasetsalproduct1_node1763337720903")
datasetsalproduct1_node1763337720903.setCatalogInfo(catalogDatabase="dataset_salary_2024",catalogTableName="dataset-salary-maxbyjobtitle")
datasetsalproduct1_node1763337720903.setFormat("csv")
datasetsalproduct1_node1763337720903.writeFrame(datasetsalarymaxbyjobtitle_node1763337294380)
# Script generated for node dataset-sal-product2
EvaluateDataQuality().process_rows(frame=datasetsalarymaxbylevel_node1763337548077, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1763336751841", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (datasetsalarymaxbylevel_node1763337548077.count() >= 1):
   datasetsalarymaxbylevel_node1763337548077 = datasetsalarymaxbylevel_node1763337548077.coalesce(1)
datasetsalproduct2_node1763337748372 = glueContext.getSink(path="s3://amzn-s3-dataset-salary/product_2/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="datasetsalproduct2_node1763337748372")
datasetsalproduct2_node1763337748372.setCatalogInfo(catalogDatabase="dataset_salary_2024",catalogTableName="dataset-salary-maxbylevel")
datasetsalproduct2_node1763337748372.setFormat("csv")
datasetsalproduct2_node1763337748372.writeFrame(datasetsalarymaxbylevel_node1763337548077)
job.commit()