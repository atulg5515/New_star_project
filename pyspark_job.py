from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import length,udf,col,sha2,lit
from pyspark.sql.functions import *
import pyspark
from pyspark import SQLContext
import json,os
import sys
from pyspark.sql.utils import AnalysisException
import json
import random
import hashlib
import json
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import to_date,col,udf

from pyspark.sql import functions as f
from datetime import date
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import xxhash64


spark = SparkSession.builder.appName("Delta-Lake") \
    .config("spark.jars.packages", \
            "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", \
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",\
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


spark.sparkContext.addPyFile("s3://atul-landingbucket-batch08/delta-core_2.12-0.8.0.jar")

from delta import *
from delta.tables import DeltaTable

app_config=spark.read.option("multiline","true").json("s3://atul-landingbucket-batch08/updated_app config file.json").toJSON().collect()
app_config=json.loads(app_config[0])
raw_path=app_config["ingest-dataset"]["destination"]["data-location"]
staging_path=app_config["transformation-dataset"]["destination"]["data-location"]
partition=app_config["transformation-dataset"]["partition-cols"]
#Datasets=app_config["ingest-dataset"]["datasets"]
file_format=app_config["ingest-dataset"]["source"]["file-format"]
lookup_path=app_config["lookup-dataset"]["data-location"]
print(lookup_path)
pii_cols=app_config["lookup-dataset"]["pi-cols"]
print(file_format)

dataset_from_dag =sys.argv[1]
#dataset_from_dag = 'actives.parquet'
print('dataset_from_dag:'+dataset_from_dag)


class sparkjob:
    def __init__(self):
        pass   
    def read_data(self,source_path,val,file_format):
        df=spark.read.load(source_path+val,format=file_format)
        return df
    def write_data(self,df,path,val,file_format,partition):
        df.write.mode("overwrite").partitionBy(partition).format(file_format).save(path+val)
    def masking_columns(self,df):
        mask_fields=[]
        for cols in app_config["transformation-dataset"]["masking-cols"]:
                if(cols in df.columns):
                    mask_fields.append(cols)
        for cols in mask_fields:
                df=df.withColumn("masked_"+cols,sha2(col(cols).cast("binary"),256))
        return df
    def casting_columns(self,df):
            df=df.withColumn("Advertising_id", df["Advertising_id"].cast(StringType()))
            df=df.withColumn("month", df["month"].cast(StringType()))
            df=df.withColumn("date",to_date("date"))
            return df
    def change_type(self,df):
            df=df.withColumn("location_source",concat_ws(",",col("location_source")))
            df=df.withColumn("user_longitude",df["user_longitude"].cast(DecimalType(precision=10, scale=7)))
            df=df.withColumn("user_lattitude",df["user_lattitude"].cast(DecimalType(precision=10, scale=7)))
            return df
    def look_up(self,df,pii_cols,lookup_path):
        source_df = df.withColumn("start_date",f.current_date())
        source_df = source_df.withColumn("end_date",f.lit("null"))
        pii_cols = [i for i in pii_cols if i in df.columns]
        required_cols= []
        target_values= {}
        for col in pii_cols:
            if col in df.columns:
                required_cols+= [col,"masked_"+col]
        required_cols_df=required_cols+ ['start_date','end_date']
        source_df = source_df.select(*required_cols_df)
        target_values={}
        for i in required_cols:
            target_values[i] = "modified."+i
            target_values['start_date'] = f.current_date()
            target_values['active_flag'] = "true" 
            target_values['end_date'] = "null"
        s3_path=lookup_path
        print(source_df.show())
        try:
            targetTable = DeltaTable.forPath(spark,s3_path)
            print("target_table is present")
        except AnalysisException:
                print('Table is not present')
                source_df = source_df.withColumn("active_flag",f.lit("true"))
                source_df.write.format("delta").mode("overwrite").save(s3_path)
                print('Table Created')
                targetTable = DeltaTable.forPath(spark,s3_path)
                return

        targetTable = spark.read.format("delta").load(s3_path)
        print(targetTable.show())
        joined_df= source_df.join(targetTable,(source_df.Advertising_id == targetTable.Advertising_id) & \
                          (targetTable.active_flag == "true"),"leftouter") \
                            .select(source_df["*"], \
                                    targetTable.Advertising_id.alias("target_Advertising_id"), \
                                    targetTable.user_id.alias("target_user_id"), \
                                    targetTable.masked_Advertising_id.alias("target_Advertising_id_mask"), \
                                    targetTable.masked_user_id.alias("target_user_id_mask"))
        filter_df= joined_df.filter(xxhash64(joined_df.Advertising_id,joined_df.user_id,joined_df.masked_Advertising_id,
                            joined_df.masked_user_id)!=xxhash64(joined_df.target_Advertising_id,joined_df.target_user_id,
                             joined_df.target_Advertising_id_mask,joined_df.target_user_id_mask))
        merge_df= filter_df.withColumn("MERGE_KEY",concat(filter_df.Advertising_id))
        dummy_df= filter_df.filter("target_Advertising_id is not null").withColumn("MERGE_KEY",lit(None))
        scdc_df = merge_df.union(dummy_df)
        Delta_table = DeltaTable.forPath(spark,s3_path)
        Delta_table.alias("target_delta").merge(scdc_df.alias("modified"),condition = "concat(target_delta.advertising_id) = modified.MERGE_KEY and target_delta.active_flag='true'").whenMatchedUpdate(
                set = {                  # Set current to false and endDate to source's effective date."active_flag" : "False",
                "active_flag" : "False",
                "end_date" : f.current_date()
              }
            ).whenNotMatchedInsert(
              values = target_values
            ).execute()
        print(targetTable.show())
        
job=sparkjob()



df=job.read_data(raw_path,dataset_from_dag,file_format)
df=job.masking_columns(df)
print(df.show(5))
job.look_up(df,pii_cols,lookup_path)
df=job.casting_columns(df)
df=job.change_type(df)
job.write_data(df,staging_path,dataset_from_dag,file_format,partition)
