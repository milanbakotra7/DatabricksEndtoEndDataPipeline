import dlt
from pyspark.sql.functions import *

# Expectations
my_rules = {
            "rule1" : "product_id IS NOT NULL",
            "rule2" : "product_name IS NOT NULL"
        }

@dlt.table()
@dlt.expect_all_or_drop(my_rules)

def DimProducts_stage():
            df = spark.readStream.option("skipChangeCommits","true").table("datapipeline.silver.products")
            return df

@dlt.view
def DimProduct_view():
            df = spark.readStream.table("DimProducts_stage")
            return df

dlt.create_streaming_table("DimProduct")

dlt.apply_changes(
            target = "DimProduct",
            source = "DimProduct_view",
            keys = ["product_id"],
            sequence_by = "product_id",
            stored_as_scd_type = 2
        )
