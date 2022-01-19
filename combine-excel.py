import pyspark.pandas as ps
from pyspark.sql.functions import *
from datetime import date 

aliceMapFilePath = "dbfs:/mnt/data/ape/mapping/Alice_Mapping_status.xlsx"
channelOdsMapFilePath = "dbfs:/mnt/data/ape/mapping/Channel ODS Mapping Status.xlsx"
siqpMapFilePath = "dbfs:/mnt/data/ape/mapping/SIQP Mapping Status.xlsx"
consolidatedMapFilePath = "dbfs:/mnt/data/ape/joined/edwr-map-consolidated.csv"

aliceMapDf = ps.read_excel(aliceMapFilePath, sheet_name="Mapping", usecols="A:H,J:O,S:V").to_spark()
channelOdsMapDf = ps.read_excel(channelOdsMapFilePath, sheet_name="Channel-ODS", usecols="A:M").to_spark()
siqpMapDf = ps.read_excel(siqpMapFilePath, sheet_name="SIQP mapping", usecols="A:O").to_spark() 

# aliceMapDf.printSchema()
# channelOdsMapDf.printSchema()
# siqpMapDf.printSchema()

aDf = aliceMapDf.select(lit("Alice").alias("Area"),
                        col("Mapping Status").alias("MappingStatus"),
                        col("Alice EDW Extract Name").alias("ExtractName"),
                        col("Domain").alias("Domain"), 
                        col("Sub-Domain").alias("SubDomain"),                        
                        col("EDW Column Name").alias("EDWColumnName"),
                        col("EDW Table Name ").alias("EDWTableName"),
                        col("EAP Column Name").alias("EAPColumnName"),
                        col("EAP Table Name").alias("EAPTableName"),
                        lit(None).alias("EAPConsumptionTableName"),
                        lit(None).alias("EAPConsumptionColumnName"),
                        col("EAP Schema Name").alias("EAPSchemaName"),
                        col("Comment").alias("Comments")
                       )

cDf = channelOdsMapDf.select(lit("ChannelODS").alias("Area"),
                        col("Mapping Status").alias("MappingStatus"),
                        col("Extract Name/Workflow Name").alias("ExtractName"),
                        col("Domain").alias("Domain"), 
                        col("Sub-Domain").alias("SubDomain"),                        
                        col("EDW Physical Column Name").alias("EDWColumnName"),
                        col("EDW table Name").alias("EDWTableName"),
                        col("EAP Column").alias("EAPColumnName"),
                        col("EAP Table ").alias("EAPTableName"),
                        col("EAP Consumption Table ").alias("EAPConsumptionTableName"),
                        col("EAP Consumption Column ").alias("EAPConsumptionColumnName"),
                        lit(None).alias("EAPSchemaName"),
                        col("Comments").alias("Comments")
                       )

sDf = siqpMapDf.select(lit("SIQP").alias("Area"),
                        col("Mapping Status").alias("MappingStatus"),
                        col("Extract Name/Workflow Name").alias("ExtractName"),
                        col("Domain").alias("Domain"), 
                        col("Subdomain (Data Subject)").alias("SubDomain"),                        
                        col("EDW Physical Column Name").alias("EDWColumnName"),
                        col("EDW table Name").alias("EDWTableName"),
                        col("EAP Column").alias("EAPColumnName"),
                        col("EAP Consumption Table").alias("EAPConsumptionTableName"),
                        col("EAP Consumption Column").alias("EAPConsumptionColumnName"),
                        col("EAP Table").alias("EAPTableName"),
                        lit(None).alias("EAPSchemaName"),
                        col("Comments").alias("Comments")
                       )

# aDf.display()
# cDf.display()
# sDf.display()
mapDf = aDf.union(cDf).union(sDf)
mapPdf = mapDf.to_pandas_on_spark()
# mapDf.printSchema()
# mapDf.filter(col('Area') == 'ChannelODS').display()
today = date.today()
outFilePath = consolidatedMapFilePath + today.strftime("%b-%d-%Y")
mapDf.coalesce(1).write \
     .mode("Overwrite") \
     .csv(outFilePath, header=True)

# mapPdf.to_excel(outFilePath + ".xlsx")
