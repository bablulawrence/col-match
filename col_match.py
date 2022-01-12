from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('colMatch').getOrCreate()

def getDatabases():
    return spark.sql('SHOW DATABASES')

def getTables(databases):
    tableSchema = StructType([
            StructField('database', StringType(), False),
            StructField('tableName', StringType(), False),
            StructField('isTemporary', BooleanType(), False)
    ])
    itr = databases.rdd.toLocalIterator()    
    tables = spark.createDataFrame([], tableSchema)
    for x in itr:
        spark.sql(f"USE {x['databaseName']}")
        table = spark.sql('SHOW TABLES')
        tables = tables.union(table)
    return tables


def getSchema(tables): 
    itr = tables.rdd.toLocalIterator()
    for x in itr:
        spark.sql(f"USE {x['database']}")
        schema = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }")
        schema.show()

tables = getTables(getDatabases())
tables.show()

getSchema(tables)

