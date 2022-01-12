from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('colMatch').getOrCreate()

def getDatabases():
    try: 
        return spark.sql('SHOW DATABASES')
    except Exception as e: 
        logger("Unable to get databases")
        raise e

def getTables(databases):
    tableSchema = StructType([
            StructField('database', StringType(), False),
            StructField('tableName', StringType(), False),
            StructField('isTemporary', BooleanType(), False)
    ])
    tables = spark.createDataFrame([], tableSchema)
    itr = databases.rdd.toLocalIterator()    
    for x in itr:
        try: 
            spark.sql(f"USE {x['databaseName']}")
            table = spark.sql('SHOW TABLES')
            tables = tables.union(table)
        except Exception as e:
            logger(f"Unable to list tables for database {x['databaseName']}")            
    return tables


def getColumns(tables): 
    columnSchema = StructType([
            StructField('col_name', StringType(), False),
            StructField('data_type', StringType(), False),
            StructField('comment', StringType(), False),
            StructField('database', StringType(), False),
            StructField('tableName', StringType(), False),
            StructField('isTemporary', BooleanType(), False)
    ])
    columns = spark.createDataFrame([], columnSchema)
    itr = tables.rdd.toLocalIterator()
    try: 
        for x in itr:
            spark.sql(f"USE {x['database']}")
            cols = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }")
            cols = cols.withColumn('database', lit(x['database'])) \
                        .withColumn('tableName', lit(x['tableName'])) \
                        .withColumn('isTemporary', lit(x['isTemporary']))
            columns = columns.union(cols)
    except Exception as e:
        logger(f"Unable to get columns for table {x['tableName']}")        
    return columns

columns = getColumns(getTables(getDatabases()))
columns.show(100)

