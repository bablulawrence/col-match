import json
import logging
from pyspark.sql.functions import * 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys


def getDatabases(spark, dbList):
    try: 
        logging.error(dbList)
        if not dbList:
            return spark.sql('SHOW DATABASES')
        else:     
            return spark.sql('SHOW DATABASES').filter(col('databaseName').isin(dbList))
    except Exception as e: 
        logging.error("Unable to get databases")
        raise e

def getTables(spark, databases, tableList):
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
            if not tableList:
                table = spark.sql('SHOW TABLES')
            else:
                table = spark.sql('SHOW TABLES').filter(col('tableName').isin(tableList))
            tables = tables.union(table)
        except Exception as e:
            logging.error(f"Unable to list tables for database {x['databaseName']}")            
    return tables


def getColumns(spark, tables, columList): 
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
            if not columList:
                cols = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }")
            else: 
                cols = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }").filter(col('col_name').isin(columList))
            cols = cols.withColumn('database', lit(x['database'])) \
                        .withColumn('tableName', lit(x['tableName'])) \
                        .withColumn('isTemporary', lit(x['isTemporary']))

            columns = columns.union(cols)
    except Exception as e:
        logging.error(f"Unable to get columns for table {x['tableName']}")        
    return columns

def readParams(filePath):
    with open(filePath) as param_file:
       return json.load(param_file)

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

params = readParams('params.json')
logging.info(json.dumps(params))

spark = SparkSession.builder.appName('colMatch').getOrCreate()
# dbs = getDatabases(spark, params['databases'])
# dbs.show()
databases =  getDatabases(spark, params['databases'])
tables = getTables(spark, databases, params['tables'])
columns = getColumns(spark, tables, params['columns'])
columns.show(100)

