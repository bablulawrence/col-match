import json
import logging
import sys
import re
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from thefuzz import fuzz
from thefuzz import process

def readParams(filePath):
    with open(filePath) as param_file:
       return json.load(param_file)

def readCsvFiles(spark, filePath):
    df = spark.read \
            .option('inferSchema', True) \
            .option('header', True) \
            .csv(filePath)
    return df

def writeCsvFile(df, filePath):
    try: 
        df.coalesce(1).write \
            .option('header', True) \
            .mode('overwrite') \
            .csv(filePath)
    except Exception as e: 
        logging.error(f"Unable to write output file")
        raise

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


def getColumns(spark, tables, columnList, fuzzyScorer=fuzz.ratio): 
    
    def getScore(columns):
        
        if (fuzzyScorer == 'partial'):
            scorer = fuzz.partial_ratio
        elif(fuzzyScorer == 'token-sort'):
            scorer = fuzz.token_sort_ratio
        elif(fuzzyScorer == 'token-set'):
            scorer = fuzz.token_set_ratio
        else:
            scorer = fuzz.ratio

        maxScore = process.extractOne(columns, columnList, scorer=scorer)[1]
        columns = process.extract(columns, columnList, scorer=scorer)
        return { 'matchCols': json.dumps(columns), 'maxScore': maxScore}

    schema = StructType([
        StructField('matchCols', StringType(), False), 
        StructField('maxScore', IntegerType(), False)
    ])
    getScoreUdf = udf(getScore, schema)
    columnSchema = StructType([
            StructField('col_name', StringType(), False),
            StructField('data_type', StringType(), False),
            StructField('comment', StringType(), False),
            StructField('matchResult', schema, False),
            StructField('database', StringType(), False),
            StructField('tableName', StringType(), False),
            StructField('isTemporary', BooleanType(), False)
    ])
    columns = spark.createDataFrame([], columnSchema)
    itr = tables.rdd.toLocalIterator()
    try: 
        for x in itr:
            spark.sql(f"USE {x['database']}")
            if not columnList:
                cols = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }") \
                    .withColumn('maxResult', lit(None)) 
            else: 
                cols = spark.sql(f"DESCRIBE EXTENDED { x['tableName'] }") \
                    .withColumn('matchResult', getScoreUdf(col('col_name'))) 
            cols = cols.withColumn('database', lit(x['database'])) \
                        .withColumn('tableName', lit(x['tableName'])) \
                        .withColumn('isTemporary', lit(x['isTemporary']))

            columns = columns.union(cols)
        columns = columns.select(col('col_name').alias('colName'), 
                                 col('data_type').alias('dataType'),
                                 'comment','database','tableName', 'isTemporary',
                                 col('matchResult.maxScore').alias('maxScore'),
                                 col('matchResult.matchCols').alias('matchCols'))
    except Exception as e:
        logging.error(f"Unable to get columns for table {x['tableName']}")
    return columns.sort(col('maxScore').desc())

def matchColumns(spark, sourceDF, sourceMatchColName, targetDF, targetMatchColName, fuzzyScorer=fuzz.ratio): 
    
    targetCols = targetDF.rdd.map(lambda x: x[targetMatchColName]).collect()
    logging.info(targetCols)
    def getScore(sourceCols):
        
        if (fuzzyScorer == 'partial'):
            scorer = fuzz.partial_ratio
        elif(fuzzyScorer == 'token-sort'):
            scorer = fuzz.token_sort_ratio
        elif(fuzzyScorer == 'token-set'):
            scorer = fuzz.token_set_ratio
        else:
            scorer = fuzz.ratio

        match = process.extractOne(sourceCols, targetCols, scorer=scorer)
        return { 'matchedCols': match[0], 'maxScore': match[1]}

    schema = StructType([
        StructField('matchedCols', StringType(), False), 
        StructField('maxScore', IntegerType(), False)
    ])
    getScoreUdf = udf(getScore, schema)    

    try:         
        df =  sourceDF.withColumn('matchResult', getScoreUdf(col(sourceMatchColName))) \
                .withColumn('maxScore', col('matchResult.maxScore')) \
                .withColumn('matchedCols', col('matchResult.matchedCols')) \
                .drop('matchResult')
    except Exception as e:
        logging.error(f"Unable to match columns")
        raise 
    return df.sort(col('maxScore').desc())

def replaceSpecialChars(s): 
    return re.sub('[^a-zA-Z0-9]', '_', s)

def getSelectExpression(leftCols, rightCols):
    expr = "SELECT "    
    for col in leftCols:
        colr = replaceSpecialChars(col)
        expr += f" LEFT.`{col}` AS L_{colr}, "
    for col in rightCols:
        colr = replaceSpecialChars(col)
        expr += f" RIGHT.`{col}` AS R_{colr}, "           
    expr = expr[:-2] #Remove the extra ',' at the end of expression
    expr += " FROM LEFT CROSS JOIN RIGHT;"    
    return expr
# def filterCols()

def fuzzyJoin(spark, leftDF, leftDFMatchCol, leftFileExcludeCols, 
            rightDF, rightDFMatchCol, rightFileExcludeCols, threshold):
    leftDF.createOrReplaceTempView('LEFT')
    rightDF.createOrReplaceTempView('RIGHT')  
    lcols = list(set(leftDF.columns) - set(leftFileExcludeCols))
    rcols = list(set(rightDF.columns) - set(rightFileExcludeCols))
    return spark.sql(getSelectExpression(lcols, rcols)) \
            .withColumn('LevenshteinDistance', levenshtein(upper(col(f"L_{replaceSpecialChars(leftDFMatchCol)}")),
                                                            upper(col(f"R_{replaceSpecialChars(rightDFMatchCol)}")))) \
            .filter(f"LevenshteinDistance <= {threshold}") \
            .sort(col('LevenshteinDistance').asc())
