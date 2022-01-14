import json
import logging
import sys
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from thefuzz import fuzz
from thefuzz import process

def readCsvFiles(spark, filePath):
    df = spark.read \
            .option('inferSchema', True) \
            .option('header', True) \
            .csv(filePath)
    return df

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
