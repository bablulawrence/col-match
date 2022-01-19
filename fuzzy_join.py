import argparse
from pyspark.sql import SparkSession
from utils.fuzzy import readParams, readCsvFiles, writeCsvFile, fuzzyJoin
from pyspark.sql.functions import * 

#Joins two datasets based on fuzzy logic
parser = argparse.ArgumentParser(description='Fuzzy join two files',
 epilog="python fuzzy_join.py --paramFilePath 'params-fuzzy-join.json'" 
)
parser.add_argument('--paramFilePath', dest='paramFilePath', type=str, help='Path of the parameter file')
args = parser.parse_args()

params = readParams(args.paramFilePath)
spark = SparkSession.builder.appName('fuzzy_join').getOrCreate()

if (params['leftFileRowReadLimit'] > 0):
    leftDF = readCsvFiles(spark, params['leftFilePath']).limit(params['leftFileRowReadLimit'])
else: 
    leftDF = readCsvFiles(spark, params['leftFilePath'])

if (params['rightFileRowReadLimit'] > 0):
    rightFile = readCsvFiles(spark, params['rightFilePath']).limit(params['rightFileRowReadLimit'])
else:
    rightFile = readCsvFiles(spark, params['rightFilePath'])

joinedDF = fuzzyJoin(spark, leftDF, params['leftFileMatchCol'], params['leftFileExcludeCols'],
             rightFile, params['rightFileMatchCol'], params['rightFileExcludeCols'], params['threshold'])
joinedDF.show(5, truncate=False)
writeCsvFile(joinedDF, params['joinedFilePath'])