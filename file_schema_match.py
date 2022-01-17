import argparse
from pyspark.sql import SparkSession
from utils.fuzzy import readParams, readCsvFiles, writeCsvFile, matchColumns

#Extract column names from target file and compares it with the source file using fuzzy logic
parser = argparse.ArgumentParser(description='Fuzzy match column names in two schema files',
 epilog="python file_schema_match.py --paramFilePath 'params-file-schema-match.json'" 
)
parser.add_argument('--paramFilePath', dest='paramFilePath', type=str, help='Path of the parameter file')
args = parser.parse_args()

params = readParams(args.paramFilePath)
spark = SparkSession.builder.appName('fileSchemaMatch').getOrCreate()

if (params['sourceSchemaRowLimit'] > 0):
    sourceSchemaDF = readCsvFiles(spark, params['sourceSchemaFilePath']).limit(params['sourceSchemaRowLimit'])
else: 
    sourceSchemaDF = readCsvFiles(spark, params['sourceSchemaFilePath'])

if (params['targetSchemaRowLimit'] > 0):
    targetSchemaDF = readCsvFiles(spark, params['targetSchemaFilePath']).limit(params['targetSchemaRowLimit'])
else:
    targetSchemaDF = readCsvFiles(spark, params['targetSchemaFilePath'])

matchedDF = matchColumns(spark, sourceSchemaDF, params['sourceSchemaMatchCol'],
                         targetSchemaDF, params['targetSchemaMatchCol'], params['fuzzyScorer'])
matchedDF.show(5, truncate=False)
writeCsvFile(matchedDF, params['matchedFilePath'])