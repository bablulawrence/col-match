import argparse
from pyspark.sql import SparkSession
from utils.database import readParams, writeCsvFile
from utils.fileSchema import readCsvFiles, matchColumns 

parser = argparse.ArgumentParser(description='Fuzzy match column names in two schema files',
 epilog="python file_schema_match.py --paramFilePath 'params-file-schema-match.json'" 
)
parser.add_argument('--paramFilePath', dest='paramFilePath', type=str, help='Path of the parameter file')
args = parser.parse_args()

params = readParams(args.paramFilePath)
spark = SparkSession.builder.appName('fileSchemaMatch').getOrCreate()

sourceSchemaDF = readCsvFiles(spark, params['sourceSchemaFilePath']).limit(params['sourceSchemaRowLimit'])
targetSchemaDF = readCsvFiles(spark, params['targetSchemaFilePath']).limit(params['targetSchemaRowLimit'])
matchedDF = matchColumns(spark, sourceSchemaDF, params['sourceSchemaMatchCol'],
                         targetSchemaDF, params['targetSchemaMatchCol'], params['fuzzyScorer'])
matchedDF.show(5, truncate=False)
writeCsvFile(matchedDF, params['matchedFilePath'])