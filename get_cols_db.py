import argparse
from pyspark.sql import SparkSession
from utils.database import readParams, getDatabases, getTables, getColumns, writeCsvFile

parser = argparse.ArgumentParser(description='Get matched columns from specified tables in given databases',
 epilog="python get_cols_db.py --paramFilePath 'params-get-cols-db.json' --outputFilePath 'dbfs:/mnt/data/get_columns/columns1.csv' --fuzzyScorer 'token-sort'" 
)
parser.add_argument('--paramFilePath', dest='paramFilePath', type=str, help='Path of the parameter file')
parser.add_argument('--outputFilePath', dest='outputFilePath', type=str, help='Path of the output file')
parser.add_argument('--fuzzyScorer', dest='fuzzyScorer', type=str, help='Fuzzy ratio type')
args = parser.parse_args()

params = readParams(args.paramFilePath)
spark = SparkSession.builder.appName('getColsDb').getOrCreate()
databases =  getDatabases(spark, params['databases'])
tables = getTables(spark, databases, params['tables'])
matchedColDF = getColumns(spark, tables, params['columns'], args.fuzzyScorer)
matchedColDF.show(10, truncate=False)
writeCsvFile(matchedColDF, args.outputFilePath)
