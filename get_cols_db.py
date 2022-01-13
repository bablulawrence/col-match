import argparse
from pyspark.sql import SparkSession
from utils.database import getDatabases, getTables, getColumns, readParams

parser = argparse.ArgumentParser(description='Get columns',
 epilog="python get_cols_db.py --paramFilePath 'params.json' --outputFilePath 'dbfs:/mnt/data/get_columns/columns1.csv' --fuzzyScorer 'token-sort'" 
)
parser.add_argument('--paramFilePath', dest='paramFilePath', type=str, help='Path of the parameter file')
parser.add_argument('--outputFilePath', dest='outputFilePath', type=str, help='Path of the output file')
parser.add_argument('--fuzzyScorer', dest='fuzzyScorer', type=str, help='Fuzzy ratio type')
args = parser.parse_args()

params = readParams(args.paramFilePath)
spark = SparkSession.builder.appName('colMatch').getOrCreate()
databases =  getDatabases(spark, params['databases'])
tables = getTables(spark, databases, params['tables'])
columns = getColumns(spark, tables, params['columns'], args.fuzzyScorer)
columns.show(10, truncate=False)

try: 
    columns.coalesce(1).write \
        .option('header', True) \
        .mode('overwrite') \
        .csv(args.outputFilePath)
except Exception as e: 
    logging.error(f"Unable to write output file")
    raise
