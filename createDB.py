
import duckdb

# Connect to a database file (this will create the file if it doesn't exist)
conn = duckdb.connect('UK_Census-DB.duckdb')
