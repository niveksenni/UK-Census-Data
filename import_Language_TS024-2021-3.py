import pandas as pd
import duckdb

fileName= r'data/TS067-2021-3.xlsx'
tableName = 'language'
sheetName = 'Dataset'

# Step 1: Load the "Census - Education" sheet from the Excel file
##########################################################################################
df = pd.read_excel(fileName, sheet_name=sheetName, dtype_backend='pyarrow')

# Connect to a database file (this will create the file if it doesn't exist)
print("Connecting to DuckDB database...")
conn = duckdb.connect('data/UKCensusDB.duckdb')

# Drop the table if it exists
print("Dropping existing table if it exists...")
drop_stmt = f"DROP TABLE IF EXISTS {tableName};"
conn.execute(drop_stmt)

# Create the table with columns matching the DataFrame
# (DuckDB can infer types from the DataFrame)
conn.execute(f"CREATE TABLE {tableName} AS SELECT * FROM df")

# Optional: Verify
result = conn.execute(f"SELECT * FROM {tableName} LIMIT 10").fetchdf()
print(result)

# Close the connection
conn.close()