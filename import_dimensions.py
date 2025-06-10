import pandas as pd
import duckdb

sheetName = 'Dataset'
# Connect to a database file (this will create the file if it doesn't exist)
print("Connecting to DuckDB database...")
conn = duckdb.connect('data/UKCensusDB.duckdb')
tables = {'ethnicity': 'data/TS021-2021-3.xlsx',
          'education': 'data/TS067-2021-3.xlsx',
          'language': 'data/TS024-2021-3.xlsx',
          'occupation': 'data/TS063-2021-5.xlsx',
          'industry': 'data/TS060-2021-5.xlsx',
          'age': 'data/TS007-2021-3.xlsx',
          'ecoactive': 'data/TS066-2021-6.xlsx',
          'housing': 'data/RM133-2021-3.xlsx',
          'tenure': 'data/RM135-2021-3.xlsx',
          'transport': 'data/TS045-2021-4.xlsx',
          'dwelling': 'data/RM205-2021-2.xlsx',
          'worktravel': 'data/TS061-2021-6.xlsx',
          'genhealth': 'data/TS037-2021-3.xlsx',
          'religion': 'data/TS030-2021-3.xlsx'
          }

def import_table(tableName, fileName):
    # Load the "Census - {tableName}" sheet from the Excel file
    df = pd.read_excel(fileName, sheet_name=sheetName, dtype_backend='pyarrow')
    print(f"DF:{tableName} loaded with {len(df)} rows and {len(df.columns)} columns.")

    # Drop the table if it exists
    drop_stmt = f"DROP TABLE IF EXISTS {tableName};"
    conn.execute(drop_stmt)

    # Create the table with columns matching the DataFrame
    conn.execute(f"CREATE TABLE {tableName} AS SELECT * FROM df")
    conn.commit()

# Loop through all tables and import
for tableName, fileName in tables.items():
    import_table(tableName, fileName)

# Close the connection
conn.close()
print("All tables imported successfully.")