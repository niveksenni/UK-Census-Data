
import pandas as pd
import duckdb

# Define the column specifications (widths) and names
colspecs =     [(0, 6),(6, 15),(15, 23),(23, 29),(29, 35),(35, 36),(36, 42),(42, 49),(49, 50),(50, 59),(59, 68),(68, 77),(77, 86),(86, 95),(95, 104),(104, 113),
                (113, 122),(122, 131),(131, 140),(140, 149),(149, 158),(158, 167),(167, 176),(176, 185),(185, 194),(194, 203),(203, 205),(205, 208),(208, 218),
                (218, 228),(228, 237),(237, 246),(246, 255),(255, 260),(260, 269)
                ]
column_names = ['PCode7','PCode8','UnitPCode','IntroDate','TermDate','UserType','NatGridRefEasting','NatGridRefNorthing','NatGridRefQual','OutArea11','County',
                'CountyElecDiv','LocalAuthDistrict','Ward','NHSER','Country','Region','PCON','TTWA','ITL','NatPark','LSOA21','MSOA21','WorkplaceZone','SubICB',
                'BUA22','RU11IND','CensusOutputArea11','Latitude','Londitude','LEP1','LEP2','PoliceArea','IMD','ICB'
                ]
dtype_dict = {'PCode7': 'string','PCode8': 'string','UnitPCode': 'string','IntroDate': 'Int64','TermDate': 'Int64','UserType': 'Int8',
    'NatGridRefEasting': 'Int64','NatGridRefNorthing': 'Int64','NatGridRefQual': 'Int8','OutArea11': 'string','County': 'string',
    'CountyElecDiv': 'string','LocalAuthDistrict': 'string','Ward': 'string','NHSER': 'string','Country': 'string',
    'Region': 'string','PCON': 'string','TTWA': 'string','ITL': 'string','NatPark': 'string','LSOA21': 'string',
    'MSOA21': 'string','WorkplaceZone': 'string','SubICB': 'string','BUA22': 'string','RU11IND': 'string',
    'CensusOutputArea11': 'string','Latitude': 'float64','Londitude': 'float64','LEP1': 'string','LEP2': 'string',
    'PoliceArea': 'string','IMD': 'Int64','ICB': 'string'
    }
widths =[7,8,8,6,6,1,6,7,1,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,2,3,10,10,9,9,9,5,9]

fileName= r'data/Data/NSPL21_NOV_2023_UK.txt'

# Read the fixed-width text file
print("Reading the fixed-width text file...")
df = pd.read_fwf(
    fileName,
    widths=widths,
    names=column_names,
    dtype=dtype_dict,
    dtype_backend='pyarrow'
)

# Connect to a database file (this will create the file if it doesn't exist)
print("Connecting to DuckDB database...")
conn = duckdb.connect('data/UKCensusDB.duckdb')

# Create the table and insert data from the DataFrame
# Drop the table if it exists
print("Dropping existing table if it exists...")
drop_stmt = "DROP TABLE IF EXISTS postcode_lookup;"
conn.execute(drop_stmt)

print("Creating table and inserting data...")
create_stmt = "CREATE TABLE postcode_lookup (PCode7 VARCHAR, PCode8 VARCHAR, UnitPCode VARCHAR, IntroDate INTEGER, TermDate INTEGER, UserType TINYINT, NatGridRefEasting INTEGER, NatGridRefNorthing INTEGER, NatGridRefQual TINYINT, OutArea11 VARCHAR, County VARCHAR, CountyElecDiv VARCHAR, LocalAuthDistrict VARCHAR, Ward VARCHAR, NHSER VARCHAR, Country VARCHAR, Region VARCHAR, PCON VARCHAR, TTWA VARCHAR, ITL VARCHAR, NatPark VARCHAR, LSOA21 VARCHAR, MSOA21 VARCHAR, WorkplaceZone VARCHAR, SubICB VARCHAR, BUA22 VARCHAR, RU11IND VARCHAR, CensusOutputArea11 VARCHAR, Latitude DOUBLE, Londitude DOUBLE, LEP1 VARCHAR, LEP2 VARCHAR, PoliceArea VARCHAR, IMD INTEGER, ICB VARCHAR);"
conn.execute(create_stmt)
# Insert the DataFrame into the table

# Register the DataFrame as a DuckDB view
conn.register('df_view', df)

# Insert data from the DataFrame into the table
conn.execute("INSERT INTO postcode_lookup SELECT * FROM df_view")

#sql = "CREATE TABLE postcode_lookup AS SELECT * FROM df"
#conn.execute(sql) 

# Verify the data

result = conn.execute("SELECT * FROM postcode_lookup limit 1000").fetchall()
print(result)

# Close the connection
conn.close()
