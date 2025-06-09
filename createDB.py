
import duckdb
import pandas as pd

# Define the column specifications (widths) and names
colspecs =     [(0, 6),(6, 15),(15, 23),(23, 29),(29, 35),(35, 36),(36, 42),(42, 49),(49, 50),(50, 59),(59, 68),(68, 77),(77, 86),(86, 95),(95, 104),(104, 113),
                (113, 122),(122, 131),(131, 140),(140, 149),(149, 158),(158, 167),(167, 176),(176, 185),(185, 194),(194, 203),(203, 212),(212, 221),(221, 230),
                (230, 239),(239, 248),(248, 250),(250, 253),(253, 263),(263, 273),(273, 282),(282, 291),(291, 300),(300, 305),(305, 314),(314, 323)
                ]
column_names = ['PCode7','PCode8','UnitPCode','IntroDate','TermDate','UserType','NatGridRefEasting','NatGridRefNorthing','NatGridRefQual','OutArea11','County',
                'CountyElecDiv','LocalAuthDistrict','Ward','HLTHAU','NHSER','Country','Region','PCON','EERegion','TECLEC','TTWA','PCT','ITL','NatPark','LSOA11',
                'MSOA11','WorkplaceZone','SubICB','BUA11','BUASD11','RU11IND','CensusOutputArea11','Latitude','Londitude','LEP1','LEP2','PoliceArea','IMD',
                'CALNVC','STP'
                ]
widths =[7,8,8,6,6,1,6,7,1,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,2,3,10,10,9,9,9,5,9,9]
fileName= r'NSPL21_NOV_2023_UK.txt'
#NSPL21_NOV_2023_UK.txt
# Read the fixed-width text file
df = pd.read_fwf(fileName, widths=widths, names=column_names)

# Connect to a database file (this will create the file if it doesn't exist)
conn = duckdb.connect('UK-Census-DB.duckdb')

# Create the table and insert data from the DataFrame
conn.execute("""
CREATE TABLE postcode_lookup AS SELECT * FROM df
""")

# Verify the data
result = conn.execute("SELECT * FROM postcode_lookup").fetchall()
print(result)

# Close the connection
conn.close()
