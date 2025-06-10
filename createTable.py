
import pandas as pd
import duckdb

# Connect to a database file (this will create the file if it doesn't exist)
conn = duckdb.connect('data/UKCensusDB.duckdb')

table = 'postcode_lookup'
create_stmt = "CREATE TABLE postcode_lookup1 (PCode7 VARCHAR, PCode8 VARCHAR, UnitPCode VARCHAR, IntroDate INTEGER, TermDate INTEGER, UserType SMALLINT, NatGridRefEasting INTEGER, NatGridRefNorthing INTEGER, NatGridRefQual SMALLINT, OutArea11 VARCHAR, County VARCHAR, CountyElecDiv VARCHAR, LocalAuthDistrict VARCHAR, Ward VARCHAR, NHSER VARCHAR, Country VARCHAR, Region VARCHAR, PCON VARCHAR, TTWA VARCHAR, ITL VARCHAR, NatPark VARCHAR, LSOA21 VARCHAR, MSOA21 VARCHAR, WorkplaceZone VARCHAR, SubICB VARCHAR, BUA22 VARCHAR, RU11IND VARCHAR, CensusOutputArea11 VARCHAR, Latitude DOUBLE, Londitude DOUBLE, LEP1 VARCHAR, LEP2 VARCHAR, PoliceArea VARCHAR, IMD INTEGER, ICB VARCHAR);"

columns = conn.execute(create_stmt).fetchall()

# Close the connection
conn.close()