from Services.DataService import BrightSpaceData as data
from Repositories.AENCRepository import Repository as aencRepository
from Repositories.NorthwindRepository import Repository as northwindRepository
from Repositories.AdventureWorksRepository import Repository as adventureWorksRepository
from Repositories.SalesRepository import Repository as salesRepository
import Tools.utils as utils

# server consts
server = ''
username = ''
password = ''
driver = '{ODBC Driver 17 for SQL Server}'
trustedConnection = 'yes'
dataService = data(server, username, password, driver, trustedConnection)

# db names
northwind = 'Northwind'
AENC = 'AENC'
adventureWorks = 'AdventureWorks'
sales = 'Sales_db'

# getting the data
initialStar = utils.createEmptyStarFrame()

northwindStar = dataService.getData(northwindRepository, northwind)
aencStar = dataService.getData(aencRepository, AENC)
adventureWorksStar = dataService.getData(adventureWorksRepository, adventureWorks)
salesStar = dataService.getData(salesRepository, sales)

# merging consts
factFrameName = 'order_details_df'
idRegex = '_id'

# merging the data
print('START MERGING________')

print('NORTHWIND DF')
df = utils.mergeDiagrams(initialStar, northwindStar, factFrameName, idRegex)

print('AENC DF')
df = utils.mergeDiagrams(df, aencStar, factFrameName, idRegex)

print('STATS BEFORE ADVENTURE WORKS')
utils.dupC(df)

print('ADVENTUREWORKS DF')
df = utils.mergeDiagrams(df, adventureWorksStar, factFrameName, idRegex)

utils.dupC(df)

print('SALES DF')
df = utils.mergeDiagrams(df, salesStar, factFrameName, idRegex)

# check for duplicates, duplicates in adventureworks CUSTOMER_id are immortal and wont even die when using DISTINCT or drop_duplicates
utils.dupC(df)

# dropping duplicate dates added during merge
df['day_df'].drop_duplicates(subset=['DAY_date'])
