from Services.DataService import BrightSpaceData as data
from Repositories.AENCRepository import Repository as aencRepository
from Repositories.NorthwindRepository import Repository as northwindRepository
from Repositories.AdventureWorksRepository import Repository as adventureWorksRepository
from Repositories.SalesRepository import Repository as salesRepository
import pandas as pd
import Tools.utils as utils

server = 'outdoorfusionserver.database.windows.net'

driver = '{ODBC Driver 17 for SQL Server}'
dataService = data(server, username, password, driver)

northwind = 'Northwind'
AENC = 'AENC'
adventureWorks = 'AdventureWorks'
sales = 'Sales_db'

northwindStar = dataService.getData(northwindRepository, northwind)
aencStar = dataService.getData(aencRepository, AENC)
adventureWorksStar = dataService.getData(adventureWorksRepository, adventureWorks)
salesStar = dataService.getData(salesRepository, sales)


def mergeFrame(starFrame, frameTwo):
    resultFrame = starFrame.copy()

    for table in frameTwo:
        resultFrame[table] = pd.concat([resultFrame[table], frameTwo[table]])

    return resultFrame


utils.mergeFrame(aencStar, northwindStar)
