import Tools.utils as utils
from Repositories.AENCRepository import AENCRepository as aencRepository
from Repositories.NorthwindRepository import NorthwindRepository as northwindRepository
from Repositories.AdventureWorksRepository import AdventureRepository as adventureWorksRepository
from Repositories.SalesRepository import SalesRepository as salesRepository
from Repositories.Repository import Repository as repository


class BrightSpaceData:

    def __init__(self, server, username, password, driver, trustedConnection):
        self.server = server
        self.username = username
        self.password = password
        self.driver = driver
        self.trustedConnection = trustedConnection

    def constructConnectionString(self, dbName):
        return f"DRIVER={self.driver};SERVER={self.server};DATABASE={dbName};UID={self.username};PWD={self.password};trusted_connection={self.trustedConnection};"

    def getStarData(self, repository, dbName):
        connectionString = self.constructConnectionString(dbName)

        repository = repository(connectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        employee_df = repository.getEmployeeDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        product_df.name = 'Product'
        customer_df.name = 'Customer'
        employee_df.name = 'Employee'
        day_df.name = 'Order_Date'
        order_details_df.name = 'Order_Details'

        data = [product_df, customer_df, employee_df, day_df, order_details_df]

        return data

    def saveData(self, repository, dbName, dataFrame, table):
        connectionString = self.constructConnectionString(dbName)
        repository = repository(connectionString)

        repository.saveData(dataFrame, table)

    def updateData(self, repository, outputDbName, dataframesToUpdate):
        for table in dataframesToUpdate:
            self.saveData(repository, outputDbName, dataframesToUpdate[table], table)

    def dropTableData(self, repository, dbName, tableName):
        connectionString = self.constructConnectionString(dbName)

        repository = repository(connectionString)
        repository.dropTable(tableName)

    def completeUpdateStar(self, outputDb):
        # creating the save frame
        completeStar = utils.createEmptyStarFrame()

        for table in completeStar:
            self.dropTableData(repository, outputDb, table.name)

        # getting the data
        dataSets = [
            ['Northwind', northwindRepository],
            ['AENC', aencRepository],
            ['AdventureWorks', adventureWorksRepository],
            ['Sales_db', salesRepository]
        ]

        # merging consts
        factFrameName = 'Order_Details'
        factFramePrimaryKey = 'ORDER_DETAIL_id'
        idRegex = '_id'

        for dataSet in dataSets:
            print(f'ADDING: {dataSet[0]}')
            starToAdd = self.getStarData(dataSet[1], dataSet[0])
            completeStar = utils.mergeStarDiagrams(completeStar, starToAdd, factFrameName, factFramePrimaryKey, idRegex)

        # Check for duplicates, duplicates in adventureworks CUSTOMER_id are immortal and wont even die when using DISTINCT or drop_duplicates
        utils.dupC(completeStar)

        print('FIXING DATA')
        # dropping duplicate dates added during merge
        dateFrame = next((df for df in completeStar if df.name == 'Order_Date'), None)
        dateFrame.drop_duplicates(subset=['DAY_date'])
        mainData = [dateFrame if df.name == 'Order_Date' else df for df in completeStar]

        print('UPLOADING DATA')
        for table in mainData:
            self.saveData(repository, outputDb, table, table.name)
