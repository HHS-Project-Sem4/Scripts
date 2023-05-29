from Repositories.AdventureWorksRepository import Repository as adventureWorksRepository
from Repositories.AENCRepository import Repository as AENCRepository
from Repositories.NorthwindRepository import Repository as northwindRepository
from Repositories.SalesRepository import Repository as salesRepository


class BrightSpaceData:

    def getAdventureWorksData(self):
        adventureWorksConnectionString = "Driver={SQL Server};Server=DESKTOP-8INVJ1O\SQLEXPRESS;Database=AdventureWorks;trusted_connection=yes"

        repository = adventureWorksRepository(adventureWorksConnectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        employee_df = repository.getEmployeeDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        adventureWorksData = {'product_df': product_df,
                              'customer_df': customer_df,
                              'employee_df': employee_df,
                              'day_df': day_df,
                              'order_details_df': order_details_df}

        return adventureWorksData

    def getNorthwindData(self):
        northwindConnectionString = "Driver={SQL Server};Server=DESKTOP-8INVJ1O\SQLEXPRESS;Database=Northwind;trusted_connection=yes"
        repository = northwindRepository(northwindConnectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        employee_df = repository.getEmployeeDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        northwindData = {'product_df': product_df,
                         'customer_df': customer_df,
                         'employee_df': employee_df,
                         'day_df': day_df,
                         'order_details_df': order_details_df}

        return northwindData

    def getAENCData(self):
        AENCConnectionString = "Driver={SQL Server};Server=DESKTOP-8INVJ1O\SQLEXPRESS;Database=AENC;trusted_connection=yes"
        repository = AENCRepository(AENCConnectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        employee_df = repository.getEmployeeDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        AENCData = {'product_df': product_df,
                    'customer_df': customer_df,
                    'employee_df': employee_df,
                    'day_df': day_df,
                    'order_details_df': order_details_df}

        return AENCData

    def getSalesData(self):
        salesConnectionString = "Driver={SQL Server};Server=DESKTOP-8INVJ1O\SQLEXPRESS;Database=Sales_db;trusted_connection=yes"
        repository = salesRepository(salesConnectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        salesData = {'product_df': product_df,
                     'customer_df': customer_df,
                     'day_df': day_df,
                     'order_details_df': order_details_df}

        return salesData
