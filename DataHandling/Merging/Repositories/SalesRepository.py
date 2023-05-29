import pandas as pd
from Tools import utils
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


class Repository:
    def __init__(self, connectionString):
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})
        engine = create_engine(connection_url)

        self.salesDataFrame = pd.read_sql("SELECT * FROM Sales", engine)

    def getProductDataFrame(self):
        sales = utils.addGuidsUnique(self.salesDataFrame, 'Product', 'prod_id')

        columns = ['Product', 'Product_Category', 'Sub_Category', 'Unit_Cost']
        newFrames = utils.splitFrames(sales, 'prod_id', columns)
        self.salesDataFrame = newFrames[0]
        productData = newFrames[1]

        newColumnNames = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_category', 'PRODUCT_sub_category', 'PRODUCT_prod_cost']
        productData.columns = newColumnNames

        return productData

    def getCustomerDataFrame(self):
        self.salesDataFrame = utils.addGuids(self.salesDataFrame, 'CUSTOMER_id')
        newFrames = utils.splitFrames(self.salesDataFrame, 'CUSTOMER_id', ['Country', 'State'])

        self.salesDataFrame = newFrames[0]
        customerData = newFrames[1]

        newColumnNames = ['CUSTOMER_id', 'CUSTOMER_country', 'CUSTOMER_state']
        customerData.columns = newColumnNames

        return customerData

    def getEmployeeDataFrame(self):
        return pd.DataFrame()

    def getDayDataFrame(self):
        newFrames = utils.splitFrames(self.salesDataFrame, 'Date', [])

        self.salesDataFrame = newFrames[0]
        orderDates = newFrames[1]

        dateFormat = '%Y-%m-%d'
        DAY_date = utils.getDayDate(orderDates, 'Date', dateFormat)

        return DAY_date

    # Sales doesn't have a header ID or for Detail
    def getOrderDetailsDataFrame(self):
        selectedColumn = ['Order_Quantity', 'Unit_Price', 'Date', 'CUSTOMER_id', 'prod_id']
        orderDetailsData = self.salesDataFrame[selectedColumn]

        renameColumns = ['ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'DAY_date', 'CUSTOMER_id',
                         'PRODUCT_id']
        orderDetailsData.columns = renameColumns

        return orderDetailsData
