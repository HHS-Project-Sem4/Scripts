import pandas as pd
from Tools import utils
from Repositories.Repository import Repository

class SalesRepository(Repository):
    def __init__(self, connectionString):
        super(SalesRepository, self).__init__(connectionString)

        self.salesDataFrame = pd.read_sql("SELECT null AS OrderID, * FROM Sales", self.engine)

    def getProductDataFrame(self):
        sales = utils.addIntIDUnique(self.salesDataFrame, 'Product', 'prod_id')

        columns = ['Product', 'Product_Category', 'Sub_Category', 'Unit_Cost']
        newFrames = utils.splitFrames(sales, 'prod_id', columns)
        self.salesDataFrame = newFrames[0]
        productData = newFrames[1]

        newColumnNames = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_category', 'PRODUCT_sub_category', 'PRODUCT_prod_cost']
        productData.columns = newColumnNames

        return productData

    def getCustomerDataFrame(self):
        # self.salesDataFrame = utils.addIntID(self.salesDataFrame, 'CUSTOMER_id')
        # newFrames = utils.splitFrames(self.salesDataFrame, 'CUSTOMER_id', ['Country', 'State'])
        #
        # self.salesDataFrame = newFrames[0]
        # customerData = newFrames[1]
        #
        # newColumnNames = ['CUSTOMER_id', 'CUSTOMER_country', 'CUSTOMER_state']
        # customerData.columns = newColumnNames
        #
        # return customerData

        return pd.DataFrame()

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
        # selectedColumn = ['Order_Quantity', 'Unit_Price', 'Date', 'CUSTOMER_id', 'prod_id']
        selectedColumn = ['OrderID', 'Order_Quantity', 'Unit_Price', 'Date', 'prod_id']

        orderDetailsData = self.salesDataFrame[selectedColumn]
        orderDetailsData = utils.addIntID(orderDetailsData, 'OrderID')

        # renameColumns = ['ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'DAY_date', 'CUSTOMER_id', 'PRODUCT_id']

        renameColumns = ['ORDER_DETAIL_id', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'DAY_date',
                         'PRODUCT_id']
        orderDetailsData.columns = renameColumns

        return orderDetailsData
