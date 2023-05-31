import pandas as pd
from Tools import utils
from Repositories.Repository import Repository
class NorthwindRepository(Repository):

    def __init__(self, connectionString):
        super(NorthwindRepository, self).__init__(connectionString)

    # can add a category on top as food/drinks etc?
    def getProductDataFrame(self):
        productJoinQuery = """
        SELECT ProductID, ProductName, CategoryName
        FROM products p 
        JOIN Categories c ON p.CategoryID = c.CategoryID
        """

        productData = pd.read_sql(productJoinQuery, self.engine)

        newColumnNames = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_sub_category']
        productData.columns = newColumnNames

        return productData

    def getCustomerDataFrame(self):
        customerJoinQuery = """
        SELECT CustomerID, Address, City, Country, CompanyName
        FROM customers
        """

        customerData = pd.read_sql(customerJoinQuery, self.engine)

        newColumnNames = ['CUSTOMER_id', 'CUSTOMER_address', 'CUSTOMER_city', 'CUSTOMER_country',
                          'CUSTOMER_company_name']
        customerData.columns = newColumnNames

        return customerData

    def getEmployeeDataFrame(self):
        employeeJoinQuery = """
        SELECT EmployeeID, FirstName, City, Country
        FROM employees
        """

        employeeData = pd.read_sql(employeeJoinQuery, self.engine)

        newColumnNames = ['EMPLOYEE_id', 'EMPLOYEE_first_name', 'EMPLOYEE_city', 'EMPLOYEE_country']
        employeeData.columns = newColumnNames

        return employeeData

    def getDayDataFrame(self):
        orderDates = pd.read_sql("SELECT DISTINCT OrderDate FROM orders", self.engine)

        dateFormat = '%Y-%m-%d'
        DAY_date = utils.getDayDate(orderDates, 'OrderDate', dateFormat)

        return DAY_date

    def getOrderDetailsDataFrame(self):
        orderDetailsQuery = """
        SELECT null AS OrderID ,o.OrderID AS OrderHeaderID ,Quantity, UnitPrice, OrderDate, EmployeeID, CustomerID, ProductID
        FROM [order details] od
        JOIN orders o ON od.OrderID = o.OrderID
        """

        orderDetailsData = pd.read_sql(orderDetailsQuery, self.engine)
        orderDetailsData = utils.addIntID(orderDetailsData, 'OrderID')

        renameColumns = ['ORDER_DETAIL_id',
                         'ORDER_HEADER_id', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'DAY_date',
                         'EMPLOYEE_id', 'CUSTOMER_id', 'PRODUCT_id']
        orderDetailsData.columns = renameColumns

        return orderDetailsData


