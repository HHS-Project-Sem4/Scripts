import pandas as pd
from Tools import utils
import pyodbc


class Repository:
    def __init__(self, connectionString):
        self.dbConnection = pyodbc.connect(connectionString)

    def getProductDataFrame(self):
        productJoinQuery = """
        SELECT p.ProductID, p.Name, pc.Name AS CategoryName, psc.Name AS SubCategoryName, p.Color, p.StandardCost, pi.Quantity
        FROM production.Product p
        JOIN production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        JOIN production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        JOIN Production.ProductInventory pi ON p.ProductID = pi.ProductID
        """

        productData = pd.read_sql(productJoinQuery, self.dbConnection)

        renameColumns = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_category', 'PRODUCT_sub_category', 'PRODUCT_colour',
                         'PRODUCT_prod_cost', 'PRODUCT_storage_quantity']
        productData.columns = renameColumns

        return productData

    def getCustomerDataFrame(self):
        customerJoinQuery = """
        SELECT c.CustomerID, a.AddressLine1, a.City, sp.Name AS StateName, cr.Name AS CountryName, s.Name AS CompanyName
        FROM sales.Customer c
        JOIN person.Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN sales.Store s  ON c.StoreID = s.BusinessEntityID
        JOIN person.BusinessEntityAddress bea ON c.StoreID = bea.BusinessEntityID OR c.PersonID = bea.BusinessEntityID
        JOIN person.Address a ON bea.AddressID = a.AddressID
        JOIN person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        JOIN person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        """

        customerData = pd.read_sql(customerJoinQuery, self.dbConnection)

        renameColumns = ['CUSTOMER_id', 'CUSTOMER_address', 'CUSTOMER_city', 'CUSTOMER_state', 'CUSTOMER_country',
                         'CUSTOMER_company_name']
        customerData.columns = renameColumns

        return customerData

    def getEmployeeDataFrame(self):
        employeeJoinQuery = """
        SELECT p.BusinessEntityID, p.FirstName, p.LastName, a.City, psp.Name AS StateName, cr.Name AS CountryName
        FROM sales.SalesPerson sp
        JOIN person.Person p ON sp.BusinessEntityID = p.BusinessEntityID
        JOIN person.BusinessEntity be ON p.BusinessEntityID = be.BusinessEntityID
        JOIN person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
        JOIN person.Address a ON bea.AddressID = a.AddressID
        JOIN person.StateProvince psp ON a.StateProvinceID = psp.StateProvinceID
        JOIN person.CountryRegion cr ON psp.CountryRegionCode = cr.CountryRegionCode
        """

        employeeData = pd.read_sql(employeeJoinQuery, self.dbConnection)

        # Rename columns
        employeeData.rename(columns={'BusinessEntityID': 'EMPLOYEE_id',
                                     'FirstName': 'EMPLOYEE_first_name',
                                     'LastName': 'EMPLOYEE_last_name',
                                     'City': 'EMPLOYEE_city',
                                     'StateName': 'EMPLOYEE_state',
                                     'CountryName': 'EMPLOYEE_country'},
                            inplace=True)

        return employeeData

    def getDayDataFrame(self):
        orderDates = pd.read_sql("SELECT DISTINCT OrderDate FROM sales.SalesOrderHeader", self.dbConnection)

        dateFormat = '%Y-%m-%d'
        DAY_date = utils.getDayDate(orderDates, 'OrderDate', dateFormat)

        return DAY_date

    def getOrderDetailsDataFrame(self):
        orderDetailsQuery = """
        SELECT SalesOrderDetailID, sd.SalesOrderID,OrderQty, UnitPrice, OrderDate,SalesPersonID, CustomerID ,ProductID
        FROM sales.SalesOrderDetail sd
        JOIN sales.SalesOrderHeader sh ON sd.SalesOrderID = sh.SalesOrderID
        """

        orderDetailsData = pd.read_sql(orderDetailsQuery, self.dbConnection)

        renameColumns = ['ORDER_DETAIL_id', 'ORDER_HEADER_id', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price',
                         'DAY_date',
                         'EMPLOYEE_id', 'CUSTOMER_id', 'PRODUCT_id']
        orderDetailsData.columns = renameColumns

        return orderDetailsData
