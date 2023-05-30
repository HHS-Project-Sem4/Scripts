import pandas as pd
from Tools import utils
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class Repository:
    def __init__(self, connectionString):
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})
        self.engine = create_engine(connection_url)


    def getProductDataFrame(self):
        productJoinQuery = """
        SELECT p.ProductID, p.Name, pc.Name AS CategoryName, psc.Name AS SubCategoryName, p.Color, p.StandardCost, SUM(pi.Quantity) AS Quantity
        FROM production.Product p
        FULL OUTER JOIN production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        FULL OUTER JOIN production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        JOIN Production.ProductInventory pi ON p.ProductID = pi.ProductID
        GROUP BY p.ProductID, p.Name, pc.Name, psc.Name, p.Color, p.StandardCost
        """

        productData = pd.read_sql(productJoinQuery, self.engine)

        renameColumns = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_category', 'PRODUCT_sub_category', 'PRODUCT_colour',
                         'PRODUCT_prod_cost', 'PRODUCT_storage_quantity']
        productData.columns = renameColumns

        return productData

    def getCustomerDataFrame(self):
        # Distinct because of the one to many from customer -> address, only other easy way is to just exclude anything related to address

        customerJoinQuery = """
        SELECT DISTINCT c.CustomerID, a.AddressLine1, a.City, sp.Name AS StateName, cr.Name AS CountryName, s.Name AS CompanyName
        FROM sales.Customer c
        JOIN person.Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN sales.Store s  ON c.StoreID = s.BusinessEntityID
        JOIN person.BusinessEntityAddress bea ON c.StoreID = bea.BusinessEntityID OR c.PersonID = bea.BusinessEntityID
        JOIN person.Address a ON bea.AddressID = a.AddressID
        JOIN person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        JOIN person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        """

        customerData = pd.read_sql(customerJoinQuery, self.engine)

        renameColumns = ['CUSTOMER_id', 'CUSTOMER_address', 'CUSTOMER_city', 'CUSTOMER_state', 'CUSTOMER_country',
                         'CUSTOMER_company_name']
        customerData.columns = renameColumns

        # dropping duplicates on this column because of the one to many relationship with businessentity and businessEntityAddress, only other way is to just exclude the address etc data
        customerData.drop_duplicates(subset=['CUSTOMER_id'])

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

        employeeData = pd.read_sql(employeeJoinQuery, self.engine)

        # Rename columns
        renameColumns = ['EMPLOYEE_id', 'EMPLOYEE_first_name', 'EMPLOYEE_last_name', 'EMPLOYEE_city', 'EMPLOYEE_state',
                         'EMPLOYEE_country']
        employeeData.columns = renameColumns

        return employeeData

    def getDayDataFrame(self):
        orderDates = pd.read_sql("SELECT DISTINCT OrderDate FROM sales.SalesOrderHeader", self.engine)

        dateFormat = '%Y-%m-%d'
        DAY_date = utils.getDayDate(orderDates, 'OrderDate', dateFormat)

        return DAY_date

    def getOrderDetailsDataFrame(self):
        orderDetailsQuery = """
        SELECT SalesOrderDetailID, sd.SalesOrderID,OrderQty, UnitPrice, OrderDate,SalesPersonID, CustomerID ,ProductID
        FROM sales.SalesOrderDetail sd
        JOIN sales.SalesOrderHeader sh ON sd.SalesOrderID = sh.SalesOrderID
        """

        orderDetailsData = pd.read_sql(orderDetailsQuery, self.engine)

        renameColumns = ['ORDER_DETAIL_id', 'ORDER_HEADER_id', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price',
                         'DAY_date',
                         'EMPLOYEE_id', 'CUSTOMER_id', 'PRODUCT_id']
        orderDetailsData.columns = renameColumns

        return orderDetailsData
