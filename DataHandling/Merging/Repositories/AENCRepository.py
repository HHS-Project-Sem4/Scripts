import pandas as pd
from Tools import utils
import pyodbc


class Repository:
    def __init__(self, connectionString):
        self.dbConnection = pyodbc.connect(connectionString)

    def getProductDataFrame(self):

        productJoinQuery = """
        SELECT id, description, name, category, color, quantity
        FROM product
        """

        productData = pd.read_sql(productJoinQuery, self.dbConnection)

        newColumnNames = ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_sub_category', 'PRODUCT_category', 'PRODUCT_color',
                          'PRODUCT_storage_quantity']
        productData.columns = newColumnNames

        return productData


    def getCustomerDataFrame(self):
        customerJoinQuery = """
        SELECT id, address, city, state_name, region, country, company_name
        FROM customer c
        JOIN state s ON c.state = s.state_id
        """

        customerData = pd.read_sql(customerJoinQuery, self.dbConnection)

        newColumnNames = ['CUSTOMER_id', 'CUSTOMER_address', 'CUSTOMER_city', 'CUSTOMER_state', 'CUSTOMER_region',
                          'CUSTOMER_country', 'CUSTOMER_company_name']
        customerData.columns = newColumnNames

        return customerData


    def getEmployeeDataFrame(self):
        employeeJoinQuery = """
        SELECT emp_id, emp_fname, emp_lname, city
        FROM employee
        """

        employeeData = pd.read_sql(employeeJoinQuery, self.dbConnection)

        newColumnNames = ['EMPLOYEE_id', 'EMPLOYEE_first_name', 'EMPLOYEE_last_name', 'EMPLOYEE_city']
        employeeData.columns = newColumnNames

        return employeeData


    def getDayDataFrame(self):
        orderDates = pd.read_sql("SELECT DISTINCT order_date FROM sales_order", self.dbConnection)

        dateFormat = '%d-%b-%Y %H:%M:%S %p'
        DAY_date = utils.getDayDate(orderDates, 'order_date', dateFormat)

        return DAY_date


    def getOrderDetailsDataFrame(self):
        orderDetailsQuery = """
        SELECT soi.id, so.cust_id, so.order_date, so.sales_rep, soi.prod_id, p.unit_price, soi.quantity
        FROM sales_order_item soi
        JOIN sales_order so ON soi.id = so.id
        JOIN product p ON soi.prod_id = p.id
        """

        orderDetailsData = pd.read_sql(orderDetailsQuery, self.dbConnection)

        renameColumns = ['ORDER_DETAIL_NUMBER', 'CUSTOMER_id', 'DAY_date', 'EMPLOYEE_id', 'PRODUCT_id', 'ORDER_DETAIL_unit_price', 'ORDER_DETAIL_order_quantity']
        orderDetailsData.columns = renameColumns

        return orderDetailsData
