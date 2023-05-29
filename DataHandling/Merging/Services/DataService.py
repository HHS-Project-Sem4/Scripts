class BrightSpaceData:

    def __init__(self, server, username, password, driver):
        self.server = server
        self.username = username
        self.password = password
        self.driver = driver

    def getData(self, repository, dbName):
        connectionString = f"DRIVER={self.driver};SERVER={self.server};DATABASE={dbName};UID={self.username};PWD={self.password}"

        repository = repository(connectionString)

        product_df = repository.getProductDataFrame()
        customer_df = repository.getCustomerDataFrame()
        employee_df = repository.getEmployeeDataFrame()
        day_df = repository.getDayDataFrame()
        order_details_df = repository.getOrderDetailsDataFrame()

        data = {'product_df': product_df,
                'customer_df': customer_df,
                'employee_df': employee_df,
                'day_df': day_df,
                'order_details_df': order_details_df}

        return data
