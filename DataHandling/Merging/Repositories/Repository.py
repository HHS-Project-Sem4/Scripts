import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


class Repository:
    def __init__(self, connectionString):
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})
        self.engine = create_engine(connection_url)

    def getColumnData(self, table, column):
        query = f""""
        SELECT {column}
        FROM {table}        
        """

        data = pd.read_sql(query, self.engine)

        return data

    def saveData(self, dataFrame, table):
        dataFrame.to_sql(table, con=self.engine, if_exists='append', index=False)

    def dropTable(self, table_name):
        delete_query = f"DELETE FROM {table_name}"
        self.engine.execute(delete_query)
