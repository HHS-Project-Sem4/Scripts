import uuid
import pandas as pd


def getDayDate(sourceFrame, sourceColumn, dateFormat):
    dateColumns = ['DAY_date', 'DAY_MONTH_nr', 'DAY_QUARTER_nr', 'DAY_YEAR_nr']
    DAY_date = pd.DataFrame(columns=dateColumns)

    sourceFrame[sourceColumn] = pd.to_datetime(sourceFrame[sourceColumn], format=dateFormat)

    for index, row in sourceFrame.iterrows():
        return_date = row[sourceColumn]

        date_values = [return_date.month, (return_date.month - 1) // 3 + 1, return_date.year]

        date_frame = pd.DataFrame([[return_date, date_values[0], date_values[1], date_values[2]]],
                                  columns=['DAY_date', 'DAY_MONTH_nr', 'DAY_QUARTER_nr', 'DAY_YEAR_nr'])
        DAY_date = pd.concat([DAY_date, date_frame], ignore_index=True)

    return DAY_date


# splits frame based on unique vals in columns
def addIntIDUnique(sourceFrame, sourceColumn, idColumnName):
    originalFrame = sourceFrame.copy()

    uniqueValues = originalFrame[sourceColumn].unique()
    originalFrame[idColumnName] = pd.Series(dtype=int)

    newId = 0

    for value in uniqueValues:
        condition = originalFrame[sourceColumn] == value
        originalFrame.loc[condition, idColumnName] = newId

        newId += 1

    return originalFrame


# adds UUID to every row in a new column
def addUuids(sourceFrame, idColumnName):
    originalFrame = sourceFrame.copy()

    uuid_list = [str(uuid.uuid4()) for _ in range(len(originalFrame))]
    originalFrame[idColumnName] = uuid_list

    return originalFrame


def addIntID(sourceFrame, idColumnName):
    originalFrame = sourceFrame.copy()

    id_list = range(len(originalFrame))
    originalFrame[idColumnName] = id_list

    return originalFrame


def splitFrames(sourceFrame, idColumnName, childColumns):
    originalFrame = sourceFrame.copy()

    selectedSplitColumns = [idColumnName]
    selectedSplitColumns.extend(childColumns)

    splitFrame = originalFrame[selectedSplitColumns]
    splitFrame = splitFrame.drop_duplicates(subset=[idColumnName])

    removeColumns = childColumns.copy()

    originalColumns = sourceFrame.columns.tolist()
    newOriginalColumns = [column for column in originalColumns if column not in removeColumns]
    originalFrame = originalFrame[newOriginalColumns]

    return [originalFrame, splitFrame]


# makes sure that the added frames in the added stardiagram that have a table with a column that fits the idRegex
# get an int ID value that's not in the same ID column as the factFrame of the base stardiagram
def mergeDiagrams(baseStarDiagram, addedStarDiagram, factFrameName, idRegex):
    resultFrame = baseStarDiagram.copy()

    for table in addedStarDiagram:
        if table != factFrameName:
            for column in addedStarDiagram[table].columns:
                if idRegex in column:
                    createUniqueIDS(baseStarDiagram, addedStarDiagram, table, column, factFrameName)

        resultFrame[table] = pd.concat([resultFrame[table], addedStarDiagram[table]])

    return resultFrame


def createUniqueIDS(frame1, frame2, table, column, factFrameName):
    newIDValue = 0

    if not (pd.isna(frame1[factFrameName][column].max())):
        newIDValue = frame1[factFrameName][column].max()
        newIDValue += 1000

    for index, value in frame2[table][column].items():
        condition1 = frame2[factFrameName][column] == value
        frame2[factFrameName].loc[condition1, column] = newIDValue

        condition1 = frame2[table][column] == value
        frame2[table].loc[condition1, column] = newIDValue

        newIDValue += 1

def dupC(df):
    for table in df:
        print(f'CURRENT TABLE: {table}')
        for column in df[table]:
            if '_id' in column:
                duplicate_count = df[table][column].duplicated().sum()
                print(f'COLUMN: {column} HAS {duplicate_count} OF DUPLICATED VALUES')


def createEmptyStarFrame():
    productColumns = ['PRODUCT_id',
                      'PRODUCT_name',
                      'PRODUCT_category',
                      'PRODUCT_sub_category',
                      'PRODUCT_colour',
                      'PRODUCT_prod_cost',
                      'PRODUCT_storage_quantity']
    product_df = pd.DataFrame(columns=productColumns)

    product_df = product_df.astype({'PRODUCT_id': 'Int32'})

    customerColumns = ['CUSTOMER_id',
                       'CUSTOMER_address',
                       'CUSTOMER_city',
                       'CUSTOMER_state',
                       'CUSTOMER_region',
                       'CUSTOMER_country',
                       'CUSTOMER_company_name']
    customer_df = pd.DataFrame(columns=customerColumns)

    customer_df = customer_df.astype({'CUSTOMER_id': 'Int32'})

    employeeColumns = ['EMPLOYEE_id',
                       'EMPLOYEE_first_name',
                       'EMPLOYEE_last_name',
                       'EMPLOYEE_city',
                       'EMPLOYEE_state',
                       'EMPLOYEE_region',
                       'EMPLOYEE_country']
    employee_df = pd.DataFrame(columns=employeeColumns)

    employee_df = employee_df.astype({'EMPLOYEE_id': 'Int32'})

    dayColumns = ['DAY_date',
                  'DAY_MONTH_nr',
                  'DAY_QUARTER_nr',
                  'DAY_YEAR_nr']
    day_df = pd.DataFrame(columns=dayColumns)

    day_df = day_df.astype({'DAY_date': 'datetime64[ns]',
                            'DAY_MONTH_nr': 'Int8',
                            'DAY_QUARTER_nr': 'Int8',
                            'DAY_YEAR_nr': 'Int16'})

    orderDetailsColumns = ['ORDER_DETAIL_id',
                           'ORDER_HEADER_id',
                           'ORDER_DETAIL_order_quantity',
                           'ORDER_DETAIL_unit_price',
                           'DAY_date',
                           'EMPLOYEE_id',
                           'CUSTOMER_id',
                           'PRODUCT_id']
    order_details_df = pd.DataFrame(columns=orderDetailsColumns)

    order_details_df = order_details_df.astype({'ORDER_DETAIL_id': 'Int32',
                                                'ORDER_HEADER_id': 'Int32',
                                                'DAY_date': 'datetime64[ns]',
                                                'EMPLOYEE_id': 'Int32',
                                                'CUSTOMER_id': 'Int32',
                                                'PRODUCT_id': 'Int32'
                                                })

    data = {'product_df': product_df,
            'customer_df': customer_df,
            'employee_df': employee_df,
            'day_df': day_df,
            'order_details_df': order_details_df}

    return data
