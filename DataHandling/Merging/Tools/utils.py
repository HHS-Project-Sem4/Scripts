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


def mergeStarDiagrams(baseStarDiagram, addedStarDiagram, factFrameName, factFramePrimaryKey, idRegex):
    resultFrame = baseStarDiagram.copy()

    baseFactFrame = next((df for df in resultFrame if df.name == factFrameName), None)
    addedFactFrame = next((df for df in addedStarDiagram if df.name == factFrameName), None)

    for baseTable, addTable in zip(baseStarDiagram, addedStarDiagram):

        tableName = addTable.name

        if addTable.name != factFrameName:
            for column in addTable.columns:
                if idRegex in column:
                    createUniqueIDS(baseFactFrame, addedFactFrame, addTable, column)
        else:
            for column in addTable.columns:
                if column == factFramePrimaryKey:

                    createUniqueIDS2(baseFactFrame, addTable, column)

        baseTable = pd.concat([baseTable, addTable])
        baseTable.name = tableName

        resultFrame = [baseTable if df.name == tableName else df for df in resultFrame]

    return resultFrame

# Creates a set of new ids for an ID column that is not linked to something, uses range to compare it to the frame it gets added to
def createUniqueIDS2(baseFactFrame, frameToFix, column):
    newIDValue = 0

    if not (pd.isna(baseFactFrame[column].max())):
        newIDValue = baseFactFrame[column].max()
        newIDValue += 5000

    id_list = range(newIDValue, (newIDValue + len(frameToFix)))
    frameToFix[column] = id_list

# Creates new IDS and links to the factframe
def createUniqueIDS(baseFactFrame, addedFactFrame, frameToFix, column):
    newIDValue = 0

    if not (pd.isna(baseFactFrame[column].max())):
        newIDValue = baseFactFrame[column].max()
        newIDValue += 5000

    for index, value in frameToFix[column].items():
        condition1 = addedFactFrame[column] == value
        addedFactFrame.loc[condition1, column] = newIDValue

        condition1 = frameToFix[column] == value
        frameToFix.loc[condition1, column] = newIDValue

        newIDValue += 1


def dupC(df):
    for table in df:
        for column in table:
            if '_id' in column:
                duplicate_count = table[column].duplicated().sum()
                print(f'COLUMN: {column} HAS {duplicate_count} OF DUPLICATED VALUES')


def createEmptyStarFrame():
    column_data = [
        {'name': 'Product', 'columns': ['PRODUCT_id', 'PRODUCT_name', 'PRODUCT_category', 'PRODUCT_sub_category', 'PRODUCT_colour', 'PRODUCT_prod_cost', 'PRODUCT_storage_quantity'], 'dtype': {'PRODUCT_id': 'Int32'}},
        {'name': 'Customer', 'columns': ['CUSTOMER_id', 'CUSTOMER_address', 'CUSTOMER_city', 'CUSTOMER_state', 'CUSTOMER_region', 'CUSTOMER_country', 'CUSTOMER_company_name'], 'dtype': {'CUSTOMER_id': 'Int32'}},
        {'name': 'Employee', 'columns': ['EMPLOYEE_id', 'EMPLOYEE_first_name', 'EMPLOYEE_last_name', 'EMPLOYEE_city', 'EMPLOYEE_state', 'EMPLOYEE_region', 'EMPLOYEE_country'], 'dtype': {'EMPLOYEE_id': 'Int32'}},
        {'name': 'Order_Date', 'columns': ['DAY_date', 'DAY_MONTH_nr', 'DAY_QUARTER_nr', 'DAY_YEAR_nr'], 'dtype': {'DAY_date': 'datetime64[ns]', 'DAY_MONTH_nr': 'Int8', 'DAY_QUARTER_nr': 'Int8', 'DAY_YEAR_nr': 'Int16'}},
        {'name': 'Order_Details', 'columns': ['ORDER_DETAIL_id', 'ORDER_HEADER_id', 'ORDER_DETAIL_order_quantity', 'ORDER_DETAIL_unit_price', 'DAY_date', 'EMPLOYEE_id', 'CUSTOMER_id', 'PRODUCT_id'], 'dtype': {'ORDER_DETAIL_id': 'Int32', 'ORDER_HEADER_id': 'Int32', 'DAY_date': 'datetime64[ns]', 'EMPLOYEE_id': 'Int32', 'CUSTOMER_id': 'Int32', 'PRODUCT_id': 'Int32'}}
    ]

    data = []
    for item in column_data:
        df = pd.DataFrame(columns=item['columns']).astype(item['dtype'])
        df.name = item['name']
        data.append(df)

    return data

