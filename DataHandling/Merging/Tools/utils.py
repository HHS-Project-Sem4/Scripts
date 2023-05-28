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
def addGuidsUnique(sourceFrame, sourceColumn, idColumnName):
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
def addGuids(sourceFrame, idColumnName):
    originalFrame = sourceFrame.copy()

    uuid_list = [str(uuid.uuid4()) for _ in range(len(originalFrame))]
    originalFrame[idColumnName] = uuid_list

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
