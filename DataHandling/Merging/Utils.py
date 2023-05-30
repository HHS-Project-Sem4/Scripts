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
def addIds(sourceFrame, sourceColumn, idColumnName):
    originalFrame = sourceFrame.copy()

    uniqueValues = originalFrame[sourceColumn].unique()
    originalFrame[idColumnName] = pd.Series(dtype=int)

    newId = 0

    for value in uniqueValues:
        condition = originalFrame[sourceColumn] == value
        originalFrame.loc[condition, idColumnName] = newId

        newId += 1

    return originalFrame


def splitFrames(sourceFrame, splitColumn, idColumnName, childColumns):
    originalFrame = sourceFrame.copy()

    selectedSplitColumns = childColumns.copy()
    selectedSplitColumns.extend([idColumnName, splitColumn])

    splitFrame = originalFrame[selectedSplitColumns]
    splitFrame = splitFrame.drop_duplicates(subset=[splitColumn])

    removeColumns = childColumns.copy()
    removeColumns.extend([splitColumn])

    originalColumns = sourceFrame.columns.tolist()
    newOriginalColumns = [column for column in originalColumns if column not in removeColumns]
    originalFrame = originalFrame[newOriginalColumns]

    return [originalFrame, splitFrame]