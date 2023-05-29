from DataService import BrightSpaceData as data
import pandas as pd

datasets = data()

aencStar = datasets.getAENCData()
northwindStar = datasets.getNorthwindData()


def mergeFrame(frameOne, frameTwo):
    resultFrame = frameOne.copy()

    for table in frameTwo:
        print(f'TABLE: {table}')
        for column in frameTwo[table]:
            print(f'COLUMN: {column}')
            resultFrame = pd.concat([frameTwo[table][column],frameTwo[table][column]])

    return resultFrame


mergeFrame(aencStar, northwindStar)
