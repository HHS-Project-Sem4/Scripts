from DataService import BrightSpaceData as data
import pandas as pd

datasets = data()

aencStar = datasets.getAENCData()
northwindStar = datasets.getNorthwindData()

columns = [
    'ORDER_DETAIL_id',
    'ORDER_HEADER_id',
    'ORDER_DETAIL_order_quantity',
    'ORDER_DETAIL_unit_price',
    'DAY_date',
    'EMPLOYEE_id',
    'CUSTOMER_id',
    'PRODUCT_id'
]

testStarDiagram = pd.DataFrame(columns=columns)

def addToStarData(starData, frameToAdd, middTable):
    resultFrame = starData

    for table in frameToAdd:
        print(table)

    return resultFrame

addToStarData(testStarDiagram, aencStar, 'Order_Detail')