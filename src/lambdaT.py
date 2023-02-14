import pandas as pd
import numpy as np
import csv

def lambdaT():
    # recNo,OrderNo,Size,ProdName,UnitPrice
    # df_orderDT = pd.read_csv('../stage_db/semiOrderDetailTable.csv')
    # df_prodT = pd.read_csv('../db/productTable.csv')
 
    count = 0
    with open('../db/productTable.csv', 'r') as read_product:
        csv_productReader= csv.reader(read_product)
        for prod_in_prodTable in csv_productReader:
            # [2] is ProdName
            # print(f'xxxxx{prod_in_prodTable[3]}')
            with open('../stage_db/semiOrderDetailTable.csv', 'r') as read_orderDetail:
                csv_orderDetailReader = csv.reader(read_orderDetail)
                for prod_in_orderDetailTable in csv_orderDetailReader:
                    # [3] is ProdName
                    # print(f'{prod_in_orderDetailTable[3]}')
                    if prod_in_prodTable[2] == prod_in_orderDetailTable[3]:
                        print(f'Found-->{prod_in_orderDetailTable[0]}-->', prod_in_orderDetailTable[3])
                        count += 1

    print('Total Record Count is : ', count)





lambdaT()