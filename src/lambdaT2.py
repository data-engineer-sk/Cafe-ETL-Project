import pandas as pd
import numpy as np
import csv

def createCustTable():
    # 2) ------------------------------------------->  
    # 2) ----------> Produce customerTable.csv
    # 2) ------------------------------------------->  
    df3 = pd.read_csv('../stage_db/part2.csv')

    # recNo,TxDate,Store_Name,Cust_Name,TotPrice,Payment,CardNo,Prod1,Prod2,Prod3,Prod4,Prod5,Prod6
    df3 = df3.drop(['TxDate','Store_Name','TotPrice','Payment','Prod1','Prod2','Prod3','Prod4','Prod5','Prod6'],axis=1)
    
    df_temp = df3.drop_duplicates()

    # Change 'n' to 'Cash'
    df_temp.replace('n', 'No Card', inplace = True)
    df_temp.to_csv('../db/customerTable.csv', index=False)
    
    df3 = pd.read_csv('../db/customerTable.csv')
    # Create CustNo column, initialize the columns
    df3['CustNo'] = ''

    # Declare a list
    custList = []
    count = 0
    for tempItem in df3.recNo:
        custList.append('CUS'+convertNum(tempItem))
        count += 1
    # Assign the entire list as the OrderNo
    df3.CustNo = custList

    cols = ["CustNo","recNo","Cust_Name","CardNo"]
    df3 = df3.reindex(columns=cols)

    # Verify
    # print(df3)
    df_temp.to_csv('../db/customerTable.csv', index=False)

# Add the leading zero upto 5 digit...
def convertNum(inNum):
    result = ''
    if len(str(inNum)) >= 5:
        pass
    else:
        for x in range(5-len(str(inNum))):
            result += '0'

    result += str(inNum)
    if result >= '99999':
        result = 'ERROR'
    
    # verify
    # print(result)

    return result


createCustTable()