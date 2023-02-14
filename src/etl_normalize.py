# Table Normalization
# Reference : https://datagy.io/pandas-conditional-column/

import pandas as pd
import numpy as np

def normalize():
    # 1) ------------------------------------------->      
    # 1) ----------> Produce storeTable.csv
    # 1) ------------------------------------------->        
    df1 = pd.read_csv('../stage_db/part2.csv')

    # TxDate,Store_Name,Cust_Name,TotPrice,Payment,CardNo,Prod1,Prod2,Prod3,Prod4,Prod5,Prod6
    df1 = df1.drop(['TxDate','Cust_Name','TotPrice','Payment','CardNo','Prod1','Prod2','Prod3','Prod4','Prod5','Prod6'],axis=1)
    df1.to_csv('../db/storeTable.csv', index=False)

    # Check Diplicate row
    # Remember, df5 will hold the result, and df4 just remain all the duplicate rows!!!!
    df2 = df1.drop_duplicates(subset='Store_Name')
    df2.to_csv('../db/storeTable.csv', index=False)

    df2 = pd.read_csv('../db/storeTable.csv')
    # Create StoreNo column, initialize the columns
    df2['StoreNo'] = '0'

    # Declare a list
    storeList = []
    count = 0
    for tempItem in df2.recNo:
        storeList.append('STR'+convertNum(tempItem))
        count += 1
    # Assign the entire list as the OrderNo
    df2.StoreNo = storeList

    cols = ["StoreNo","recNo","Store_Name"]
    df2 = df2.reindex(columns=cols)

    # Verify
    # print(df5)
    df2.to_csv('../db/storeTable.csv', index=False)

    # 2) ------------------------------------------->  
    # 2) ----------> Produce customerTable.csv
    # 2) ------------------------------------------->  
    df3 = pd.read_csv('../stage_db/part2.csv')

    # recNo,TxDate,Store_Name,Cust_Name,TotPrice,Payment,CardNo,Prod1,Prod2,Prod3,Prod4,Prod5,Prod6
    df3 = df3.drop(['TxDate','Store_Name','TotPrice','Payment','Prod1','Prod2','Prod3','Prod4','Prod5','Prod6'],axis=1)
    
    # Change 'n' to 'Cash'
    df3.replace('n', 'No Card', inplace = True)
    df3.to_csv('../db/customerTable.csv', index=False)
    
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
    df3.to_csv('../db/customerTable.csv', index=False)

    # 3) ------------------------------------------->  
    # 3) ----------> Produce semiOrderDetailTable.csv
    # 3) ------------------------------------------->  
    df4 = pd.DataFrame()

    # merging all process csv files
    df4 = pd.concat(map(pd.read_csv, ['../stage_db/process1.csv', '../stage_db/process2.csv', '../stage_db/process3.csv', '../stage_db/process4.csv', '../stage_db/process5.csv', '../stage_db/process6.csv']), ignore_index=True)
  
    # Sort the entire record
    df5 = df4.sort_values('recNo')

    df5.to_csv('../stage_db/semiOrderDetailTable.csv', index=False)

    df5 = pd.read_csv('../stage_db/semiOrderDetailTable.csv')

    # Convert all empty cell into "No_Data"
    df5.fillna("No_Data", inplace=True)
    # Drop all column with "No_Data"
    df5 = df5[df5["ProdName"]!="No_Data"]

    # Declare the OrderNo column
    df5['OrderNo'] = ""
    df5.to_csv('../stage_db/semiOrderDetailTable.csv', index=False) 

    # Declare a list
    orderDetailList = []
    count = 1
    for tempItem in df5.recNo:
        orderDetailList.append('ORD'+convertNum(tempItem))
        if (tempItem == count):
            pass
        else:
            count += 1
    # Assign the entire list as the OrderNo
    df5.OrderNo = orderDetailList

    # Set the sequence number as the index
    cols = ["recNo","OrderNo","Size","ProdName","UnitPrice"]
    df5 = df5.reindex(columns=cols)
    df5.set_index('recNo')
    df5['recNo'] = df5.index + 1

    # Verify
    # print(df2)
    df5.to_csv('../stage_db/semiOrderDetailTable.csv', index=False)

    # 4) ------------------------------------------------->  
    # 4) ----------> Produce productTable.csv
    # 4) ------------------------------------------------->    
    df6 = pd.read_csv('../stage_db/semiOrderDetailTable.csv')

    # Set the sequence number as the index
    cols = ["Size","ProdName","UnitPrice"]
    df6 = df6.reindex(columns=cols)

    # Drop all column with "No_Data"
    df_temp = df6.drop_duplicates()  
    df_temp.to_csv('../db/productTable.csv', index=False) 

    df6 = pd.read_csv('../db/productTable.csv')
    cols = ["recNo","Size","ProdName","UnitPrice","ProdNo"]
    df6 = df6.reindex(columns=cols)
    df6.set_index('recNo')
    df6['recNo'] = df6.index + 1
    # Declare a list
    productList = []
    count = 1
    for tempItem in df6.recNo:
        productList.append('PRD'+convertNum(tempItem))
        if (tempItem == count):
            pass
        else:
            count += 1
    # Assign the entire list as the OrderNo
    df6.ProdNo = productList

    # Set the sequence number as the index
    cols = ["ProdNo","Size","ProdName","UnitPrice"]
    df6 = df6.reindex(columns=cols)

    # Verify
    # print(df2)
    df6.to_csv('../db/productTable.csv', index=False)    

    # 5) ------------------------------------------->
    # 5) ----------> Produce orderTable.csv
    # 5) ------------------------------------------->    
    df7 = pd.read_csv('../stage_db/part2.csv')

    df7['StoreNo'] =''
    df7['CustNo'] =''
    df7['OrderNo'] = '0'
    # Set the sequence number as the index
    cols = ["recNo","TxDate","Store_Name","Cust_Name","TotPrice","Payment","CardNo","Prod1","Prod2","Prod3","Prod4","Prod5","Prod6", "OrderNo","StoreNo","CustNo"]
    df7 = df7.reindex(columns=cols)
    df7.to_csv('../db/orderTable.csv', index=False)

    # Set the sequence number as the index
    cols = ["OrderNo", "recNo","TxDate","StoreNo","Store_Name","CustNo","Cust_Name","Payment","TotPrice"]
    df7 = df7.reindex(columns=cols)
    
    # Declare a list
    orderList = []
    count = 0
    for tempItem in df7.recNo:
        orderList.append('ORD'+convertNum(tempItem))
        count += 1
    # Assign the entire list as the OrderNo
    df7.OrderNo = orderList
    df7.to_csv('../db/orderTable.csv', index=False)


    # 6) ------------------------------------------->  
    # 6) ----------> Produce OrderDetailTable.csv
    # 6) ------------------------------------------->  
    df8 = pd.read_csv('../stage_db/semiOrderDetailTable.csv')

    # Declare the OrderNo column
    df8['ProdNo'] = ""
    df8.to_csv('../db/orderDetailTable.csv', index=False) 

    # Set the sequence number as the index
    cols = ["recNo","OrderNo","ProdNo","Size","ProdName","UnitPrice"]
    df8 = df8.reindex(columns=cols)
    df8.set_index('recNo')
    df8['recNo'] = df8.index + 1

    # Verify
    # print(df2)
    df8.to_csv('../db/orderDetailTable.csv', index=False)

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

normalize()