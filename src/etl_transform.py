# Use Apache Spark to perform the ETL Transform
# pip install pyspark
# pip instal findspark
# pyspark --version

import pandas as pd
import numpy as np

def transform():

    # 1--------->  Basic Transform ----------
    #   I. Rename the columns
    #   II. Convert the date format
    df = pd.read_csv('../data/chesterfield_25-08-2021_09-00-00.csv')

    # - Step 1) Convert the column name
    df.rename(columns = {'Timestamp of Purchase':'TxDate','Store Name':'Store_Name', 'Customer Name':'Cust_Name'}, inplace = True)
    df.rename(columns = {'Basket Items (Name, Size & Price)':'Item','Total Price':'TotPrice', 'Cash/Card':'Payment'}, inplace = True)
    df.rename(columns = {'Card Number (Empty if Cash)':'CardNo'}, inplace = True)
    # print(df)

    # - Step 2) Convert the date format
    # df1 = df.TxDate.replace('/','-', regex=True)  # in dataframe
    # df2 = pd.concat([df1,df])                     # Concatinate
    # - Step 2) Convert the date format
    # Read thought the Year column and convert it to 'DD-MM-YYYY' format
    count = 0   
    for iterateTxDate in df.TxDate:
        # df.loc[count,'TxDate'] = (iterateTxDate.replace('/','-'))+":00"
        df.loc[count,'TxDate'] = iterateTxDate[6:10]+'-'+iterateTxDate[3:5]+'-'+iterateTxDate[0:2]+' '+iterateTxDate[11:]
        count += 1        

    # - Step 3) Convert integer to string, truncate the last two digit '.0'
    df['CardNo']=df['CardNo'].apply(str).str[:-2]
    # df.info()  Check the str if convert correctly

    # 2--------> More Transform for Item decomposition (Name, Size and Price)
    #   I. e.g. "Large Flavoured iced latte - Caramel - 3.25, Regular Flavoured iced latte - Hazelnut - 2.75, Regular Flavoured iced latte - Caramel - 2.75, Large Flavoured iced latte - Hazelnut - 3.25, Regular Flavoured latte - Hazelnut - 2.55, Regular Flavoured iced latte - Hazelnut - 2.75"
    #           Split string column into multiple columns pandas
    #  II. Drop the Item column

    new_columns = df.Item.str.split(',', expand=True)

    count = 1
    for xItem in new_columns:
        df['Prod'+str(count)]=new_columns[count-1].str.strip()
        print(df.loc[count])
        count += 1

    # Drop the 'Indicator' column
    df = df.drop(['Item'], axis=1)

    # - Step 4) Write to the csv file
    df.to_csv('../stage_db/part1.csv', index=False)

    print(df)

    # 3--------> Add the record number and rename the file to 'part2.csv'
    #   I. Add the record number (index number in this file)
    #  II. Rename the file to 'part2.csv'
    df1 = pd.read_csv('../stage_db/part1.csv')

    # Set the sequence number as the index
    cols = ["recNo","TxDate","Store_Name","Cust_Name","TotPrice","Payment","CardNo","Prod1","Prod2","Prod3","Prod4","Prod5","Prod6"]
    df1 = df1.reindex(columns=cols)
    df1.set_index('recNo')
    df1['recNo'] = df1.index + 1
    df1.to_csv('../stage_db/part2.csv', index=False)

    # 4--------> Spilt the item table into product table
    #            and break down the 6 product columns into details
    # --- Prepare for the schema for product table ---------------------------------------
    df2 = pd.read_csv('../stage_db/part2.csv')
    # Drop the 'TxDate, 'Store_Name', 'Cust_Name', 'TotPrice', 'Payment', 'CardNo' column
    df2 = df2.drop(['Store_Name', 'Cust_Name', 'TotPrice', 'Payment', 'CardNo'], axis=1)
    df2.to_csv('../stage_db/itemTable.csv', index=False)

    df2 = pd.read_csv('../stage_db/itemTable.csv')
    # Drop the unused columns

    df2.to_csv('../stage_db/tempProduct.csv', index=False)

    # Start process the 1st product
    df2 = pd.read_csv('../stage_db/tempProduct.csv')
    newProd = df2['Prod1'].str.split(' ', n=1, expand=True)

    df2['Size']= newProd[0].str.strip()
    df2['Token1']= newProd[1].str.strip()

    newProd1 = df2['Token1'].str.rsplit('-', n=1, expand=True)
    df2['ProdName']= newProd1[0].str.strip()
    df2['UnitPrice']= newProd1[1].str.strip()

    df2 = df2.drop(['Prod1','Token1','Prod2','Prod3','Prod4','Prod5','Prod6'], axis=1)
    df2.to_csv('../stage_db/process1.csv', index=False)

    # Start process the 2nd product
    df3 = pd.read_csv('../stage_db/tempProduct.csv')
    df3['Prod2'].str.strip()
    newProd = df3['Prod2'].str.split(' ', n=1, expand=True)

    df3['Size']= newProd[0].str.strip()
    df3['Token2']= newProd[1].str.strip()

    newProd1 = df3['Token2'].str.rsplit('-', n=1, expand=True)
    df3['ProdName']= newProd1[0].str.strip()
    df3['UnitPrice']= newProd1[1].str.strip()
   
    df3 = df3.drop(['Prod1','Prod2','Token2','Prod3','Prod4','Prod5','Prod6'], axis=1)
    df3.to_csv('../stage_db/process2.csv', index=False)

    # Start process the 3rd product
    df4 = pd.read_csv('../stage_db/tempProduct.csv')
    newProd = df4['Prod3'].str.split(' ', n=1, expand=True)

    df4['Size']= newProd[0].str.strip()
    df4['Token3']= newProd[1].str.strip()

    newProd1 = df4['Token3'].str.rsplit('-', n=1, expand=True)
    df4['ProdName']= newProd1[0].str.strip()
    df4['UnitPrice']= newProd1[1].str.strip()

    df4 = df4.drop(['Prod1','Prod2','Prod3','Token3','Prod4','Prod5','Prod6'], axis=1)
    df4.to_csv('../stage_db/process3.csv', index=False)

    # Start process the 4th product
    df5 = pd.read_csv('../stage_db/tempProduct.csv')
    newProd = df5['Prod4'].str.split(' ', n=1, expand=True)

    df5['Size']= newProd[0].str.strip()
    df5['Token4']= newProd[1].str.strip()

    newProd1 = df5['Token4'].str.rsplit('-', n=1, expand=True)
    df5['ProdName']= newProd1[0].str.strip()
    df5['UnitPrice']= newProd1[1].str.strip()

    df5 = df5.drop(['Prod1','Prod2','Prod3', 'Prod4','Token4','Prod5','Prod6'], axis=1)
    df5.to_csv('../stage_db/process4.csv', index=False)

    # Start process the 5th product
    df6 = pd.read_csv('../stage_db/tempProduct.csv')
    newProd = df6['Prod5'].str.split(' ', n=1, expand=True)

    df6['Size']= newProd[0].str.strip()
    df6['Token5']= newProd[1].str.strip()

    newProd1 = df6['Token5'].str.rsplit('-', n=1, expand=True)
    df6['ProdName']= newProd1[0].str.strip()
    df6['UnitPrice']= newProd1[1].str.strip()

    df6 = df6.drop(['Prod1','Prod2','Prod3','Prod4','Prod5','Token5', 'Prod6'], axis=1)
    df6.to_csv('../stage_db/process5.csv', index=False)

    # Start process the 6th product
    df2 = pd.read_csv('../stage_db/tempProduct.csv')
    newProd = df2['Prod6'].str.split(' ', n=1, expand=True)

    df2['Size']= newProd[0].str.strip()
    df2['Token6']= newProd[1].str.strip()

    newProd1 = df2['Token6'].str.rsplit('-', n=1, expand=True)
    df2['ProdName']= newProd1[0].str.strip()
    df2['UnitPrice']= newProd1[1].str.strip()

    df2 = df2.drop(['Prod1','Prod2','Prod3','Prod4','Prod5','Prod6','Token6'], axis=1)
    df2.to_csv('../stage_db/process6.csv', index=False)
    # print(df)

transform()