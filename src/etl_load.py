# Project      : Showcase Project
# Program Name : load_data.py
# Description  : Load the transformed data to data warehouse (mySQL)
#              : Load a consolidated Generation's FYP csv files to database
#              :    1) customerTable.csv
#              :    2) storeTable.csv
#              :    3) orderTable.csv
#              :    4) orderDetailTable.csv
# Outputs      : Data in the remote database (e.g. MySQL/PostgreSQL under AWS)
# Developers   : Samuel Ko
###############################################################################################

import pandas as pd
import mysql_connector as db
import os
from dotenv import load_dotenv

def insert_to_db():
    load_dotenv()
 
    # Establish the database connection
    connection = db.mysql_connect()

    ############### Upload consolidated StoreTable.csv #############
    dataPath_db = os.environ.get("PATH_db")
    fileName = dataPath_db + 'storeTable.csv'
    # inserting customer data to MySQL database (or to RDS in AWS)
    storeData = pd.read_csv (fileName)   
    for row in storeData.itertuples():
        sql = """
        INSERT INTO StoreTable (
            StoreNo, RecNo, Store_Name)
            VALUES (%s,%s,%s)
        """ 
        val = (row.StoreNo, row.recNo, row.Store_Name)
        cursor = connection.cursor()
        cursor.execute(sql, val)

    ############### Upload consolidated customerTable.csv #############
    dataPath_db = os.environ.get("PATH_db")
    fileName = dataPath_db + 'customerTable.csv'
    # inserting customer data to MySQL database (or to RDS in AWS)
    custData = pd.read_csv (fileName)   
    for row in custData.itertuples():
        sql = """
        INSERT INTO CustomerTable (
            CustNo, RecNo, Cust_Name)
            VALUES (%s,%s,%s)
        """ 
        val = (row.CustNo, row.recNo, row.Cust_Name)
        cursor = connection.cursor()
        cursor.execute(sql, val)

    ####################### Upload productTable.csv #####################
    # inserting Product data to MySQL database (or to RDS in AWS)
    dataPath_db = os.environ.get("PATH_db")
    fileName = dataPath_db + 'productTable.csv'
    productData = pd.read_csv (fileName) 
    for row in productData.itertuples():
        sql = """
        INSERT INTO ProductTable (
            ProdNo, Size, ProdName, UnitPrice)
            VALUES (%s,%s,%s,%s)
        """ 
        val = (row.ProdNo, row.Size, row.ProdName, row.UnitPrice)
        cursor = connection.cursor()
        cursor.execute(sql, val)

    ####################### Upload orderTable.csv #####################
    # inserting Order data to MySQL database (or to RDS in AWS)
    dataPath_db = os.environ.get("PATH_db")
    fileName = dataPath_db + 'orderTable.csv'
    orderData = pd.read_csv (fileName) 
    for row in orderData.itertuples():
        sql = """
        INSERT INTO OrderTable (
            OrderNo, RecNo, TxDate, Store_Name,
            Cust_Name, Payment, TotPrice)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """ 
        val = (row.OrderNo, row.recNo, row.TxDate, 
                row.Store_Name, row.Cust_Name, row.Payment,
                row.TotPrice)
        cursor = connection.cursor()
        cursor.execute(sql, val)

    ####################### Upload orderDetailTable.csv #####################
    # inserting Order Detail data to MySQL database (or to RDS in AWS)
    dataPath_db = os.environ.get("PATH_db")
    fileName = dataPath_db + 'orderDetailTable.csv'
    orderDetailData = pd.read_csv (fileName) 
    for row in orderDetailData.itertuples():
        sql = """
        INSERT INTO OrderDetailTable (
            RecNo, OrderNo, Size, ProdNo, ProdName, UnitPrice)
            VALUES (%s,%s,%s,%s,%s,%s)
        """ 
        val = (row.recNo, row.OrderNo, row.Size, 
                row.ProdNo, row.ProdName, row.UnitPrice)
        cursor = connection.cursor()
        cursor.execute(sql, val)

    connection.commit()
    cursor.close()
    print("All Data Are Uploaded!.")

def load_data_to_db():
    # Upload all data to MySQL/ RDS in AWS
    # 1) kpi
    # 2) kpi_percentage
    # 3) indicator

    insert_to_db()

load_data_to_db()