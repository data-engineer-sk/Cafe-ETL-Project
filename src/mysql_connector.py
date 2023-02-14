import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

def get_env_variables():
    load_dotenv()
    HOST = os.environ.get("mysql_host")
    USER = os.environ.get("mysql_user")
    PASSWORD = os.environ.get("mysql_pass")
    DB_NAME = os.environ.get("mysql_db")

    return HOST, USER, PASSWORD, DB_NAME

def mysql_connect():
    try:
        inHOST, inUSER, inPASSWORD, inDATABASE = get_env_variables()

        params = {
                'host': inHOST,
                'user': inUSER,
                'password': inPASSWORD,
                'db': inDATABASE
        }

        connection = mysql.connector.connect(**params)
    except Error as e:
        print("Error while connecting to MySQL", e)
    
    return connection

mysql_connect()