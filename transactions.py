# import findspark
# findspark.init()

# from art import *
import mysql.connector as dbconnect
# import pyspark
# from pyspark.sql import SparkSession


def get_transactions(zip_code, month, year, transacton_type, state):
    try:
        myConnection = dbconnect.connect(host = 'localhost',
        port='3306',
        user= 'root',
        password = 'password',
        database='creditcard_capstone')
    
        if myConnection.is_connected():
            print('Successfully connect to the Database')
            cursor = myConnection.cursor()
            
            query = 'Select cust_zip, cust_state, from cdw_sapp_cust right join cdw_sapp_creditcard on cdw_sapp_cust.cust_ssn = cdw_sapp_creditcard.cust_ssn'
    except Error as e:
        print('Error while connecting to Database', e)
    finally:
        if myConnection.is_connected():
            cursor.close()
            myConnection.close()
        print('Database connection has closed')
        
    return myConnection


def main():
    zip_code = input('Enter the Zip Code: ')
    month = input('Enter the month: ')
    year = input('Enter the year: ')
    transaction_type = input('Enter the type of transaction: ')
    state = input('Enter the State of the branch: ')
    
    transactions = get_transactions(zip_code,month,year,transaction_type,state)

    