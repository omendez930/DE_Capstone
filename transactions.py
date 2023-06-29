
import mysql.connector as dbconnect
from dotenv import load_dotenv
import mysql.connector as dbconnect
import sys
import os
load_dotenv()

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')

def get_transactions(zip_code, month, year):
    #connect to Mysql database using evironmental variables
    try:
        myConnection = dbconnect.connect(host=host,
            port=port,
            user=user,
            password=password,
            database=database)
    
        #create db cursor to iterate over the rows in a db
        cursor = myConnection.cursor()
        #query provides results for zipcodes in the database. Type, zipcode, month, year, and day are provided
        query = """select transaction_id,cust_state, cust_zip, month, year, day, TRANSACTION_TYPE \
                from cdw_sapp_customer\
                join cdw_sapp_credit_card on cdw_sapp_customer.ssn = cdw_sapp_credit_card.cust_ssn where cdw_sapp_customer.cust_zip = %s AND cdw_sapp_credit_card.month= %s AND cdw_sapp_credit_card.year = %s Order By day Desc"""
        cursor.execute(query,(zip_code,month,year))
        #retrieve all roe from database cursor
        transaction_table = cursor.fetchall()
        if transaction_table:
            return transaction_table
        else:
            print('The information given does not exist in our database')
    #if eror connectionaborted error occurs print the message
    except ConnectionAbortedError as e:
        print('Error while connecting to Database', e)
    finally:
        if myConnection.is_connected():
            cursor.close()
            myConnection.close()
        
        
    return myConnection


def total_transaction_by_type(transaction_type):
    try:
        conn = dbconnect.connect(host=host,
            port=port,
            user=user,
            password=password,
            database=database)
        #create db cursor to iterate over the rows in a db
        cursor = conn.cursor()
        #query that returns the total value amount by the transaction type
        query = 'select count(transaction_id), round(sum(transaction_value),2) as Total from cdw_sapp_credit_card where transaction_type = %s group by transaction_type'
        cursor.execute(query,(transaction_type,))
        total = cursor.fetchone()
        #print(total)
        #if True return the row provided by the query
        if total:
            return total
        else:
            print('The transaction type does not exist. Please try again')
    except:
        print('Programming error, could not process parameters')
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def transactions_by_state(state):
    try:
        myconn = dbconnect.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        #create db cursor to iterate over the rows in a db
        cursor = myconn.cursor()
        #query that returns the total number of transactions type, and the total amount for the transaction type joined by branch code and filtered by state
        query = 'select b.branch_code, b.branch_name, count(*) as transaction_count, round(sum(c.transaction_value), 2) as total_amount \
                from cdw_sapp_branch b \
                join cdw_sapp_credit_card c on b.BRANCH_CODE = c.BRANCH_CODE \
                where b.BRANCH_STATE = %s \
                group by b.branch_code, b.branch_name \
                order by b.branch_code'
        
        cursor.execute(query,(state,))
        total_by_state = cursor.fetchall()
        if total_by_state:
        #print(total_by_state)
            return total_by_state
        else:
            print('State not located, please try again.')
    except:
        print('Connection timed out')
    finally:
        if myconn.is_connected:
            cursor.close()
            myconn.close()
            print('Database connection has closed')
        

def transaction_by_dates(ssn,start_date,end_date):
    myconn = dbconnect.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    
    cursor = myconn.cursor()
    query = 'select concat(year,"-",month,"-",day) as Transaction_date, TRANSACTION_VALUE,TRANSACTION_TYPE\
             from cdw_sapp_credit_card\
             where cust_ssn = %s and concat(year,"-",month,"-",day) >= (%s) and concat(year,"-",month,"-",day) <= (%s) order by year desc, month desc, day desc;'
    cursor.execute(query,(ssn,start_date,end_date))
    transaction_dates = cursor.fetchall()
    if transaction_dates:
        return transaction_dates
    else:
        print('Dates are not available.')
