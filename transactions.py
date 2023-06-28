# import findspark
# findspark.init()

from creditcard_art import *
import mysql.connector as dbconnect
import sys
import creditcard_art
from pprint import pp


def get_transactions(zip_code, month, year, transacton_type, state):
    try:
        myConnection = dbconnect.connect(host = 'localhost',
        port='3306',
        user= 'root',
        password = 'password',
        database='creditcard_capstone')
    
        if myConnection.is_connected():
            print('Successfully connected to the Database')
            cursor = myConnection.cursor()
            
            query = """select transaction_id,cust_state, cust_zip, month, year, day, TRANSACTION_TYPE \
                    from cdw_sapp_customer\
                    join cdw_sapp_credit_card on cdw_sapp_customer.ssn = cdw_sapp_credit_card.cust_ssn where cdw_sapp_customer.cust_zip = %s AND cdw_sapp_credit_card.month= %s AND cdw_sapp_credit_card.year = %s Order By day Desc"""
            cursor.execute(query,(zip_code,month,year))
            
            transaction_table = cursor.fetchall()
            
        return transaction_table
    except ConnectionAbortedError as e:
        print('Error while connecting to Database', e)
    finally:
        if myConnection.is_connected():
            cursor.close()
            myConnection.close()
        
        
    return myConnection


def total_transaction_by_type(transaction_type):
    try:
        conn = dbconnect.connect(host = 'localhost',
            port='3306',
            user= 'root',
            password = 'password',
            database='creditcard_capstone')
        
        cursor = conn.cursor()
        query = 'select count(transaction_id), round(sum(transaction_value),2) as Total from cdw_sapp_credit_card where transaction_type = %s group by transaction_type'
        cursor.execute(query,(transaction_type,))
        total = cursor.fetchone()
        #print(total)
        return total
    except:
        print('Programming error, could not process parameters')
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def transactions_by_state(state):
    try:
        myconn = dbconnect.connect(
            host='localhost',
            port='3306',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        
        cursor = myconn.cursor()
        query = 'select b.branch_code, b.branch_name, count(*) as transaction_count, round(sum(c.transaction_value), 2) as total_amount \
                from cdw_sapp_branch b \
                join cdw_sapp_credit_card c on b.BRANCH_CODE = c.BRANCH_CODE \
                where b.BRANCH_STATE = %s \
                group by b.branch_code, b.branch_name \
                order by b.branch_code'
        
        cursor.execute(query,(state,))
        total_by_state = cursor.fetchall()
        #print(total_by_state)
        return total_by_state
    except:
        print('Connection timed out')
    finally:
        if myconn.is_connected:
            cursor.close()
            myconn.close()
            print('Database connection has closed') 
    
# def customer_breakdown(first,middle,last):
#     try:
#         myconn = dbconnect.connect(
#             host='localhost',
#             port='3306',
#             user='root',
#             password='password',
#             database='creditcard_capstone'
#         )
#         cursor = myconn.cursor()
#         query = "select c.first_name, c.middle_name,c.last_name, c.CREDIT_CARD_NO,cc.TRANSACTION_TYPE,round(cc.TRANSACTION_VALUE,2) \
#                 from cdw_sapp_customer c \
#                 join cdw_sapp_credit_card cc on c.ssn = cc.CUST_SSN \
#                 where c.first_name = %s and c.middle_name = %s and c.last_name= %s"
#         cursor.execute(query,(first,middle,last))
#         cust_breakdown = cursor.fetchall()
#         #print(cust_breakdown)
#         return cust_breakdown
#     except:
#         print('Connection timed out')
#     finally:
#         if myconn.is_connected():
#             cursor.close()
#             myconn.close()
            
# def monthly_cc_bill(fn,ln,month,year):
#     myconn = dbconnect.connect(
#         host='localhost',
#         port='3306',
#         user='root',
#         password='password',
#         database='creditcard_capstone'
#     )
#     cursor = myconn.cursor()
#     query = 'select c.credit_card_no, sum(cc.transaction_value) as total \
#             from cdw_sapp_customer c \
#             join cdw_sapp_credit_card cc on c.ssn = cc.cust_ssn\
#             where c.first_name = %s and c.last_name = %s and cc.month = %s and cc.year = %s \
#             group by c.CREDIT_CARD_NO'
#     cursor.execute(query,(fn,ln,month,year,))
#     cc_monthly_bill = cursor.fetchone()
#     return cc_monthly_bill

def transaction_by_dates(ssn,start_date,end_date):
    myconn = dbconnect.connect(
        host='localhost',
        port='3306',
        user='root',
        password='password',
        database='creditcard_capstone'
    )
    
    cursor = myconn.cursor()
    query = 'select concat(year,"-",month,"-",day) as Transaction_date, TRANSACTION_VALUE,TRANSACTION_TYPE\
             from cdw_sapp_credit_card\
             where cust_ssn = %s and concat(year,"-",month,"-",day) >= (%s) and concat(year,"-",month,"-",day) <= (%s) order by year desc, month desc, day desc;'
    cursor.execute(query,(ssn,start_date,end_date))
    transaction_dates = cursor.fetchall()
    return transaction_dates
