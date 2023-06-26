# import findspark
# findspark.init()

from art import *
import mysql.connector as dbconnect
import sys
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
    
    #def customer_details
    #
def main():
    # cc = text2art('1234 5678 9100 0000', 'block')
    # expiration_date = text2art('Exp: 00/00', 'small')
    # cardholder = text2art('Cardhoder: John DOE', 'small_slant')
    
    # print(cc)
    # print(expiration_date)
    # print(cardholder)
    
    print('Welcome to the Credit Card transactions overview, please fill out the requested prompts.')
    while True:
        zip_code = input('Enter a Zip Code, or enter q to quit: ')
        if zip_code != 'q' and zip_code != '':
            month = input('Enter the month: ')
            if month != 'q' and month != '':
                year = input('Enter the year: ')
                if year != 'q' and year != '':
                    transaction_type = input('Enter the type of transaction: ')
                    if transaction_type != 'q' or transaction_type !='':
                        state = input('Enter the State of the branch: ')   
        else:
            print('Transaction not found')
            sys.exit()

        transactions = get_transactions(zip_code,month,year,transaction_type,state)
        print(transactions)
        if transactions:
            print('***************************************************************************TRANSACTIONS*****************************************************************************')
            for transaction in transactions:
                print(f'Transaction ID: {transaction[0]}')
                print(f'State: {transaction[1]}')
                print(f'Zip Code: {transaction[2]}')
                print(f'Month: {transaction[3]}')
                print(f'Year: {transaction[4]}')
                print(f'Day: {transaction[5]}')
                print(f'Transaction Type: {transaction[6]}')
                print('----------------------------------------------------------------------------------------')
        
        total_value_by_type = total_transaction_by_type(transaction_type)
        if  total_value_by_type:
            print(f'Total Value for {transaction_type}')
            print(f'Count: {total_value_by_type[0]}')
            print(f'Total: {total_value_by_type[1]}')

        else:
            print('Value not located')
        
        total_by_state = transactions_by_state(state)
        if total_by_state:
            print('********************************************************************************************')
            print()
            print(f'Total transactions for {state}')
            print()
            branch = ('Branch Code','Branch Name', 'Transaction Count', 'Total Amount')
            for branches in branch:
                print(branches[:],sep=' | ', end=' ')
                print()
            for state in total_by_state:
                print(state[0], end=' ')
                print(state[1], end=' ')
                print(state[2], end=' ')
                print(state[3], end=' ')
        
        #customer_details = get_customer_details()        
    

if __name__ == '__main__':
    main()