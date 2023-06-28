# import findspark
# findspark.init()

from creditcard_art import *
import mysql.connector as dbconnect
import sys
import creditcard_art
from state import transactions_by_state as t_state
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

# def transactions_by_state(state):
#     try:
#         myconn = dbconnect.connect(
#             host='localhost',
#             port='3306',
#             user='root',
#             password='password',
#             database='creditcard_capstone'
#         )
        
#         cursor = myconn.cursor()
#         query = 'select b.branch_code, b.branch_name, count(*) as transaction_count, round(sum(c.transaction_value), 2) as total_amount \
#                 from cdw_sapp_branch b \
#                 join cdw_sapp_credit_card c on b.BRANCH_CODE = c.BRANCH_CODE \
#                 where b.BRANCH_STATE = %s \
#                 group by b.branch_code, b.branch_name \
#                 order by b.branch_code'
        
#         cursor.execute(query,(state,))
#         total_by_state = cursor.fetchall()
#         #print(total_by_state)
#         return total_by_state
#     except:
#         print('Connection timed out')
#     finally:
#         if myconn.is_connected:
#             cursor.close()
#             myconn.close()
#             print('Database connection has closed') 
    
def customer_breakdown(first,middle,last):
    try:
        myconn = dbconnect.connect(
            host='localhost',
            port='3306',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        cursor = myconn.cursor()
        query = "select c.first_name, c.middle_name,c.last_name, c.CREDIT_CARD_NO,cc.TRANSACTION_TYPE,round(cc.TRANSACTION_VALUE,2) \
                from cdw_sapp_customer c \
                join cdw_sapp_credit_card cc on c.ssn = cc.CUST_SSN \
                where c.first_name = %s and c.middle_name = %s and c.last_name= %s"
        cursor.execute(query,(first,middle,last))
        cust_breakdown = cursor.fetchall()
        #print(cust_breakdown)
        return cust_breakdown
    except:
        print('Connection timed out')
    finally:
        if myconn.is_connected():
            cursor.close()
            myconn.close()
            
def monthly_cc_bill(fn,ln,month,year):
    myconn = dbconnect.connect(
        host='localhost',
        port='3306',
        user='root',
        password='password',
        database='creditcard_capstone'
    )
    cursor = myconn.cursor()
    query = 'select c.credit_card_no, sum(cc.transaction_value) as total \
            from cdw_sapp_customer c \
            join cdw_sapp_credit_card cc on c.ssn = cc.cust_ssn\
            where c.first_name = %s and c.last_name = %s and cc.month = %s and cc.year = %s \
            group by c.CREDIT_CARD_NO'
    cursor.execute(query,(fn,ln,month,year,))
    cc_monthly_bill = cursor.fetchone()
    return cc_monthly_bill

def transaction_by_dates(ssn,start_date,end_date):
    myconn = dbconnect.connect(
        host='localhost',
        port='3306',
        user='root',
        password='password',
        database='creditcard_capstone'
    )
    
    cursor = myconn.cursor()
    query = 'select concat(day,"-",month,"-",year) as transaction_date, TRANSACTION_VALUE \
            from cdw_sapp_credit_card \
            where cust_ssn = %s and concat(day,"-",month,"-",year) between %s and %s order by concat(year,month,day) desc;'
    cursor.execute(query,(ssn,start_date,end_date))
    transaction_dates = cursor.fetchall()
    return transaction_dates
def main():
    creditcard_art
    
    
    print('Welcome to the Credit Card transactions overview, please fill out the requested prompts.')
    while True:
        initial_start = int(input('Select one of the following options: \n1-zipcode overview\n2-Transation Type\n3-state lookup\n4-Client Details\n'))
        if initial_start == 1:
            zip_code = input('Enter a Zip Code, or enter q to quit: ')
            if zip_code != 'q' and zip_code != '':
                month = input('Enter the month: ')
                if month != 'q' and month != '':
                    year = input('Enter the year: ')
                    if year != 'q' and year != '':
                        transaction_type = input('Enter the type of transaction: ')
                        if transaction_type != 'q' or transaction_type !='':
                            state = input('Enter the State of the branch: ') 
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
        elif initial_start == 2:
            transaction_type = input('Enter the type of transaction (Bills, Education, Grocery, etc..): ')
            if transaction_type != 'q' or transaction_type != '':
                total_value_by_type = total_transaction_by_type(transaction_type)
                if  total_value_by_type:
                    print(f'Total Value for {transaction_type}')
                    print(f'Count: {total_value_by_type[0]}')
                    print(f'Total: {total_value_by_type[1]}')

                else:
                    print('Value not located')
            else:
                print('Nothing was entered')
        
        elif initial_start == 3:
            state = input('Enter the State of the branch: ') 
            if  state != 'q' or state != '':
                total_by_state = t_state(state)
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
                        print(state)
            else:
                print('No state was entered')
            
        
        elif initial_start == 4:
            first_name = input('Enter your name: ')
            if first_name !='q' or first_name != '':
                middle_name = input('Enter your middle name: ')
                last_name = input('Enter your Last name: ')
                decision = int(input('Select the following\n1 - for acount details\n2- Monthly credit card bill\n3 - Transactions performed between certain dates\n'))
                if decision == 1:
                    acct_details = customer_breakdown(first_name,middle_name,last_name)
                    print(f'Account overview for {first_name} {last_name}')
                    
                    for acct in acct_details:
                        pp(acct)
                elif decision == 2:
                    month = input('Enter month: ')
                    year = input('Ente year: ')
                    monthly_bill = monthly_cc_bill(first_name,last_name,month,year)
                    print('***********************************************************************')
                    print(f'Credit Card bill as of {month}/{year} for {first_name} {last_name}')
                    print('CC\t\t', end='\t')
                    print('Total')
                    for bill in monthly_bill:
                        print(bill, end='\t')
                    print('\n**********************************************************************')
                elif decision == 3:
                    ssn = input('Enter SSN: ')
                    start_date = input('Enter start date in the format d-m-yyyy: ')
                    end_date = input('Enter end date i the format d-m-yyyy')
                    transaction_dates = transaction_by_dates(ssn,start_date,end_date)
                    print('***********************************************')
                    print(f'Transactions between {start_date} and {end_date}')
                    print('***********************************************')
                    print('Date', end='\t')
                    print('Charge')
                    for date in transaction_dates:
                        print(date[0],date[1])
                    print('***********************************************\n')
                    
                    
                    #print(transaction_dates)                    
                
            
        
                
        
        
            
            
                
                  
        
        
        
        
        
        
        
        # cust_breakdown = customer_breakdown()
        # print(cust_breakdown)
        
    

if __name__ == '__main__':
    main()