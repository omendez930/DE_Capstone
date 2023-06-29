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

def customer_breakdown(first,middle,last):
    try:
        myconn = dbconnect.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        cursor = myconn.cursor()
        query = "select c.first_name, c.middle_name,c.last_name, c.CREDIT_CARD_NO,cc.TRANSACTION_TYPE,round(cc.TRANSACTION_VALUE,2) \
                from cdw_sapp_customer c \
                join cdw_sapp_credit_card cc on c.ssn = cc.CUST_SSN \
                where c.first_name = %s and c.middle_name = %s and c.last_name= %s"
        cursor.execute(query,(first,middle,last))
        cust_breakdown = cursor.fetchall()
        #print(cust_breakdown)
        if cust_breakdown:
            return cust_breakdown
        else:
            print('Account not found.')
    except:
        print('Connection timed out')
    finally:
        if myconn.is_connected():
            cursor.close()
            myconn.close()
            
def modify_account_details(ssn):
    try:
        myconn = dbconnect.connect(
            host='localhost',
            port='3306',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        cursor = myconn.cursor()
        query = 'select * from cdw_sapp_customer where ssn = %s'
        cursor.execute(query,(ssn,))
        customer = cursor.fetchone()
        
        if customer:
            print('Current Account Details')
            print(f'SSN: {customer[-2]}')
            print(f'Name: {customer[8]} {customer[9]}')
            print(f'Email: {customer[4]}')
            print(f'Phone: {customer[5]}000')
            print(f'Address: {customer[-1]}, {customer[0]}, {customer[2]}, {customer[6]}{customer[7]}')
            
            print('\nEnter the new account details:')
            
            #prompt for new details
            
            name = input('Name: ')
            if name != '':
                email = input('Email: ')
                if email !='':
                    phone = input('Phone: ')
                    if phone != '':
                        address = input('Address: ')
            
            #update the acount details i the address
            query = 'update cdw_sapp_customer\
                    set name = %s, email = %s, phone = %s, address = %s\
                    where ssn = %s'
            
            cursor.execute(query,(name,email,phone,address,ssn))
            myconn.commit()
            print('\nAccount details updated successfully!')
            return customer
        else:
            print('Customer not found')
        
    except:
        ('Connection interrupted')
    finally:
        if myconn.is_connected():
            cursor.close()
            myconn.close()
    

# customer = input('Enter SSN: ')
# acct = modify_account_details(customer)
# print(acct)