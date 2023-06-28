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

def monthly_cc_bill(fn,ln,month,year):
    try:
        myconn = dbconnect.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
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
    except:
        print('Connection has closed')
    finally:
        if myconn.is_connected():
            cursor.close()
            myconn.close()
            

