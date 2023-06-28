import mysql.connector as dbconnect

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