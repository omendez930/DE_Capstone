from creditcard_art import *
import mysql.connector as dbconnect
import sys
import creditcard_art
from transactions import transactions_by_state as t_state
from transactions import get_transactions as gt
from transactions import transaction_by_dates as t_dates
from transactions import total_transaction_by_type as tt_type
from cc import monthly_cc_bill
from customer import customer_breakdown as cust_b, modify_account_details as acct_d
from pprint import pp


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
                    
            transactions = gt(zip_code,month,year)
            #print(type(transactions))
            if transactions == type(transactions):
                print(f'****************TRANSACTIONS occured in {zip_code}****************************')
                for transaction in transactions:
                    print(f'Transaction ID: {transaction[0]}')
                    print(f'State: {transaction[1]}')
                    print(f'Zip Code: {transaction[2]}')
                    print(f'Month: {transaction[3]}')
                    print(f'Year: {transaction[4]}')
                    print(f'Day: {transaction[5]}')
                    print(f'Transaction Type: {transaction[6]}')
                    print('----------------------------------------------------------------------------------------')
                else:
                    print('Please try again')
        elif initial_start == 2:
            transaction_type = input('Enter the type of transaction (Bills, Education, Grocery, Entertainment,Gas,Test, and Healthcare): ').lower()
            if transaction_type != 'q' or transaction_type != '':
                total_value_by_type = tt_type(transaction_type)
                if  total_value_by_type:
                    print('****************************************************************')
                    print(f'Total Value for {transaction_type}')
                    print(f'Count: {total_value_by_type[0]}')
                    print(f'Total: {total_value_by_type[1]}')
                    print('***************************************************************')

                else:
                    print('Value not located')
            else:
                print('Nothing was entered')
        
        elif initial_start == 3:
            state = input('Enter the State of the branch: ').lower()
            if  state != 'q' or state != '':
                total_by_state = t_state(state)
                if total_by_state:
                    print('\n********************************************************************************************')
                    print(f'Total transactions for {state}')
                    branch = ('Branch Code','Branch Name', 'Transaction Count', 'Total Amount')
                    for branches in branch:
                        print(branches, end=' ')
                    for state in total_by_state:
                        print()
                        print(state)
                    print('*******************************************************************************\n')
            else:
                print('No state was entered')
            
        
        elif initial_start == 4:
            first_name = input('Enter your name: ').lower()
            if first_name !='q' or first_name != '':
                middle_name = input('Enter your middle name: ').lower()
                last_name = input('Enter your Last name: ').lower()
                decision = int(input('Select the following\n1 - for acount details\n2- Monthly credit card bill\n3 - Transactions performed between certain dates\n4 - Change Account information\n'))
                if decision == 1:
                    acct_details = cust_b(first_name,middle_name,last_name)
                    if acct_details != None:
                        print(f'Account overview for {first_name} {last_name}')
                        
                        for acct in acct_details:
                            pp(acct)
                    else:
                        print('Please try again')
                elif decision == 2:
                    month = input('Enter month: ')
                    year = input('Enter year: ')
                    monthly_bill = monthly_cc_bill(first_name,last_name,month,year)
                    if monthly_bill != None:
                        print('***********************************************************************')
                        print(f'Credit Card bill as of {month}/{year} for {first_name} {last_name}')
                        print('CC\t\t', end='\t')
                        print('Total')
                        for bill in monthly_bill:
                            print(bill, end='\t')
                        print('\n**********************************************************************')
                    else:
                        print('Please try again')
                elif decision == 3:
                    ssn = input('Enter SSN: ')
                    start_date = input('Enter start date in the format yyyy-m-d: ')
                    end_date = input('Enter end date i the format yyyy-m-d: ')
                    transaction_dates = t_dates(ssn,start_date,end_date)
                    if transaction_dates != None:
                        print('***********************************************')
                        print(f'Transactions for {ssn} between {start_date} and {end_date}')
                        print('***********************************************')
                        print('Date', end='\t')
                        print('Charge')
                        for date in transaction_dates:
                            print(date[0],date[1])
                        print('***********************************************\n')
                    else:
                        print('Please try again')
                elif decision == 4:
                    ssn = input('Enter SSN: ')
                    customer = acct_d(ssn)
                    print(customer)    

if __name__ == '__main__':
    main()