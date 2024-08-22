import psycopg2
import logging
import json
import re
from datetime import datetime

# Configure logging
logging.basicConfig(filename='database_operations.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_query(query, params=None):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="runpharmacy",
            user="pawan",
            password="password",
            host="host",
            port="5432"
        )
        cursor = conn.cursor()
        
        cursor.execute(query, params)
        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            return rows
        else:
            conn.commit()
            return None

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def customer_exists(email_id):
    query = "SELECT 1 FROM dev.customer WHERE Customer_Email_ID = %s LIMIT 1;"
    result = execute_query(query, (email_id,))
    return bool(result)

def validate_email(email):
    # Check for valid email formats
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None

def validate_dob(dob):
    try:
        dob_date = datetime.strptime(dob, "%Y-%m-%d")
        today = datetime.now()
        age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
        return age >= 16
    except ValueError:
        return False

def validate_mobile(mobile_no):
    # Check for a valid 10-digit Indian mobile number
    mobile_regex = r'^\d{10}$'
    return re.match(mobile_regex, mobile_no) is not None

def add_customer(first_name, last_name, dob, email, mobile_no, address):
    # Convert address input to JSONB format
    address_data = {
        "Address_Nick_Name": address['Address_Nick_Name'],
        "Address_Line_1": address['Address_Line_1'],
        "Address_Line_2": address['Address_Line_2'],
        "Address_City": address['Address_City'],
        "Address_State": address['Address_State'],
        "Address_Pincode": address['Address_Pincode'],
        "Address_Lat": address['Address_Lat'],
        "Address_Long": address['Address_Long'],
        "Is_Default_address": address['Is_Default_address']
    }
    address_json = json.dumps([address_data])
    
    query = """
    INSERT INTO dev.customer (
        Customer_First_Name, 
        Customer_Last_Name, 
        Customer_DOB, 
        Customer_Email_ID, 
        Customer_Mobile_No, 
        Is_Email_ID_Verified, 
        Is_Phone_No_Verified, 
        Customer_Address, 
        Created_By, 
        Updated_By
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    params = (first_name, last_name, dob, email, mobile_no, 'Y', 'Y', address_json, 'System', 'Pawan')
    execute_query(query, params)
    print("Customer added successfully!")

def update_customer(email):
    # Collect new details
    first_name = input("Enter new Customer First Name: ")
    last_name = input("Enter new Customer Last Name: ")
    mobile_no = input("Enter new Customer Mobile No: ")

    if not validate_mobile(mobile_no):
        print("Customer Mobile No is not in a valid 10-digit format.")
        return

    # Collect new address details
    address = {
        'Address_Nick_Name': input("Enter new address nickname: "),
        'Address_Line_1': input("Enter new address line 1: "),
        'Address_Line_2': input("Enter new address line 2: "),
        'Address_City': input("Enter new address city: "),
        'Address_State': input("Enter new address state: "),
        'Address_Pincode': input("Enter new address pincode: "),
        'Address_Lat': input("Enter new address latitude (default 0): ") or '0',
        'Address_Long': input("Enter new address longitude (default 0): ") or '0',
        'Is_Default_address': input("Is this the default address? (Y/N): ").upper()
    }

    if address['Is_Default_address'] not in ('Y', 'N'):
        print("Invalid input for default address. Must be 'Y' or 'N'.")
        return

    # Convert address input to JSONB format
    address_data = {
        "Address_Nick_Name": address['Address_Nick_Name'],
        "Address_Line_1": address['Address_Line_1'],
        "Address_Line_2": address['Address_Line_2'],
        "Address_City": address['Address_City'],
        "Address_State": address['Address_State'],
        "Address_Pincode": address['Address_Pincode'],
        "Address_Lat": address['Address_Lat'],
        "Address_Long": address['Address_Long'],
        "Is_Default_address": address['Is_Default_address']
    }
    address_json = json.dumps([address_data])
    
    query = """
    UPDATE dev.customer
    SET Customer_First_Name = %s,
        Customer_Last_Name = %s,
        Customer_Mobile_No = %s,
        Customer_Address = %s,
        Updated_By = %s
    WHERE Customer_Email_ID = %s;
    """
    params = (first_name, last_name, mobile_no, address_json, 'Pawan', email)
    execute_query(query, params)
    print("Customer details updated successfully!")

def main():
    first_name = input("Enter Customer First Name: ")
    last_name = input("Enter Customer Last Name: ")
    dob = input("Enter Customer DOB (YYYY-MM-DD): ")
    
    if not validate_dob(dob):
        print("Date of Birth is invalid or age is less than 16.")
        return
    
    email = input("Enter Customer Email ID: ")
    
    if not validate_email(email):
        print("Customer Email ID is not in a valid format.")
        return
    
    if customer_exists(email):
        print("Email ID already exists.")
        update_choice = input("Do you want to update your account details? (Y/N): ").upper()
        if update_choice == 'Y':
            update_customer(email)
        else:
            print("Exiting without updates.")
        return
    
    mobile_no = input("Enter Customer Mobile No: ")
    
    if not validate_mobile(mobile_no):
        print("Customer Mobile No is not in a valid 10-digit format.")
        return
    
    # Collect address details
    address = {
        'Address_Nick_Name': input("Enter address nickname: "),
        'Address_Line_1': input("Enter address line 1: "),
        'Address_Line_2': input("Enter address line 2: "),
        'Address_City': input("Enter address city: "),
        'Address_State': input("Enter address state: "),
        'Address_Pincode': input("Enter address pincode: "),
        'Address_Lat': input("Enter address latitude (default 0): ") or '0',
        'Address_Long': input("Enter address longitude (default 0): ") or '0',
        'Is_Default_address': input("Is this the default address? (Y/N): ").upper()
    }
    
    if address['Is_Default_address'] not in ('Y', 'N'):
        print("Invalid input for default address. Must be 'Y' or 'N'.")
        return
    
    add_customer(first_name, last_name, dob, email, mobile_no, address)

if __name__ == "__main__":
    main()
