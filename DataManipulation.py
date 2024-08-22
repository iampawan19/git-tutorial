import psycopg2
import logging
import json
import re
import bcrypt
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

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        print(f"Database error: {e}")
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

def get_existing_customer(email):
    query = "SELECT Customer_Address, Customer_Password FROM dev.customer WHERE Customer_Email_ID = %s;"
    result = execute_query(query, (email,))
    if result:
        return result[0]
    return None

def get_existing_addresses(email):
    query = "SELECT Customer_Address FROM dev.customer WHERE Customer_Email_ID = %s;"
    result = execute_query(query, (email,))
    if result:
        return json.loads(result[0][0])
    return []

def validate_email(email):
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
    mobile_regex = r'^\d{10}$'
    return re.match(mobile_regex, mobile_no) is not None

def get_pincode_details(pincode):
    query = "SELECT StateName, district FROM dev.pincode WHERE pincode = %s LIMIT 1;"
    result = execute_query(query, (pincode,))
    if result:
        return result[0]  # Return the tuple (StateName, district)
    return None

def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def add_customer(first_name, last_name, dob, email, mobile_no, address, password):
    pincode_details = get_pincode_details(address['Address_Pincode'])
    
    if not pincode_details:
        print("Invalid pincode.")
        return
    
    address['Address_State'] = pincode_details[0]
    address['Address_City'] = pincode_details[1]
    
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
    
    hashed_password = hash_password(password)
    
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
        Customer_Password, 
        Created_By, 
        Updated_By
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    params = (first_name, last_name, dob, email, mobile_no, 'Y', 'Y', address_json, hashed_password, 'System', 'Pawan')
    execute_query(query, params)
    print("Customer added successfully!")

def update_customer(email):
    existing_customer = get_existing_customer(email)
    
    if not existing_customer:
        print("Customer does not exist.")
        return
    
    first_name = input("Enter new Customer First Name: ")
    last_name = input("Enter new Customer Last Name: ")
    mobile_no = input("Enter new Customer Mobile No: ")

    if not validate_mobile(mobile_no):
        print("Customer Mobile No is not in a valid 10-digit format.")
        return

    address = {
        'Address_Nick_Name': input("Enter new address nickname: "),
        'Address_Line_1': input("Enter new address line 1: "),
        'Address_Line_2': input("Enter new address line 2: "),
        'Address_Pincode': input("Enter new address pincode: "),
        'Address_Lat': input("Enter new address latitude (default 0): ") or '0',
        'Address_Long': input("Enter new address longitude (default 0): ") or '0',
        'Is_Default_address': input("Is this the default address? (Y/N): ").upper()
    }

    if address['Is_Default_address'] not in ('Y', 'N'):
        print("Invalid input for default address. Must be 'Y' or 'N'.")
        return

    pincode_details = get_pincode_details(address['Address_Pincode'])
    
    if not pincode_details:
        print("Invalid pincode.")
        return

    address['Address_State'] = pincode_details[0]
    address['Address_City'] = pincode_details[1]

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
    
    new_password = input("Enter new password (leave blank if no change): ")
    if new_password:
        hashed_password = hash_password(new_password)
        password_query = ", Customer_Password = %s"
        password_params = (hashed_password,)
    else:
        password_query = ""
        password_params = ()

    query = f"""
    UPDATE dev.customer
    SET Customer_First_Name = %s,
        Customer_Last_Name = %s,
        Customer_Mobile_No = %s,
        Customer_Address = %s
        {password_query},
        Updated_By = %s
    WHERE Customer_Email_ID = %s;
    """
    params = (first_name, last_name, mobile_no, address_json, *password_params, 'Pawan', email)
    execute_query(query, params)
    print("Customer details updated successfully!")

def add_new_address(email):
    addresses = get_existing_addresses(email)
    if len(addresses) >= 5:
        print("Customer already has 5 addresses. Cannot add more.")
        return

    new_address = {
        'Address_Nick_Name': input("Enter new address nickname: "),
        'Address_Line_1': input("Enter new address line 1: "),
        'Address_Line_2': input("Enter new address line 2: "),
        'Address_Pincode': input("Enter new address pincode: "),
        'Address_Lat': input("Enter new address latitude (default 0): ") or '0',
        'Address_Long': input("Enter new address longitude (default 0): ") or '0',
        'Is_Default_address': input("Is this the default address? (Y/N): ").upper()
    }

    if new_address['Is_Default_address'] not in ('Y', 'N'):
        print("Invalid input for default address. Must be 'Y' or 'N'.")
        return

    pincode_details = get_pincode_details(new_address['Address_Pincode'])
    
    if not pincode_details:
        print("Invalid pincode.")
        return

    new_address['Address_State'] = pincode_details[0]
    new_address['Address_City'] = pincode_details[1]

    query = """
    UPDATE dev.customer
    SET Customer_Address = Customer_Address || %s::jsonb,
        Updated_By = %s
    WHERE Customer_Email_ID = %s;
    """
    params = (json.dumps([new_address]), 'Pawan', email)
    execute_query(query, params)
    print("New address added successfully!")

def delete_address(email):
    addresses = get_existing_addresses(email)
    
    if not addresses:
        print("No addresses found for this customer.")
        return
    
    print("Existing addresses:")
    for idx, address in enumerate(addresses, 1):
        print(f"{idx}. {address['Address_Nick_Name']}, {address['Address_Line_1']}, {address['Address_City']}, {address['Address_State']}, {address['Address_Pincode']}")
    
    try:
        index_to_delete = int(input("Enter the number of the address you want to delete: ")) - 1
        if 0 <= index_to_delete < len(addresses):
            addresses.pop(index_to_delete)
            
            query = """
            UPDATE dev.customer
            SET Customer_Address = %s::jsonb,
                Updated_By = %s
            WHERE Customer_Email_ID = %s;
            """
            params = (json.dumps(addresses), 'Pawan', email)
            execute_query(query, params)
            print("Address deleted successfully!")
        else:
            print("Invalid selection.")
    except ValueError:
        print("Invalid input. Please enter a valid number.")

def main():
    first_name = input("Enter Customer First Name: ")
    last_name = input("Enter Customer Last Name: ")
    dob = input("Enter Customer DOB (YYYY-MM-DD): ")
    email = input("Enter Customer Email ID: ")

    if not validate_email(email):
        print("Customer Email ID is not in a valid format.")
        return

    if not validate_dob(dob):
        print("Customer must be at least 16 years old.")
        return                                    

    if customer_exists(email):
        print("Email ID already exists.")
        update_choice = input("Do you want to update your account details? (Y/N): ").upper()
        if update_choice == 'Y':
            update_customer(email)
        else:
            add_choice = input("Do you want to add a new address? (Y/N): ").upper()
            if add_choice == 'Y':
                add_new_address(email)
            else:
                delete_choice = input("Do you want to delete an address? (Y/N): ").upper()
                if delete_choice == 'Y':
                    delete_address(email)
    else:
        mobile_no = input("Enter Customer Mobile No: ")
        if not validate_mobile(mobile_no):
            print("Customer Mobile No is not in a valid 10-digit format.")
            return

        password = input("Enter a password: ")
        address = {
            'Address_Nick_Name': input("Enter address nickname: "),
            'Address_Line_1': input("Enter address line 1: "),
            'Address_Line_2': input("Enter address line 2: "),
            'Address_Pincode': input("Enter address pincode: "),
            'Address_Lat': input("Enter address latitude (default 0): ") or '0',
            'Address_Long': input("Enter address longitude (default 0): ") or '0',
            'Is_Default_address': input("Is this the default address? (Y/N): ").upper()
        }

        if address['Is_Default_address'] not in ('Y', 'N'):
            print("Invalid input for default address. Must be 'Y' or 'N'.")
            return

        add_customer(first_name, last_name, dob, email, mobile_no, address, password)

if __name__ == "__main__":
    main()
