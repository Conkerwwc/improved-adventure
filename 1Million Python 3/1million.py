import psycopg2
from psycopg2 import OperationalError, ProgrammingError
import csv  # Import the csv module to read the CSV file

# --- Connection Parameters ---
# Based on Psycopg2 Python File
# Change the password to your own.
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '0451',
    'host': 'localhost',
    'port': '5432'
}

# --- Database Setup Function ---
def setup_database(conn):
    """
    Creates the 'customers' table in the database if it doesn't exist.
    """
    # Notice the SQL-friendly snake_case column names
    # 'ON CONFLICT (customer_id) DO NOTHING'
    # script runnable multiple times without errors on duplicate keys.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        customer_id VARCHAR(100) UNIQUE NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        company VARCHAR(255),
        city VARCHAR(255),
        country VARCHAR(255),
        phone_1 VARCHAR(100),
        phone_2 VARCHAR(100),
        email VARCHAR(255),
        subscription_date DATE, 
        website VARCHAR(255)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
        conn.commit()
        print("Successfully ensured 'customers' table exists.")
    except Exception as e:
        conn.rollback()
        print(f"Error during table setup: {e}")
        raise # Re-raise the exception to stop the script if setup fails

# --- CSV Processing and Batch Insertion Function ---


def process_and_insert_csv(conn, csv_filename, batch_size=1000):
    """
    Reads a CSV file line-by-line, filters rows, and inserts them
    into the 'customers' table in batches to handle large files.
    """
    
    # Match CSV file exactly
    CSV_HEADERS = [
        'Index', 'Customer Id', 'First Name', 'Last Name', 'Company', 'City', 
        'Country', 'Phone 1', 'Phone 2', 'Email', 'Subscription Date', 'Website'
    ]
    
    # These are the corresponding columns in your PostgreSQL table
    DB_COLUMNS = [
        'customer_id', 'first_name', 'last_name', 'company', 'city', 
        'country', 'phone_1', 'phone_2', 'email', 'subscription_date', 'website'
    ]
    
    # Exclude 'Index' from the DB insert
    # This SQL query is built to match the DB_COLUMNS order
    sql_insert = f"""
    INSERT INTO customers ({', '.join(DB_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        company = EXCLUDED.company,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        phone_1 = EXCLUDED.phone_1,
        phone_2 = EXCLUDED.phone_2,
        email = EXCLUDED.email,
        subscription_date = EXCLUDED.subscription_date,
        website = EXCLUDED.website;
    """
    
    batch = []
    total_processed = 0
    total_inserted = 0

    print(f"Starting to process '{csv_filename}'...")

    try:
        with open(csv_filename, mode='r', encoding='utf-8') as f:
            # Using csv.DictReader to easily access columns by name
            reader = csv.DictReader(f)
            
            for row in reader:
                total_processed += 1
                
                # --- Filtering Logic ---
                first_name = row.get('First Name', '')
                last_name = row.get('Last Name', '')
                
                if first_name.startswith('A') and last_name.startswith('F'):
                    # 'None' or empty strings for dates
                    sub_date = row.get('Subscription date')
                    if not sub_date: # If sub_date is "" or None
                        sub_date = None 
                
                    # Build the tuple of data IN THE ORDER of DB_COLUMNS
                    data_tuple = (
                        row.get('Customer Id'),
                        first_name,
                        last_name,
                        row.get('Company'),
                        row.get('City'),
                        row.get('Country'),
                        row.get('Phone 1'),
                        row.get('Phone 2'),
                        row.get('Email'),
                        sub_date,
                        row.get('Website')
                    )
                    batch.append(data_tuple)
                    total_inserted += 1

                # When the batch is full, execute it
                if len(batch) >= batch_size:
                    with conn.cursor() as cur:
                        cur.executemany(sql_insert, batch)
                    print(f"  ... inserted batch of {len(batch)} rows. (Total processed: {total_processed})")
                    batch = [] # Reset the batch

        # Insert any remaining rows (the last batch)
        if batch:
            with conn.cursor() as cur:
                cur.executemany(sql_insert, batch)
            print(f"  ... inserted final batch of {len(batch)} rows.")
        
        print(f"\n--- Processing Complete ---")
        print(f"Total rows processed from CSV: {total_processed}")
        print(f"Total rows inserted/updated in DB: {total_inserted}")

    except FileNotFoundError:
        print(f"Error: Input file '{csv_filename}' not found.")
    except Exception as e:
        print(f"An error occurred while processing the file: {e}")
        raise # Re-raise error to trigger a rollback

# --- Database Query Function ---
def get_filtered_customers(conn):
    """
    Queries the database for the customers, sorted by subscription date.
    """
    
    sql_query = """
    SELECT first_name, last_name, company, subscription_date
    FROM customers
    WHERE first_name LIKE 'A%' AND last_name LIKE 'F%'
    ORDER BY subscription_date DESC;
    """
    
    print("\n--- Querying Database for Inserted Customers (Sorted by Date) ---")
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            results = cur.fetchall()
            
            if not results:
                print("No matching customers found in the database.")
            else:
                for row in results:
                    # row[0]=first_name, row[1]=last_name, etc.
                    print(f"Name: {row[0]} {row[1]}, Company: {row[2]}, Subscribed: {row[3]}")

    except Exception as e:
        print(f"An error occurred while querying the database: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    
    # === IMPORTANT: remember CSV file's name ===
    CSV_FILE_TO_PROCESS = "customers.csv" 
    
    conn = None
    try:
        # --- Part 1: Connect and Setup Table ---
        conn = psycopg2.connect(**DB_PARAMS)
        print("Database connection established.")
        setup_database(conn) # Create the table 

        # --- Part 2: Process CSV and Insert Data ---
        # This function does all the batch inserts
        process_and_insert_csv(conn, CSV_FILE_TO_PROCESS, batch_size=1000)
        
        # --- Part 3: Commit all changes ---
        # ONCE after all batches are successfully processed.
        conn.commit()
        print("\nAll database changes have been committed.")

        # --- Part 4: Query and Show Results ---
        # sorting idea
        get_filtered_customers(conn)

    except OperationalError as e:
         print(f"CRITICAL: Connection Error. Check credentials/host/port.")
         print(f"Details: {e}")
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"An error occurred. Rolling back all changes.")
        print(f"Details: {e}")
        if conn:
            conn.rollback() # Rollback all changes from the transaction
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")