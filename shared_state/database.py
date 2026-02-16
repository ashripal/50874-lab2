import sqlite3
import threading

DB_NAME = "tax_system_threaded.db"

# GLOBAL MUTEX for Shared State
# All threads must acquire this lock before writing to the database.
DB_LOCK = threading.Lock()

def get_connection():
    # check_same_thread=False allows multiple threads to use this connection
    # provided they are synchronized (which we will do with DB_LOCK).
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    # We acquire the lock even for initialization to be safe
    with DB_LOCK:
        conn = get_connection()
        c = conn.cursor()

        # --- RAW INPUT TABLES ---
        c.execute('''CREATE TABLE IF NOT EXISTS taxpayers (
            taxpayer_id TEXT PRIMARY KEY,
            state TEXT,
            w2_income REAL,
            num_children INTEGER
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS income_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxpayer_id TEXT,
            year_offset INTEGER,
            amount REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS asset_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxpayer_id TEXT,
            asset_id TEXT,
            date TEXT,
            quantity REAL,
            unit_price REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS asset_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxpayer_id TEXT,
            asset_id TEXT,
            date TEXT,
            quantity REAL,
            unit_price REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS donations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxpayer_id TEXT,
            amount REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        # --- INTERMEDIATE COMPUTATION TABLES ---
        c.execute('''CREATE TABLE IF NOT EXISTS capital_gains_result (
            taxpayer_id TEXT PRIMARY KEY,
            net_capital_gain REAL,
            total_sales_proceeds REAL,
            total_cost_basis REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS federal_calculations (
            taxpayer_id TEXT PRIMARY KEY,
            gross_income REAL,
            ewma_income REAL,
            surcharge_amount REAL,
            standard_deduction_val REAL,
            itemized_deduction_val REAL,
            deduction_method TEXT,
            taxable_income REAL,
            bracket_tax REAL,
            final_federal_tax REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS state_calculations (
            taxpayer_id TEXT PRIMARY KEY,
            state_name TEXT,
            california_gross_basis REAL,
            texas_taxable_basis REAL,
            final_state_tax REAL,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS final_liability (
            taxpayer_id TEXT PRIMARY KEY,
            total_federal_tax INTEGER,
            total_state_tax INTEGER,
            FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
        )''')

        conn.commit()
        conn.close()
        print(f"Threaded Database {DB_NAME} initialized.")

if __name__ == "__main__":
    init_db()