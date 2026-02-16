import sqlite3

DB_NAME = "tax_system.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()

    # --- RAW INPUT TABLES ---
    
    # Stores basic household data
    c.execute('''CREATE TABLE IF NOT EXISTS taxpayers (
        taxpayer_id TEXT PRIMARY KEY,
        state TEXT,
        w2_income REAL,
        num_children INTEGER
    )''')

    # Stores the 5-year income history (for High-Income Surcharge)
    c.execute('''CREATE TABLE IF NOT EXISTS income_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        taxpayer_id TEXT,
        year_offset INTEGER, -- -1 to -5
        amount REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # Stores investment purchases
    c.execute('''CREATE TABLE IF NOT EXISTS asset_purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        taxpayer_id TEXT,
        asset_id TEXT,
        date TEXT,
        quantity REAL,
        unit_price REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # Stores investment sales
    c.execute('''CREATE TABLE IF NOT EXISTS asset_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        taxpayer_id TEXT,
        asset_id TEXT,
        date TEXT,
        quantity REAL,
        unit_price REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # Stores charitable donations
    c.execute('''CREATE TABLE IF NOT EXISTS donations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        taxpayer_id TEXT,
        amount REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # --- INTERMEDIATE COMPUTATION TABLES (For Debugging) ---

    # Stores Capital Gains results (FIFO matching)
    c.execute('''CREATE TABLE IF NOT EXISTS capital_gains_result (
        taxpayer_id TEXT PRIMARY KEY,
        net_capital_gain REAL,
        total_sales_proceeds REAL,
        total_cost_basis REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # Stores Federal tax logic breakdown
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

    # Stores State tax logic breakdown
    c.execute('''CREATE TABLE IF NOT EXISTS state_calculations (
        taxpayer_id TEXT PRIMARY KEY,
        state_name TEXT,
        california_gross_basis REAL,
        texas_taxable_basis REAL,
        final_state_tax REAL,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    # Stores final output liability
    c.execute('''CREATE TABLE IF NOT EXISTS final_liability (
        taxpayer_id TEXT PRIMARY KEY,
        total_federal_tax INTEGER,
        total_state_tax INTEGER,
        FOREIGN KEY(taxpayer_id) REFERENCES taxpayers(taxpayer_id)
    )''')

    conn.commit()
    conn.close()
    print(f"Database {DB_NAME} initialized successfully.")

if __name__ == "__main__":
    init_db()