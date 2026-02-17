import asyncio
import json
import aiosqlite
import logging
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = "taxes.db"

def calculate_ewma(history):
    """
    Computes the Exponentially Weighted Moving Average (EWMA) for the 5-year income history.
    Formula:
      E_1 = Y_{t-5}
      E_k = alpha * Y_{t-(6-k)} + (1 - alpha) * E_{k-1} for k=2..5
    alpha = 0.6
    """
    # history[0] = Y_{t-5}, history[4] = Y_{t-1} [cite: 142-144]
    
    alpha = Decimal("0.6")
    
    # Base case: E_1 = Y_{t-5}
    e_k = Decimal(str(history[0]))
    
    # Recursive steps for k=2 to 5
    for income in history[1:]:
        y_curr = Decimal(str(income))
        e_k = (alpha * y_curr) + ((Decimal("1") - alpha) * e_k)
        
    return e_k

async def insert_taxpayer(db, record):
    """
    Parses a single household record and inserts it into the database.
    Assumes a flat JSON structure based on A.5.1 field definitions.
    """
    taxpayer_id = record['taxpayer_id']
    
    # 1. Calculate EWMA [cite: 84-87]
    # Accessed directly from root, not nested
    ewma = calculate_ewma(record['prior_five_years_income'])
    
    # 2. Insert into TAXPAYERS table
    await db.execute(
        """
        INSERT INTO taxpayers (taxpayer_id, state, w2_income, num_children, ewma_income, processing_status)
        VALUES (?, ?, ?, ?, ?, 'PENDING')
        """,
        (
            taxpayer_id,
            record['state'],
            str(record['w2_income']),
            record['num_children'],
            str(ewma)
        )
    )
    
    # 3. Insert INCOME HISTORY
    history = record['prior_five_years_income']
    history_params = [(taxpayer_id, i - 5, str(amount)) for i, amount in enumerate(history)]
    await db.executemany(
        "INSERT INTO income_history (taxpayer_id, year_offset, amount) VALUES (?, ?, ?)",
        history_params
    )

    # 4. Insert CHARITABLE DONATIONS
    # Accessed directly from root [cite: 158]
    donations = record.get('charitable_donations', [])
    if donations:
        donation_params = [(taxpayer_id, str(amount)) for amount in donations]
        await db.executemany(
            "INSERT INTO charitable_donations (taxpayer_id, amount) VALUES (?, ?)",
            donation_params
        )

    # 5. Insert ASSET TRANSACTIONS
    transactions = []
    
    # Process Purchases (Accessed from root) [cite: 146]
    for p in record.get('purchases', []):
        transactions.append((
            taxpayer_id,
            p['asset_id'],
            p['date'],
            'BUY',
            str(p['quantity']),
            str(p['unit_price']),
            str(p['quantity']) # Initial remaining_quantity for FIFO
        ))
        
    # Process Sales (Accessed from root) [cite: 147]
    for s in record.get('sales', []):
        transactions.append((
            taxpayer_id,
            s['asset_id'],
            s['date'],
            'SELL',
            str(s['quantity']),
            str(s['unit_price']),
            None 
        ))
        
    if transactions:
        await db.executemany(
            """
            INSERT INTO asset_transactions 
            (taxpayer_id, asset_id, transaction_date, transaction_type, quantity, unit_price, remaining_quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            transactions
        )

async def ingest_file(file_path):
    """
    Main ingestion loop. Reads NDJSON line by line.
    """
    print(f"Starting ingestion of {file_path}...")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA synchronous = OFF")
        await db.execute("BEGIN TRANSACTION")
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        record = json.loads(line)
                        await insert_taxpayer(db, record)
                    except json.JSONDecodeError:
                        logging.error(f"Skipping invalid JSON line: {line[:50]}...")
                    except Exception as e:
                        logging.error(f"Error processing record: {e}")
                        # Re-raise to stop execution on schema mismatch during dev
                        raise 
                        
            await db.commit()
            print("Ingestion complete. Database hydrated.")
            
        except Exception as e:
            await db.rollback()
            logging.error(f"Critical error during ingestion: {e}")
            raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python ingestion.py <input_file.ndjson>")
    else:
        asyncio.run(ingest_file(sys.argv[1]))