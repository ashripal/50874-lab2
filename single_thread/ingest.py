import json
import argparse
import sys
from database import get_connection, init_db

def ingest_data(input_file):
    # Initialize DB first to ensure tables exist
    init_db()
    
    conn = get_connection()
    c = conn.cursor()
    
    print(f"Ingesting data from {input_file}...")
    
    try:
        with open(input_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_num}: {e}")
                    continue

                # 1. Parse Household Data
                taxpayer_id = record.get('taxpayer_id')
                state = record.get('state')
                w2_income = record.get('w2_income', 0.0)
                num_children = record.get('num_children', 0)
                
                # Insert into Taxpayers
                c.execute('''INSERT OR REPLACE INTO taxpayers 
                             (taxpayer_id, state, w2_income, num_children)
                             VALUES (?, ?, ?, ?)''', 
                             (taxpayer_id, state, w2_income, num_children))
                
                # 2. Parse Earned Income History (Array of 5 numbers)
                history = record.get('prior_five_years_income', [])
                if len(history) == 5:
                    # Input array is [Y_t-5, Y_t-4, Y_t-3, Y_t-2, Y_t-1]
                    # Map to offsets -5 through -1
                    offsets = [-5, -4, -3, -2, -1]
                    for offset, amount in zip(offsets, history):
                        c.execute('''INSERT INTO income_history 
                                     (taxpayer_id, year_offset, amount)
                                     VALUES (?, ?, ?)''', 
                                     (taxpayer_id, offset, amount))
                
                # 3. Parse Investment Data (Purchases)
                purchases = record.get('purchases', [])
                for p in purchases:
                    c.execute('''INSERT INTO asset_purchases 
                                 (taxpayer_id, asset_id, date, quantity, unit_price)
                                 VALUES (?, ?, ?, ?, ?)''',
                                 (taxpayer_id, p['asset_id'], p['date'], p['quantity'], p['unit_price']))

                # 4. Parse Investment Data (Sales)
                sales = record.get('sales', [])
                for s in sales:
                    c.execute('''INSERT INTO asset_sales 
                                 (taxpayer_id, asset_id, date, quantity, unit_price)
                                 VALUES (?, ?, ?, ?, ?)''',
                                 (taxpayer_id, s['asset_id'], s['date'], s['quantity'], s['unit_price']))

                # 5. Parse Charitable Giving
                donations = record.get('charitable_donations', [])
                for amount in donations:
                    c.execute('''INSERT INTO donations 
                                 (taxpayer_id, amount)
                                 VALUES (?, ?)''',
                                 (taxpayer_id, amount))
                                 
        conn.commit()
        print(f"Ingestion complete. Processed {line_num} records.")
        
    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest tax data from NDJSON.')
    parser.add_argument('-inputFile', required=True, help='Path to NDJSON input file')
    # Note: -outputFile is required by the spec for the main runner, 
    # but strictly speaking the ingest step doesn't produce the output file yet.
    # We will ignore it for this specific module if passed, or just take inputFile.
    
    args, unknown = parser.parse_known_args()
    
    ingest_data(args.inputFile)