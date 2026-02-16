import json
import argparse
import sys
import threading
import queue
from database import get_connection, init_db, DB_LOCK

# Tunable parameter: Number of worker threads
NUM_WORKERS = 4

def worker(q, conn):
    """
    Worker thread function.
    Consumes lines from queue, parses JSON, and inserts into DB using Mutex.
    """
    c = conn.cursor()
    
    while True:
        try:
            # Get a line from the queue
            # timeout=1 prevents hanging if something goes wrong
            line = q.get(timeout=1) 
        except queue.Empty:
            # If queue is empty and producer is done, we exit
            break
            
        if line is None: # Sentinel value to stop
            break

        try:
            # CPU INTENSIVE: Parsing
            record = json.loads(line)
            
            taxpayer_id = record.get('taxpayer_id')
            state = record.get('state')
            w2_income = record.get('w2_income', 0.0)
            num_children = record.get('num_children', 0)
            
            history = record.get('prior_five_years_income', [])
            purchases = record.get('purchases', [])
            sales = record.get('sales', [])
            donations = record.get('charitable_donations', [])

            # CRITICAL SECTION: Database Write
            # We acquire the lock to ensure atomicity of the transaction
            with DB_LOCK:
                # 1. Taxpayers
                c.execute('''INSERT OR REPLACE INTO taxpayers 
                             (taxpayer_id, state, w2_income, num_children)
                             VALUES (?, ?, ?, ?)''', 
                             (taxpayer_id, state, w2_income, num_children))
                
                # 2. History
                if len(history) == 5:
                    offsets = [-5, -4, -3, -2, -1]
                    for offset, amount in zip(offsets, history):
                        c.execute('''INSERT INTO income_history 
                                     (taxpayer_id, year_offset, amount)
                                     VALUES (?, ?, ?)''', 
                                     (taxpayer_id, offset, amount))

                # 3. Purchases
                for p in purchases:
                    c.execute('''INSERT INTO asset_purchases 
                                 (taxpayer_id, asset_id, date, quantity, unit_price)
                                 VALUES (?, ?, ?, ?, ?)''',
                                 (taxpayer_id, p['asset_id'], p['date'], p['quantity'], p['unit_price']))

                # 4. Sales
                for s in sales:
                    c.execute('''INSERT INTO asset_sales 
                                 (taxpayer_id, asset_id, date, quantity, unit_price)
                                 VALUES (?, ?, ?, ?, ?)''',
                                 (taxpayer_id, s['asset_id'], s['date'], s['quantity'], s['unit_price']))

                # 5. Donations
                for amount in donations:
                    c.execute('''INSERT INTO donations 
                                 (taxpayer_id, amount)
                                 VALUES (?, ?)''',
                                 (taxpayer_id, amount))
                
                # Commit strictly within the lock to prevent partial reads by other threads
                conn.commit()

        except json.JSONDecodeError:
            print(f"Thread {threading.current_thread().name}: JSON Error")
        except Exception as e:
            print(f"Thread {threading.current_thread().name}: Error {e}")
        finally:
            q.task_done()

def ingest_data_threaded(input_file):
    init_db()
    
    # Shared connection for all threads (allowed due to check_same_thread=False)
    # Note: In high-scale apps, you might give each thread its own connection.
    # Here, we share one to demonstrate the Mutex lock explicitly.
    conn = get_connection()
    
    work_queue = queue.Queue()
    threads = []
    
    print(f"Starting ingestion with {NUM_WORKERS} threads...")
    
    # Spawn workers
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(work_queue, conn), name=f"Worker-{i}")
        t.start()
        threads.append(t)
        
    # Producer: Read file and fill queue
    try:
        with open(input_file, 'r') as f:
            for line in f:
                if line.strip():
                    work_queue.put(line)
    except FileNotFoundError:
        print("Input file not found.")
        
    # Stop signal: Add sentinel values for each worker
    for _ in range(NUM_WORKERS):
        work_queue.put(None)
        
    # Wait for all threads to finish
    for t in threads:
        t.join()
        
    conn.close()
    print("Threaded Ingestion Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-inputFile', required=True)
    args, _ = parser.parse_known_args()
    ingest_data_threaded(args.inputFile)