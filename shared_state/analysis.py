import sqlite3
import threading
import queue
from database import get_connection, DB_LOCK

# Tunable: Number of worker threads
NUM_WORKERS = 4

def calculate_tax_for_person(tp_id, conn):
    """
    Performs the full tax computation pipeline for a single taxpayer.
    """
    c = conn.cursor()
    
    # --- PHASE 1: FETCH RAW DATA (Critical Section) ---
    with DB_LOCK:
        # Fetch basic info
        c.execute("SELECT * FROM taxpayers WHERE taxpayer_id=?", (tp_id,))
        tp_row = c.fetchone()
        
        # Fetch History
        c.execute("SELECT year_offset, amount FROM income_history WHERE taxpayer_id=?", (tp_id,))
        history_rows = c.fetchall()
        
        # Fetch Assets
        c.execute("SELECT * FROM asset_purchases WHERE taxpayer_id=? ORDER BY date ASC, id ASC", (tp_id,))
        purchases = [dict(row) for row in c.fetchall()]
        
        c.execute("SELECT * FROM asset_sales WHERE taxpayer_id=? ORDER BY date ASC, id ASC", (tp_id,))
        sales = [dict(row) for row in c.fetchall()]
        
        # Fetch Donations
        c.execute("SELECT amount FROM donations WHERE taxpayer_id=?", (tp_id,))
        donation_rows = c.fetchall()

    # --- PHASE 2: COMPUTE (No Lock - Parallelizable) ---
    
    # [cite_start]A. Capital Gains (FIFO) [cite: 32-34]
    realized_gains = 0.0
    total_sales_proceeds = 0.0
    total_cost_basis = 0.0
    
    # Initialize 'remaining' tracker
    for p in purchases:
        p['remaining'] = p['quantity']
        
    for sale in sales:
        qty_to_sell = sale['quantity']
        sale_proceeds = qty_to_sell * sale['unit_price']
        cost_basis_for_sale = 0.0
        
        for p in purchases:
            if qty_to_sell <= 0: break
            if p['remaining'] > 0:
                taken = min(qty_to_sell, p['remaining'])
                p['remaining'] -= taken
                qty_to_sell -= taken
                cost_basis_for_sale += taken * p['unit_price']
                
        gain = sale_proceeds - cost_basis_for_sale
        realized_gains += gain
        total_sales_proceeds += sale_proceeds
        total_cost_basis += cost_basis_for_sale

    # [cite_start]B. EWMA Calculation [cite: 83-88]
    history_map = {r['year_offset']: r['amount'] for r in history_rows}
    e_val = history_map.get(-5, 0.0)
    alpha = 0.6
    for k in [-4, -3, -2, -1]:
        val = history_map.get(k, 0.0)
        e_val = (alpha * val) + ((1 - alpha) * e_val)
    ewma_income = e_val

    # C. Federal Tax Logic
    w2_income = tp_row['w2_income']
    net_cap_gain = realized_gains
    gross_income = w2_income + net_cap_gain
    
    # Surcharge
    surcharge = 0.0
    if ewma_income > 1000000:
        surcharge = 0.02 * gross_income

    # Deductions
    std_deduction = 10000.0
    
    total_donations = sum(r['amount'] for r in donation_rows)
    
    # Child Tax Calculation
    # Base for child tax is Gross - Charitable
    taxable_base_for_child = gross_income - total_donations
    child_deduction = 0.0
    if taxable_base_for_child > 0:
        rate = min(tp_row['num_children'], 10) * 0.01
        child_deduction = rate * taxable_base_for_child
        
    itemized_total = total_donations + child_deduction
    
    if itemized_total > std_deduction:
        deduction_val = itemized_total
        method = "Itemized"
    else:
        deduction_val = std_deduction
        method = "Standard"
        
    taxable_income = max(0.0, gross_income - deduction_val)
    
    # Brackets
    fed_tax = 0.0
    rem = taxable_income
    
    # 0-100k @ 5%
    chunk = min(rem, 100000); fed_tax += chunk * 0.05; rem -= chunk
    # 100k-200k @ 10%
    if rem > 0: chunk = min(rem, 100000); fed_tax += chunk * 0.10; rem -= chunk
    # 200k-300k @ 15%
    if rem > 0: chunk = min(rem, 100000); fed_tax += chunk * 0.15; rem -= chunk
    # >300k @ 20%
    if rem > 0: fed_tax += rem * 0.20
    
    final_federal_tax = fed_tax + surcharge

    # D. State Tax Logic
    state = tp_row['state']
    final_state_tax = 0.0
    ca_basis = 0.0
    tx_basis = 0.0
    
    if state == "California":
        # 4% wage + 6% pos cap gain
        ca_basis = (0.04 * w2_income) + (0.06 * max(0.0, net_cap_gain))
        final_state_tax = ca_basis
        if surcharge > 0:
            final_state_tax *= 1.05
            
    elif state == "Texas":
        tx_basis = taxable_income
        rem = tx_basis
        tx_tax = 0.0
        
        # 0-90k @ 3%
        chunk = min(rem, 90000); tx_tax += chunk * 0.03; rem -= chunk
        # 90k-200k @ 5%
        if rem > 0: chunk = min(rem, 110000); tx_tax += chunk * 0.05; rem -= chunk
        # >200k @ 7%
        if rem > 0: tx_tax += rem * 0.07
        
        if deduction_val > 15000:
            tx_tax *= 0.99
            
        final_state_tax = tx_tax

    # --- PHASE 3: WRITE RESULTS (Critical Section) ---
    with DB_LOCK:
        # 1. Capital Gains Result
        c.execute('''INSERT OR REPLACE INTO capital_gains_result 
                     VALUES (?, ?, ?, ?)''', 
                     (tp_id, net_cap_gain, total_sales_proceeds, total_cost_basis))
                     
        # 2. Federal Calculations
        c.execute('''INSERT OR REPLACE INTO federal_calculations 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (tp_id, gross_income, ewma_income, surcharge, 
                      std_deduction, itemized_total, method, 
                      taxable_income, fed_tax, final_federal_tax))
                      
        # 3. State Calculations
        c.execute('''INSERT OR REPLACE INTO state_calculations 
                     VALUES (?, ?, ?, ?, ?)''',
                     (tp_id, state, ca_basis, tx_basis, final_state_tax))
                     
        # 4. Final Liability
        c.execute('''INSERT OR REPLACE INTO final_liability 
                     VALUES (?, ?, ?)''',
                     (tp_id, int(round(final_federal_tax)), int(round(final_state_tax))))
                     
        conn.commit()

def worker(q, conn):
    while True:
        try:
            tp_id = q.get(timeout=1)
        except queue.Empty:
            break
            
        if tp_id is None:
            break
            
        try:
            calculate_tax_for_person(tp_id, conn)
        except Exception as e:
            print(f"Error processing {tp_id}: {e}")
        finally:
            q.task_done()

def run_analysis_threaded():
    print(f"Starting Threaded Analysis with {NUM_WORKERS} workers...")
    conn = get_connection()
    q = queue.Queue()
    
    # 1. Load the Queue (Fast, one query)
    # We lock here briefly to ensure we get a consistent snapshot
    with DB_LOCK:
        c = conn.cursor()
        c.execute("SELECT taxpayer_id FROM taxpayers")
        ids = [row['taxpayer_id'] for row in c.fetchall()]
        
    for i in ids:
        q.put(i)
        
    # 2. Start Workers
    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(q, conn))
        t.start()
        threads.append(t)
        
    # 3. Add Sentinels
    for _ in range(NUM_WORKERS):
        q.put(None)
        
    # 4. Wait
    for t in threads:
        t.join()
        
    conn.close()
    print("Threaded Analysis Complete.")

if __name__ == "__main__":
    run_analysis_threaded()