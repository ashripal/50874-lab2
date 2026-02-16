import sqlite3
from database import get_connection

# --- PASS 1: ASSETS & INCOME HISTORY ---

def calculate_capital_gains(conn):
    """
    Calculates Net Capital Gain using FIFO matching.
    Populates: capital_gains_result table.
    """
    c = conn.cursor()
    
    # Get all taxpayers
    c.execute("SELECT taxpayer_id FROM taxpayers")
    taxpayers = [row['taxpayer_id'] for row in c.fetchall()]
    
    for tp_id in taxpayers:
        # Fetch all purchases and sales for this taxpayer, ordered by date
        # Note: In a real system, we would sort by date. 
        # The spec implies FIFO based on "earliest purchased shares".
        c.execute("SELECT * FROM asset_purchases WHERE taxpayer_id=? ORDER BY date ASC, id ASC", (tp_id,))
        purchases = [dict(row) for row in c.fetchall()]
        
        c.execute("SELECT * FROM asset_sales WHERE taxpayer_id=? ORDER BY date ASC, id ASC", (tp_id,))
        sales = [dict(row) for row in c.fetchall()]
        
        # FIFO Logic
        realized_gains = 0.0
        total_sales_proceeds = 0.0
        total_cost_basis = 0.0
        
        # We need to track remaining quantity for each purchase
        # Add a 'remaining' field to the purchase dicts
        for p in purchases:
            p['remaining'] = p['quantity']
            
        for sale in sales:
            qty_to_sell = sale['quantity']
            sale_price = sale['unit_price']
            
            # Match against purchases
            cost_basis_for_sale = 0.0
            
            # Iterate through purchases to find the earliest ones with remaining shares
            for p in purchases:
                if qty_to_sell <= 0:
                    break
                
                if p['remaining'] > 0:
                    taken = min(qty_to_sell, p['remaining'])
                    
                    # Update tracking
                    p['remaining'] -= taken
                    qty_to_sell -= taken
                    
                    # Accumulate basis
                    cost_basis_for_sale += taken * p['unit_price']
            
            # If we ran out of purchases but still have shares to sell, 
            # this implies missing data or data error. 
            # For this implementation, we assume valid data (basis of 0 for phantom shares).
            
            sale_proceeds = sale['quantity'] * sale_price
            gain = sale_proceeds - cost_basis_for_sale
            
            realized_gains += gain
            total_sales_proceeds += sale_proceeds
            total_cost_basis += cost_basis_for_sale
            
        # Store result
        c.execute('''INSERT OR REPLACE INTO capital_gains_result 
                     (taxpayer_id, net_capital_gain, total_sales_proceeds, total_cost_basis)
                     VALUES (?, ?, ?, ?)''', 
                     (tp_id, realized_gains, total_sales_proceeds, total_cost_basis))
    
    conn.commit()
    print("Pass 1 (Capital Gains) complete.")

def calculate_ewma(conn):
    """
    Calculates the 5-year EWMA for the High-Income Surcharge .
    Populates: Part of federal_calculations (pre-computation).
    """
    c = conn.cursor()
    c.execute("SELECT taxpayer_id FROM taxpayers")
    taxpayers = [row['taxpayer_id'] for row in c.fetchall()]
    
    alpha = 0.6
    
    for tp_id in taxpayers:
        # Fetch history Y_t-5 to Y_t-1
        # The spec defines E1 = Y_t-5. 
        # Then Ek = alpha * Y_t-(6-k) + (1-alpha) * E_k-1
        
        c.execute("SELECT year_offset, amount FROM income_history WHERE taxpayer_id=? ORDER BY year_offset ASC", (tp_id,))
        rows = c.fetchall()
        
        # Create a map for easy lookup
        history = {row['year_offset']: row['amount'] for row in rows}
        
        # If no history, assume 0
        y_minus_5 = history.get(-5, 0.0)
        
        # E1
        e_val = y_minus_5
        
        # Iterate k from 2 to 5
        # k=2 -> Year -4
        # k=3 -> Year -3
        # k=4 -> Year -2
        # k=5 -> Year -1
        for k, year_offset in enumerate([-4, -3, -2, -1], start=2):
            income_val = history.get(year_offset, 0.0)
            e_val = (alpha * income_val) + ((1 - alpha) * e_val)
            
        final_ewma = e_val
        
        # We will store this in the federal_calculations table (upsert)
        # We initialize the row here
        c.execute('''INSERT OR REPLACE INTO federal_calculations 
                     (taxpayer_id, ewma_income) VALUES (?, ?)''', 
                     (tp_id, final_ewma))
                     
    conn.commit()
    print("Pass 1 (EWMA) complete.")

# --- PASS 2: FEDERAL TAX LOGIC ---

def calculate_federal_tax(conn):
    """
    Calculates Gross Income, Deductions, and Federal Tax.
    Populates: federal_calculations.
    """
    c = conn.cursor()
    
    # Fetch all data needed joining taxpayers, capital gains, and ewma
    query = '''
        SELECT t.taxpayer_id, t.w2_income, t.num_children,
               cg.net_capital_gain,
               fc.ewma_income
        FROM taxpayers t
        LEFT JOIN capital_gains_result cg ON t.taxpayer_id = cg.taxpayer_id
        LEFT JOIN federal_calculations fc ON t.taxpayer_id = fc.taxpayer_id
    '''
    c.execute(query)
    rows = c.fetchall()
    
    for row in rows:
        tp_id = row['taxpayer_id']
        w2 = row['w2_income']
        # If no capital gain record, assume 0
        net_cap_gain = row['net_capital_gain'] if row['net_capital_gain'] is not None else 0.0
        
        # 1. Gross Income [cite: 22]
        gross_income = w2 + net_cap_gain
        
        # 2. Surcharge [cite: 89-92]
        ewma = row['ewma_income'] if row['ewma_income'] is not None else 0.0
        surcharge = 0.0
        if ewma > 1000000:
            surcharge = 0.02 * gross_income
            
        # 3. Deductions
        
        # Option A: Standard Deduction [cite: 47]
        standard_ded = 10000.0
        
        # Option B: Itemized Deduction
        # Sum of charitable donations
        c.execute("SELECT SUM(amount) as total_donations FROM donations WHERE taxpayer_id=?", (tp_id,))
        don_row = c.fetchone()
        charitable_ded = don_row['total_donations'] if don_row['total_donations'] else 0.0
        
        # Child Tax Deduction [cite: 62-69]
        # "Percentage of taxable income before the child deduction is applied"
        # Taxable_Base = Gross - Charitable
        # (Assuming itemized path for this calculation)
        taxable_base_for_child = gross_income - charitable_ded
        
        num_children = row['num_children']
        child_rate = min(num_children, 10) * 0.01 # 1% per child, max 10%
        
        child_ded_val = 0.0
        if taxable_base_for_child > 0:
            child_ded_val = child_rate * taxable_base_for_child
            
        itemized_total = charitable_ded + child_ded_val
        
        # Select best deduction [cite: 74]
        if itemized_total > standard_ded:
            deduction_val = itemized_total
            deduction_method = "Itemized"
        else:
            deduction_val = standard_ded
            deduction_method = "Standard"
            
        # 4. Taxable Income [cite: 73]
        # "Any excess deduction beyond gross income is discarded" [cite: 45]
        taxable_income = max(0.0, gross_income - deduction_val)
        
        # 5. Progressive Brackets [cite: 76]
        # 0-100k: 5%
        # 100k-200k: 10%
        # 200k-300k: 15%
        # >300k: 20%
        
        bracket_tax = 0.0
        remaining_income = taxable_income
        
        # Chunk 1: 0 to 100,000
        chunk = min(remaining_income, 100000)
        bracket_tax += chunk * 0.05
        remaining_income -= chunk
        
        # Chunk 2: 100,000 to 200,000 (Size 100k)
        if remaining_income > 0:
            chunk = min(remaining_income, 100000)
            bracket_tax += chunk * 0.10
            remaining_income -= chunk
            
        # Chunk 3: 200,000 to 300,000 (Size 100k)
        if remaining_income > 0:
            chunk = min(remaining_income, 100000)
            bracket_tax += chunk * 0.15
            remaining_income -= chunk
            
        # Chunk 4: > 300,000
        if remaining_income > 0:
            bracket_tax += remaining_income * 0.20
            
        final_federal_tax = bracket_tax + surcharge
        
        # Update DB
        c.execute('''UPDATE federal_calculations 
                     SET gross_income=?, surcharge_amount=?, 
                         standard_deduction_val=?, itemized_deduction_val=?,
                         deduction_method=?, taxable_income=?, bracket_tax=?, 
                         final_federal_tax=?
                     WHERE taxpayer_id=?''',
                     (gross_income, surcharge, standard_ded, itemized_total,
                      deduction_method, taxable_income, bracket_tax, final_federal_tax,
                      tp_id))
                      
    conn.commit()
    print("Pass 2 (Federal Tax) complete.")

# --- PASS 3: STATE TAX LOGIC ---

def calculate_state_tax(conn):
    """
    Calculates State Tax for CA or TX.
    Populates: state_calculations, final_liability.
    """
    c = conn.cursor()
    
    # Need State, W2, Net Cap Gain, Fed Surcharge Status, Fed Taxable Income, Fed Deductions
    query = '''
        SELECT t.taxpayer_id, t.state, t.w2_income,
               cg.net_capital_gain,
               fc.surcharge_amount, fc.taxable_income, 
               (fc.standard_deduction_val) as std_val,
               (fc.itemized_deduction_val) as item_val,
               fc.deduction_method,
               fc.final_federal_tax
        FROM taxpayers t
        LEFT JOIN capital_gains_result cg ON t.taxpayer_id = cg.taxpayer_id
        LEFT JOIN federal_calculations fc ON t.taxpayer_id = fc.taxpayer_id
    '''
    c.execute(query)
    rows = c.fetchall()
    
    for row in rows:
        tp_id = row['taxpayer_id']
        state = row['state']
        final_state_tax = 0.0
        
        # --- CALIFORNIA LOGIC [cite: 103-111] ---
        if state == "California":
            w2 = row['w2_income']
            net_gain = row['net_capital_gain'] if row['net_capital_gain'] else 0.0
            
            # CA Basis [cite: 106]
            # 4% Wage + 6% max(0, Net Capital Gain)
            ca_tax_base = (0.04 * w2) + (0.06 * max(0.0, net_gain))
            
            final_state_tax = ca_tax_base
            
            # CA Surcharge [cite: 111]
            # If Federal Surcharge > 0, apply 5% surcharge to state level
            if row['surcharge_amount'] > 0:
                # Interpreted as 5% increase to the state tax bill
                final_state_tax = final_state_tax * 1.05
                
            c.execute('''INSERT OR REPLACE INTO state_calculations 
                         (taxpayer_id, state_name, california_gross_basis, final_state_tax)
                         VALUES (?, ?, ?, ?)''',
                         (tp_id, state, ca_tax_base, final_state_tax))

        # --- TEXAS LOGIC [cite: 112-119] ---
        elif state == "Texas":
            # Uses Federal Taxable Income [cite: 117]
            taxable_inc = row['taxable_income']
            
            # Brackets [cite: 114]
            # 0-90k: 3%
            # 90k-200k: 5%
            # >200k: 7%
            
            tx_tax = 0.0
            rem = taxable_inc
            
            # Chunk 1: 0-90k
            chunk = min(rem, 90000)
            tx_tax += chunk * 0.03
            rem -= chunk
            
            # Chunk 2: 90k-200k (Size 110k)
            if rem > 0:
                chunk = min(rem, 110000)
                tx_tax += chunk * 0.05
                rem -= chunk
                
            # Chunk 3: >200k
            if rem > 0:
                tx_tax += rem * 0.07
                
            # Discount [cite: 118]
            # If fed deductions > 15,000, apply 1% deduction to computed state tax
            deduction_used = row['item_val'] if row['deduction_method'] == 'Itemized' else row['std_val']
            
            if deduction_used > 15000:
                tx_tax = tx_tax * 0.99
                
            final_state_tax = tx_tax
            
            c.execute('''INSERT OR REPLACE INTO state_calculations 
                         (taxpayer_id, state_name, texas_taxable_basis, final_state_tax)
                         VALUES (?, ?, ?, ?)''',
                         (tp_id, state, taxable_inc, final_state_tax))
        
        # --- FINAL LIABILITY [cite: 121, 125] ---
        fed_tax_rounded = int(round(row['final_federal_tax']))
        state_tax_rounded = int(round(final_state_tax))
        
        c.execute('''INSERT OR REPLACE INTO final_liability
                     (taxpayer_id, total_federal_tax, total_state_tax)
                     VALUES (?, ?, ?)''',
                     (tp_id, fed_tax_rounded, state_tax_rounded))
                     
    conn.commit()
    print("Pass 3 (State Tax & Final Liability) complete.")

def run_analysis():
    conn = get_connection()
    print("Starting Analysis Pipeline...")
    calculate_capital_gains(conn)
    calculate_ewma(conn)
    calculate_federal_tax(conn)
    calculate_state_tax(conn)
    print("Analysis Pipeline Finished.")
    conn.close()

if __name__ == "__main__":
    run_analysis()