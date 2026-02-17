-- Enable foreign key constraints to ensure data integrity
PRAGMA foreign_keys = ON;

-- 1. TAXPAYERS (Household Data)
-- Stores the core identity and status of each record from the input file.
CREATE TABLE IF NOT EXISTS taxpayers (
    taxpayer_id TEXT PRIMARY KEY,
    state TEXT CHECK(state IN ('California', 'Texas')) NOT NULL, -- [cite: 103, 104]
    w2_income DECIMAL(15, 2) NOT NULL, -- [cite: 15]
    num_children INTEGER NOT NULL DEFAULT 0, -- [cite: 140]
    
    -- Intermediate Calculation: EWMA
    -- We store this here to avoid recalculating it during the surcharge phase.
    ewma_income DECIMAL(15, 2), -- [cite: 84-87]
    
    -- Processing Workflow Status
    -- PENDING: Ingested, waiting for calculation
    -- COMPLETED: Tax calculated
    -- ERROR: Something went wrong
    processing_status TEXT DEFAULT 'PENDING' CHECK(processing_status IN ('PENDING', 'COMPLETED', 'ERROR'))
);

-- 2. INCOME HISTORY
-- Stores the raw 5-year history used to calculate the EWMA for the High-Income Surcharge.
CREATE TABLE IF NOT EXISTS income_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxpayer_id TEXT NOT NULL,
    year_offset INTEGER NOT NULL, -- e.g., -1 for T-1, -5 for T-5 [cite: 82]
    amount DECIMAL(15, 2) NOT NULL,
    FOREIGN KEY (taxpayer_id) REFERENCES taxpayers(taxpayer_id) ON DELETE CASCADE
);

-- 3. CHARITABLE DONATIONS
-- Stores individual donation records for Itemized Deduction logic.
CREATE TABLE IF NOT EXISTS charitable_donations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxpayer_id TEXT NOT NULL,
    amount DECIMAL(15, 2) NOT NULL, -- [cite: 56]
    FOREIGN KEY (taxpayer_id) REFERENCES taxpayers(taxpayer_id) ON DELETE CASCADE
);

-- 4. ASSET TRANSACTIONS (Purchases and Sales)
-- Stores the raw investment log. We normalize purchases and sales into one table 
-- with a 'transaction_type' to simplify time-ordering.
CREATE TABLE IF NOT EXISTS asset_transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxpayer_id TEXT NOT NULL,
    asset_id TEXT NOT NULL, -- e.g., "Ananya's Wool Whimsy" [cite: 24]
    transaction_date TEXT NOT NULL, -- ISO-8601 YYYY-MM-DD [cite: 150]
    transaction_type TEXT CHECK(transaction_type IN ('BUY', 'SELL')) NOT NULL,
    quantity DECIMAL(15, 3) NOT NULL, -- [cite: 42]
    unit_price DECIMAL(15, 2) NOT NULL,
    
    -- FIFO Tracking Field
    -- For 'BUY' records, this tracks how many shares are still available to be sold.
    -- Initialized to 'quantity' upon insertion.
    remaining_quantity DECIMAL(15, 3), 
    
    FOREIGN KEY (taxpayer_id) REFERENCES taxpayers(taxpayer_id) ON DELETE CASCADE
);

-- 5. REALIZED GAINS (Intermediate Computation)
-- This table acts as the "audit trail" for the FIFO matching logic. 
-- It links a specific SELL to the specific BUY lot(s) that fulfilled it.
CREATE TABLE IF NOT EXISTS realized_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxpayer_id TEXT NOT NULL,
    sell_transaction_id INTEGER NOT NULL,
    buy_transaction_id INTEGER NOT NULL,
    
    matched_quantity DECIMAL(15, 3) NOT NULL,
    
    -- The calculated gain/loss for this specific match: q * (p_sell - p_buy)
    gain_amount DECIMAL(15, 2) NOT NULL, -- [cite: 30]
    
    FOREIGN KEY (taxpayer_id) REFERENCES taxpayers(taxpayer_id) ON DELETE CASCADE,
    FOREIGN KEY (sell_transaction_id) REFERENCES asset_transactions(transaction_id),
    FOREIGN KEY (buy_transaction_id) REFERENCES asset_transactions(transaction_id)
);

-- 6. FINAL TAX COMPUTATIONS
-- Stores the final breakdown. This separates Federal and State logic for clarity.
CREATE TABLE IF NOT EXISTS tax_computations (
    taxpayer_id TEXT PRIMARY KEY,
    
    -- Federal Components
    federal_gross_income DECIMAL(15, 2), -- W2 + Net Capital Gains [cite: 22]
    federal_taxable_income DECIMAL(15, 2), -- Gross - Deductions [cite: 74]
    federal_deduction_type TEXT CHECK(federal_deduction_type IN ('STANDARD', 'ITEMIZED')), -- [cite: 75]
    federal_surcharge DECIMAL(15, 2), -- [cite: 93]
    total_federal_tax DECIMAL(15, 0), -- Rounded to nearest dollar [cite: 42, 127]
    
    -- State Components
    state_surcharge DECIMAL(15, 2), -- Only applies to CA [cite: 113]
    total_state_tax DECIMAL(15, 0), -- Rounded to nearest dollar [cite: 127]
    
    FOREIGN KEY (taxpayer_id) REFERENCES taxpayers(taxpayer_id) ON DELETE CASCADE
);

-- INDEXES for Performance
-- Creating indexes on foreign keys to speed up joins during the async processing loop.
CREATE INDEX IF NOT EXISTS idx_history_taxpayer ON income_history(taxpayer_id);
CREATE INDEX IF NOT EXISTS idx_donations_taxpayer ON charitable_donations(taxpayer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_taxpayer_date ON asset_transactions(taxpayer_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_realized_taxpayer ON realized_gains(taxpayer_id);