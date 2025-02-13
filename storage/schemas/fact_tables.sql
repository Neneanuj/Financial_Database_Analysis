-- Balance Sheet
CREATE TABLE IF NOT EXISTS balance_sheet (
    cik VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    total_assets NUMERIC(18,2),
    total_liabilities NUMERIC(18,2),
    equity NUMERIC(18,2),
    CONSTRAINT pk_balance PRIMARY KEY (cik, filing_date)
);

-- Income Statement
CREATE TABLE IF NOT EXISTS income_statement (
    cik VARCHAR(20) NOT NULL,
    fiscal_year INT,
    revenue NUMERIC(18,2),
    operating_expenses NUMERIC(18,2),
    net_income NUMERIC(18,2)
);

-- Cash Flow Statement
CREATE TABLE IF NOT EXISTS cash_flow (
    cik VARCHAR(20) NOT NULL,
    period_end DATE,
    operating_cash NUMERIC(18,2),
    investing_cash NUMERIC(18,2),
    financing_cash NUMERIC(18,2)
);