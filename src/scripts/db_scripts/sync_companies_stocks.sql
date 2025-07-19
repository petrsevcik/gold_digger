INSERT INTO stocks (id, symbol, short_name, website)
SELECT id, symbol, short_name, website
FROM companies
ON DUPLICATE KEY UPDATE
    symbol = VALUES(symbol),
    short_name = VALUES(short_name),
    website = VALUES(website);