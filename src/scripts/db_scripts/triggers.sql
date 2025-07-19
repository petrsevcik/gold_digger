CREATE TRIGGER trg_companies_after_update
AFTER UPDATE ON companies
FOR EACH ROW
BEGIN
    UPDATE stocks
    SET
        symbol = NEW.symbol,
        short_name = NEW.short_name,
        website = NEW.website
    WHERE id = NEW.id;
END;

CREATE TRIGGER trg_companies_after_insert
AFTER INSERT ON companies
FOR EACH ROW
BEGIN
    INSERT INTO stocks (id, symbol, short_name, website)
    VALUES (NEW.id, NEW.symbol, NEW.short_name, NEW.website)
    ON DUPLICATE KEY UPDATE
        symbol = VALUES(symbol),
        short_name = VALUES(short_name),
        website = VALUES(website);
END;

CREATE TRIGGER trg_companies_after_delete
AFTER DELETE ON companies
FOR EACH ROW
BEGIN
    DELETE FROM stocks WHERE id = OLD.id;
END;