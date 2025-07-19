#!/usr/bin/env python3
"""
Test script for saving company data from Yahoo Finance to the database.
"""

from yfinance_client import YahooFinanceClient
from database import MySQLClient
from utils.logging_config import get_logger

logger = get_logger("services")

# add_company_to_db

# add companies_to_db

# update_company_info

# scrape_stock_prices

# scrape_options -> logic for options + tables 



def test_company_data_save():
    """Test saving company data to the database."""
    
    # Initialize clients
    db_client = MySQLClient()
    yf_client = YahooFinanceClient("NVDA")  # Test with Apple
    
    try:
        # Get company data from Yahoo Finance
        print("Fetching company data from Yahoo Finance...")
        company_data = yf_client.get_stock_info()
        
        if not company_data:
            print("No data received from Yahoo Finance")
            return
        
        print(f"Received {len(company_data)} fields from Yahoo Finance")
        print(f"Sample fields: {list(company_data.keys())[:10]}")
        
        # Debug: Check for complex data types
        complex_types = {}
        for key, value in company_data.items():
            if isinstance(value, (dict, list, tuple, set)):
                complex_types[key] = type(value).__name__
        
        if complex_types:
            print(f"Complex data types found: {complex_types}")
        
        # Save to database using regular insert
        print("\nSaving company data to database...")
        db_client.save_company_data(company_data)
        print("✓ Company data saved successfully!")
        
        # Test upsert functionality (this will update if record exists)
        print("\nTesting upsert functionality...")
        db_client.save_company_data_upsert(company_data)
        print("✓ Company data upserted successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db_client.close()

def test_multiple_companies():
    """Test saving data for multiple companies."""
    
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "WBD", "PATH", "LYFT", "GRAB", "NKE"]
    db_client = MySQLClient()
    
    try:
        for ticker in tickers:
            print(f"\nProcessing {ticker}...")
            yf_client = YahooFinanceClient(ticker)
            company_data = yf_client.get_stock_info()
            
            if company_data:
                db_client.save_company_data_upsert(company_data)
                print(f"✓ {ticker} data saved")
            else:
                print(f"✗ No data for {ticker}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db_client.close()

if __name__ == "__main__":
    print("Testing company data save functionality...")
    test_company_data_save()
    
    print("\n" + "="*50)
    print("Testing multiple companies...")
    #test_multiple_companies() 