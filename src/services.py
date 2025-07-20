#!/usr/bin/env python3
"""
Test script for saving company data from Yahoo Finance to the database.
"""
from typing import List
from datetime import date as datetime_date

import pandas as pd

from database import MySQLClient
from utils.logging_config import get_logger
from yfinance_client import YahooFinanceClient

logger = get_logger("services")


def add_company_to_db(ticker: str, dry_run: bool = False) -> dict | None:
    """Save company info (yf.Tciker.info) to companies table in db."""
    logger.info(f"Fetching {ticker} data from yahoo finance")

    yf_client = YahooFinanceClient(ticker)
    company_data = yf_client.get_stock_info()

    if not company_data:
        logger.error("No data received from Yahoo Finance")
        return

    logger.debug(f"Received {len(company_data)} fields from Yahoo Finance")

    if dry_run:
        logger.info("Dry run. Not saving to db. Returning data as dict")
        return company_data

    _save_company_to_db(company_data)


def add_companies_to_db(tickers: List[str], dry_run: bool = False) -> dict | None:
    result = {}
    for ticker in tickers:
        company_data = add_company_to_db(ticker=ticker, dry_run=dry_run)

        if dry_run:
            result[ticker] = company_data

    return result or None


def add_stock_price_history_to_db(
    ticker: str, period: str = "5d", dry_run: bool = False
) -> pd.DataFrame | None:
    """Save stock price (yf.Tciker.history) to stock_prices table in db."""

    logger.info(f"Fetching {ticker} data from yahoo finance")
    yf_client = YahooFinanceClient(ticker)
    price_data = yf_client.get_historical_data(period=period)

    if dry_run:
        logger.info("Dry run. Not saving to db. Returning data as pd.Dataframe")
        return price_data

    _save_price_history(ticker, price_data)


def add_stock_prices_history_to_db(
    tickers: list, period: str = "5d", dry_run: bool = False
) -> dict | None:
    result = {}
    for ticker in tickers:
        price_df = add_stock_price_history_to_db(
            ticker=ticker, period=period, dry_run=dry_run
        )

        if dry_run:
            result[ticker] = price_df

    return result or None

def scrape_options_for_ticker(ticker: str, option_type: str = None, date: str | datetime_date = None, dry_run: bool = False):
    """
    Scrape options data for a given ticker.
    
    Args:
        ticker (str): Stock ticker symbol
        option_type (str, optional): "puts" or "calls". If None, both types are returned
        date (str | date, optional): Specific date to scrape. If None, all available dates are scraped
        dry_run (bool): If True, return data without saving to database
    
    Returns:
        dict: Dictionary containing options data organized by date and type
    """
    logger.info(f"Scraping options for {ticker}")
    
    yf_client = YahooFinanceClient(ticker)
    option_dates = yf_client.get_option_dates()  # tuple of date strings ('2025-07-25')
    
    # Convert date parameter to string if it's a datetime.date object
    target_date = None
    if date is not None:
        if isinstance(date, datetime_date):
            target_date = date.strftime('%Y-%m-%d')
        else:
            target_date = str(date)
    
    # Determine which dates to process
    dates_to_process = [target_date] if target_date else option_dates
    
    # Validate option_type
    if option_type is not None and option_type not in ["puts", "calls"]:
        raise ValueError("option_type must be 'puts', 'calls', or None")
    
    # Determine which option types to process
    option_types = ["puts", "calls"] if option_type is None else [option_type]
    
    result = {}
    
    for date_str in dates_to_process:
        if date_str not in option_dates:
            logger.warning(f"Date {date_str} not available for {ticker}. Available dates: {option_dates}")
            continue
            
        logger.info(f"Processing options for {ticker} on {date_str}")
        result[date_str] = {}
        
        for opt_type in option_types:
            try:
                options_data = yf_client.get_options(date_str, opt_type)
                result[date_str][opt_type] = options_data
                logger.info(f"Retrieved {len(options_data)} {opt_type} for {ticker} on {date_str}")
                print(f"Retrieved {len(options_data)} {opt_type} for {ticker} on {date_str}")
                
                # Save to database if not dry run
                if not dry_run and options_data is not None and not options_data.empty:
                    _save_options_to_db(ticker, opt_type, options_data, date_str)
                    
            except Exception as e:
                logger.error(f"Error retrieving {opt_type} for {ticker} on {date_str}: {e}")
                result[date_str][opt_type] = None
    
    return result


def scrape_options_for_tickers(tickers: List[str], option_type: str = None, date: str | datetime_date = None, dry_run: bool = False) -> dict | None:
    """
    Scrape options data for multiple tickers.
    
    Args:
        tickers (List[str]): List of stock ticker symbols
        option_type (str, optional): "puts" or "calls". If None, both types are returned
        date (str | datetime_date, optional): Specific date to scrape. If None, all available dates are scraped
        dry_run (bool): If True, return data without saving to database
    
    Returns:
        dict: Dictionary containing options data organized by ticker, date and type
    """
    result = {}
    for ticker in tickers:
        options_data = scrape_options_for_ticker(
            ticker=ticker, 
            option_type=option_type, 
            date=date, 
            dry_run=dry_run
        )
        
        if dry_run:
            result[ticker] = options_data
    
    return result or None

# private and helper functions
def _save_company_to_db(company_data: dict) -> None:
    db_client = MySQLClient()

    # Save to database using upsert
    try:
        logger.info("Saving company data to db...")
        db_client.save_company_data_upsert(company_data)
        logger.info("Company data saved successfully!")
    except Exception as e:
        logger.error(f"Error when saving to db: {e}")
    finally:
        logger.info("Closing db connection")
        db_client.close()


def _save_price_history(ticker: str, price_data: pd.DataFrame) -> None:
    db_client = MySQLClient()

    # Save to database using upsert
    try:
        logger.info("Saving price data to db...")
        db_client.save_price_data_upsert(price_data, ticker)
        logger.info("Price data saved successfully!")
    except Exception as e:
        logger.error(f"Error when saving to db: {e}")
    finally:
        logger.info("Closing db connection")
        db_client.close()


def _save_options_to_db(ticker: str, option_type: str, options_data: pd.DataFrame, expiration_date: str) -> None:
    """Save options data to database using upsert."""
    db_client = MySQLClient()

    # Save to database using upsert
    try:
        logger.info(f"Saving {option_type} data to db for {ticker} on {expiration_date}...")
        db_client.save_options_data_upsert(options_data, ticker, option_type, expiration_date)
        logger.info(f"{option_type.capitalize()} data saved successfully!")
    except Exception as e:
        logger.error(f"Error when saving {option_type} to db: {e}")
    finally:
        logger.info("Closing db connection")
        db_client.close()


if __name__ == "__main__":
    print("Testing option functionality...")
    # Test single ticker
    # scrape_options_for_ticker("WBD", option_type="puts", date='2025-09-19')
    
    # Test multiple tickers
    print(scrape_options_for_tickers(["WBD", "PATH", "LYFT", "GRAB"]))
