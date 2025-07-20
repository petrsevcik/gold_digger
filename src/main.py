#!/usr/bin/env python3
"""
CLI for Gold Digger - Yahoo Finance data collection tool.
"""
import click
from datetime import date
from typing import List

from services import (
    add_company_to_db,
    add_companies_to_db,
    add_stock_price_history_to_db,
    add_stock_prices_history_to_db,
    scrape_options_for_ticker,
    scrape_options_for_tickers,
)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Gold Digger - Yahoo Finance data collection tool."""
    pass


@cli.group()
def companies():
    """Manage company data."""
    pass


@companies.command("add")
@click.argument("ticker", type=str)
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def add_company(ticker: str, dry_run: bool):
    """Add a single company to the database."""
    click.echo(f"Adding company data for {ticker}...")
    result = add_company_to_db(ticker=ticker, dry_run=dry_run)
    
    if dry_run and result:
        click.echo(f"Company data for {ticker}:")
        for key, value in result.items():
            click.echo(f"  {key}: {value}")
    elif not dry_run:
        click.echo(f"Company data for {ticker} saved to database.")


@companies.command("add-multiple")
@click.argument("tickers", nargs=-1, required=True)
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def add_multiple_companies(tickers: tuple, dry_run: bool):
    """Add multiple companies to the database."""
    ticker_list = list(tickers)
    click.echo(f"Adding company data for {len(ticker_list)} companies: {', '.join(ticker_list)}")
    
    result = add_companies_to_db(tickers=ticker_list, dry_run=dry_run)
    
    if dry_run and result:
        for ticker, data in result.items():
            click.echo(f"\nCompany data for {ticker}:")
            if data:
                for key, value in data.items():
                    click.echo(f"  {key}: {value}")
            else:
                click.echo("  No data available")
    elif not dry_run:
        click.echo(f"Company data for {len(ticker_list)} companies saved to database.")


@cli.group()
def prices():
    """Manage stock price data."""
    pass


@prices.command("add")
@click.argument("ticker", type=str)
@click.option("--period", default="5d", help="Time period for historical data (e.g., 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)")
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def add_stock_price(ticker: str, period: str, dry_run: bool):
    """Add stock price history for a single ticker."""
    click.echo(f"Adding stock price data for {ticker} (period: {period})...")
    result = add_stock_price_history_to_db(ticker=ticker, period=period, dry_run=dry_run)
    
    if dry_run and result is not None:
        click.echo(f"Stock price data for {ticker}:")
        click.echo(f"  Shape: {result.shape}")
        click.echo(f"  Columns: {list(result.columns)}")
        click.echo(f"  Date range: {result.index.min()} to {result.index.max()}")
    elif not dry_run:
        click.echo(f"Stock price data for {ticker} saved to database.")


@prices.command("add-multiple")
@click.argument("tickers", nargs=-1, required=True)
@click.option("--period", default="5d", help="Time period for historical data (e.g., 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)")
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def add_multiple_stock_prices(tickers: tuple, period: str, dry_run: bool):
    """Add stock price history for multiple tickers."""
    ticker_list = list(tickers)
    click.echo(f"Adding stock price data for {len(ticker_list)} tickers: {', '.join(ticker_list)} (period: {period})")
    
    result = add_stock_prices_history_to_db(tickers=ticker_list, period=period, dry_run=dry_run)
    
    if dry_run and result:
        for ticker, data in result.items():
            click.echo(f"\nStock price data for {ticker}:")
            if data is not None:
                click.echo(f"  Shape: {data.shape}")
                click.echo(f"  Date range: {data.index.min()} to {data.index.max()}")
            else:
                click.echo("  No data available")
    elif not dry_run:
        click.echo(f"Stock price data for {len(ticker_list)} tickers saved to database.")


@cli.group()
def options():
    """Manage options data."""
    pass


@options.command("scrape")
@click.argument("ticker", type=str)
@click.option("--type", "option_type", type=click.Choice(["puts", "calls"]), help="Option type to scrape")
@click.option("--date", type=str, help="Specific expiration date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def scrape_ticker_options(ticker: str, option_type: str, date: str, dry_run: bool):
    """Scrape options data for a single ticker."""
    click.echo(f"Scraping options for {ticker}...")
    if option_type:
        click.echo(f"  Option type: {option_type}")
    if date:
        click.echo(f"  Expiration date: {date}")
    
    result = scrape_options_for_ticker(
        ticker=ticker, 
        option_type=option_type, 
        date=date, 
        dry_run=dry_run
    )
    
    if dry_run and result:
        click.echo(f"\nOptions data for {ticker}:")
        for exp_date, options_by_type in result.items():
            click.echo(f"  Expiration: {exp_date}")
            for opt_type, data in options_by_type.items():
                if data is not None:
                    click.echo(f"    {opt_type}: {len(data)} contracts")
                else:
                    click.echo(f"    {opt_type}: No data available")
    elif not dry_run:
        click.echo(f"Options data for {ticker} saved to database.")


@options.command("scrape-multiple")
@click.argument("tickers", nargs=-1, required=True)
@click.option("--type", "option_type", type=click.Choice(["puts", "calls"]), help="Option type to scrape")
@click.option("--date", type=str, help="Specific expiration date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def scrape_multiple_ticker_options(tickers: tuple, option_type: str, date: str, dry_run: bool):
    """Scrape options data for multiple tickers."""
    ticker_list = list(tickers)
    click.echo(f"Scraping options for {len(ticker_list)} tickers: {', '.join(ticker_list)}")
    if option_type:
        click.echo(f"  Option type: {option_type}")
    if date:
        click.echo(f"  Expiration date: {date}")
    
    result = scrape_options_for_tickers(
        tickers=ticker_list, 
        option_type=option_type, 
        date=date, 
        dry_run=dry_run
    )
    
    if dry_run and result:
        for ticker, ticker_data in result.items():
            click.echo(f"\nOptions data for {ticker}:")
            for exp_date, options_by_type in ticker_data.items():
                click.echo(f"  Expiration: {exp_date}")
                for opt_type, data in options_by_type.items():
                    if data is not None:
                        click.echo(f"    {opt_type}: {len(data)} contracts")
                    else:
                        click.echo(f"    {opt_type}: No data available")
    elif not dry_run:
        click.echo(f"Options data for {len(ticker_list)} tickers saved to database.")


@cli.command("batch")
@click.argument("tickers", nargs=-1, required=True)
@click.option("--companies", is_flag=True, help="Add company data")
@click.option("--prices", is_flag=True, help="Add stock price data")
@click.option("--options", is_flag=True, help="Scrape options data")
@click.option("--price-period", default="5d", help="Time period for historical data")
@click.option("--option-type", type=click.Choice(["puts", "calls"]), help="Option type to scrape")
@click.option("--option-date", type=str, help="Specific expiration date for options")
@click.option("--dry-run", is_flag=True, help="Show data without saving to database")
def batch_operation(tickers: tuple, companies: bool, prices: bool, options: bool, 
                   price_period: str, option_type: str, option_date: str, dry_run: bool):
    """Run multiple operations on a list of tickers."""
    ticker_list = list(tickers)
    click.echo(f"Running batch operations for {len(ticker_list)} tickers: {', '.join(ticker_list)}")
    
    if not any([companies, prices, options]):
        click.echo("Please specify at least one operation: --companies, --prices, or --options")
        return
    
    if companies:
        click.echo("\n=== Adding company data ===")
        add_companies_to_db(tickers=ticker_list, dry_run=dry_run)
    
    if prices:
        click.echo("\n=== Adding stock price data ===")
        add_stock_prices_history_to_db(tickers=ticker_list, period=price_period, dry_run=dry_run)
    
    if options:
        click.echo("\n=== Scraping options data ===")
        scrape_options_for_tickers(
            tickers=ticker_list, 
            option_type=option_type, 
            date=option_date, 
            dry_run=dry_run
        )
    
    click.echo("\nBatch operations completed!")


if __name__ == "__main__":
    cli()
