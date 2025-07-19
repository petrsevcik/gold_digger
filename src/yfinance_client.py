
from typing import Any, Dict, Optional

import pandas as pd
import yfinance as yf

from utils.logging_config import get_logger


class YahooFinanceClient:
    """
    A client for fetching stock data from Yahoo Finance using the yfinance library.
    
    This class provides methods to get current stock prices, historical data,
    and other stock information for a given ticker symbol.
    """
    
    def __init__(self, ticker: str):
        """
        Initialize the Yahoo Finance client with a stock ticker.
        
        Args:
            ticker (str): The stock ticker symbol (e.g., 'AAPL', 'MSFT', 'GOOGL')
        """
        self.logger = get_logger(__class__.__name__)
        self.ticker = ticker.upper()
        self.stock = self._validate_ticker()

    def _validate_ticker(self):
        # quite dumb but if you put worng ticker you get back {'trailingPegRatio': None} -> so checking len(dict) == 1 & trailingPegRatio == None
        self.logger.info(f"Validating ticker: {self.ticker}")
        dat = yf.Ticker(self.ticker)
        if len(dat.info) == 1 and dat.info.get("trailingPegRatio") == None:
            raise ValueError(f"Invalid ticker: {self.ticker}. No data found")
        return dat
    
    def get_current_price(self) -> Optional[float]:
        """
        Get the current stock price.
        
        Returns:
            float: Current stock price, or None if not available
        """
        try:
            info = self.stock.info
            return info.get('regularMarketPrice', info.get('currentPrice'))
        except Exception as e:
            self.logger.error(f"Error getting current price for {self.ticker}: {e}")
            return None
    
    def get_stock_info(self) -> Dict[str, Any]:
        """
        Get comprehensive stock information.
        
        Returns:
            dict: Dictionary containing stock information
        """
        try:
            return self.stock.info
        except Exception as e:
            self.logger.error(f"Error getting stock info for {self.ticker}: {e}")
            return {}
    
    def get_historical_data(self, 
                          period: str = "1mo", 
                          start: Optional[str] = None, 
                          end: Optional[str] = None,
                          interval: str = "1d") -> Optional[pd.DataFrame]:
        """
        Get historical stock data.
        
        Args:
            period (str): Data period to download ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
            start (str, optional): Start date in YYYY-MM-DD format
            end (str, optional): End date in YYYY-MM-DD format
            interval (str): Data interval ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
        
        Returns:
            pd.DataFrame: Historical data with OHLCV columns, or None if error
        """
        try:
            if start and end:
                return self.stock.history(start=start, end=end, interval=interval)
            else:
                return self.stock.history(period=period, interval=interval)
        except Exception as e:
            self.logger.error(f"Error getting historical data for {self.ticker}: {e}")
            return None
    
    def __str__(self) -> str:
        """String representation of the client."""
        return f"YahooFinanceClient(ticker='{self.ticker}')"
    
    def __repr__(self) -> str:
        """Detailed string representation of the client."""
        return f"YahooFinanceClient(ticker='{self.ticker}')"


# Example usage
if __name__ == "__main__":
    # Create a client for Apple stock
    client = YahooFinanceClient("AAPL")
    
    # Get current price
    current_price = client.get_current_price()
    print(f"Current price of {client.ticker}: ${current_price}")
