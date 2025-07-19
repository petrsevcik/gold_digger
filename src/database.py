from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from decouple import config
from datetime import datetime
import logging
from utils.mapping import COMPANY_INFO_COLUMN_MAPPING_DICT


class MySQLClient:
    """MySQL database client for the gold_digger application."""
    
    def __init__(self):
        """Initialize the MySQL client and load environment variables."""
        self.database_url = config("DATABASE_URL", default=None)
        if not self.database_url:
            raise ValueError("DATABASE_URL not found in environment variables")
        
        # Convert the URL format from username:password@host:3306/gold_digger
        # to SQLAlchemy format: mysql+pymysql://username:password@host:3306/gold_digger
        if not self.database_url.startswith("mysql://"):
            self.database_url = f"mysql+pymysql://{self.database_url}"
        else:
            # Replace mysql:// with mysql+pymysql:// for PyMySQL
            self.database_url = self.database_url.replace("mysql://", "mysql+pymysql://")
        
        self.engine = create_engine(self.database_url)
    
    def get_table_columns(self, table_name: str = "companies") -> set:
        """
        Get the actual column names from the database table.
        
        Args:
            table_name (str): Name of the table to check
            
        Returns:
            set: Set of column names in the table
        """
        try:
            with self.engine.connect() as connection:
                # Get column information
                result = connection.execute(text(f"DESCRIBE {table_name}"))
                columns = {row[0] for row in result}
                return columns
        except Exception as e:
            logging.error(f"Error getting table columns: {e}")
            return set()

    def save_company_data(self, company_data: Dict[str, Any], table_name: str = "companies") -> None:
        """
        Save company data to the database using dynamic SQL generation.
        
        This method automatically generates the SQL INSERT statement based on the
        keys present in the company_data dictionary, making it flexible for handling
        different sets of fields from the Yahoo Finance API.
        
        Args:
            company_data (Dict[str, Any]): Dictionary containing company data from Yahoo Finance API
            table_name (str): Name of the table to insert into (default: "companies")
        
        Raises:
            ValueError: If company_data is empty or invalid
            ConnectionError: If database connection is not established
        """
        if not company_data:
            raise ValueError("Company data cannot be empty")
        
        if not self.engine:
            raise ConnectionError("Database connection not established")
        
        # Get actual database columns
        db_columns = self.get_table_columns(table_name)
        
        # Filter out None values and convert data types appropriately
        filtered_data = {}
        for key, value in company_data.items():
            if value is not None:
                # Skip complex objects that can't be stored in database
                if isinstance(value, (dict, list, tuple, set)):
                    continue
                
                # Convert camelCase keys to snake_case to match database column names
                db_key = key
                for camel_case, snake_case in COMPANY_INFO_COLUMN_MAPPING_DICT.items():
                    db_key = db_key.replace(camel_case, snake_case)
                
                # Only include fields that exist in the database
                if db_key in db_columns:
                    filtered_data[db_key] = value
                else:
                    logging.debug(f"Skipping field '{db_key}' (original: '{key}') - not in database schema")
        
        if not filtered_data:
            raise ValueError("No valid data to insert after filtering")
        
        # Generate dynamic SQL query
        columns = list(filtered_data.keys())
        placeholders = [f":{col}" for col in columns]
        
        sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        
        try:
            with self.engine.connect() as connection:
                query = text(sql)
                connection.execute(query, filtered_data)
                connection.commit()
                logging.info(f"Successfully inserted company data for symbol: {company_data.get('symbol', 'unknown')}")
        except Exception as e:
            logging.error(f"Error inserting company data: {e}")
            raise

    def save_company_data_upsert(self, company_data: Dict[str, Any], table_name: str = "companies") -> None:
        """
        Save company data to the database with upsert functionality.
        
        This method will INSERT new records or UPDATE existing ones based on the symbol.
        Uses MySQL's ON DUPLICATE KEY UPDATE syntax.
        
        Args:
            company_data (Dict[str, Any]): Dictionary containing company data from Yahoo Finance API
            table_name (str): Name of the table to insert into (default: "companies")
        
        Raises:
            ValueError: If company_data is empty or invalid
            ConnectionError: If database connection is not established
        """
        if not company_data:
            raise ValueError("Company data cannot be empty")
        
        if not self.engine:
            raise ConnectionError("Database connection not established")
        
        # Get actual database columns
        db_columns = self.get_table_columns(table_name)
        
        # Filter out None values and convert data types appropriately
        filtered_data = {}
        for key, value in company_data.items():
            if value is not None:
                # Skip complex objects that can't be stored in database
                if isinstance(value, (dict, list, tuple, set)):
                    continue
                
                # Convert camelCase keys to snake_case to match database column names
                db_key = key
                for camel_case, snake_case in COMPANY_INFO_COLUMN_MAPPING_DICT.items():
                    db_key = db_key.replace(camel_case, snake_case)
                
                # Only include fields that exist in the database
                if db_key in db_columns:
                    filtered_data[db_key] = value
                else:
                    logging.debug(f"Skipping field '{db_key}' (original: '{key}') - not in database schema")
        
        if not filtered_data:
            raise ValueError("No valid data to insert after filtering")
        
        # Generate dynamic SQL query with ON DUPLICATE KEY UPDATE
        columns = list(filtered_data.keys())
        placeholders = [f":{col}" for col in columns]
        
        # Create UPDATE clause for all columns except symbol (which is the key)
        update_clause = []
        for col in columns:
            if col != 'symbol':  # Don't update the primary key
                update_clause.append(f"{col} = VALUES({col})")
        
        sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE {', '.join(update_clause)}
        """
        
        try:
            with self.engine.connect() as connection:
                query = text(sql)
                connection.execute(query, filtered_data)
                connection.commit()
                logging.info(f"Successfully upserted company data for symbol: {company_data.get('symbol', 'unknown')}")
        except Exception as e:
            logging.error(f"Error upserting company data: {e}")
            raise

    def save_stock_price(self, stock_data: dict) -> None:
        """
        Save stock price data to the database.
        
        Args:
            stock_data (dict): Dictionary containing stock price data with keys:
                - date: Date of the stock price
                - ticker: Stock ticker symbol
                - price: Stock price value
        
        Raises:
            ValueError: If required fields are missing or have invalid format
        """
        required_fields = ["date", "ticker", "price"]
        
        # Check if all required fields are present
        for field in required_fields:
            if field not in stock_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate data types
        try:
            # Convert price to float
            price = float(stock_data["price"])
            
            # Handle date input that could be string, date or datetime
            date = stock_data["date"]
            if isinstance(date, str):
                date = datetime.strptime(date, "%Y-%m-%d").date()
            elif isinstance(date, datetime):
                date = date.date()
            elif not isinstance(date, date.__class__):
                raise ValueError("Date must be a string (YYYY-MM-DD), date or datetime object")
            
        except ValueError as e:
            raise ValueError(f"Invalid data format: {e}")
            
        # Save to database
        if self.engine:
            with self.engine.connect() as connection:
                query = text("""
                    INSERT INTO stock_prices (date, ticker, price)
                    VALUES (:date, :ticker, :price)
                """)
                connection.execute(query, {
                    "date": date,
                    "ticker": stock_data["ticker"],
                    "price": price
                })
                connection.commit()
        else:
            raise ConnectionError("Database connection not established")
    
    def save_options(self, *args, **kwargs) -> None:
        """
        Save options data to the database.
        
        Args:
            *args: Variable length argument list
            **kwargs: Arbitrary keyword arguments
        """
        pass
    
    def close(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed") 




