from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from decouple import config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from utils.logging_config import get_logger
from utils.mapping import COMPANY_INFO_COLUMN_MAPPING_DICT, OPTIONS_COLUMN_MAPPING_DICT

logger = get_logger("MySQLClient")


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
            self.database_url = self.database_url.replace(
                "mysql://", "mysql+pymysql://"
            )

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
            logger.error(f"Error getting table columns: {e}")
            return set()

    def save_company_data_upsert(
        self, company_data: Dict[str, Any], table_name: str = "companies"
    ) -> None:
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
                    logger.debug(
                        f"Skipping field '{db_key}' (original: '{key}') - not in database schema"
                    )

        if not filtered_data:
            raise ValueError("No valid data to insert after filtering")

        # Generate dynamic SQL query with ON DUPLICATE KEY UPDATE
        columns = list(filtered_data.keys())
        placeholders = [f":{col}" for col in columns]

        # Create UPDATE clause for all columns except symbol (which is the key)
        update_clause = []
        for col in columns:
            if col != "symbol":  # Don't update the primary key
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
                logger.info(
                    f"Successfully upserted company data for symbol: {company_data.get('symbol', 'unknown')}"
                )
        except Exception as e:
            logger.error(f"Error upserting company data: {e}")
            raise

    def save_price_data_upsert(self, price_data: pd.DataFrame, ticker: str) -> None:
        """
        Save stock price data from DataFrame to the database with upsert functionality.

        Args:
            price_data (pd.DataFrame): DataFrame containing stock price data with columns:
                - Date: Date of the stock price
                - Open: Opening price
                - High: Highest price
                - Low: Lowest price
                - Close: Closing price
                - Volume: Trading volume
            ticker (str): Stock ticker symbol

        Raises:
            ValueError: If required fields are missing or have invalid format
            ConnectionError: If database connection is not established
        """
        if price_data is None or price_data.empty:
            raise ValueError("Price data cannot be empty")

        # Required columns from Yahoo Finance DataFrame
        required_columns = ["Open", "High", "Low", "Close"]

        # Check if all required columns are present
        missing_columns = [
            col for col in required_columns if col not in price_data.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Prepare data for insertion
        records_to_insert = []
        for index, row in price_data.iterrows():
            # Convert pandas timestamp to datetime
            if hasattr(index, "to_pydatetime"):
                date = index.to_pydatetime()
            else:
                date = index

            # Create record with ticker
            record = {
                "date": date,
                "ticker": ticker,
                "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
                "high": float(row["High"]) if pd.notna(row["High"]) else None,
                "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
                "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
            }
            records_to_insert.append(record)

        if not records_to_insert:
            raise ValueError("No valid records to insert after processing")

        # Generate dynamic SQL query with ON DUPLICATE KEY UPDATE
        columns = ["date", "ticker", "open", "high", "low", "close"]
        placeholders = [f":{col}" for col in columns]

        # Create UPDATE clause for all columns except date and ticker (which form the unique key)
        update_clause = []
        for col in columns:
            if col not in ["date", "ticker"]:  # Don't update the unique key
                update_clause.append(f"{col} = VALUES({col})")

        sql = f"""
            INSERT INTO stock_prices ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE {', '.join(update_clause)}
        """

        try:
            with self.engine.connect() as connection:
                query = text(sql)
                # Execute batch insert
                connection.execute(query, records_to_insert)
                connection.commit()
                logger.info(
                    f"Successfully upserted {len(records_to_insert)} price records for ticker: {ticker}"
                )
        except Exception as e:
            logger.error(f"Error upserting price data: {e}")
            raise

    def save_options_data_upsert(self, options_data: pd.DataFrame, ticker: str, option_type: str, expiration_date: str) -> None:
        """
        Save options data to the database with upsert functionality.

        Args:
            options_data (pd.DataFrame): DataFrame containing options data
            ticker (str): Stock ticker symbol
            option_type (str): "puts" or "calls"
            expiration_date (str): Expiration date in YYYY-MM-DD format

        Raises:
            ValueError: If required fields are missing or have invalid format
            ConnectionError: If database connection is not established
        """
        if options_data is None or options_data.empty:
            logger.warning(f"No options data to save for {ticker} {option_type} on {expiration_date}")
            return

        # Determine table name based on option type
        table_name = f"put_options" if option_type == "puts" else "call_options"

        # Get actual database columns
        db_columns = self.get_table_columns(table_name)

        # Prepare data for insertion
        records_to_insert = []
        current_date = datetime.now().date()
        
        # First pass: collect all possible fields from all records
        all_fields = set()
        for index, row in options_data.iterrows():
            for yf_key, value in row.items():
                if pd.notna(value):
                    # Convert camelCase keys to snake_case to match database column names
                    db_key = yf_key
                    for camel_case, snake_case in OPTIONS_COLUMN_MAPPING_DICT.items():
                        db_key = db_key.replace(camel_case, snake_case)
                    
                    # Only include fields that exist in the database
                    if db_key in db_columns:
                        all_fields.add(db_key)
        
        # Second pass: create records with consistent field sets
        for index, row in options_data.iterrows():
            # Create record with all possible fields (set to None initially)
            record = {
                "ticker": ticker,
                "expiration_date": expiration_date,
                "created_at": current_date,
            }
            
            # Initialize all possible fields to None
            for field in all_fields:
                record[field] = None
            
            # Map Yahoo Finance column names to database column names
            for yf_key, value in row.items():
                if pd.notna(value):
                    # Convert camelCase keys to snake_case to match database column names
                    db_key = yf_key
                    for camel_case, snake_case in OPTIONS_COLUMN_MAPPING_DICT.items():
                        db_key = db_key.replace(camel_case, snake_case)
                    
                    # Only include fields that exist in the database
                    if db_key in db_columns:
                        # Convert data types appropriately
                        if db_key in ["strike_price", "last_price", "bid", "ask", "change", "percent_change", "implied_volatility"]:
                            record[db_key] = float(value) if value is not None else None
                        elif db_key in ["volume", "open_interest"]:
                            record[db_key] = int(value) if value is not None else None
                        elif db_key == "in_the_money":
                            record[db_key] = bool(value) if value is not None else None
                        elif db_key in ["contract_symbol", "contract_size", "currency"]:
                            record[db_key] = str(value) if value is not None else None
                        elif db_key == "last_trade_date":
                            record[db_key] = value  # Keep as is for datetime
                        else:
                            record[db_key] = value
                    else:
                        logger.debug(f"Skipping field '{db_key}' (original: '{yf_key}') - not in database schema")
            
            # Only include fields that exist in the database
            filtered_record = {k: v for k, v in record.items() if k in db_columns}
            records_to_insert.append(filtered_record)

        if not records_to_insert:
            logger.warning(f"No valid records to insert for {ticker} {option_type} on {expiration_date}")
            return

        # Generate dynamic SQL query with ON DUPLICATE KEY UPDATE
        columns = list(records_to_insert[0].keys())
        placeholders = [f":{col}" for col in columns]

        # Escape column names with backticks to handle reserved keywords
        escaped_columns = [f"`{col}`" for col in columns]

        # Create UPDATE clause for all columns except contract_symbol and created_at (which form the unique key)
        update_clause = []
        for col in columns:
            if col not in ["contract_symbol", "created_at"]:  # Don't update the unique key
                update_clause.append(f"`{col}` = VALUES(`{col}`)")

        sql = f"""
            INSERT INTO {table_name} ({', '.join(escaped_columns)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE {', '.join(update_clause)}
        """

        try:
            with self.engine.connect() as connection:
                query = text(sql)
                # Execute batch insert
                connection.execute(query, records_to_insert)
                connection.commit()
                logger.info(
                    f"Successfully upserted {len(records_to_insert)} {option_type} records for {ticker} on {expiration_date}"
                )
        except Exception as e:
            logger.error(f"Error upserting {option_type} data for {ticker} on {expiration_date}: {e}")
            raise

    def close(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
