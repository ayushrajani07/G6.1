#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Direct data collection script for G6 Platform.
Bypasses the main loop to force immediate data collection.
"""

import os
import sys
import logging
import datetime
from dotenv import load_dotenv

# Configure logging - Force DEBUG level
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Direct data collection entry point."""
    # Load environment variables
    load_dotenv()
    logger.info("===== G6 DIRECT DATA COLLECTOR =====")
    logger.info("Environment variables loaded from .env file")
    
    # Import required modules
    from src.broker.kite_provider import KiteProvider
    from src.storage.csv_sink import CsvSink
    
    # Initialize Kite provider directly
    try:
        logger.info("Initializing Kite provider...")
        api_key = os.environ.get("KITE_API_KEY")
        access_token = os.environ.get("KITE_ACCESS_TOKEN")
        
        if not api_key or not access_token:
            logger.error("KITE_API_KEY or KITE_ACCESS_TOKEN not set in environment")
            return 1
            
        kite_provider = KiteProvider(api_key=api_key, access_token=access_token)
        logger.info("Kite provider initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kite provider: {e}", exc_info=True)
        return 1
    
    # Initialize CSV sink
    csv_sink = CsvSink(base_dir='data/g6_direct_data')
    logger.info(f"CSV sink initialized with base_dir: {csv_sink.base_dir}")
    
    # Get current timestamp
    now = datetime.datetime.now()
    logger.info(f"Current timestamp: {now}")
    
    # Collect NIFTY data
    try:
        index_symbol = "NIFTY"
        logger.info(f"Collecting data for {index_symbol}...")
        
        # Get current index price
        instruments = [("NSE", "NIFTY 50")]
        ltp_data = kite_provider.get_ltp(instruments)
        logger.info(f"LTP data: {ltp_data}")
        
        # Extract LTP and calculate ATM strike
        ltp = 0
        for key, data in ltp_data.items():
            ltp = data.get('last_price', 0)
        
        # Round to nearest 50
        atm_strike = round(ltp / 50) * 50
        logger.info(f"{index_symbol} current price: {ltp}, ATM strike: {atm_strike}")
        
        # Calculate strikes to collect
        strikes = []
        for i in range(1, 6):  # 5 strikes on each side
            strikes.append(atm_strike - (i * 50))  # ITM strikes
        
        strikes.append(atm_strike)  # ATM strike
        
        for i in range(1, 6):
            strikes.append(atm_strike + (i * 50))  # OTM strikes
        
        strikes.sort()
        logger.info(f"Collecting data for strikes: {strikes}")
        
        # Get this week's expiry
        expiry_dates = kite_provider.get_expiry_dates(index_symbol)
        logger.info(f"Available expiry dates: {expiry_dates}")
        
        # Use first expiry if available
        if expiry_dates:
            expiry_date = expiry_dates[0]
            logger.info(f"Using expiry date: {expiry_date}")
            
            # Get option instruments
            instruments = kite_provider.option_instruments(index_symbol, expiry_date, strikes)
            logger.info(f"Found {len(instruments)} option instruments")
            
            if instruments:
                # Log first instrument as sample
                logger.info(f"Sample instrument: {instruments[0]}")
                
                # Convert to dictionary keyed by symbol
                options_data = {}
                for instrument in instruments:
                    symbol = instrument.get('tradingsymbol', '')
                    if symbol:
                        options_data[symbol] = instrument
                
                # Get quotes for these instruments
                if options_data:
                    quote_instruments = []
                    for symbol in options_data.keys():
                        quote_instruments.append(('NFO', symbol))
                    
                    logger.info(f"Getting quotes for {len(quote_instruments)} instruments")
                    quotes = kite_provider.get_quote(quote_instruments)
                    logger.info(f"Retrieved {len(quotes)} quotes")
                    
                    # Update options data with quote information
                    for exchange, symbol in quote_instruments:
                        key = f"{exchange}:{symbol}"
                        if key in quotes:
                            quote_data = quotes[key]
                            if symbol in options_data:
                                for field in ['last_price', 'volume', 'oi', 'depth']:
                                    if field in quote_data:
                                        options_data[symbol][field] = quote_data[field]
                    
                    # Write to CSV
                    logger.info(f"Writing {len(options_data)} records to CSV")
                    csv_sink.write_options_data(index_symbol, expiry_date, options_data, now)
                    
                    # Check if files were created
                    expected_dir = f"{csv_sink.base_dir}/{index_symbol}/{expiry_date}"
                    expected_file = f"{expected_dir}/{now.strftime('%Y-%m-%d')}.csv"
                    
                    logger.info(f"Looking for data file: {expected_file}")
                    if os.path.exists(expected_file):
                        file_size = os.path.getsize(expected_file)
                        logger.info(f"Data file created successfully! Size: {file_size} bytes")
                        
                        # Read back first few lines to verify
                        try:
                            with open(expected_file, 'r') as f:
                                lines = f.readlines()[:5]
                                logger.info(f"File preview: {lines}")
                        except Exception as e:
                            logger.error(f"Error reading file: {e}")
                    else:
                        logger.warning(f"Data file not created! Checking directory...")
                        if os.path.exists(expected_dir):
                            logger.info(f"Directory exists with contents: {os.listdir(expected_dir)}")
                        else:
                            logger.warning(f"Directory does not exist!")
                            # Try to create directory and write a test file
                            try:
                                os.makedirs(expected_dir, exist_ok=True)
                                test_file = f"{expected_dir}/test.txt"
                                with open(test_file, 'w') as f:
                                    f.write("Test file to check write permissions")
                                logger.info(f"Test file created successfully at {test_file}")
                            except Exception as e:
                                logger.error(f"Error creating test file: {e}", exc_info=True)
                
            else:
                logger.warning(f"No instruments found for {index_symbol} with expiry {expiry_date}")
        else:
            logger.warning(f"No expiry dates found for {index_symbol}")
    
    except Exception as e:
        logger.error(f"Error collecting data: {e}", exc_info=True)
    
    logger.info("===== DIRECT COLLECTION COMPLETED =====")
    return 0

if __name__ == "__main__":
    sys.exit(main())