#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to validate data collection for all supported indices.
"""

import os
import sys
import logging
import datetime
from dotenv import load_dotenv

# Configure logging with colorful formatting
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - \033[1;34m%(levelname)s\033[0m - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main test function."""
    load_dotenv()
    logger.info("\033[1;32m===== TESTING ALL INDICES =====\033[0m")
    
    from src.broker.kite_provider import KiteProvider
    from src.storage.csv_sink import CsvSink
    
    # Initialize provider and storage
    kite_provider = KiteProvider.from_env()
    csv_sink = CsvSink(base_dir='data/g6_indices_test')
    
    # Test all supported indices
    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]
    
    for index in indices:
        logger.info(f"\033[1;33m\n{'=' * 30} TESTING {index} {'=' * 30}\033[0m")
        
        try:
            # Get ATM strike
            atm_strike = kite_provider.get_atm_strike(index)
            
            # Get expiry dates
            expiry_dates = kite_provider.get_expiry_dates(index)
            
            if not expiry_dates:
                logger.warning(f"No expiry dates found for {index}")
                continue
                
            # Take first expiry
            expiry = expiry_dates[0]
            
            # Calculate strikes to collect (5 ITM, ATM, 5 OTM)
            strikes = []
            step = 100 if index == "BANKNIFTY" or index == "SENSEX" else 50
            
            for i in range(-5, 6):
                strikes.append(atm_strike + (i * step))
                
            # Get option instruments
            instruments = kite_provider.option_instruments(index, expiry, strikes)
            
            if not instruments:
                logger.warning(f"No option instruments found for {index}")
                continue
                
            # Convert to dictionary
            options_data = {}
            for instrument in instruments:
                symbol = instrument.get('tradingsymbol', '')
                if symbol:
                    options_data[symbol] = instrument
            
            # Get quotes for first 5 instruments (limit API requests)
            if options_data:
                sample_instruments = list(options_data.keys())[:5]
                quote_instruments = [('NFO', symbol) for symbol in sample_instruments]
                
                logger.info(f"Getting quotes for {len(quote_instruments)} sample instruments")
                quotes = kite_provider.get_quote(quote_instruments)
                
                # Update options data with quote information
                for exchange, symbol in quote_instruments:
                    key = f"{exchange}:{symbol}"
                    if key in quotes:
                        quote_data = quotes[key]
                        if symbol in options_data:
                            for field in ['last_price', 'volume', 'oi', 'depth']:
                                if field in quote_data:
                                    options_data[symbol][field] = quote_data[field]
                
                # Write test data
                timestamp = datetime.datetime.now()
                logger.info(f"Writing sample data for {index}")
                csv_sink.write_options_data(index, expiry, options_data, timestamp)
                
                # Check if file was created
                expected_dir = os.path.join(csv_sink.base_dir, index, str(expiry))
                expected_file = os.path.join(expected_dir, f"{timestamp.strftime('%Y-%m-%d')}.csv")
                
                if os.path.exists(expected_file):
                    file_size = os.path.getsize(expected_file)
                    logger.info(f"Data file created: {expected_file} ({file_size} bytes)")
                else:
                    logger.warning(f"Data file not created: {expected_file}")
            
        except Exception as e:
            logger.error(f"Error testing {index}: {e}", exc_info=True)
    
    logger.info("\033[1;32m\n===== TESTING COMPLETED =====\033[0m")
    return 0

if __name__ == "__main__":
    sys.exit(main())