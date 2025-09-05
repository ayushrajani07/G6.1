#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV Storage Sink for G6 Platform.
"""

import os
import csv
import json
import datetime
import logging
from typing import Dict, Any, List

class CsvSink:
    """CSV storage sink for options data."""
    
    def __init__(self, base_dir="data/g6_data"):
        """
        Initialize CSV sink.
        
        Args:
            base_dir: Base directory for CSV files
        """
        self.base_dir = base_dir
        # Create base directory if it doesn't exist
        os.makedirs(base_dir, exist_ok=True)
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"CsvSink initialized with base_dir: {base_dir}")
    
    def _clean_for_json(self, obj):
        """Convert non-serializable objects for JSON."""
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return str(obj)
    
    def write_options_data(self, index, expiry, options_data, timestamp, index_price=None, index_ohlc=None):
        """
        Write options data to CSV file.
        
        Args:
            index: Index symbol (e.g., 'NIFTY')
            expiry: Expiry date
            options_data: Dict of options data keyed by option symbol
            timestamp: Timestamp of data collection
            index_price: Current index price (if available)
            index_ohlc: Index OHLC data (if available)
        """
        self.logger.debug(f"write_options_data called with index={index}, expiry={expiry}")
        
        # Create directory structure if it doesn't exist
        self.logger.info(f"Options data received for {index} expiry {expiry}: {len(options_data)} instruments")
        
        # Determine expiry tag based on expiry date
        today = datetime.date.today()
        exp_date = expiry if isinstance(expiry, datetime.date) else datetime.datetime.strptime(str(expiry), '%Y-%m-%d').date()
        
        # Calculate days to expiry
        days_to_expiry = (exp_date - today).days
        
        # Determine expiry tag
        if days_to_expiry <= 7:
            expiry_code = "this_week"
        elif days_to_expiry <= 14:
            expiry_code = "next_week"
        elif exp_date.month == today.month:
            expiry_code = "this_month"
        else:
            expiry_code = "next_month"
            
        # Get or calculate index price
        if not index_price:
            # Use a default value based on index if nothing else is available
            defaults = {
                "NIFTY": 24800,
                "BANKNIFTY": 54200,
                "FINNIFTY": 25900,
                "MIDCPNIFTY": 22000,
                "SENSEX": 80900
            }
            index_price = defaults.get(index, 0)
            
            # Try to find index price in the first option's metadata
            for _, data in options_data.items():
                if 'index_price' in data:
                    index_price = float(data['index_price'])
                    break
        
        # Calculate ATM strike (round to nearest step size)
        if index in ["BANKNIFTY", "SENSEX"]:
            # Round to nearest 100
            atm_strike = round(float(index_price) / 100) * 100
        else:
            # Round to nearest 50
            atm_strike = round(float(index_price) / 50) * 50
            
        self.logger.info(f"Index {index} price: {index_price}, ATM strike: {atm_strike}")
        
        # Calculate PCR for this expiry
        put_oi = sum(float(data.get('oi', 0)) for data in options_data.values() 
                    if data.get('instrument_type') == 'PE')
        call_oi = sum(float(data.get('oi', 0)) for data in options_data.values() 
                    if data.get('instrument_type') == 'CE')
        pcr = put_oi / call_oi if call_oi > 0 else 0
        
        # Calculate day width if OHLC data is available
        day_width = 0
        if index_ohlc and 'high' in index_ohlc and 'low' in index_ohlc:
            day_width = float(index_ohlc.get('high', 0)) - float(index_ohlc.get('low', 0))
        
        # Update the overview file (segregated by index)
        self._write_overview_file(index, expiry_code, pcr, day_width, timestamp, index_price)
        
        # Group options by strike
        strike_data = {}
        for symbol, data in options_data.items():
            strike = float(data.get('strike', 0))
            opt_type = data.get('instrument_type', '')
            
            if strike not in strike_data:
                strike_data[strike] = {'CE': None, 'PE': None}
                
            strike_data[strike][opt_type] = data
            strike_data[strike][f"{opt_type}_symbol"] = symbol
        
        # Create expiry-specific directory
        expiry_dir = os.path.join(self.base_dir, index, expiry_code)
        os.makedirs(expiry_dir, exist_ok=True)
        
        # Create debug file
        debug_file = os.path.join(expiry_dir, f"{timestamp.strftime('%Y-%m-%d')}_debug.json")
        
        # Format timestamp for records - use actual collection time
        ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Round seconds to nearest 30 seconds (00 or 30)
        second = timestamp.second
        microsecond = timestamp.microsecond
        minute = timestamp.minute
        hour = timestamp.hour
        
        # Handle seconds rounding first
        if second % 30 < 15:
            # Round down to 00 or 30
            rounded_second = (second // 30) * 30
            rounded_timestamp = timestamp.replace(second=rounded_second, microsecond=0)
        else:
            # Round up to 30 or next minute
            rounded_second = ((second // 30) + 1) * 30
            if rounded_second == 60:
                rounded_second = 0
                minute += 1
                # Handle minute overflow
                if minute == 60:
                    minute = 0
                    hour += 1
                    # Handle hour overflow
                    if hour == 24:
                        hour = 0
                        # Day would change, but we don't handle that here
            
            rounded_timestamp = timestamp.replace(hour=hour, minute=minute, second=rounded_second, microsecond=0)
        
        # Format the rounded timestamp as shown in the Excel file
        ts_str_rounded = rounded_timestamp.strftime('%d-%m-%Y %H:%M:%S')
        
        # Process each strike and write to offset directory
        for strike, data in strike_data.items():
            offset = int(strike - atm_strike)
            
            # Format offset for directory name
            if offset > 0:
                offset_dir = f"+{offset}"
            else:
                offset_dir = f"{offset}"
            
            # Create offset directory
            option_dir = os.path.join(self.base_dir, index, expiry_code, offset_dir)
            os.makedirs(option_dir, exist_ok=True)
            
            # Create option CSV file
            option_file = os.path.join(option_dir, f"{timestamp.strftime('%Y-%m-%d')}.csv")
            
            # Check if file exists
            file_exists = os.path.isfile(option_file)
            
            # Extract call and put data
            call_data = data.get('CE', {})
            put_data = data.get('PE', {})
            
            # Calculate offset_price (ATM + offset)
            offset_price = atm_strike + offset
            
            # Get values or defaults for call options
            ce_price = float(call_data.get('last_price', 0)) if call_data else 0
            ce_avg = float(call_data.get('avg_price', 0)) if call_data else 0
            ce_vol = int(call_data.get('volume', 0)) if call_data else 0
            ce_oi = int(call_data.get('oi', 0)) if call_data else 0
            ce_iv = float(call_data.get('iv', 0)) if call_data else 0
            ce_delta = float(call_data.get('delta', 0)) if call_data else 0
            ce_theta = float(call_data.get('theta', 0)) if call_data else 0
            ce_vega = float(call_data.get('vega', 0)) if call_data else 0
            ce_gamma = float(call_data.get('gamma', 0)) if call_data else 0
            
            # Get values or defaults for put options
            pe_price = float(put_data.get('last_price', 0)) if put_data else 0
            pe_avg = float(put_data.get('avg_price', 0)) if put_data else 0
            pe_vol = int(put_data.get('volume', 0)) if put_data else 0
            pe_oi = int(put_data.get('oi', 0)) if put_data else 0
            pe_iv = float(put_data.get('iv', 0)) if put_data else 0
            pe_delta = float(put_data.get('delta', 0)) if put_data else 0
            pe_theta = float(put_data.get('theta', 0)) if put_data else 0
            pe_vega = float(put_data.get('vega', 0)) if put_data else 0
            pe_gamma = float(put_data.get('gamma', 0)) if put_data else 0
            
            # Calculate total premium
            tp_price = ce_price + pe_price
            avg_tp = ce_avg + pe_avg
            
            # Write option file with the new format
            with open(option_file, 'a' if file_exists else 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header if new file
                if not file_exists:
                    writer.writerow([
                        'timestamp', 'index', 'expiry_tag', 'offset', 'strike', 'atm', 'offset_price',
                        'ce', 'pe', 'tp', 'avg_ce', 'avg_pe', 'avg_tp',
                        'ce_vol', 'pe_vol', 'ce_oi', 'pe_oi',
                        'ce_iv', 'pe_iv', 'ce_delta', 'pe_delta', 'ce_theta', 'pe_theta',
                        'ce_vega', 'pe_vega', 'ce_gamma', 'pe_gamma'
                    ])
                
                # Write data row with properly rounded timestamp
                writer.writerow([
                    ts_str_rounded, index, expiry_code, offset, index_price, atm_strike, offset_price,
                    ce_price, pe_price, tp_price, ce_avg, pe_avg, avg_tp,
                    ce_vol, pe_vol, ce_oi, pe_oi,
                    ce_iv, pe_iv, ce_delta, pe_delta, ce_theta, pe_theta,
                    ce_vega, pe_vega, ce_gamma, pe_gamma
                ])
                
            self.logger.debug(f"Option data written to {option_file}")
        
        # Write debug JSON with all data
        with open(debug_file, 'w') as f:
            json.dump({
                'timestamp': ts_str,
                'index': index,
                'expiry': str(expiry),
                'expiry_code': expiry_code,
                'index_price': index_price,
                'atm_strike': atm_strike,
                'pcr': pcr,
                'day_width': day_width,
                'data_count': len(options_data),
                'rounded_timestamp': ts_str_rounded
            }, f, indent=2)
        
        self.logger.info(f"Data written for {index} {expiry_code}")
    
    def _write_overview_file(self, index, expiry_code, pcr, day_width, timestamp, index_price):
        """Write overview file for a specific index."""
        # Create overview directory for this index
        overview_dir = os.path.join(self.base_dir, "overview", index)
        os.makedirs(overview_dir, exist_ok=True)
        
        # Determine file path
        overview_file = os.path.join(overview_dir, f"{timestamp.strftime('%Y-%m-%d')}.csv")
        
        # Check if file exists
        file_exists = os.path.isfile(overview_file)
        
        # Format timestamp - use actual collection time with proper rounding
        second = timestamp.second
        if second % 30 < 15:
            rounded_second = (second // 30) * 30
            rounded_timestamp = timestamp.replace(second=rounded_second, microsecond=0)
        else:
            rounded_second = ((second // 30) + 1) * 30
            if rounded_second == 60:
                rounded_second = 0
                rounded_timestamp = timestamp.replace(second=rounded_second, microsecond=0)
                rounded_timestamp = rounded_timestamp + datetime.timedelta(minutes=1)
            else:
                rounded_timestamp = timestamp.replace(second=rounded_second, microsecond=0)
                
        ts_str = rounded_timestamp.strftime('%d-%m-%Y %H:%M:%S')
        
        # Read existing data to update PCR values
        pcr_values = {
            'pcr_this_week': 0,
            'pcr_next_week': 0,
            'pcr_this_month': 0,
            'pcr_next_month': 0
        }
        
        # Update the specific expiry code's PCR
        pcr_values[f'pcr_{expiry_code}'] = pcr
        
        # Write to CSV
        with open(overview_file, 'a' if file_exists else 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header if new file
            if not file_exists:
                writer.writerow([
                    'timestamp', 'index', 
                    'pcr_this_week', 'pcr_next_week', 'pcr_this_month', 'pcr_next_month',
                    'day_width'
                ])
            
            # Write data row
            writer.writerow([
                ts_str, index,
                pcr_values['pcr_this_week'], pcr_values['pcr_next_week'],
                pcr_values['pcr_this_month'], pcr_values['pcr_next_month'],
                day_width
            ])
        
        self.logger.info(f"Overview data written to {overview_file}")
    
    def read_options_overview(self, index, date=None):
        """
        Read overview data from CSV file.
        
        Args:
            index: Index symbol (e.g., 'NIFTY')
            date: Date to read data for (defaults to today)
            
        Returns:
            Dict of overview data by timestamp
        """
        # Use today's date if not specified
        if date is None:
            date = datetime.date.today()
            
        # Format date as string
        date_str = date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else date
        
        # Build file path
        overview_file = os.path.join(self.base_dir, "overview", index, f"{date_str}.csv")
        
        # Check if file exists
        if not os.path.exists(overview_file):
            self.logger.warning(f"No overview file found for {index} on {date_str}")
            return {}
        
        # Read CSV file
        overview_data = {}
        with open(overview_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                timestamp = row['timestamp']
                overview_data[timestamp] = {
                    'index': row['index'],
                    'pcr_this_week': float(row.get('pcr_this_week', 0)),
                    'pcr_next_week': float(row.get('pcr_next_week', 0)),
                    'pcr_this_month': float(row.get('pcr_this_month', 0)),
                    'pcr_next_month': float(row.get('pcr_next_month', 0)),
                    'day_width': float(row.get('day_width', 0))
                }
        
        self.logger.info(f"Read overview data from {overview_file}")
        return overview_data
        
    def read_option_data(self, index, expiry_code, offset, date=None):
        """
        Read option data for a specific offset.
        
        Args:
            index: Index symbol (e.g., 'NIFTY')
            expiry_code: Expiry code (e.g., 'this_week')
            offset: Strike offset from ATM (e.g., +50, -100)
            date: Date to read data for (defaults to today)
            
        Returns:
            List of option data points
        """
        # Use today's date if not specified
        if date is None:
            date = datetime.date.today()
            
        # Format date as string
        date_str = date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else date
        
        # Format offset for directory name
        if int(offset) > 0:
            offset_dir = f"+{int(offset)}"
        else:
            offset_dir = f"{int(offset)}"
            
        # Build file path
        option_file = os.path.join(self.base_dir, index, expiry_code, offset_dir, f"{date_str}.csv")
        
        # Check if file exists
        if not os.path.exists(option_file):
            self.logger.warning(f"No option file found for {index} {expiry_code} offset {offset} on {date_str}")
            return []
        
        # Read CSV file
        option_data = []
        with open(option_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                option_data.append({
                    'timestamp': row['timestamp'],
                    'index': row['index'],
                    'expiry_tag': row['expiry_tag'],
                    'offset': int(row['offset']),
                    'strike': float(row['strike']),
                    'atm': float(row['atm']),
                    'offset_price': float(row['offset_price']),
                    'ce': float(row['ce']),
                    'pe': float(row['pe']),
                    'tp': float(row['tp']),
                    'avg_ce': float(row['avg_ce']),
                    'avg_pe': float(row['avg_pe']),
                    'avg_tp': float(row['avg_tp']),
                    'ce_vol': int(row['ce_vol']),
                    'pe_vol': int(row['pe_vol']),
                    'ce_oi': int(row['ce_oi']),
                    'pe_oi': int(row['pe_oi']),
                    'ce_iv': float(row['ce_iv']),
                    'pe_iv': float(row['pe_iv']),
                    'ce_delta': float(row['ce_delta']),
                    'pe_delta': float(row['pe_delta']),
                    'ce_theta': float(row['ce_theta']),
                    'pe_theta': float(row['pe_theta']),
                    'ce_vega': float(row['ce_vega']),
                    'pe_vega': float(row['pe_vega']),
                    'ce_gamma': float(row['ce_gamma']),
                    'pe_gamma': float(row['pe_gamma'])
                })
        
        self.logger.info(f"Read {len(option_data)} option records from {option_file}")
        return option_data
        
    # Add this method to the CsvSink class

    def check_health(self):
        """
        Check if the CSV sink is healthy.
        
        Returns:
            Dict with health status information
        """
        try:
            # Check if base directory exists and is writable
            if not os.path.exists(self.base_dir):
                try:
                    os.makedirs(self.base_dir, exist_ok=True)
                except Exception as e:
                    return {
                        'status': 'unhealthy',
                        'message': f"Cannot create data directory: {str(e)}"
                    }
            
            # Check if we can write a test file
            test_file = os.path.join(self.base_dir, ".health_check")
            try:
                with open(test_file, 'w') as f:
                    f.write("Health check")
                os.remove(test_file)
            except Exception as e:
                return {
                    'status': 'unhealthy',
                    'message': f"Cannot write to data directory: {str(e)}"
                }
            
            # All checks passed
            return {
                'status': 'healthy',
                'message': 'CSV sink is healthy'
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f"Health check failed: {str(e)}"
            }