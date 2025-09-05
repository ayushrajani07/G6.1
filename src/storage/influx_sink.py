#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
InfluxDB sink for G6 Options Trading Platform.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class InfluxSink:
    """InfluxDB storage sink for G6 data."""
    
    def __init__(self, url='http://localhost:8086', token='', org='', bucket='g6_data'):
        """
        Initialize InfluxDB sink.
        
        Args:
            url: InfluxDB server URL
            token: InfluxDB API token
            org: InfluxDB organization
            bucket: InfluxDB bucket name
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = None
        
        try:
            from influxdb_client import InfluxDBClient
            
            # Initialize client
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.write_api = self.client.write_api()
            
            logger.info(f"InfluxDB sink initialized with bucket: {bucket}")
        except ImportError:
            logger.warning("influxdb_client package not installed, using dummy implementation")
        except Exception as e:
            logger.error(f"Error initializing InfluxDB client: {e}")
    
    def close(self):
        """Close InfluxDB client."""
        if self.client:
            try:
                self.client.close()
                logger.info("InfluxDB client closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB client: {e}")
    
    def write_options_data(self, index_symbol, expiry_date, options_data, timestamp=None):
        """
        Write options data to InfluxDB.
        
        Args:
            index_symbol: Index symbol
            expiry_date: Expiry date string or date object
            options_data: Dictionary of options data
            timestamp: Timestamp for the data (default: current time)
        """
        if not self.client or not self.write_api:
            return
        
        # Use current time if timestamp not provided
        if timestamp is None:
            timestamp = datetime.now()
        
        # Convert expiry_date to string if it's a date object
        if hasattr(expiry_date, 'strftime'):
            expiry_str = expiry_date.strftime('%Y-%m-%d')
        else:
            expiry_str = str(expiry_date)
        
        try:
            # Check if we have data
            if not options_data:
                logger.warning(f"No options data to write for {index_symbol} {expiry_date}")
                return
            
            # Create points and write to InfluxDB
            from influxdb_client import Point
            
            points = []
            for symbol, data in options_data.items():
                # Extract data
                strike = data.get('strike', 0)
                opt_type = data.get('type', '')  # 'CE' or 'PE'
                ltp = data.get('last_price', 0)
                oi = data.get('oi', 0)
                volume = data.get('volume', 0)
                iv = data.get('iv', 0)
                
                # Create point
                point = Point("option_data") \
                    .tag("index", index_symbol) \
                    .tag("expiry", expiry_str) \
                    .tag("symbol", symbol) \
                    .tag("type", opt_type) \
                    .tag("strike", str(strike)) \
                    .field("price", float(ltp)) \
                    .field("oi", float(oi)) \
                    .field("volume", float(volume)) \
                    .field("iv", float(iv)) \
                    .time(timestamp)
                
                points.append(point)
            
            # Write points
            self.write_api.write(bucket=self.bucket, record=points)
            logger.info(f"Wrote {len(points)} data points to InfluxDB")
            
        except Exception as e:
            logger.error(f"Error writing options data to InfluxDB: {e}")

class NullInfluxSink:
    """Null implementation of InfluxDB sink that does nothing."""
    
    def __init__(self):
        """Initialize null sink."""
        pass
    
    def close(self):
        """Close sink (no-op)."""
        pass
    
    def write_options_data(self, index_symbol, expiry_date, options_data, timestamp=None):
        """Write options data (no-op)."""
        pass