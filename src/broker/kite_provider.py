#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kite Provider for G6 Options Trading Platform.
"""

import os
import sys
import logging
import datetime
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
import json

logger = logging.getLogger(__name__)

# Indices and their exchange pools
POOL_FOR = {
    "NIFTY": "NFO",
    "BANKNIFTY": "NFO", 
    "FINNIFTY": "NFO",
    "MIDCPNIFTY": "NFO",  # Added MidcpNifty
    "SENSEX": "BFO",
}

# Index name mappings for LTP queries
INDEX_MAPPING = {
    "NIFTY": ("NSE", "NIFTY 50"),
    "BANKNIFTY": ("NSE", "NIFTY BANK"),
    "FINNIFTY": ("NSE", "NIFTY FIN SERVICE"),
    "MIDCPNIFTY": ("NSE", "NIFTY MIDCAP SELECT"), 
    "SENSEX": ("BSE", "SENSEX"),
}

class KiteProvider:
    """Real Kite API provider."""
    
    @classmethod
    def from_env(cls):
        """Create KiteProvider from environment variables."""
        api_key = os.environ.get("KITE_API_KEY")
        access_token = os.environ.get("KITE_ACCESS_TOKEN")
        
        if not api_key or not access_token:
            raise ValueError("KITE_API_KEY or KITE_ACCESS_TOKEN not set")
        
        return cls(api_key=api_key, access_token=access_token)
    
    def __init__(self, api_key=None, access_token=None):
        """Initialize KiteProvider."""
        self.api_key = api_key
        self.access_token = access_token
        self.kite = None
        self._instruments_cache = {}  # Cache for instruments
        self._expiry_dates_cache = {}  # Cache for expiry dates
        
        if not api_key or not access_token:
            logger.warning("API key or access token missing, trying to load from environment")
            self.api_key = os.environ.get("KITE_API_KEY")
            self.access_token = os.environ.get("KITE_ACCESS_TOKEN")
        
        self.initialize_kite()
        # Log only the first few chars of API key for security
        safe_api_key = f"{self.api_key[:4]}...{self.api_key[-4:]}" if self.api_key else "None"
        logger.info(f"KiteProvider initialized with API key: {safe_api_key}")
    
    def initialize_kite(self):
        """Initialize Kite Connect client."""
        try:
            from kiteconnect import KiteConnect
            
            # Initialize Kite
            self.kite = KiteConnect(api_key=self.api_key)
            
            # Set access token
            if self.access_token:
                self.kite.set_access_token(self.access_token)
                
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kite Connect: {e}")
            return False
    
    def close(self):
        """Clean up resources."""
        logger.info("KiteProvider closed")
    
    def get_quote(self, instruments):
        """
        Get current quotes for instruments.
        """
        try:
            # Format instruments
            formatted_instruments = []
            for exchange, tradingsymbol in instruments:
                formatted_instruments.append(f"{exchange}:{tradingsymbol}")
            
            # Re-initialize Kite to ensure fresh token
            self.initialize_kite()
            
            # Get quotes
            quotes = self.kite.quote(formatted_instruments)
            return quotes
        except Exception as e:
            logger.error(f"Failed to get quotes: {e}")
            return {}
    
    def get_instruments(self, exchange=None):
        """Get all tradeable instruments."""
        try:
            # Check cache first
            cache_key = exchange or "all"
            if cache_key in self._instruments_cache:
                logger.debug(f"Using cached instruments for {exchange}")
                return self._instruments_cache[cache_key]
            
            # Re-initialize Kite to ensure fresh token
            self.initialize_kite()
            
            instruments = self.kite.instruments(exchange)
            
            # Cache the results
            self._instruments_cache[cache_key] = instruments
            logger.info(f"Retrieved {len(instruments)} {exchange} instruments (cached)")
            
            return instruments
        except Exception as e:
            logger.error(f"Failed to get instruments: {e}")
            return []
    
    def get_ltp(self, instruments):
        """Get last traded price for instruments."""
        try:
            # Format instruments
            formatted_instruments = []
            for exchange, tradingsymbol in instruments:
                formatted_instruments.append(f"{exchange}:{tradingsymbol}")
            
            # Re-initialize Kite to ensure fresh token
            self.initialize_kite()
            
            # Get LTP
            ltp = self.kite.ltp(formatted_instruments)
            return ltp
        except Exception as e:
            logger.error(f"Failed to get LTP: {e}")
            return {}
    
    def get_expiry_dates(self, index_symbol):
        """
        Get all available expiry dates for an index.
        """
        try:
            # Check cache first
            if index_symbol in self._expiry_dates_cache:
                logger.debug(f"Using cached expiry dates for {index_symbol}")
                return self._expiry_dates_cache[index_symbol]
            
            # Get ATM strike for the index
            atm_strike = self.get_atm_strike(index_symbol)
            
            # Get instruments based on the exchange pool
            exchange_pool = POOL_FOR.get(index_symbol, "NFO")
            instruments = self.get_instruments(exchange_pool)
            
            # Filter for options that match the index and are near the ATM strike
            opts = [
                inst for inst in instruments
                if str(inst.get("segment", "")).endswith("-OPT")  # Is an option
                and abs(float(inst.get("strike", 0)) - atm_strike) <= 500  # Near ATM
                and index_symbol in str(inst.get("tradingsymbol", ""))  # Matches index symbol
            ]
            
            # Parse and dedupe expiries
            today = datetime.date.today()
            expiry_dates = set()
            
            for opt in opts:
                expiry = opt.get("expiry")
                
                # Handle datetime.date object
                if isinstance(expiry, datetime.date):
                    if expiry >= today:
                        expiry_dates.add(expiry)
                # Handle string format
                elif isinstance(expiry, str):
                    try:
                        expiry_date = datetime.datetime.strptime(expiry, '%Y-%m-%d').date()
                        if expiry_date >= today:
                            expiry_dates.add(expiry_date)
                    except ValueError:
                        pass
            
            # Sort dates
            sorted_dates = sorted(list(expiry_dates))
            
            logger.info(f"┌─ Expiry Dates for {index_symbol.upper()} ─" + "─" * 40)
            if sorted_dates:
                # Group by month for better readability
                by_month = {}
                for d in sorted_dates:
                    month_key = f"{d.year}-{d:02d}"
                    if month_key not in by_month:
                        by_month[month_key] = []
                    by_month[month_key].append(d)
                    
                for month, dates in sorted(by_month.items()):
                    logger.info(f"│ {month}: {', '.join(d.strftime('%d') for d in dates)}")
                    
                # Show weekly expiries
                weeklies = sorted_dates[:2] if len(sorted_dates) >= 2 else sorted_dates
                logger.info(f"│ Next expiries: {', '.join(str(d) for d in weeklies)}")
            else:
                logger.info(f"│ No expiry dates found")
            logger.info("└" + "─" * 50)
            
            # Cache the results
            self._expiry_dates_cache[index_symbol] = sorted_dates
            
            if not sorted_dates:
                # Fallback: use current week's Thursday and next week's Thursday
                today = datetime.date.today()
                
                # Find next Thursday (weekday 3)
                days_until_thursday = (3 - today.weekday()) % 7
                if days_until_thursday == 0:
                    days_until_thursday = 7  # If today is Thursday, use next week
                
                this_week = today + datetime.timedelta(days=days_until_thursday)
                next_week = this_week + datetime.timedelta(days=7)
                
                fallback_dates = [this_week, next_week]
                logger.info(f"Using fallback expiry dates for {index_symbol}: {fallback_dates}")
                
                self._expiry_dates_cache[index_symbol] = fallback_dates
                return fallback_dates
                
            return sorted_dates
            
        except Exception as e:
            logger.error(f"Failed to get expiry dates: {e}", exc_info=True)
            
            # Fallback to calculated expiry dates
            today = datetime.date.today()
            days_until_thursday = (3 - today.weekday()) % 7
            this_week = today + datetime.timedelta(days=days_until_thursday)
            next_week = this_week + datetime.timedelta(days=7)
            
            fallback_dates = [this_week, next_week]
            logger.info(f"Using emergency fallback expiry dates for {index_symbol}: {fallback_dates}")
            return fallback_dates
    
    def get_weekly_expiries(self, index_symbol):
        """
        Get weekly expiry dates for an index.
        Returns first two upcoming expiries.
        """
        try:
            # Get all expiry dates
            all_expiries = self.get_expiry_dates(index_symbol)
            
            # Return first two (this week and next week)
            weekly_expiries = all_expiries[:2] if len(all_expiries) >= 2 else all_expiries
            return weekly_expiries
        except Exception as e:
            logger.error(f"Error getting weekly expiries: {e}")
            return []
    
    def get_monthly_expiries(self, index_symbol):
        """
        Get monthly expiry dates for an index.
        Groups expiries by month and returns the last expiry of each month.
        """
        try:
            # Get all expiry dates
            all_expiries = self.get_expiry_dates(index_symbol)
            
            # Group by month
            by_month = {}
            today = datetime.date.today()
            
            for expiry in all_expiries:
                if expiry >= today:
                    month_key = (expiry.year, expiry.month)
                    if month_key not in by_month:
                        by_month[month_key] = []
                    by_month[month_key].append(expiry)
            
            # Get last expiry of each month
            monthly_expiries = []
            for _, expiries in sorted(by_month.items()):
                monthly_expiries.append(max(expiries))
            
            return monthly_expiries
        except Exception as e:
            logger.error(f"Error getting monthly expiries: {e}")
            return []
    
    def resolve_expiry(self, index_symbol, expiry_rule):
        """
        Resolve expiry date based on rule.
        
        Valid rules:
        - this_week: Next weekly expiry (for NIFTY, SENSEX)
        - next_week: Following weekly expiry (for NIFTY, SENSEX)
        - this_month: Current month's expiry (for all indices)
        - next_month: Next month's expiry (for all indices)
        """
        try:
            # For indices that only have monthly expiries
            monthly_only_indices = ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
            
            # Get all expiry dates for this index
            all_expiries = self.get_expiry_dates(index_symbol)
            if not all_expiries:
                logger.warning(f"No expiry dates found for {index_symbol}")
                return self._fallback_expiry(expiry_rule)
                
            # Group expiries by month for monthly identification
            by_month = {}
            today = datetime.date.today()
            
            for expiry in all_expiries:
                if expiry >= today:
                    month_key = (expiry.year, expiry.month)
                    if month_key not in by_month:
                        by_month[month_key] = []
                    by_month[month_key].append(expiry)
            
            # Sort month keys chronologically
            sorted_months = sorted(by_month.keys())
            
            if not sorted_months:
                logger.warning(f"No future expiries found for {index_symbol}")
                return self._fallback_expiry(expiry_rule)
                
            # Get monthly expiries (last expiry of each month)
            monthly_expiries = []
            for month in sorted_months:
                expiries = by_month[month]
                # Monthly expiry is the last expiry of the month
                monthly_expiries.append(max(expiries))
                
            # Handle expiry rules
            if expiry_rule == 'this_week' and index_symbol not in monthly_only_indices:
                # For indices with weekly expiries, return first available expiry
                logger.info(f"Resolved 'this_week' for {index_symbol} to {all_expiries[0]}")
                return all_expiries[0]
                
            elif expiry_rule == 'next_week' and index_symbol not in monthly_only_indices:
                # Return second available expiry if there is one
                if len(all_expiries) >= 2:
                    logger.info(f"Resolved 'next_week' for {index_symbol} to {all_expiries[1]}")
                    return all_expiries[1]
                else:
                    logger.info(f"Only one expiry available, using {all_expiries[0]} for 'next_week'")
                    return all_expiries[0]
                    
            elif expiry_rule == 'this_month':
                # Return the first monthly expiry
                if monthly_expiries:
                    logger.info(f"Resolved 'this_month' for {index_symbol} to {monthly_expiries[0]}")
                    return monthly_expiries[0]
                else:
                    # Fallback to first available expiry
                    logger.info(f"No monthly expiry found, using {all_expiries[0]} for 'this_month'")
                    return all_expiries[0]
                    
            elif expiry_rule == 'next_month':
                # Return the second monthly expiry if available
                if len(monthly_expiries) >= 2:
                    logger.info(f"Resolved 'next_month' for {index_symbol} to {monthly_expiries[1]}")
                    return monthly_expiries[1]
                elif monthly_expiries:
                    # Fallback to first monthly
                    logger.info(f"Only one monthly expiry available, using {monthly_expiries[0]} for 'next_month'")
                    return monthly_expiries[0]
                else:
                    # Last resort: use first available
                    logger.info(f"No monthly expiries found, using {all_expiries[0]} for 'next_month'")
                    return all_expiries[0]
            else:
                # Fallback for unknown rules: use first available expiry
                logger.warning(f"Unknown expiry rule '{expiry_rule}', using first available expiry")
                logger.info(f"Using {all_expiries[0]} for unknown rule '{expiry_rule}'")
                return all_expiries[0]
                
        except Exception as e:
            logger.error(f"Failed to resolve expiry: {e}", exc_info=True)
            return self._fallback_expiry(expiry_rule)
    
    def _fallback_expiry(self, expiry_rule):
        """Calculate fallback expiry when no API data is available."""
        today = datetime.date.today()
        
        # Find next Thursday (weekday 3) for weekly expiry
        days_until_thursday = (3 - today.weekday()) % 7
        if days_until_thursday == 0:  # Today is Thursday
            days_until_thursday = 7   # Use next Thursday
        
        this_week = today + datetime.timedelta(days=days_until_thursday)
        next_week = this_week + datetime.timedelta(days=7)
        
        # Calculate last Thursday of current month for monthly expiry
        if today.month == 12:
            next_month_start = datetime.date(today.year + 1, 1, 1)
        else:
            next_month_start = datetime.date(today.year, today.month + 1, 1)
        
        last_day = next_month_start - datetime.timedelta(days=1)
        days_to_subtract = (last_day.weekday() - 3) % 7
        this_month = last_day - datetime.timedelta(days=days_to_subtract)
        
        # Calculate last Thursday of next month
        if next_month_start.month == 12:
            month_after_next = datetime.date(next_month_start.year + 1, 1, 1)
        else:
            month_after_next = datetime.date(next_month_start.year, next_month_start.month + 1, 1)
        
        last_day_next = month_after_next - datetime.timedelta(days=1)
        days_to_subtract = (last_day_next.weekday() - 3) % 7
        next_month = last_day_next - datetime.timedelta(days=days_to_subtract)
        
        # Select appropriate fallback based on rule
        if expiry_rule == 'this_week':
            logger.info(f"Using fallback this_week expiry: {this_week}")
            return this_week
        elif expiry_rule == 'next_week':
            logger.info(f"Using fallback next_week expiry: {next_week}")
            return next_week
        elif expiry_rule == 'this_month':
            logger.info(f"Using fallback this_month expiry: {this_month}")
            return this_month
        elif expiry_rule == 'next_month':
            logger.info(f"Using fallback next_month expiry: {next_month}")
            return next_month
        else:
            logger.info(f"Using default fallback expiry: {this_week}")
            return this_week
    
    def get_atm_strike(self, index_symbol):
        """
        Get ATM strike for an index.
        """
        logger.debug(f"Getting ATM strike for {index_symbol}")
        
        # Get instrument mapping
        instruments = [INDEX_MAPPING.get(index_symbol, ("NSE", index_symbol))]
        
        # Get LTP
        ltp_data = self.get_ltp(instruments)
        
        if not ltp_data:
            logger.error(f"No LTP data returned for {index_symbol}")
            # Return a default value as fallback
            return 20000 if index_symbol == "BANKNIFTY" else 22000
        
        # Extract LTP
        for key, data in ltp_data.items():
            ltp = data.get('last_price', 0)
            
            # Round to appropriate strike
            if index_symbol == "BANKNIFTY" or index_symbol == "SENSEX":
                # Round to nearest 100
                atm_strike = round(ltp / 100) * 100
            else:
                # Round to nearest 50
                atm_strike = round(ltp / 50) * 50
            
            logger.info(f"LTP for {index_symbol}: {ltp}")
            logger.info(f"ATM strike for {index_symbol}: {atm_strike}")
            return atm_strike
        
        logger.error(f"Could not determine ATM strike for {index_symbol}")
        return 20000 if index_symbol == "BANKNIFTY" else 22000
    
    def option_instruments(self, index_symbol, expiry_date, strikes):
        """
        Get option instruments for specific expiry and strikes.
        """
        try:
            # Convert expiry_date to string format YYYY-MM-DD for comparison
            if hasattr(expiry_date, 'strftime'):
                expiry_str = expiry_date.strftime('%Y-%m-%d')
                expiry_obj = expiry_date
            else:
                # Try to parse string to date
                try:
                    expiry_obj = datetime.datetime.strptime(str(expiry_date), '%Y-%m-%d').date()
                    expiry_str = str(expiry_date)
                except:
                    logger.error(f"Could not parse expiry date: {expiry_date}")
                    expiry_obj = datetime.date.today()
                    expiry_str = expiry_obj.strftime('%Y-%m-%d')
            
            # Determine the appropriate exchange
            exchange_pool = POOL_FOR.get(index_symbol, "NFO")
            
            # Get instruments
            instruments = self.get_instruments(exchange_pool)
            logger.info(f"Searching for {index_symbol} options (expiry: {expiry_date}, exchange: {exchange_pool})")
            
            # Filter for matching instruments
            matching_instruments = []
            
            for instrument in instruments:
                # Check if it's a CE or PE option
                is_option = (instrument.get('instrument_type') == 'CE' or 
                             instrument.get('instrument_type') == 'PE')
                
                # Check if symbol matches our index
                tradingsymbol = instrument.get('tradingsymbol', '')
                symbol_matches = index_symbol in tradingsymbol
                
                # Check expiry match - handle both date objects and strings
                instrument_expiry = instrument.get('expiry')
                expiry_matches = False
                
                if isinstance(instrument_expiry, datetime.date):
                    expiry_matches = instrument_expiry == expiry_obj
                elif isinstance(instrument_expiry, str):
                    expiry_matches = instrument_expiry == expiry_str
                
                # Check if strike is in our list
                strike = float(instrument.get('strike', 0))
                strike_matches = any(abs(strike - s) < 0.01 for s in strikes)
                
                if is_option and symbol_matches and expiry_matches and strike_matches:
                    matching_instruments.append(instrument)
            
            # Group by strike and type for better reporting
            strikes_summary = {}
            for inst in matching_instruments:
                strike = float(inst.get('strike', 0))
                opt_type = inst.get('instrument_type', '')
                
                if strike not in strikes_summary:
                    strikes_summary[strike] = {'CE': 0, 'PE': 0}
                
                strikes_summary[strike][opt_type] += 1
            
            # Log summary
            logger.info(f"┌─ Options for {index_symbol} (Expiry: {expiry_date}) ─" + "─" * 30)
            logger.info(f"│ Found {len(matching_instruments)} matching instruments")
            
            if strikes_summary:
                logger.info("│ Strike    CE  PE")
                logger.info("│ " + "─" * 15)
                for strike in sorted(strikes_summary.keys()):
                    ce_count = strikes_summary[strike]['CE']
                    pe_count = strikes_summary[strike]['PE']
                    logger.info(f"│ {strike:<8.1f} {ce_count:>2}  {pe_count:>2}")
            logger.info("└" + "─" * 50)
            
            return matching_instruments
        
        except Exception as e:
            logger.error(f"Failed to get option instruments: {e}", exc_info=True)
            return []
    
    # Add alias for compatibility
    def get_option_instruments(self, index_symbol, expiry_date, strikes):
        """Alias for option_instruments."""
        return self.option_instruments(index_symbol, expiry_date, strikes)
        
        
class DummyKiteProvider:
    """Dummy Kite provider for testing and fallback purposes."""
    
    def __init__(self):
        """Initialize DummyKiteProvider."""
        self.current_time = datetime.datetime.now()
        logger.info("DummyKiteProvider initialized")
    
    def close(self):
        """Clean up resources."""
        logger.info("DummyKiteProvider closed")
    
    def get_quote(self, instruments):
        """Get current quotes for instruments."""
        quotes = {}
        
        for exchange, tradingsymbol in instruments:
            # Generate synthetic data
            strike = 0
            opt_type = ""
            
            # Try to extract strike and type from tradingsymbol
            if "CE" in tradingsymbol:
                opt_type = "CE"
                strike_str = tradingsymbol.split("CE")[0][-5:]
                try:
                    strike = float(strike_str)
                except:
                    pass
            elif "PE" in tradingsymbol:
                opt_type = "PE"
                strike_str = tradingsymbol.split("PE")[0][-5:]
                try:
                    strike = float(strike_str)
                except:
                    pass
            
            # Generate price based on option type and strike
            base_price = 100.0
            if strike > 0:
                if opt_type == "CE":
                    base_price = max(0, 24800 - strike + 200)
                else:  # PE
                    base_price = max(0, strike - 24800 + 200)
            
            quotes[f"{exchange}:{tradingsymbol}"] = {
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "last_price": base_price,
                "average_price": base_price * 1.02,  # Slightly higher than last price
                "volume": 100000,
                "oi": 50000,
                "timestamp": self.current_time.isoformat(),
                "depth": {
                    "buy": [
                        {"price": base_price * 0.99, "quantity": 100, "orders": 10},
                        {"price": base_price * 0.98, "quantity": 200, "orders": 20},
                    ],
                    "sell": [
                        {"price": base_price * 1.01, "quantity": 100, "orders": 10},
                        {"price": base_price * 1.02, "quantity": 200, "orders": 20},
                    ]
                }
            }
        
        return quotes
    
    def get_instruments(self, exchange=None):
        """Get dummy instruments."""
        if exchange == "NFO":
            return [
                {
                    "instrument_token": 1,
                    "exchange_token": "1",
                    "tradingsymbol": "NIFTY25SEP24800CE",
                    "name": "NIFTY",
                    "last_price": 100,
                    "expiry": datetime.date(2025, 9, 30),
                    "strike": 24800,
                    "tick_size": 0.05,
                    "lot_size": 50,
                    "instrument_type": "CE",
                    "segment": "NFO-OPT",
                    "exchange": "NFO"
                },
                {
                    "instrument_token": 2,
                    "exchange_token": "2",
                    "tradingsymbol": "NIFTY25SEP24800PE",
                    "name": "NIFTY",
                    "last_price": 100,
                    "expiry": datetime.date(2025, 9, 30),
                    "strike": 24800,
                    "tick_size": 0.05,
                    "lot_size": 50,
                    "instrument_type": "PE",
                    "segment": "NFO-OPT",
                    "exchange": "NFO"
                },
                {
                    "instrument_token": 3,
                    "exchange_token": "3",
                    "tradingsymbol": "BANKNIFTY25SEP54000CE",
                    "name": "BANKNIFTY",
                    "last_price": 100,
                    "expiry": datetime.date(2025, 9, 30),
                    "strike": 54000,
                    "tick_size": 0.05,
                    "lot_size": 25,
                    "instrument_type": "CE",
                    "segment": "NFO-OPT",
                    "exchange": "NFO"
                }
            ]
        return []
    
    def get_ltp(self, instruments):
        """Get last traded price for instruments."""
        ltp_data = {}
        
        for exchange, tradingsymbol in instruments:
            # Generate LTP based on index
            if "NIFTY 50" in tradingsymbol:
                price = 24800.0
            elif "NIFTY BANK" in tradingsymbol:
                price = 54000.0
            elif "NIFTY FIN SERVICE" in tradingsymbol:
                price = 26000.0
            elif "NIFTY MIDCAP SELECT" in tradingsymbol:
                price = 12000.0
            elif "SENSEX" in tradingsymbol:
                price = 81000.0
            else:
                price = 1000.0
                
            ltp_data[f"{exchange}:{tradingsymbol}"] = {
                "instrument_token": 1,
                "last_price": price
            }
            
        return ltp_data
    
    def get_atm_strike(self, index_symbol):
        """Get ATM strike for an index."""
        if index_symbol == "NIFTY":
            return 24800
        elif index_symbol == "BANKNIFTY":
            return 54000
        elif index_symbol == "FINNIFTY":
            return 26000
        elif index_symbol == "MIDCPNIFTY":
            return 12000
        elif index_symbol == "SENSEX":
            return 81000
        else:
            return 20000
    
    def get_expiry_dates(self, index_symbol):
        """Get dummy expiry dates."""
        today = datetime.date.today()
        
        # Generate weekly expiries (Thursdays)
        days_to_thur = (3 - today.weekday()) % 7
        if days_to_thur == 0:
            days_to_thur = 7  # If today is Thursday, go to next week
        
        this_thur = today + datetime.timedelta(days=days_to_thur)
        next_thur = this_thur + datetime.timedelta(days=7)
        
        # Generate monthly expiry (last Thursday of month)
        if today.month == 12:
            next_month = datetime.date(today.year + 1, 1, 1)
        else:
            next_month = datetime.date(today.year, today.month + 1, 1)
        
        last_day = next_month - datetime.timedelta(days=1)
        days_to_last_thur = (last_day.weekday() - 3) % 7
        monthly_expiry = last_day - datetime.timedelta(days=days_to_last_thur)
        
        # For next month's expiry
        if next_month.month == 12:
            month_after_next = datetime.date(next_month.year + 1, 1, 1)
        else:
            month_after_next = datetime.date(next_month.year, next_month.month + 1, 1)
        
        last_day_next = month_after_next - datetime.timedelta(days=1)
        days_to_last_thur_next = (last_day_next.weekday() - 3) % 7
        next_month_expiry = last_day_next - datetime.timedelta(days=days_to_last_thur_next)
        
        # Return appropriate expiries based on index
        if index_symbol in ["NIFTY", "SENSEX"]:
            # Weekly and monthly expiries
            return [this_thur, next_thur, monthly_expiry, next_month_expiry]
        else:
            # Only monthly expiries
            return [monthly_expiry, next_month_expiry]
    
    def resolve_expiry(self, index_symbol, expiry_rule):
        """Resolve expiry date based on rule."""
        expiry_dates = self.get_expiry_dates(index_symbol)
        monthly_only_indices = ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
        
        if expiry_rule == 'this_week' and index_symbol not in monthly_only_indices:
            return expiry_dates[0]
        elif expiry_rule == 'next_week' and index_symbol not in monthly_only_indices:
            return expiry_dates[1]
        elif expiry_rule == 'this_month':
            # Monthly indices have index 0, weekly have index 2
            idx = 0 if index_symbol in monthly_only_indices else 2
            return expiry_dates[min(idx, len(expiry_dates) - 1)]
        elif expiry_rule == 'next_month':
            # Monthly indices have index 1, weekly have index 3
            idx = 1 if index_symbol in monthly_only_indices else 3
            return expiry_dates[min(idx, len(expiry_dates) - 1)]
        else:
            # Default to first expiry
            return expiry_dates[0]
    
    def option_instruments(self, index_symbol, expiry_date, strikes):
        """Get dummy option instruments."""
        instruments = []
        
        # Format expiry for tradingsymbol
        if isinstance(expiry_date, datetime.date):
            expiry_str = expiry_date.strftime('%y%b').upper()
        else:
            expiry_str = "25SEP"  # Default
        
        for strike in strikes:
            # Add CE instrument
            ce_instrument = {
                "instrument_token": int(strike * 10 + 1),
                "exchange_token": str(int(strike * 10 + 1)),
                "tradingsymbol": f"{index_symbol}{expiry_str}{int(strike)}CE",
                "name": index_symbol,
                "last_price": 100.0,
                "expiry": expiry_date if isinstance(expiry_date, datetime.date) else datetime.date(2025, 9, 30),
                "strike": float(strike),
                "tick_size": 0.05,
                "lot_size": 50 if index_symbol == "NIFTY" else 25,
                "instrument_type": "CE",
                "segment": "NFO-OPT",
                "exchange": "NFO"
            }
            instruments.append(ce_instrument)
            
            # Add PE instrument
            pe_instrument = {
                "instrument_token": int(strike * 10 + 2),
                "exchange_token": str(int(strike * 10 + 2)),
                "tradingsymbol": f"{index_symbol}{expiry_str}{int(strike)}PE",
                "name": index_symbol,
                "last_price": 100.0,
                "expiry": expiry_date if isinstance(expiry_date, datetime.date) else datetime.date(2025, 9, 30),
                "strike": float(strike),
                "tick_size": 0.05,
                "lot_size": 50 if index_symbol == "NIFTY" else 25,
                "instrument_type": "PE",
                "segment": "NFO-OPT",
                "exchange": "NFO"
            }
            instruments.append(pe_instrument)
        
        return instruments
    
    # Add alias for compatibility
    def get_option_instruments(self, index_symbol, expiry_date, strikes):
        """Alias for option_instruments."""
        return self.option_instruments(index_symbol, expiry_date, strikes)
        
        
    def check_health(self):
    """
    Check if the provider is healthy and connected.
    
    Returns:
        Dict with health status information
    """
    try:
        # Simple check - try to get NIFTY LTP
        ltp = self.get_ltp("NIFTY")
        
        # If we get a price, the connection is working
        if ltp and ltp > 0:
            return {
                'status': 'healthy',
                'message': 'Kite provider is connected',
                'data': {'ltp': ltp}
            }
        else:
            return {
                'status': 'degraded',
                'message': 'Kite provider returned invalid price'
            }
    except Exception as e:
        # Check if we need to refresh the token
        if "token expired" in str(e).lower() or "invalid token" in str(e).lower():
            try:
                self.logger.info("Token expired, attempting to refresh")
                self.refresh_access_token()
                return {
                    'status': 'degraded',
                    'message': 'Token refreshed, reconnecting'
                }
            except Exception as refresh_error:
                return {
                    'status': 'unhealthy',
                    'message': f"Token refresh failed: {str(refresh_error)}"
                }
        
        return {
            'status': 'unhealthy',
            'message': f"Connection check failed: {str(e)}"
        }