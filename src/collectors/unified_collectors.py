#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified collectors for G6 Platform.
"""

import logging
import datetime
from typing import Dict, Any
import json
from market_hours import is_market_open, get_next_market_open


logger = logging.getLogger(__name__)

def run_unified_collectors(index_params, providers, csv_sink, influx_sink, metrics):
    """
    Run unified collectors for all configured indices.
    
    Args:
        index_params: Configuration parameters for indices
        providers: Data providers
        csv_sink: CSV storage sink
        influx_sink: InfluxDB storage sink
        metrics: Metrics registry
    """
    # Track the collection timestamp
    now = datetime.datetime.now()
    
    # Initialize data quality checker
    data_quality = DataQualityChecker()
    
    # Track the collection timestamp
    now = datetime.datetime.now()
    
    # Check if equity market is open
    if not is_market_open(market_type="equity", session_type="regular"):
        next_open = get_next_market_open(market_type="equity", session_type="regular")
        wait_time = (next_open - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        logger.info("Equity market is closed. Next market open: %s (in %.1f minutes)", 
                    next_open, wait_time/60)
        return
    
    logger.info("Equity market is open, starting collection")
    
    # Process each index
    for index_symbol, params in index_params.items():
        # Skip disabled indices
        if not params.get('enable', True):
            continue
        
        logger.info(f"Collecting data for {index_symbol}")
        
        try:
            # Get index price and OHLC data
            index_price, index_ohlc = providers.get_index_data(index_symbol)
            
            # Get ATM strike
            atm_strike = providers.get_ltp(index_symbol)
            logger.info(f"{index_symbol} ATM strike: {atm_strike}")
            
            # Update metrics if available
            if metrics:
                try:
                    metrics.index_price.labels(index=index_symbol).set(index_price)
                    metrics.index_atm.labels(index=index_symbol).set(atm_strike)
                except:
                    logger.debug(f"Failed to update metrics for {index_symbol}")
            
            # Process each expiry
            for expiry_rule in params.get('expiries', ['this_week']):
                try:
                    # Resolve expiry date
                    expiry_date = providers.resolve_expiry(index_symbol, expiry_rule)
                    logger.info(f"{index_symbol} {expiry_rule} expiry resolved to: {expiry_date}")
                    
                    # Calculate strikes to collect
                    strikes_otm = params.get('strikes_otm', 10)
                    strikes_itm = params.get('strikes_itm', 10)
                    
                    strike_step = 50.0  # Default step
                    if index_symbol in ['BANKNIFTY', 'SENSEX']:
                        strike_step = 100.0
                    
                    strikes = []
                    # Add ITM strikes
                    for i in range(1, strikes_itm + 1):
                        strikes.append(float(atm_strike - (i * strike_step)))
                    
                    # Add ATM strike
                    strikes.append(float(atm_strike))
                    
                    # Add OTM strikes
                    for i in range(1, strikes_otm + 1):
                        strikes.append(float(atm_strike + (i * strike_step)))
                    
                    # Sort strikes
                    strikes.sort()
                    
                    logger.info(f"Collecting {len(strikes)} strikes for {index_symbol} {expiry_rule}: {strikes}")
                    
                    # Get option instruments
                    instruments = providers.get_option_instruments(index_symbol, expiry_date, strikes)
                    
                    if not instruments:
                        logger.warning(f"No option instruments found for {index_symbol} expiry {expiry_date}")
                        continue
                    
                    # Enrich instruments with quote data (including avg_price)
                    enriched_data = providers.enrich_with_quotes(instruments)
                    
                    if not enriched_data:
                        logger.warning(f"No quote data available for {index_symbol} expiry {expiry_date}")
                        continue
                    
                    # Write data to storage with the index price and OHLC
                    # Use the current timestamp when writing data
                    collection_time = datetime.datetime.now()
                    logger.info(f"Writing {len(enriched_data)} records to CSV sink")
                    csv_sink.write_options_data(
                        index_symbol,
                        expiry_date,
                        enriched_data,
                        collection_time,
                        index_price=index_price,
                        index_ohlc=index_ohlc
                    )
                    
                    # Write to InfluxDB if enabled
                    if influx_sink:
                        influx_sink.write_options_data(
                            index_symbol, 
                            expiry_date,
                            enriched_data,
                            collection_time
                        )
                    
                    # Update metrics
                    if metrics:
                        # Count options collected
                        try:
                            metrics.options_collected.labels(index=index_symbol, expiry=expiry_rule).set(len(enriched_data))
                        except:
                            logger.debug(f"Failed to update metrics for {index_symbol} options collected")
                        
                        # Update PCR (Put-Call Ratio)
                        try:
                            call_oi = sum(float(data.get('oi', 0)) for data in enriched_data.values() 
                                        if data.get('instrument_type') == 'CE')
                            put_oi = sum(float(data.get('oi', 0)) for data in enriched_data.values() 
                                        if data.get('instrument_type') == 'PE')
                            
                            pcr = put_oi / call_oi if call_oi > 0 else 0
                            metrics.pcr.labels(index=index_symbol, expiry=expiry_rule).set(pcr)
                        except:
                            logger.debug(f"Failed to calculate PCR for {index_symbol}")
                    
                    # Log success
                    logger.info(f"Successfully collected {len(enriched_data)} options for {index_symbol} {expiry_rule}")
                    
                except Exception as e:
                    logger.error(f"Error collecting data for {index_symbol} {expiry_rule}: {e}")
                    if metrics:
                        try:
                            metrics.collection_errors.labels(index=index_symbol, expiry=expiry_rule).inc()
                        except:
                            logger.debug("Failed to increment collection errors metric")
            
        except Exception as e:
            logger.error(f"Error processing index {index_symbol}: {e}")
    
    # Update collection time metrics
    if metrics:
        try:
            collection_time = (datetime.datetime.now() - now).total_seconds()
            metrics.collection_duration.observe(collection_time)
            metrics.collection_cycles.inc()
        except Exception as e:
            logger.error(f"Failed to update collection metrics: {e}")