#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G6 Options Trading Platform - Advanced Main Application
Includes all extended functionality.
"""

from __future__ import annotations

import os
import sys
import time
import signal
import logging
import threading
import argparse
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, Mapping, Optional, Sequence

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    env_loaded = True
except ImportError:
    env_loaded = False
    print("Warning: python-dotenv not installed. Environment variables must be set manually.")

# G6 imports
from .config.config_loader import ConfigLoader
from .broker.kite_provider import KiteProvider
from .collectors.providers_interface import Providers
from .collectors.enhanced_collector import run_enhanced_collectors
from .storage.csv_sink import CsvSink
from .storage.influx_sink import InfluxSink, NullInfluxSink
from .metrics.metrics import get_metrics_registry, start_metrics_server
from .utils.market_hours import is_market_open, sleep_until_market_open
from .analytics.spread_builder import SpreadBuilder
from .analytics.option_chain import OptionChainAnalytics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("g6_platform.log")
    ]
)

logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
SHUTDOWN_REQUESTED = False

def signal_handler(signum, frame):
    """Handle termination signals."""
    global SHUTDOWN_REQUESTED
    logger.info(f"Signal {signum} received, shutting down gracefully...")
    SHUTDOWN_REQUESTED = True

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="G6 Options Trading Platform")
    parser.add_argument(
        "--config", 
        help="Path to config file", 
        default=os.environ.get("CONFIG_PATH", "config/g6_config.json")
    )
    parser.add_argument(
        "--market-hours-only", 
        help="Only run during market hours", 
        action="store_true"
    )
    parser.add_argument(
        "--run-once", 
        help="Run once and exit", 
        action="store_true"
    )
    parser.add_argument(
        "--use-enhanced", 
        help="Use enhanced collector with all features", 
        action="store_true",
        default=True
    )
    parser.add_argument(
        "--analytics", 
        help="Run option chain analytics", 
        action="store_true"
    )
    return parser.parse_args()

def init_providers(config) -> Providers:
    """Initialize data providers."""
    try:
        # Create KiteProvider from environment or config
        kite_provider = KiteProvider.from_env()
        logger.info("KiteProvider initialized successfully")
        
        # Return provider wrapper
        return Providers(kite_provider=kite_provider)
    except Exception as e:
        logger.error(f"Failed to initialize KiteProvider: {e}")
        raise

def init_storage(config) -> tuple:
    """Initialize data storage."""
    try:
        # Initialize CSV storage
        csv_dir = config.storage.csv_dir if hasattr(config, 'storage') else "data/csv"
        csv_sink = CsvSink(base_dir=csv_dir)
        
        # Initialize InfluxDB if enabled
        influx_enabled = (hasattr(config, 'storage') and 
                         hasattr(config.storage, 'influx_enabled') and 
                         config.storage.influx_enabled)
        
        if influx_enabled:
            # Get InfluxDB config
            influx_url = config.storage.influx_url
            influx_org = config.storage.influx_org
            influx_bucket = config.storage.influx_bucket
            influx_token = os.environ.get("INFLUX_TOKEN", "")
            
            influx_sink = InfluxSink(
                url=influx_url,
                token=influx_token,
                org=influx_org,
                bucket=influx_bucket
            )
        else:
            influx_sink = NullInfluxSink()
            
        logger.info("Storage initialized successfully")
        return csv_sink, influx_sink
    except Exception as e:
        logger.error(f"Failed to initialize storage: {e}")
        raise

def run_analytics(providers, config):
    """Run option chain analytics."""
    try:
        # Create analytics instances
        option_chain_analytics = OptionChainAnalytics(providers)
        
        # Loop through each index
        for index_symbol in config.index_params.keys():
            try:
                # Resolve expiry for this week
                expiry_date = providers.resolve_expiry(index_symbol, "this_week")
                
                # Calculate PCR
                pcr = option_chain_analytics.calculate_pcr(index_symbol, expiry_date)
                logger.info(f"{index_symbol} PCR: OI={pcr['oi_pcr']:.2f}, Volume={pcr['volume_pcr']:.2f}")
                
                # Calculate max pain
                max_pain = option_chain_analytics.calculate_max_pain(index_symbol, expiry_date)
                logger.info(f"{index_symbol} Max Pain: {max_pain}")
                
                # Calculate support/resistance
                levels = option_chain_analytics.calculate_support_resistance(index_symbol, expiry_date)
                logger.info(f"{index_symbol} Support: {levels['support']}")
                logger.info(f"{index_symbol} Resistance: {levels['resistance']}")
                
            except Exception as e:
                logger.error(f"Analytics error for {index_symbol}: {e}")
                
    except Exception as e:
        logger.error(f"Failed to run analytics: {e}")

def main():
    """Main entry point."""
    global SHUTDOWN_REQUESTED
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    args = parse_args()
    
    logger.info("Starting G6 Options Trading Platform v2.0 (Advanced)")
    
    try:
        # Load configuration
        config = ConfigLoader.load(args.config)
        
        # Initialize metrics
        metrics = get_metrics_registry()
        
        # Start metrics server if configured
        metrics_port = config.orchestration.prometheus_port if hasattr(config, 'orchestration') else 9108
        start_metrics_server(port=metrics_port)
        
        # Initialize components
        providers = init_providers(config)
        csv_sink, influx_sink = init_storage(config)
        
        # Run analytics if requested
        if args.analytics:
            run_analytics(providers, config)
            if args.run_once:
                return
                
        # Get collection interval
        interval_sec = config.orchestration.run_interval_sec if hasattr(config, 'orchestration') else 60
        
        logger.info(f"Starting collection loop with {interval_sec}s interval")
        
        # Main loop
        while not SHUTDOWN_REQUESTED:
            try:
                start_time = time.time()
                
                # Check if we should only run during market hours
                if args.market_hours_only and not is_market_open():
                    next_open = get_next_market_open()
                    wait_time = int((next_open - datetime.now(timezone.utc)).total_seconds())
                    logger.info(f"Market closed. Next open at {next_open.isoformat()}. Waiting {wait_time} seconds.")
                    
                    # Sleep until next check or shutdown
                    sleep_time = min(interval_sec, wait_time, 300)  # Max 5 minutes between checks
                    for _ in range(int(sleep_time)):
                        if SHUTDOWN_REQUESTED:
                            break
                        time.sleep(1)
                    continue
                
                # Run collection based on selected mode
                if args.use_enhanced:
                    run_enhanced_collectors(
                        index_params=config.index_params,
                        providers=providers,
                        csv_sink=csv_sink,
                        influx_sink=influx_sink,
                        metrics=metrics,
                        only_during_market_hours=False,  # We're already checking above
                    )
                else:
                    # Fall back to unified collectors
                    from .collectors.unified_collectors import run_unified_collectors
                    run_unified_collectors(
                        index_params=config.index_params,
                        providers=providers,
                        csv_sink=csv_sink,
                        influx_sink=influx_sink,
                        metrics=metrics,
                    )
                
                # If run-once mode, exit loop
                if args.run_once:
                    break
                    
                # Calculate sleep time to maintain interval
                elapsed = time.time() - start_time
                sleep_time = max(0.1, interval_sec - elapsed)
                
                # Sleep until next run or shutdown
                for _ in range(int(sleep_time)):
                    if SHUTDOWN_REQUESTED:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Collection cycle failed: {e}")
                time.sleep(5)  # Short delay on error
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1
    finally:
        logger.info("Shutting down...")
        # Clean up resources
        if 'providers' in locals():
            providers.close()
    
    logger.info("Shutdown complete")
    return 0

if __name__ == "__main__":
    sys.exit(main())