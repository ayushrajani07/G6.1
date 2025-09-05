#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Debug mode for G6 Platform with real API calls.
"""

import logging
import os
import sys
from pathlib import Path

# Configure root logger for maximum verbosity
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Import necessary components
from src.config.config_loader import ConfigLoader
from src.broker.kite_provider import KiteProvider
from src.collectors.providers_interface import Providers
from src.collectors.unified_collectors import run_unified_collectors
from src.storage.csv_sink import CsvSink
from src.storage.influx_sink import NullInfluxSink
from src.metrics.metrics import get_metrics_registry

def main():
    """Debug mode main function."""
    print("=== G6 Platform Debug Mode ===")
    
    # 1. Load configuration
    config_path = os.environ.get("CONFIG_PATH", "config/g6_config.json")
    print(f"Loading config from: {config_path}")
    config = ConfigLoader.load(config_path)
    
    # 2. Initialize Kite Provider
    print("Initializing KiteProvider from environment variables")
    try:
        kite_provider = KiteProvider.from_env()
        print("✓ KiteProvider initialized successfully")
    except Exception as e:
        print(f"✗ KiteProvider initialization failed: {e}")
        return 1
    
    # 3. Initialize Providers wrapper
    providers = Providers(kite_provider=kite_provider)
    
    # 4. Initialize storage
    csv_dir = "data/csv_debug"
    os.makedirs(csv_dir, exist_ok=True)
    csv_sink = CsvSink(base_dir=csv_dir)
    influx_sink = NullInfluxSink()
    
    # 5. Initialize metrics
    metrics = get_metrics_registry()
    
    # 6. Test get_atm_strike
    try:
        for index in config.index_params.keys():
            atm = providers.get_atm_strike(index)
            print(f"Index: {index}, ATM Strike: {atm}")
    except Exception as e:
        print(f"Error getting ATM strikes: {e}")
    
    # 7. Run a single collection cycle
    print("\nRunning single collection cycle...")
    try:
        run_unified_collectors(
            index_params=config.index_params,
            providers=providers,
            csv_sink=csv_sink,
            influx_sink=influx_sink,
            metrics=metrics
        )
        print("✓ Collection cycle completed successfully")
        print(f"Data saved to {os.path.abspath(csv_dir)}")
    except Exception as e:
        print(f"✗ Collection cycle failed: {e}")
        
    # Clean up
    providers.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())