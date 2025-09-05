#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G6 Options Trading Platform - Debug Startup
Helps diagnose where the program is getting stuck.
"""

import logging
import sys
import time
import os
from datetime import datetime

# Configure logging to be more immediate
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("debug")

# Add this at the beginning before any imports
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.info("dotenv package not available, skipping .env loading")

def main():
    """Main diagnostic function."""
    logger.info("Starting diagnostic...")
    
    # Print API key status (masked for security)
    api_key = os.environ.get("KITE_API_KEY", "")
    if api_key:
        masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:] if len(api_key) > 8 else "****"
        logger.info(f"Found KITE_API_KEY in environment: {masked_key}")
    else:
        logger.warning("KITE_API_KEY not found in environment")
    
    # Step 1: Import core modules
    logger.info("Step 1: Importing config module...")
    from src.config.config_loader import ConfigLoader
    logger.info("Config module imported successfully")
    
    # Rest of the function remains the same...
    # ...