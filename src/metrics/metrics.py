#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metrics for G6 Options Trading Platform.
Sets up a Prometheus metrics server.
"""

import logging
import threading
from prometheus_client import start_http_server, Summary, Counter, Gauge

logger = logging.getLogger(__name__)

class MetricsRegistry:
    """Metrics registry for G6 Platform."""
    
    def __init__(self):
        """Initialize metrics."""
        # Collection metrics
        self.collection_duration = Summary('g6_collection_duration_seconds', 
                                          'Time spent collecting data')
        
        self.collection_cycles = Counter('g6_collection_cycles_total',
                                       'Number of collection cycles run')
        
        self.collection_errors = Counter('g6_collection_errors_total',
                                       'Number of collection errors',
                                       ['index', 'expiry'])
        
        # Index metrics
        self.index_price = Gauge('g6_index_price',
                              'Current index price',
                              ['index'])
        
        self.index_atm = Gauge('g6_index_atm_strike',
                            'ATM strike price',
                            ['index'])
        
        # Collection stats
        self.options_collected = Gauge('g6_options_collected',
                                    'Number of options collected',
                                    ['index', 'expiry'])
        
        # Market metrics
        self.pcr = Gauge('g6_put_call_ratio',
                      'Put-Call Ratio',
                      ['index', 'expiry'])
        
        # Option metrics
        self.option_price = Gauge('g6_option_price',
                              'Option price',
                              ['index', 'expiry', 'strike', 'type'])
        
        self.option_volume = Gauge('g6_option_volume',
                               'Option volume',
                               ['index', 'expiry', 'strike', 'type'])
        
        self.option_oi = Gauge('g6_option_oi',
                           'Option open interest',
                           ['index', 'expiry', 'strike', 'type'])
        
        self.option_iv = Gauge('g6_option_iv',
                           'Option implied volatility',
                           ['index', 'expiry', 'strike', 'type'])
        
        # Generate Greek metrics
        self._init_greek_metrics()
        
        logger.info(f"Initialized {len(self.__dict__)} metrics for g6_platform")
    
    def _init_greek_metrics(self):
        """Initialize metrics for option Greeks."""
        greek_names = ['delta', 'theta', 'gamma', 'vega']
        
        for greek in greek_names:
            metric_name = f"option_{greek}"
            setattr(self, metric_name, Gauge(
                f'g6_option_{greek}',
                f'Option {greek}',
                ['index', 'expiry', 'strike', 'type']
            ))

def setup_metrics_server(port=9108, host="0.0.0.0"):
    """Set up metrics server and return metrics registry."""
    # Start server in a thread
    start_http_server(port, addr=host)
    logger.info(f"Metrics server started on {host}:{port}")
    logger.info(f"Metrics available at http://{host}:{port}/metrics")
    
    # Create metrics registry
    metrics = MetricsRegistry()
    
    # Return metrics and a function to stop the server if needed
    return metrics, lambda: None  # No direct way to stop the server