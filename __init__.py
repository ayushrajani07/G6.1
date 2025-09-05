# -*- coding: utf-8 -*-
"""G6 Options Trading Platform."""

# Import key components for convenience
try:
    from .broker.kite_provider import KiteProvider, DummyKiteProvider
except ImportError:
    # Skip if not available yet
    pass