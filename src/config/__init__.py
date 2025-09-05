# -*- coding: utf-8 -*-
"""
Configuration module for G6 Platform.
"""

from .config_loader import ConfigLoader

# For backward compatibility
def load_config(config_path):
    """Legacy function to load config."""
    return ConfigLoader.load(config_path)