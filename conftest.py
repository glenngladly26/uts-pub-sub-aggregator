"""
Pytest configuration file
Letakkan di root project: pubsub-aggregator/conftest.py
"""
import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))