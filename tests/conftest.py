"""
Pytest configuration for milvus_cli tests.
"""
import sys
import os

# Add milvus_cli directory to path for imports
milvus_cli_path = os.path.join(os.path.dirname(__file__), '..', 'milvus_cli')
sys.path.insert(0, os.path.abspath(milvus_cli_path))
