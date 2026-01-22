import os
from typing import Optional


class TestConfig:
    """
    Configuration management for test parameters
    
    This class handles loading test configuration from environment variables
    or .env files to avoid hardcoding sensitive information like URIs and tokens.
    """
    
    def __init__(self):
        """Initialize test configuration"""
        self._load_env_file()
    
    def _load_env_file(self):
        """Load environment variables from .env or test.env file if it exists"""
        # Try multiple possible env file names
        possible_files = ['.env', 'test.env']
        env_file_path = None
        
        for filename in possible_files:
            file_path = os.path.join(os.path.dirname(__file__), filename)
            if os.path.exists(file_path):
                env_file_path = file_path
                break
        if env_file_path and os.path.exists(env_file_path):
            try:
                with open(env_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            # Remove quotes if present
                            value = value.strip('"\'')
                            os.environ[key.strip()] = value
                print(f"Loaded configuration from: {env_file_path}")
            except Exception as e:
                print(f"Warning: Could not load env file {env_file_path}: {e}")
    
    @property
    def milvus_uri(self) -> str:
        """
        Get Milvus URI from environment variables
        
        Environment variable: MILVUS_TEST_URI
        Default: http://127.0.0.1:19530
        """
        return os.getenv(
            'MILVUS_TEST_URI', 
            'http://127.0.0.1:19530'
        )
    
    @property
    def milvus_token(self) -> Optional[str]:
        """
        Get Milvus token from environment variables
        
        Environment variable: MILVUS_TEST_TOKEN
        Default: None
        """
        return os.getenv('MILVUS_TEST_TOKEN')
    
    @property
    def tls_mode(self) -> int:
        """
        Get TLS mode from environment variables
        
        Environment variable: MILVUS_TEST_TLS_MODE
        Default: 0 (no encryption)
        """
        try:
            return int(os.getenv('MILVUS_TEST_TLS_MODE', '0'))
        except ValueError:
            return 0
    
    @property
    def cert_path(self) -> Optional[str]:
        """
        Get certificate path from environment variables
        
        Environment variable: MILVUS_TEST_CERT_PATH
        Default: None
        """
        return os.getenv('MILVUS_TEST_CERT_PATH')
    
    @property
    def test_collection_prefix(self) -> str:
        """
        Get test collection name prefix
        
        Environment variable: MILVUS_TEST_COLLECTION_PREFIX
        Default: test_collection
        """
        return os.getenv('MILVUS_TEST_COLLECTION_PREFIX', 'test_collection')
    
    def get_connection_params(self) -> dict:
        """
        Get all connection parameters as a dictionary
        
        Returns:
            dict: Connection parameters for Milvus
        """
        params = {
            'uri': self.milvus_uri,
            'tlsmode': self.tls_mode,
        }
        
        if self.milvus_token:
            params['token'] = self.milvus_token
        
        if self.cert_path:
            params['cert'] = self.cert_path
            
        return params
    
    def print_config(self) -> None:
        """Print current configuration (without sensitive information)"""
        print("Test Configuration:")
        print(f"  URI: {self.milvus_uri}")
        print(f"  Token: {'***' if self.milvus_token else 'None'}")
        print(f"  TLS Mode: {self.tls_mode}")
        print(f"  Cert Path: {self.cert_path or 'None'}")
        print(f"  Collection Prefix: {self.test_collection_prefix}")


# Global test configuration instance
test_config = TestConfig()


# Allow running this file directly to print configuration
if __name__ == "__main__":
    print("=" * 60)
    print("Milvus CLI Unit Test Configuration")
    print("=" * 60)
    test_config.print_config()
    print("\nTo change configuration, create a test.env file in this directory")
    print("or set environment variables (see README.md for details)")
