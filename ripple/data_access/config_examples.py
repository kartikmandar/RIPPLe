from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class ButlerConfig:
    """
    Configuration class for Butler connections.
    This class handles configuration parameters for both local and remote Butler instances.
    """
    # Repository configuration
    repo_path: Optional[str] = None
    server_url: Optional[str] = None

    # Authentication configuration
    access_token: Optional[str] = None
    token_username: str = "x-oauth-basic"
    auth_method: str = "none"  # 'token', 'none', or 'certificate'

    # Data configuration
    collections: List[str] = field(default_factory=list)
    instrument: Optional[str] = None
    
    # Performance configuration
    cache_size: int = 1000
    timeout: float = 30.0
    retry_attempts: int = 3
    
    # Additional configuration
    max_connections: int = 5
    max_workers: int = 4
    batch_size: int = 100
    enable_performance_monitoring: bool = False
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Basic validation
        if not self.repo_path and not self.server_url:
            raise ValueError("Either repo_path or server_url must be specified")

        if self.repo_path and self.server_url:
            # Both specified is allowed but might generate warnings
            pass

        # Authentication validation
        if self.auth_method not in ["token", "none", "certificate"]:
            raise ValueError("auth_method must be one of: 'token', 'none', 'certificate'")

        if self.auth_method == "token" and not self.access_token:
            raise ValueError("access_token is required when auth_method is 'token'")

        if self.auth_method == "token" and not self.server_url:
            raise ValueError("server_url is required when using token authentication")

        if self.access_token and self.auth_method != "token":
            raise ValueError("access_token can only be used with auth_method='token'")
        
        if self.cache_size < 0:
            raise ValueError("cache_size must be non-negative")

        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")

        if self.max_connections <= 0:
            raise ValueError("max_connections must be positive")

        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")

        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

    def get(self, key: str, default=None):
        """
        Get configuration value with default fallback.

        Args:
            key (str): Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return getattr(self, key, default)


def get_default_config():
    """Get a default ButlerConfig instance."""
    return ButlerConfig(
        repo_path="/tmp/butler_repo",
        collections=["raw/all"],
        instrument="HSC",
        cache_size=1000,
        timeout=30.0,
        retry_attempts=3
    )


def get_production_config():
    """Get a production ButlerConfig instance."""
    return ButlerConfig(
        server_url="https://butler.lsst.org",
        collections=["HSC/runs/RC2"],
        cache_size=5000,
        timeout=120.0,
        retry_attempts=5,
        max_connections=10,
        max_workers=8,
        enable_performance_monitoring=True
    )


def get_rsp_config():
    """Get a Rubin Science Platform ButlerConfig instance with token authentication."""
    import os
    access_token = os.environ.get("RSP_ACCESS_TOKEN")
    if not access_token:
        raise ValueError(
            "RSP_ACCESS_TOKEN environment variable is required for RSP access. "
            "Create a token at https://data.lsst.cloud/ and set the environment variable."
        )

    return ButlerConfig(
        server_url="https://data.lsst.cloud/api/butler/",
        access_token=access_token,
        token_username="x-oauth-basic",
        auth_method="token",
        collections=["2.2i/runs/DP0.2"],
        instrument="LSSTCam",
        cache_size=5000,
        timeout=120.0,
        retry_attempts=5,
        max_connections=10,
        max_workers=8,
        enable_performance_monitoring=True
    )


def validate_config(config):
    """Validate a ButlerConfig instance."""
    errors = []
    warnings = []

    # Check for required fields
    if not config.repo_path and not config.server_url:
        errors.append("Either repo_path or server_url must be specified")

    # Check for potential issues
    if config.repo_path and config.server_url:
        warnings.append("Both repo_path and server_url specified - using repo_path")

    # Authentication validation
    if config.auth_method not in ["token", "none", "certificate"]:
        errors.append("auth_method must be one of: 'token', 'none', 'certificate'")

    if config.auth_method == "token" and not config.access_token:
        errors.append("access_token is required when auth_method is 'token'")

    if config.auth_method == "token" and not config.server_url:
        errors.append("server_url is required when using token authentication")

    if config.access_token and config.auth_method != "token":
        errors.append("access_token can only be used with auth_method='token'")

    # Performance validation
    if config.cache_size > 10000:
        warnings.append("Large cache_size may impact memory usage")

    if config.timeout > 300:
        warnings.append("Long timeout may cause slow failure detection")

    if config.retry_attempts > 10:
        warnings.append("High retry_attempts may cause prolonged failures")

    if config.max_workers > 16:
        warnings.append("High max_workers may impact system performance")

    if not config.collections:
        errors.append("collections cannot be empty")

    if config.cache_size < 0:
        errors.append("cache_size must be non-negative")

    if config.timeout <= 0:
        errors.append("timeout must be positive")

    if config.retry_attempts < 0:
        errors.append("retry_attempts must be non-negative")

    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }