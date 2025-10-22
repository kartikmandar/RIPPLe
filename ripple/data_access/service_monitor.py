"""
Service Status Monitor for RSP Services

This module provides functionality to monitor the availability of RSP services
(TAP and SIAv2) and gracefully handle service unavailability.
"""

import logging
import time
import os
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import requests
from pathlib import Path

from .exceptions import ButlerConnectionError, DataAccessError


class ServiceStatusMonitor:
    """
    Monitor the status of RSP services and provide graceful degradation.
    """

    def __init__(self, tap_url: str = "https://data.lsst.cloud/api/tap",
                 sia_url: str = "https://data.lsst.cloud/api/dp02/query",
                 cache_duration_minutes: int = 5):
        """
        Initialize service status monitor.

        Args:
            tap_url: TAP service endpoint URL
            sia_url: SIAv2 service endpoint URL
            cache_duration_minutes: How long to cache status results
        """
        self.tap_url = tap_url
        self.sia_url = sia_url
        self.cache_duration = timedelta(minutes=cache_duration_minutes)

        # Service status cache
        self._status_cache: Dict[str, Tuple[bool, datetime, str]] = {}

        # Configuration for user notification
        self.status_log_file = Path("./logs/service_status.log")
        self.status_log_file.parent.mkdir(exist_ok=True)

        self.logger = logging.getLogger(__name__)

    def _is_cache_valid(self, service: str) -> bool:
        """Check if cached status is still valid."""
        if service not in self._status_cache:
            return False

        _, timestamp, _ = self._status_cache[service]
        return datetime.now() - timestamp < self.cache_duration

    def _update_cache(self, service: str, status: bool, message: str) -> None:
        """Update the service status cache."""
        self._status_cache[service] = (status, datetime.now(), message)

        # Log the status change
        status_str = "UP" if status else "DOWN"
        log_message = f"{datetime.now().isoformat()} - {service.upper()} service is {status_str}: {message}"

        try:
            with open(self.status_log_file, 'a') as f:
                f.write(log_message + '\n')
        except Exception as e:
            self.logger.warning(f"Could not write to status log file: {e}")

    def _check_tap_service(self) -> Tuple[bool, str]:
        """Check TAP service availability."""
        try:
            # Simple test query to TAP service
            test_query = "SELECT 1 FROM TAP_SCHEMA.tables LIMIT 1"

            # Use environment token for authentication if available
            access_token = os.environ.get("RSP_ACCESS_TOKEN")
            headers = {}
            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'

            # Make the request
            response = requests.post(
                f"{self.tap_url}/sync",
                data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': test_query},
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                return True, "Service responding normally"
            elif response.status_code == 401:
                return False, "Authentication failed"
            elif response.status_code == 403:
                return False, "Access forbidden"
            elif response.status_code == 500:
                return False, "Internal server error"
            else:
                return False, f"Unexpected status code: {response.status_code}"

        except requests.exceptions.Timeout:
            return False, "Request timeout"
        except requests.exceptions.ConnectionError:
            return False, "Connection error"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def _check_sia_service(self) -> Tuple[bool, str]:
        """Check SIAv2 service availability."""
        try:
            # Test with minimal SIAv2 parameters
            test_params = {
                'POS': '62.0,-37.0;0.1',
                'MAXREC': '1'
            }

            # Use Basic authentication for SIAv2
            access_token = os.environ.get("RSP_ACCESS_TOKEN")
            auth = None
            if access_token:
                from requests.auth import HTTPBasicAuth
                auth = HTTPBasicAuth("x-oauth-basic", access_token)

            # Make the request
            response = requests.post(
                self.sia_url,
                data=test_params,
                auth=auth,
                timeout=10
            )

            if response.status_code == 200:
                # Check if it's actual data or an HTML 404 page
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type or 'application/xml' in content_type:
                    return True, "Service responding normally"
                elif 'text/html' in content_type:
                    # Check if it's a 404 page in HTML disguise
                    if '404' in response.text or 'not found' in response.text.lower():
                        return False, "Service returns 404 error page"
                    else:
                        return False, f"Unexpected HTML response: {response.text[:100]}"
                else:
                    return True, f"Service responding with content-type: {content_type}"

            elif response.status_code == 404:
                return False, "Endpoint not found (404)"
            elif response.status_code == 401:
                return False, "Authentication failed"
            elif response.status_code == 403:
                return False, "Access forbidden"
            elif response.status_code == 500:
                return False, "Internal server error"
            else:
                return False, f"Unexpected status code: {response.status_code}"

        except requests.exceptions.Timeout:
            return False, "Request timeout"
        except requests.exceptions.ConnectionError:
            return False, "Connection error"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def get_service_status(self, service: str) -> Tuple[bool, str]:
        """
        Get the current status of a service.

        Args:
            service: Service name ('tap' or 'sia')

        Returns:
            Tuple of (is_available, message)
        """
        if self._is_cache_valid(service):
            return self._status_cache[service][:2]

        if service.lower() == 'tap':
            status, message = self._check_tap_service()
        elif service.lower() == 'sia':
            status, message = self._check_sia_service()
        else:
            raise ValueError(f"Unknown service: {service}")

        self._update_cache(service, status, message)
        return status, message

    def check_all_services(self) -> Dict[str, Tuple[bool, str]]:
        """
        Check status of all services.

        Returns:
            Dictionary mapping service names to (status, message) tuples
        """
        return {
            'tap': self.get_service_status('tap'),
            'sia': self.get_service_status('sia')
        }

    def is_service_available(self, service: str) -> bool:
        """
        Quick check if a service is available.

        Args:
            service: Service name ('tap' or 'sia')

        Returns:
            True if service is available, False otherwise
        """
        status, _ = self.get_service_status(service)
        return status

    def wait_for_service(self, service: str, max_wait_minutes: int = 5,
                        check_interval_seconds: int = 30) -> bool:
        """
        Wait for a service to become available.

        Args:
            service: Service name to wait for
            max_wait_minutes: Maximum time to wait
            check_interval_seconds: How often to check

        Returns:
            True if service became available, False if timeout reached
        """
        max_wait_seconds = max_wait_minutes * 60
        start_time = time.time()

        self.logger.info(f"Waiting for {service.upper()} service to become available...")

        while time.time() - start_time < max_wait_seconds:
            status, message = self.get_service_status(service)
            if status:
                self.logger.info(f"{service.upper()} service is now available: {message}")
                return True

            self.logger.info(f"{service.upper()} service still unavailable: {message}. Retrying in {check_interval_seconds} seconds...")
            time.sleep(check_interval_seconds)

        self.logger.warning(f"Timeout waiting for {service.upper()} service to become available")
        return False

    def print_service_status(self) -> None:
        """Print current service status to console."""
        print("\n" + "="*60)
        print("RSP Service Status Check")
        print("="*60)

        services = self.check_all_services()

        for service_name, (status, message) in services.items():
            status_icon = "✓" if status else "✗"
            status_text = "UP" if status else "DOWN"
            print(f"{status_icon} {service_name.upper()} service: {status_text}")
            print(f"   Details: {message}")

        print("="*60)

    def get_service_recommendations(self) -> Dict[str, Any]:
        """
        Get recommendations based on service status.

        Returns:
            Dictionary with recommendations for using available services
        """
        services = self.check_all_services()

        recommendations = {
            'available_services': [],
            'unavailable_services': [],
            'recommendations': [],
            'workarounds': []
        }

        for service_name, (status, message) in services.items():
            if status:
                recommendations['available_services'].append(service_name)
            else:
                recommendations['unavailable_services'].append({
                    'service': service_name,
                    'reason': message
                })

        # Generate specific recommendations
        tap_available = services['tap'][0]
        sia_available = services['sia'][0]

        if tap_available and not sia_available:
            recommendations['recommendations'].append(
                "Use TAP service for all data access queries"
            )
            recommendations['workarounds'].append(
                "Query image metadata through TAP instead of SIAv2"
            )
        elif not tap_available and sia_available:
            recommendations['recommendations'].append(
                "Use SIAv2 service for image searches"
            )
        elif not tap_available and not sia_available:
            recommendations['recommendations'].append(
                "Both services are unavailable - try again later"
            )
        else:
            recommendations['recommendations'].append(
                "Both services are available - use SIAv2 for images, TAP for catalogs"
            )

        return recommendations


def create_service_monitor(tap_url: Optional[str] = None,
                          sia_url: Optional[str] = None) -> ServiceStatusMonitor:
    """
    Factory function to create a service monitor with default RSP URLs.

    Args:
        tap_url: Optional custom TAP URL
        sia_url: Optional custom SIA URL

    Returns:
        Configured ServiceStatusMonitor instance
    """
    default_tap_url = "https://data.lsst.cloud/api/tap"
    default_sia_url = "https://data.lsst.cloud/api/dp02/query"

    return ServiceStatusMonitor(
        tap_url=tap_url or default_tap_url,
        sia_url=sia_url or default_sia_url
    )